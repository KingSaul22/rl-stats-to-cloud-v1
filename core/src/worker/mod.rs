use crate::SinkReceiver;
use crate::StateSender;
use crate::config::AppConfig;
use crate::connector::{SinkError, TelemetrySink};
use futures_util::StreamExt;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

mod actors;
mod aggregation;
mod context;
mod events;
mod transformer;

use actors::{run_event_feed_actor, run_historical_actor, run_live_state_actor};
use transformer::normalize_payload;

pub use context::SessionContext;
use events::TransientLaneMessage;
pub use events::{IngestClass, IngestEnvelope, RocketLeagueEvent};

type TcpTarget = (String, u16);
const LIVE_STATE_CAPACITY: usize = 2_048;
const EVENT_FEED_CAPACITY: usize = 2_048;
const HISTORICAL_CAPACITY: usize = 8_192;
const RETRY_BACKOFF_BASE_SECONDS: u64 = 1;
const RETRY_BACKOFF_MAX_SECONDS: u64 = 32;
const EVENT_FEED_MAX_FAILURES: u32 = 3;
const COMPACTION_MAX_FAILURES: u32 = 3;
const AGGREGATION_MAX_FAILURES: u32 = 3;
const COMPACTION_TARGETS: [&str; 2] = ["live_state", "live_events_feed"];
const COMPACTION_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionReason {
    Destroyed,
    Ended,
    IdTransition,
}

#[derive(Clone)]
struct RoutingLanes {
    live_state: mpsc::Sender<TransientLaneMessage>,
    event_feed: mpsc::Sender<TransientLaneMessage>,
    historical: mpsc::Sender<IngestEnvelope>,
}

#[derive(Default)]
struct RoutingStats {
    live_state_drops: u64,
    event_feed_losses: u64,
    historical_overflows: u64,
}

struct SentinelGuard {
    arc: Arc<RwLock<String>>,
}

impl Drop for SentinelGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.arc.write() {
            guard.clear(); // Reset "\0" -> "" on panic unwind
        }
    }
}

#[derive(Clone)]
pub struct RocketLeagueWorker {
    websocket_url: String,
    reconnect_delay: Duration,
    sink_receiver: SinkReceiver,
    state_sender: StateSender,
}

impl RocketLeagueWorker {
    #[must_use]
    pub fn from_config(
        config: &AppConfig,
        state_sender: StateSender,
        sink_receiver: SinkReceiver,
    ) -> Self {
        Self {
            websocket_url: config.websocket_url.clone(),
            reconnect_delay: Duration::from_secs(config.reconnect_delay_seconds),
            sink_receiver,
            state_sender,
        }
    }

    pub async fn run(&self) {
        self.run_until_cancelled(CancellationToken::new()).await;
    }

    pub async fn run_until_cancelled(&self, shutdown: CancellationToken) {
        let mut shutdown_logged = false;
        let sink = self.sink_receiver.borrow().clone();
        let mut session_context = SessionContext::new(None, None);

        let (live_state_sender, live_state_receiver) = mpsc::channel(LIVE_STATE_CAPACITY);
        let (event_feed_sender, event_feed_receiver) = mpsc::channel(EVENT_FEED_CAPACITY);
        let (historical_sender, historical_receiver) = mpsc::channel(HISTORICAL_CAPACITY);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let live_task = tokio::spawn(run_live_state_actor(
            live_state_receiver,
            Arc::clone(&sink),
            shutdown.clone(),
        ));
        let event_feed_task = tokio::spawn(run_event_feed_actor(
            event_feed_receiver,
            Arc::clone(&sink),
            shutdown.clone(),
        ));
        let historical_task = tokio::spawn(run_historical_actor(
            historical_receiver,
            Arc::clone(&sink),
            shutdown.clone(),
        ));

        let mut routing_stats = RoutingStats::default();
        let mut sequence = 0_u64;
        let mut last_compaction_seq = 0_u64;
        let mut cached_game_seconds_remaining = None;
        let mut last_aggregated_match_id: Option<String> = None;
        let mut cached_podium_active = false;
        let mut cached_historical_active = false;

        loop {
            if shutdown.is_cancelled() {
                self.log_shutdown_once(&mut shutdown_logged);
                break;
            }

            let session_result = tokio::select! {
                () = shutdown.cancelled() => {
                    self.log_shutdown_once(&mut shutdown_logged);
                    break;
                }
                result = self.run_session(
                    &shutdown,
                    &lanes,
                    &sink,
                    &mut sequence,
                    &mut last_compaction_seq,
                    &mut cached_game_seconds_remaining,
                    &mut last_aggregated_match_id,
                    &mut cached_podium_active,
                    &mut cached_historical_active,
                    &mut session_context,
                    &mut routing_stats,
                ) => result,
            };

            match session_result {
                Ok(()) => {
                    if shutdown.is_cancelled() {
                        self.log_shutdown_once(&mut shutdown_logged);
                        break;
                    }

                    eprintln!(
                        "Session ended cleanly. Reconnecting in {} seconds...",
                        self.reconnect_delay.as_secs()
                    );
                }
                Err(err) => {
                    self.set_connected(false);
                    eprintln!(
                        "Connection/session error: {}. Retrying in {} seconds...",
                        err,
                        self.reconnect_delay.as_secs()
                    );
                }
            }

            tokio::select! {
                () = shutdown.cancelled() => {
                    self.log_shutdown_once(&mut shutdown_logged);
                    break;
                }
                () = sleep(self.reconnect_delay) => {}
            }
        }

        shutdown.cancel();
        drop(lanes);

        Self::join_actor("live_state", live_task).await;
        Self::join_actor("event_feed", event_feed_task).await;
        Self::join_actor("historical", historical_task).await;
    }

    fn log_shutdown_once(&self, shutdown_logged: &mut bool) {
        if !*shutdown_logged {
            self.set_connected(false);
            println!("Shutting down safely...");
            *shutdown_logged = true;
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Session runner coordinates transport, routing, and lifecycle state."
    )]
    async fn run_session(
        &self,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        if self.websocket_url.starts_with("tcp://") {
            return self
                .run_raw_tcp_session(
                    shutdown,
                    lanes,
                    sink,
                    sequence,
                    last_compaction_seq,
                    cached_game_seconds_remaining,
                    last_aggregated_match_id,
                    cached_podium_active,
                    cached_historical_active,
                    session_context,
                    routing_stats,
                )
                .await;
        }

        match self
            .run_websocket_session(
                shutdown,
                lanes,
                sink,
                sequence,
                last_compaction_seq,
                cached_game_seconds_remaining,
                last_aggregated_match_id,
                cached_podium_active,
                cached_historical_active,
                session_context,
                routing_stats,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                if Self::should_fallback_to_tcp(&err) {
                    eprintln!(
                        "Endpoint did not complete WebSocket handshake; falling back to raw TCP stream mode."
                    );
                    self.run_raw_tcp_session(
                        shutdown,
                        lanes,
                        sink,
                        sequence,
                        last_compaction_seq,
                        cached_game_seconds_remaining,
                        last_aggregated_match_id,
                        cached_podium_active,
                        cached_historical_active,
                        session_context,
                        routing_stats,
                    )
                    .await
                } else {
                    Err(err)
                }
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "WebSocket loop needs shared mutable routing/session state references."
    )]
    async fn run_websocket_session(
        &self,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        let (stream, _) = connect_async(&self.websocket_url)
            .await
            .map_err(|err| format!("could not connect to {} ({})", self.websocket_url, err))?;

        self.set_connected(true);
        println!("Connected (WebSocket) to {}", self.websocket_url);

        let (_, mut read) = stream.split();

        loop {
            let next_message = tokio::select! {
                () = shutdown.cancelled() => {
                    self.set_connected(false);
                    return Ok(());
                }
                message = read.next() => message,
            };

            let Some(message_result) = next_message else {
                return Ok(());
            };

            let message = message_result.map_err(|err| format!("read error: {err}"))?;

            match message {
                Message::Text(text) => {
                    self.handle_payload(
                        &text,
                        shutdown,
                        lanes,
                        sink,
                        sequence,
                        last_compaction_seq,
                        cached_game_seconds_remaining,
                        last_aggregated_match_id,
                        cached_podium_active,
                        cached_historical_active,
                        session_context,
                        routing_stats,
                    )
                    .await?;
                }
                Message::Binary(bytes) => match String::from_utf8(bytes.clone()) {
                    Ok(text) => {
                        self.handle_payload(
                            &text,
                            shutdown,
                            lanes,
                            sink,
                            sequence,
                            last_compaction_seq,
                            cached_game_seconds_remaining,
                            last_aggregated_match_id,
                            cached_podium_active,
                            cached_historical_active,
                            session_context,
                            routing_stats,
                        )
                        .await?;
                    }
                    Err(err) => eprintln!("Skipping non-UTF8 binary frame: {err}"),
                },
                Message::Close(frame) => {
                    self.set_connected(false);
                    if let Some(frame) = frame {
                        println!(
                            "WebSocket closed by server (code: {}, reason: {}).",
                            frame.code, frame.reason
                        );
                    } else {
                        println!("WebSocket closed by server.");
                    }
                    return Ok(());
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "TCP loop needs shared mutable routing/session state references."
    )]
    async fn run_raw_tcp_session(
        &self,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        let (host, port) = self.resolve_tcp_target()?;
        let address = format!("{host}:{port}");

        let mut stream = TcpStream::connect(&address)
            .await
            .map_err(|err| format!("could not connect to {address} ({err})"))?;

        self.set_connected(true);
        println!("Connected (TCP stream) to {address}");

        let mut read_buffer = [0_u8; 4096];
        let mut pending = String::new();

        loop {
            let read = tokio::select! {
                () = shutdown.cancelled() => {
                    self.set_connected(false);
                    return Ok(());
                }
                read_result = stream.read(&mut read_buffer) => {
                    read_result.map_err(|err| format!("tcp read error: {err}"))?
                }
            };

            if read == 0 {
                self.set_connected(false);
                return Ok(());
            }

            let chunk = String::from_utf8_lossy(&read_buffer[..read]);
            pending.push_str(&chunk);
            self.consume_json_stream(
                &mut pending,
                shutdown,
                lanes,
                sink,
                sequence,
                last_compaction_seq,
                cached_game_seconds_remaining,
                last_aggregated_match_id,
                cached_podium_active,
                cached_historical_active,
                session_context,
                routing_stats,
            )
            .await?;

            if pending.len() > 512 * 1024 {
                eprintln!("Dropping oversized undecodable TCP buffer.");
                pending.clear();
            }
        }
    }

    fn should_fallback_to_tcp(error_message: &str) -> bool {
        let lower = error_message.to_ascii_lowercase();
        lower.contains("httparse")
            || lower.contains("invalid http")
            || lower.contains("protocol error")
    }

    fn resolve_tcp_target(&self) -> Result<TcpTarget, String> {
        if let Some(raw) = self.websocket_url.strip_prefix("tcp://") {
            let mut parts = raw.rsplitn(2, ':');
            let port_str = parts
                .next()
                .ok_or_else(|| "invalid tcp URL port".to_string())?;
            let host = parts
                .next()
                .ok_or_else(|| "invalid tcp URL host".to_string())?
                .to_string();

            let port = port_str
                .parse::<u16>()
                .map_err(|err| format!("invalid tcp port: {err}"))?;

            return Ok((host, port));
        }

        let parsed = Url::parse(&self.websocket_url)
            .map_err(|err| format!("invalid websocket URL {} ({})", self.websocket_url, err))?;

        let host = parsed
            .host_str()
            .ok_or_else(|| "websocket URL missing host".to_string())?
            .to_string();

        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| "websocket URL missing port".to_string())?;

        Ok((host, port))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Stream parser delegates stateful routing and compaction checks per payload."
    )]
    async fn consume_json_stream(
        &self,
        pending: &mut String,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        loop {
            let mut stream = serde_json::Deserializer::from_str(pending).into_iter::<Value>();
            let item = stream.next();

            match item {
                Some(Ok(value)) => {
                    let consumed = stream.byte_offset();
                    if consumed == 0 {
                        break;
                    }

                    self.handle_value(
                        value,
                        shutdown,
                        lanes,
                        sink,
                        sequence,
                        last_compaction_seq,
                        cached_game_seconds_remaining,
                        last_aggregated_match_id,
                        cached_podium_active,
                        cached_historical_active,
                        session_context,
                        routing_stats,
                    )
                    .await?;
                    pending.drain(..consumed);
                }
                Some(Err(err)) => {
                    if err.is_eof() {
                        break;
                    }

                    eprintln!("Skipping malformed streamed JSON segment: {err}");
                    let consumed = stream.byte_offset().saturating_add(1);
                    if consumed > 0 && consumed <= pending.len() {
                        pending.drain(..consumed);
                    } else {
                        pending.clear();
                    }
                    break;
                }
                None => break,
            }
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Payload handler forwards shared runtime state into value handler."
    )]
    async fn handle_payload(
        &self,
        payload: &str,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        let parsed: Value = match serde_json::from_str(payload) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("Skipping invalid JSON payload: {err}");
                return Ok(());
            }
        };

        self.handle_value(
            parsed,
            shutdown,
            lanes,
            sink,
            sequence,
            last_compaction_seq,
            cached_game_seconds_remaining,
            last_aggregated_match_id,
            cached_podium_active,
            cached_historical_active,
            session_context,
            routing_stats,
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Value handler requires routing/session/compaction shared mutable state."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Value handler spans event classification, compaction trigger detection, flush, aggregation, and reset."
    )]
    async fn handle_value(
        &self,
        mut parsed: Value,
        shutdown: &CancellationToken,
        lanes: &RoutingLanes,
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        sequence: &mut u64,
        last_compaction_seq: &mut u64,
        cached_game_seconds_remaining: &mut Option<u64>,
        last_aggregated_match_id: &mut Option<String>,
        cached_podium_active: &mut bool,
        cached_historical_active: &mut bool,
        session_context: &mut SessionContext,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        Self::unwrap_double_encoded_data(&mut parsed);

        let Some(event_name) = parsed.get("Event").and_then(Value::as_str) else {
            println!("Received JSON without Event field.");
            return Ok(());
        };
        let parsed_event = match serde_json::from_value::<RocketLeagueEvent>(Value::String(
            event_name.to_string(),
        )) {
            Ok(event) => event,
            Err(err) => {
                eprintln!("Failed to parse Event field '{event_name}': {err}");
                RocketLeagueEvent::Unknown(event_name.to_string())
            }
        };

        if event_name.is_empty() {
            println!("Received JSON with empty Event field.");
            return Ok(());
        }

        let previous_match_id = session_context.active_match_id.clone();

        match parsed_event {
            RocketLeagueEvent::GoalReplayStart => session_context.in_replay = true,
            RocketLeagueEvent::GoalReplayEnd => session_context.in_replay = false,
            RocketLeagueEvent::MatchEnded => *cached_podium_active = true,
            RocketLeagueEvent::MatchInitialized => *cached_historical_active = true,
            _ => {}
        }

        if Self::is_replay_active_in_payload(&parsed) {
            session_context.in_replay = true;
        }

        if let Some(observed_game_seconds) = transformer::extract_game_seconds_remaining(&parsed) {
            *cached_game_seconds_remaining = Some(observed_game_seconds);
        }

        session_context.update_from_payload(&parsed);

        if session_context.active_match_id != previous_match_id {
            if let Ok(mut guard) = session_context.blue_team_id.write() {
                *guard = None;
            }
            if let Ok(mut guard) = session_context.orange_team_id.write() {
                *guard = None;
            }
            if let Ok(mut guard) = session_context.teams_resolved_for_match.write() {
                guard.clear();
            }
        }

        let needs_resolution = session_context.teams_resolved_for_match.try_read().is_ok_and(|guard| {
            *guard != session_context.active_match_id && !guard.starts_with('\0')
        });

        if needs_resolution {
            let players_val = transformer::extract_player_telemetry(&parsed);
            if let Some(players) = players_val.as_object()
                && !players.is_empty()
            {
                let mut blue_ids = Vec::new();
                let mut orange_ids = Vec::new();
                for (id, data) in players {
                    if let Some(team) = data.get("team").and_then(Value::as_u64) {
                        if team == 0 {
                            blue_ids.push(id.clone());
                        } else if team == 1 {
                            orange_ids.push(id.clone());
                        }
                    }
                }

                if !blue_ids.is_empty() || !orange_ids.is_empty() {
                    if let Ok(mut guard) = session_context.teams_resolved_for_match.write() {
                        *guard = "\0".to_string();
                    }

                    let sink = Arc::clone(sink);
                    let blue_team_id_arc = Arc::clone(&session_context.blue_team_id);
                    let orange_team_id_arc = Arc::clone(&session_context.orange_team_id);
                    let resolved_arc = Arc::clone(&session_context.teams_resolved_for_match);
                    let match_id = session_context.active_match_id.clone();
                    let shutdown_clone = shutdown.clone();

                    tokio::spawn(async move {
                        let sentinel = SentinelGuard {
                            arc: Arc::clone(&resolved_arc),
                        };

                        let (blue, orange) = futures_util::future::join(
                            aggregation::resolve_team_id(
                                &sink,
                                &blue_ids,
                                &match_id,
                                "blue",
                                &shutdown_clone,
                            ),
                            aggregation::resolve_team_id(
                                &sink,
                                &orange_ids,
                                &match_id,
                                "orange",
                                &shutdown_clone,
                            ),
                        )
                        .await;

                        if let Ok(mut guard) = blue_team_id_arc.write() {
                            *guard = Some((match_id.clone(), blue.clone()));
                        }
                        if let Ok(mut guard) = orange_team_id_arc.write() {
                            *guard = Some((match_id.clone(), orange.clone()));
                        }
                        if let Ok(mut guard) = resolved_arc.write() {
                            guard.clone_from(&match_id);
                        }
                        std::mem::forget(sentinel);
                        info!("Team inference completed: match={} blue={} orange={}", match_id, blue, orange);
                    });
                }
            }
        }

        self.set_last_event(event_name);
        *sequence = sequence.saturating_add(1);
        let class = Self::classify_event(&parsed_event);

        if let Some(reason) = Self::compaction_reason(
            &parsed_event,
            previous_match_id.as_str(),
            session_context.active_match_id.as_str(),
        ) {
            if *last_compaction_seq < *sequence {
                Self::flush_transient_lanes(lanes, shutdown, *sequence, reason).await?;

                let snapshot = Self::request_live_state_snapshot(&lanes.live_state, shutdown).await;
                if let Some(state) = snapshot {
                    let has_players = state
                        .get("player_telemetry")
                        .and_then(Value::as_object)
                        .is_some_and(|obj| !obj.is_empty());

                    let already_aggregated = last_aggregated_match_id.as_deref().unwrap_or("")
                        == previous_match_id.as_str();

                    if has_players && !already_aggregated && *cached_historical_active {
                        aggregation::upload_aggregation(
                            sink,
                            previous_match_id.as_str(),
                            &state,
                            shutdown,
                        )
                        .await;
                        *last_aggregated_match_id = Some(previous_match_id.clone());
                    }

                    let _ = lanes.live_state.send(TransientLaneMessage::Reset).await;
                }

                if let Err(err) = Self::compact_transient_nodes(
                    sink,
                    shutdown,
                    *sequence,
                    reason,
                    previous_match_id.as_str(),
                    session_context.active_match_id.as_str(),
                )
                .await
                {
                    eprintln!(
                        "Compaction warning: cleanup failed at seq={} reason={reason:?} previous_match_id={} next_match_id={} error={}",
                        *sequence, previous_match_id, session_context.active_match_id, err
                    );
                }
                if matches!(
                    reason,
                    CompactionReason::IdTransition | CompactionReason::Destroyed
                ) {
                    *cached_podium_active = false;
                    *cached_historical_active = false;
                }
            }

            *last_compaction_seq = *sequence;
        }

        if session_context.in_replay && class == IngestClass::Historical {
            eprintln!(
                "Skipping historical event '{}' (seq={}) due to active replay.",
                event_name, *sequence
            );
            return Ok(());
        }

        let payload = normalize_payload(class, &parsed, event_name, session_context);
        let envelope = IngestEnvelope {
            seq: *sequence,
            event_type: event_name.to_string(),
            payload,
            class,
            active_match_id: session_context.active_match_id.clone(),
        };

        Self::route_envelope(
            envelope,
            lanes,
            routing_stats,
            *cached_game_seconds_remaining,
            *cached_podium_active,
            *cached_historical_active,
        )
    }

    fn compaction_reason(
        event: &RocketLeagueEvent,
        previous_match_id: &str,
        current_match_id: &str,
    ) -> Option<CompactionReason> {
        if matches!(event, RocketLeagueEvent::MatchDestroyed) {
            return Some(CompactionReason::Destroyed);
        }

        if matches!(event, RocketLeagueEvent::MatchEnded) {
            return Some(CompactionReason::Ended);
        }

        if !previous_match_id.is_empty()
            && !current_match_id.is_empty()
            && previous_match_id != current_match_id
        {
            return Some(CompactionReason::IdTransition);
        }

        None
    }

    async fn compact_transient_nodes(
        sink: &Arc<dyn TelemetrySink + Send + Sync>,
        shutdown: &CancellationToken,
        seq: u64,
        reason: CompactionReason,
        previous_match_id: &str,
        current_match_id: &str,
    ) -> Result<(), SinkError> {
        for target in COMPACTION_TARGETS {
            let mut failures = 0_u32;
            loop {
                if shutdown.is_cancelled() {
                    return Ok(());
                }

                match sink.delete_node(target).await {
                    Ok(()) => {
                        eprintln!(
                            "Compaction info: deleted node={target} seq={seq} reason={reason:?} previous_match_id={previous_match_id} next_match_id={current_match_id}"
                        );
                        break;
                    }
                    Err(terminal_error @ SinkError::Terminal { .. }) => {
                        return Err(terminal_error);
                    }
                    Err(
                        retryable_error @ (SinkError::RateLimited { .. }
                        | SinkError::TransientNetwork { .. }),
                    ) => {
                        failures = failures.saturating_add(1);
                        if failures > COMPACTION_MAX_FAILURES {
                            return Err(retryable_error);
                        }

                        let delay = actors::calculate_full_jitter_backoff(failures);
                        eprintln!(
                            "Compaction warning: retrying node={} seq={} reason={reason:?} failures={} retrying_in_ms={}.",
                            target,
                            seq,
                            failures,
                            delay.as_millis()
                        );
                        tokio::select! {
                            () = shutdown.cancelled() => return Ok(()),
                            () = sleep(delay) => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush_transient_lanes(
        lanes: &RoutingLanes,
        shutdown: &CancellationToken,
        seq: u64,
        reason: CompactionReason,
    ) -> Result<(), String> {
        let (live_ack_sender, live_ack_receiver) = oneshot::channel();
        let live_flush_sent = Self::send_flush_request(
            "live_state",
            &lanes.live_state,
            live_ack_sender,
            shutdown,
            seq,
            reason,
        )
        .await;

        let (event_ack_sender, event_ack_receiver) = oneshot::channel();
        let event_flush_sent = Self::send_flush_request(
            "event_feed",
            &lanes.event_feed,
            event_ack_sender,
            shutdown,
            seq,
            reason,
        )
        .await;

        if !live_flush_sent {
            return Err(format!(
                "Compaction aborted: live_state flush barrier failed at seq={seq} reason={reason:?}"
            ));
        }

        if !event_flush_sent {
            return Err(format!(
                "Compaction aborted: event_feed flush barrier failed at seq={seq} reason={reason:?}"
            ));
        }

        Self::wait_for_flush_ack("live_state", live_ack_receiver, shutdown, seq, reason).await;
        Self::wait_for_flush_ack("event_feed", event_ack_receiver, shutdown, seq, reason).await;

        Ok(())
    }

    async fn send_flush_request(
        lane_name: &str,
        lane_sender: &mpsc::Sender<TransientLaneMessage>,
        ack: oneshot::Sender<()>,
        shutdown: &CancellationToken,
        seq: u64,
        reason: CompactionReason,
    ) -> bool {
        tokio::select! {
            () = shutdown.cancelled() => false,
            send_result = timeout(
                COMPACTION_FLUSH_TIMEOUT,
                lane_sender.send(TransientLaneMessage::Flush { ack }),
            ) => {
                match send_result {
                    Ok(Ok(())) => true,
                    Ok(Err(_)) => {
                        eprintln!(
                            "Compaction warning: failed to enqueue flush on lane={lane_name} seq={seq} reason={reason:?} because lane is closed."
                        );
                        false
                    }
                    Err(_) => {
                        eprintln!(
                            "Compaction warning: timeout while enqueueing flush on lane={lane_name} seq={seq} reason={reason:?}. Proceeding with cleanup."
                        );
                        false
                    }
                }
            }
        }
    }

    async fn wait_for_flush_ack(
        lane_name: &str,
        ack: oneshot::Receiver<()>,
        shutdown: &CancellationToken,
        seq: u64,
        reason: CompactionReason,
    ) {
        tokio::select! {
            () = shutdown.cancelled() => {}
            ack_result = timeout(COMPACTION_FLUSH_TIMEOUT, ack) => {
                match ack_result {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        eprintln!(
                            "Compaction warning: flush ack sender dropped for lane={lane_name} seq={seq} reason={reason:?}. Proceeding with cleanup."
                        );
                    }
                    Err(_) => {
                        eprintln!(
                            "Compaction warning: timeout waiting flush ack on lane={lane_name} seq={seq} reason={reason:?}. Proceeding with cleanup."
                        );
                    }
                }
            }
        }
    }

    async fn request_live_state_snapshot(
        lane: &mpsc::Sender<TransientLaneMessage>,
        shutdown: &CancellationToken,
    ) -> Option<Value> {
        let (sender, receiver) = oneshot::channel();
        tokio::select! {
            () = shutdown.cancelled() => None,
            send_result = timeout(
                COMPACTION_FLUSH_TIMEOUT,
                lane.send(TransientLaneMessage::Snapshot { result: sender }),
            ) => {
                match send_result {
                    Ok(Ok(())) => {
                        tokio::select! {
                            () = shutdown.cancelled() => None,
                            recv_result = receiver => recv_result.ok(),
                        }
                    }
                    Ok(Err(_)) => {
                        eprintln!("Compaction warning: failed to request live-state snapshot because lane is closed.");
                        None
                    }
                    Err(_) => {
                        eprintln!("Compaction warning: timeout requesting live-state snapshot. Proceeding with cleanup.");
                        None
                    }
                }
            }
        }
    }

    fn route_envelope(
        envelope: IngestEnvelope,
        lanes: &RoutingLanes,
        routing_stats: &mut RoutingStats,
        cached_game_seconds_remaining: Option<u64>,
        podium_active: bool,
        historical_active: bool,
    ) -> Result<(), String> {
        let should_mirror = Self::is_high_value_historical_event(envelope.event_type.as_str());

        match envelope.class {
            IngestClass::LiveState => Self::try_send_live_state(envelope, lanes, routing_stats),
            IngestClass::EventFeed => {
                let is_lifecycle = Self::is_lifecycle_event(envelope.event_type.as_str());
                let is_match_initialized = envelope.event_type.as_str() == "MatchInitialized";
                let should_send_to_historical =
                    should_mirror || (is_lifecycle && historical_active) || is_match_initialized;
                let historical_copy = should_send_to_historical.then(|| envelope.clone());
                if !is_lifecycle && !podium_active {
                    Self::try_send_event_feed(envelope, lanes, routing_stats)?;
                }
                if let Some(copy) = historical_copy {
                    let enriched =
                        Self::enrich_historical_timing(copy, cached_game_seconds_remaining);
                    Self::try_send_historical(enriched, lanes, routing_stats)?;
                }
                Ok(())
            }
            IngestClass::Historical => {
                let is_lifecycle = Self::is_lifecycle_event(envelope.event_type.as_str());
                let event_feed_copy = (!is_lifecycle)
                    .then(|| should_mirror.then(|| envelope.clone()))
                    .flatten();
                let enriched =
                    Self::enrich_historical_timing(envelope, cached_game_seconds_remaining);
                Self::try_send_historical(enriched, lanes, routing_stats)?;
                if let Some(copy) = event_feed_copy {
                    Self::try_send_event_feed(copy, lanes, routing_stats)?;
                }
                Ok(())
            }
        }
    }

    fn enrich_historical_timing(
        mut envelope: IngestEnvelope,
        cached_game_seconds_remaining: Option<u64>,
    ) -> IngestEnvelope {
        let Some(cached_seconds) = cached_game_seconds_remaining else {
            return envelope;
        };

        let needs_enrichment = envelope
            .payload
            .get("game_seconds_remaining")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0);

        if needs_enrichment && let Some(payload) = envelope.payload.as_object_mut() {
            payload.insert(
                "game_seconds_remaining".to_string(),
                Value::from(cached_seconds),
            );
        }

        envelope
    }

    fn try_send_live_state(
        envelope: IngestEnvelope,
        lanes: &RoutingLanes,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        let sequence = envelope.seq;
        let event_type = envelope.event_type.clone();

        match lanes
            .live_state
            .try_send(TransientLaneMessage::Event(envelope))
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                routing_stats.live_state_drops = routing_stats.live_state_drops.saturating_add(1);
                warn!(
                    "Live-state lane is full (backpressure). Dropping live-state payload to preserve ingestion flow (dropped_total={}, seq={}, event={}).",
                    routing_stats.live_state_drops, sequence, event_type
                );
                Ok(())
            }
            Err(TrySendError::Closed(_)) => Err(format!(
                "Live-state lane is closed. Cannot send live-state payload (seq={sequence}, event={event_type})."
            )),
        }
    }

    fn try_send_event_feed(
        envelope: IngestEnvelope,
        lanes: &RoutingLanes,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        match lanes
            .event_feed
            .try_send(TransientLaneMessage::Event(envelope))
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(message)) => match message {
                TransientLaneMessage::Event(dropped) => {
                    routing_stats.event_feed_losses =
                        routing_stats.event_feed_losses.saturating_add(1);
                    warn!(
                        "Event feed lane is full (backpressure). Dropping event feed payload to prevent ingestion stall (dropped_total={}, seq={}, event={}).",
                        routing_stats.event_feed_losses, dropped.seq, dropped.event_type
                    );
                    Ok(())
                }
                _ => Ok(()),
            },
            Err(TrySendError::Closed(_)) => {
                Err("Event feed lane actor channel closed unexpectedly.".to_string())
            }
        }
    }

    fn try_send_historical(
        envelope: IngestEnvelope,
        lanes: &RoutingLanes,
        routing_stats: &mut RoutingStats,
    ) -> Result<(), String> {
        match lanes.historical.try_send(envelope) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(dropped)) => {
                routing_stats.historical_overflows =
                    routing_stats.historical_overflows.saturating_add(1);
                warn!(
                    "Historical lane is full (backpressure). Dropping historical event to prevent ingestion stall! (dropped_total={}, seq={}, event={}).",
                    routing_stats.historical_overflows, dropped.seq, dropped.event_type
                );
                Ok(())
            }
            Err(TrySendError::Closed(dropped)) => Err(format!(
                "Historical lane actor channel closed unexpectedly (seq={}, event={}).",
                dropped.seq, dropped.event_type
            )),
        }
    }

    fn is_high_value_historical_event(event_type: &str) -> bool {
        matches!(
            event_type,
            "GoalScored"
                | "Goal"
                | "StatfeedEvent"
                | "MatchInitialized"
                | "MatchEnded"
                | "PodiumStart"
        )
    }

    fn is_lifecycle_event(event_type: &str) -> bool {
        matches!(
            event_type,
            "MatchEnded" | "MatchDestroyed" | "MatchInitialized"
        )
    }

    const fn classify_event(event: &RocketLeagueEvent) -> IngestClass {
        match event {
            // Carril 1: LiveState (Frecuencia ultra-alta para snapshots y sincronización de reloj)
            RocketLeagueEvent::UpdateState | RocketLeagueEvent::ClockUpdatedSeconds => {
                IngestClass::LiveState
            }

            // Carril 2: EventFeed (Hitos de partida, eventos de ciclo de vida y métricas de juego secundarias)
            RocketLeagueEvent::GoalReplayStart
            | RocketLeagueEvent::GoalReplayEnd
            | RocketLeagueEvent::GoalReplayWillEnd
            | RocketLeagueEvent::MatchCreated
            | RocketLeagueEvent::MatchInitialized
            | RocketLeagueEvent::MatchDestroyed
            | RocketLeagueEvent::MatchEnded
            | RocketLeagueEvent::MatchPaused
            | RocketLeagueEvent::MatchUnpaused
            | RocketLeagueEvent::CountdownBegin
            | RocketLeagueEvent::RoundStarted
            | RocketLeagueEvent::PodiumStart
            | RocketLeagueEvent::ReplayCreated
            | RocketLeagueEvent::BallHit
            | RocketLeagueEvent::CrossbarHit
            | RocketLeagueEvent::StatfeedEvent => IngestClass::EventFeed,

            // Carril 3: Historical (Eventos transaccionales críticos de fin de juego o estructuras no reconocidas)
            RocketLeagueEvent::GoalScored | RocketLeagueEvent::Unknown(_) => {
                IngestClass::Historical
            }
        }
    }

    fn is_replay_active_in_payload(parsed: &Value) -> bool {
        transformer::find_value_by_keys(parsed, &["bReplay", "b_replay", "bReplay"])
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    fn unwrap_double_encoded_data(parsed: &mut Value) {
        const DATA_KEYS: &[&str] = &["Data", "data"];

        let Value::Object(envelope) = parsed else {
            return;
        };

        for key in DATA_KEYS {
            let Some(data_value) = envelope.get_mut(*key) else {
                continue;
            };

            let data_str = match data_value {
                Value::String(s) => std::mem::take(s),
                _ => return,
            };

            if data_str.trim().is_empty() {
                return;
            }

            match serde_json::from_str::<Value>(&data_str) {
                Ok(Value::Object(inner_data)) => {
                    *data_value = Value::Object(inner_data);
                }
                Ok(other) => {
                    *data_value = other;
                }
                Err(err) => {
                    eprintln!("Failed to unwrap double-encoded Data field: {err}");
                }
            }
        }
    }

    async fn join_actor(actor_name: &str, task: JoinHandle<()>) {
        if let Err(err) = task.await {
            eprintln!("Telemetry {actor_name} actor join error: {err}");
        }
    }

    fn set_connected(&self, connected: bool) {
        let mut new_state = self.state_sender.borrow().clone();
        if new_state.is_connected != connected {
            new_state.is_connected = connected;
            let _ = self.state_sender.send(new_state);
        }
    }

    fn set_last_event(&self, event_name: &str) {
        let mut new_state = self.state_sender.borrow().clone();
        if new_state.last_event != event_name {
            new_state.last_event = event_name.to_string();
            let _ = self.state_sender.send(new_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn taxonomy_maps_clock_variants_to_live_state() {
        let updates = [
            RocketLeagueEvent::from_event_name("UpdateState".to_string()),
            RocketLeagueEvent::from_event_name("ClockUpdated".to_string()),
            RocketLeagueEvent::from_event_name("ClockUpdatedSeconds".to_string()),
        ];

        for event in updates {
            assert!(matches!(
                RocketLeagueWorker::classify_event(&event),
                IngestClass::LiveState
            ));
        }
    }

    #[test]
    fn ingress_normalization_produces_v2_live_state_shape() {
        let session_context = SessionContext::new(
            Some("match_cfg_1".to_string()),
            Some("session_cfg_1".to_string()),
        );
        let raw = json!({
            "Event": "UpdateState",
            "time_remaining_seconds": 123,
            "blue": 2,
            "orange": 1,
            "Players": [
                {
                    "PrimaryId": "player_1_id",
                    "boost": 85,
                    "score": 450,
                    "goals": 1
                }
            ]
        });

        let normalized = normalize_payload(
            IngestClass::LiveState,
            &raw,
            "UpdateState",
            &session_context,
        );

        assert_eq!(normalized.get("is_active"), Some(&Value::Bool(true)));
        assert_eq!(
            normalized.get("session_id"),
            Some(&Value::String("session_cfg_1".to_string()))
        );
        assert_eq!(
            normalized.get("match_id"),
            Some(&Value::String("match_cfg_1".to_string()))
        );
        assert_eq!(
            normalized.get("time_remaining_seconds"),
            Some(&Value::from(123_u64))
        );

        assert!(normalized.get("score").and_then(Value::as_object).is_some());
        let Some(score) = normalized.get("score").and_then(Value::as_object) else {
            return;
        };
        assert_eq!(score.get("blue"), Some(&Value::from(2_u64)));
        assert_eq!(score.get("orange"), Some(&Value::from(1_u64)));

        assert!(
            normalized
                .get("player_telemetry")
                .and_then(Value::as_object)
                .is_some()
        );
        let Some(players) = normalized
            .get("player_telemetry")
            .and_then(Value::as_object)
        else {
            return;
        };
        assert!(
            players
                .get("player_1_id")
                .and_then(Value::as_object)
                .is_some()
        );
        let Some(player) = players.get("player_1_id").and_then(Value::as_object) else {
            return;
        };
        assert_eq!(player.get("boost"), Some(&Value::from(85_u64)));
        assert_eq!(player.get("score"), Some(&Value::from(450_i64)));
        assert_eq!(player.get("goals"), Some(&Value::from(1_u64)));
    }

    #[test]
    fn fallback_match_identity_is_generated_and_prefixed() {
        let mut session_context = SessionContext::new(None, None);
        let raw = json!({
            "Event": "Goal",
            "timestamp_ms": 1_715_000_120_000_u64,
            "game_seconds_remaining": 280_u64,
            "details": {"speed_kph": 105_u64}
        });

        session_context.update_from_payload(&raw);

        assert!(session_context.active_match_id.starts_with("match_"));
        let suffix = session_context.active_match_id.strip_prefix("match_");
        assert!(
            matches!(suffix, Some(value) if !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit()))
        );

        let normalized = normalize_payload(IngestClass::Historical, &raw, "Goal", &session_context);
        assert_eq!(
            normalized.get("match_id"),
            Some(&Value::String(session_context.active_match_id.clone()))
        );
        assert_eq!(
            normalized.get("session_id"),
            Some(&Value::String(session_context.active_session_id.clone()))
        );
    }

    #[test]
    fn classify_event_segments_lanes_cleanly() {
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::UpdateState),
            IngestClass::LiveState
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::ClockUpdatedSeconds),
            IngestClass::LiveState
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::MatchInitialized),
            IngestClass::EventFeed
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::StatfeedEvent),
            IngestClass::EventFeed
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::GoalScored),
            IngestClass::Historical
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::Unknown("Save".to_string())),
            IngestClass::Historical
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::Unknown(
                "Demolition".to_string()
            )),
            IngestClass::Historical
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::Unknown("Other".to_string())),
            IngestClass::Historical
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::GoalReplayStart),
            IngestClass::EventFeed
        ));
        assert!(matches!(
            RocketLeagueWorker::classify_event(&RocketLeagueEvent::GoalReplayEnd),
            IngestClass::EventFeed
        ));
    }

    #[test]
    fn route_envelope_mirrors_statfeed_to_historical_lane() {
        let (live_state_sender, _live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 9,
            event_type: "StatfeedEvent".to_string(),
            payload: json!({"Event": "StatfeedEvent"}),
            class: IngestClass::EventFeed,
            active_match_id: "match_1".to_string(),
        };

        let _ = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        let feed_message = event_feed_receiver.try_recv();
        assert!(matches!(
            feed_message,
            Ok(TransientLaneMessage::Event(IngestEnvelope {
                event_type,
                class: IngestClass::EventFeed,
                ..
            })) if event_type == "StatfeedEvent"
        ));

        let historical_message = historical_receiver.try_recv();
        assert!(matches!(
            historical_message,
            Ok(IngestEnvelope {
                event_type,
                class: IngestClass::EventFeed,
                ..
            }) if event_type == "StatfeedEvent"
        ));
    }

    #[test]
    fn route_envelope_mirrors_goal_to_event_feed_lane() {
        let (live_state_sender, _live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 12,
            event_type: "GoalScored".to_string(),
            payload: json!({"Event": "GoalScored"}),
            class: IngestClass::Historical,
            active_match_id: "match_1".to_string(),
        };

        let _ = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        let historical_message = historical_receiver.try_recv();
        assert!(matches!(
            historical_message,
            Ok(IngestEnvelope {
                event_type,
                class: IngestClass::Historical,
                ..
            }) if event_type == "GoalScored"
        ));

        let feed_message = event_feed_receiver.try_recv();
        assert!(matches!(
            feed_message,
            Ok(TransientLaneMessage::Event(IngestEnvelope {
                event_type,
                class: IngestClass::Historical,
                ..
            })) if event_type == "GoalScored"
        ));
    }

    #[test]
    fn enrich_historical_timing_uses_cached_clock_for_zero_time() {
        let envelope = IngestEnvelope {
            seq: 21,
            event_type: "StatfeedEvent".to_string(),
            payload: json!({
                "game_seconds_remaining": 0_u64,
                "type": "statfeedevent"
            }),
            class: IngestClass::EventFeed,
            active_match_id: "match_1".to_string(),
        };

        let enriched = RocketLeagueWorker::enrich_historical_timing(envelope, Some(94));
        assert_eq!(
            enriched.payload.get("game_seconds_remaining"),
            Some(&Value::from(94_u64))
        );
    }

    #[test]
    fn enrich_historical_timing_preserves_non_zero_time() {
        let envelope = IngestEnvelope {
            seq: 22,
            event_type: "GoalScored".to_string(),
            payload: json!({
                "game_seconds_remaining": 183_u64,
                "type": "goal"
            }),
            class: IngestClass::Historical,
            active_match_id: "match_1".to_string(),
        };

        let enriched = RocketLeagueWorker::enrich_historical_timing(envelope, Some(94));
        assert_eq!(
            enriched.payload.get("game_seconds_remaining"),
            Some(&Value::from(183_u64))
        );
    }

    #[test]
    fn route_envelope_sends_live_state_only_to_live_lane() {
        let (live_state_sender, mut live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 1,
            event_type: "UpdateState".to_string(),
            payload: json!({}),
            class: IngestClass::LiveState,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_ok());
        assert!(live_state_receiver.try_recv().is_ok());
        assert!(event_feed_receiver.try_recv().is_err());
        assert!(historical_receiver.try_recv().is_err());
    }

    #[test]
    fn route_envelope_sends_regular_event_feed_only_to_event_feed_when_podium_inactive() {
        let (live_state_sender, mut live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 2,
            event_type: "BallHit".to_string(),
            payload: json!({}),
            class: IngestClass::EventFeed,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_ok());
        assert!(live_state_receiver.try_recv().is_err());
        assert!(event_feed_receiver.try_recv().is_ok());
        assert!(historical_receiver.try_recv().is_err());
    }

    #[test]
    fn route_envelope_suppresses_regular_event_feed_during_podium() {
        let (live_state_sender, mut live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 3,
            event_type: "BallHit".to_string(),
            payload: json!({}),
            class: IngestClass::EventFeed,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            true,
            false,
        );

        assert!(result.is_ok());
        assert!(live_state_receiver.try_recv().is_err());
        assert!(event_feed_receiver.try_recv().is_err());
        assert!(historical_receiver.try_recv().is_err());
    }

    #[test]
    fn route_envelope_routes_unknown_historical_event_only_to_historical() {
        let (live_state_sender, mut live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, mut event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 4,
            event_type: "SomethingNeverSeen".to_string(),
            payload: json!({}),
            class: IngestClass::Historical,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_ok());
        assert!(live_state_receiver.try_recv().is_err());
        assert!(event_feed_receiver.try_recv().is_err());
        assert!(historical_receiver.try_recv().is_ok());
    }

    #[test]
    fn route_envelope_routes_match_initialized_to_historical_even_when_historical_inactive() {
        let (live_state_sender, mut live_state_receiver) = mpsc::channel(1);
        let (event_feed_sender, _event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, mut historical_receiver) = mpsc::channel(1);
        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 5,
            event_type: "MatchInitialized".to_string(),
            payload: json!({}),
            class: IngestClass::EventFeed,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_ok());
        assert!(live_state_receiver.try_recv().is_err());
        assert!(historical_receiver.try_recv().is_ok());
    }

    #[test]
    fn route_envelope_returns_error_when_target_lane_is_closed() {
        let (live_state_sender, live_state_receiver) = mpsc::channel::<TransientLaneMessage>(1);
        let (event_feed_sender, _event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, _historical_receiver) = mpsc::channel(1);
        drop(live_state_receiver);

        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 6,
            event_type: "UpdateState".to_string(),
            payload: json!({}),
            class: IngestClass::LiveState,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_err());
    }

    #[test]
    fn route_envelope_increments_drop_stats_when_lanes_are_full() {
        let (live_state_sender, _live_state_receiver) = mpsc::channel::<TransientLaneMessage>(1);
        let (event_feed_sender, _event_feed_receiver) = mpsc::channel(1);
        let (historical_sender, _historical_receiver) = mpsc::channel(1);

        // Fill the live_state lane first
        let fill_envelope = IngestEnvelope {
            seq: 100,
            event_type: "UpdateState".to_string(),
            payload: json!({}),
            class: IngestClass::LiveState,
            active_match_id: "match_1".to_string(),
        };
        let _ = live_state_sender.try_send(TransientLaneMessage::Event(fill_envelope));

        let lanes = RoutingLanes {
            live_state: live_state_sender,
            event_feed: event_feed_sender,
            historical: historical_sender,
        };

        let mut routing_stats = RoutingStats::default();
        let envelope = IngestEnvelope {
            seq: 7,
            event_type: "UpdateState".to_string(),
            payload: json!({}),
            class: IngestClass::LiveState,
            active_match_id: "match_1".to_string(),
        };

        let result = RocketLeagueWorker::route_envelope(
            envelope,
            &lanes,
            &mut routing_stats,
            None,
            false,
            false,
        );

        assert!(result.is_ok());
        assert_eq!(routing_stats.live_state_drops, 1);
    }
}
