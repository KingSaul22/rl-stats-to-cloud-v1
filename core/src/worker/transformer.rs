use super::context::SessionContext;
use super::events::IngestClass;
use serde_json::{Map, Value};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizedPayloadKind {
    LiveState,
    EventFeed,
    Historical,
}

pub fn normalize_payload(
    class: IngestClass,
    raw: &Value,
    event_type: &str,
    session_context: &SessionContext,
) -> Value {
    match normalized_payload_kind(class) {
        NormalizedPayloadKind::LiveState => normalize_live_state(raw, session_context),
        NormalizedPayloadKind::EventFeed => normalize_event_feed(raw, event_type, session_context),
        NormalizedPayloadKind::Historical => normalize_historical(raw, event_type, session_context),
    }
}

pub const fn normalized_payload_kind(class: IngestClass) -> NormalizedPayloadKind {
    match class {
        IngestClass::LiveState => NormalizedPayloadKind::LiveState,
        IngestClass::EventFeed => NormalizedPayloadKind::EventFeed,
        IngestClass::Historical => NormalizedPayloadKind::Historical,
    }
}

pub fn normalize_live_state(raw: &Value, session_context: &SessionContext) -> Value {
    let time_remaining_seconds = extract_u64_from_keys(
        raw,
        &[
            "time_remaining_seconds",
            "timeRemainingSeconds",
            "seconds_remaining",
            "secondsRemaining",
            "remaining_seconds",
            "remainingSeconds",
            "clock_seconds_remaining",
            "clockSecondsRemaining",
            "TimeSeconds",
        ],
    );

    let score = extract_score_object(raw);
    let player_telemetry = extract_player_telemetry(raw);

    let mut payload = Map::new();
    payload.insert("is_active".to_string(), Value::Bool(true));
    payload.insert(
        "session_id".to_string(),
        Value::String(session_context.active_session_id.clone()),
    );
    payload.insert(
        "match_id".to_string(),
        Value::String(session_context.active_match_id.clone()),
    );
    if let Some(time_remaining_seconds) = time_remaining_seconds {
        payload.insert(
            "time_remaining_seconds".to_string(),
            Value::from(time_remaining_seconds),
        );
    }
    if let Some(score) = score {
        payload.insert("score".to_string(), score);
    }
    payload.insert("player_telemetry".to_string(), player_telemetry);
    if let Some(is_overtime) = find_value_by_keys(raw, &["bOvertime"]).and_then(Value::as_bool) {
        payload.insert("is_overtime".to_string(), Value::Bool(is_overtime));
    }
    if let Some(is_replay) = find_value_by_keys(raw, &["bReplay"]).and_then(Value::as_bool) {
        payload.insert("is_replay".to_string(), Value::Bool(is_replay));
    }
    if let Some(has_winner) = find_value_by_keys(raw, &["bHasWinner"]).and_then(Value::as_bool) {
        payload.insert("has_winner".to_string(), Value::Bool(has_winner));
    }
    if let Some(winner) = extract_winner_name(raw) {
        payload.insert("winner".to_string(), Value::String(winner));
    }
    if let Some(arena) = extract_string_from_keys(raw, &["Arena", "arena"]) {
        payload.insert("arena".to_string(), Value::String(arena));
    }

    if let Ok(guard) = session_context.blue_team_id.try_read()
        && let Some((match_id, id)) = guard.as_ref()
        && match_id == &session_context.active_match_id
    {
        payload.insert("blue_team_id".to_string(), Value::String(id.clone()));
    }
    if let Ok(guard) = session_context.orange_team_id.try_read()
        && let Some((match_id, id)) = guard.as_ref()
        && match_id == &session_context.active_match_id
    {
        payload.insert("orange_team_id".to_string(), Value::String(id.clone()));
    }

    Value::Object(payload)
}

fn extract_winner_name(raw: &Value) -> Option<String> {
    extract_string_from_keys(raw, &["Winner"]).or_else(|| {
        find_value_by_keys(raw, &["Winner"])
            .and_then(|winner| extract_string_from_keys(winner, &["Name"]))
    })
}

pub fn normalize_event_feed(
    raw: &Value,
    event_type: &str,
    session_context: &SessionContext,
) -> Value {
    let mut payload = Map::new();
    payload.insert(
        "timestamp_ms".to_string(),
        Value::from(current_timestamp_ms()),
    );
    payload.insert(
        "game_seconds_remaining".to_string(),
        Value::from(extract_game_seconds_remaining(raw).unwrap_or(0)),
    );
    payload.insert(
        "type".to_string(),
        Value::String(canonical_event_type(event_type)),
    );
    payload.insert(
        "match_id".to_string(),
        Value::String(session_context.active_match_id.clone()),
    );
    payload.insert(
        "session_id".to_string(),
        Value::String(session_context.active_session_id.clone()),
    );

    if let Some(attacker_id) = extract_string_from_keys(
        raw,
        &[
            "attacker_id",
            "attackerId",
            "player_id",
            "playerId",
            "scorer_id",
            "scorerId",
            "actor_id",
            "actorId",
        ],
    ) {
        payload.insert("attacker_id".to_string(), Value::String(attacker_id));
    }

    if let Some(victim_id) = extract_string_from_keys(
        raw,
        &[
            "victim_id",
            "victimId",
            "target_id",
            "targetId",
            "defender_id",
            "defenderId",
        ],
    ) {
        payload.insert("victim_id".to_string(), Value::String(victim_id));
    }

    match event_type {
        "StatfeedEvent" => {
            if let Some(primary_player) = find_value_by_keys(raw, &["MainTarget"])
                .and_then(|target| extract_string_from_keys(target, &["Name"]))
            {
                payload.insert("primary_player".to_string(), Value::String(primary_player));
            }
            if let Some(stat_type) = extract_string_from_keys(raw, &["EventName"]) {
                payload.insert("stat_type".to_string(), Value::String(stat_type));
            }
        }
        "BallHit" => {
            if let Some(ball) = find_value_by_keys(raw, &["Ball"]) {
                if let Some(pre_hit_speed) = extract_u64_from_keys(ball, &["PreHitSpeed"]) {
                    payload.insert("pre_hit_speed".to_string(), Value::from(pre_hit_speed));
                }
                if let Some(post_hit_speed) = extract_u64_from_keys(ball, &["PostHitSpeed"]) {
                    payload.insert("post_hit_speed".to_string(), Value::from(post_hit_speed));
                }
            }
        }
        _ => {}
    }
    Value::Object(payload)
}

pub fn normalize_historical(
    raw: &Value,
    event_type: &str,
    session_context: &SessionContext,
) -> Value {
    let mut payload = Map::new();
    payload.insert(
        "timestamp_ms".to_string(),
        Value::from(extract_timestamp_ms(raw).unwrap_or_else(current_timestamp_ms)),
    );
    payload.insert(
        "game_seconds_remaining".to_string(),
        Value::from(extract_game_seconds_remaining(raw).unwrap_or(0)),
    );
    payload.insert(
        "type".to_string(),
        Value::String(canonical_event_type(event_type)),
    );
    payload.insert(
        "match_id".to_string(),
        Value::String(session_context.active_match_id.clone()),
    );
    payload.insert(
        "session_id".to_string(),
        Value::String(session_context.active_session_id.clone()),
    );

    if let Some(player_id) = extract_string_from_keys(
        raw,
        &[
            "player_id",
            "playerId",
            "attacker_id",
            "attackerId",
            "scorer_id",
            "scorerId",
            "actor_id",
            "actorId",
        ],
    ) {
        payload.insert("player_id".to_string(), Value::String(player_id));
    }

    if let Some(team) = find_value_by_keys(raw, &["Scorer"])
        .and_then(|scorer| extract_u64_from_keys(scorer, &["TeamNum", "team_num", "teamNum"]))
        .map(|team_num| match team_num {
            0 => "blue",
            1 => "orange",
            _ => "unknown",
        })
    {
        payload.insert("team".to_string(), Value::String(team.to_string()));
    }

    if let Some(scorer_name) = find_value_by_keys(raw, &["Scorer"])
        .and_then(|scorer| extract_string_from_keys(scorer, &["Name"]))
    {
        payload.insert("scorer".to_string(), Value::String(scorer_name));
    }
    if let Some(assister_name) = find_value_by_keys(raw, &["Assister"])
        .and_then(|assister| extract_string_from_keys(assister, &["Name"]))
    {
        payload.insert("assister".to_string(), Value::String(assister_name));
    }

    let details = extract_details_object(raw);
    payload.insert("details".to_string(), details);
    Value::Object(payload)
}

pub fn canonical_event_type(event_type: &str) -> String {
    match event_type {
        "UpdateState" | "ClockUpdated" | "ClockUpdatedSeconds" => "live_state".to_string(),
        "Goal" | "GoalScored" => "goal".to_string(),
        "Save" | "EpicSave" => "save".to_string(),
        "Demolition" | "Demo" => "demo".to_string(),
        other => other.to_ascii_lowercase(),
    }
}

pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_millis())
        .try_into()
        .map_or(u64::MAX, |value| value)
}

pub fn extract_timestamp_ms(raw: &Value) -> Option<u64> {
    extract_u64_from_keys(
        raw,
        &[
            "timestamp_ms",
            "timestampMs",
            "TimestampMs",
            "timestamp",
            "Timestamp",
        ],
    )
}

pub fn extract_game_seconds_remaining(raw: &Value) -> Option<u64> {
    extract_u64_from_keys(
        raw,
        &[
            "game_seconds_remaining",
            "gameSecondsRemaining",
            "seconds_remaining",
            "secondsRemaining",
            "time_remaining_seconds",
            "timeRemainingSeconds",
            "TimeSeconds",
            "remaining_seconds",
            "remainingSeconds",
        ],
    )
}

pub fn extract_score_object(raw: &Value) -> Option<Value> {
    let mut score = Map::new();

    if let Some((blue, orange)) = extract_scores_from_teams(raw) {
        if let Some(blue) = blue {
            score.insert("blue".to_string(), Value::from(blue));
        }
        if let Some(orange) = orange {
            score.insert("orange".to_string(), Value::from(orange));
        }
    } else {
        if let Some(blue) = extract_u64_from_keys(
            raw,
            &["blue", "blue_score", "blueScore", "score_blue", "scoreBlue"],
        ) {
            score.insert("blue".to_string(), Value::from(blue));
        }
        if let Some(orange) = extract_u64_from_keys(
            raw,
            &[
                "orange",
                "orange_score",
                "orangeScore",
                "score_orange",
                "scoreOrange",
            ],
        ) {
            score.insert("orange".to_string(), Value::from(orange));
        }
    }

    (!score.is_empty()).then_some(Value::Object(score))
}

fn extract_scores_from_teams(raw: &Value) -> Option<(Option<u64>, Option<u64>)> {
    let teams = find_value_by_keys(raw, &["Teams"])?;
    let mut blue = None;
    let mut orange = None;

    match teams {
        Value::Array(values) => {
            for team in values {
                apply_team_score(team, &mut blue, &mut orange);
            }
        }
        Value::Object(object) => {
            for team in object.values() {
                apply_team_score(team, &mut blue, &mut orange);
            }
        }
        _ => {}
    }

    if blue.is_some() || orange.is_some() {
        Some((blue, orange))
    } else {
        None
    }
}

fn apply_team_score(team: &Value, blue: &mut Option<u64>, orange: &mut Option<u64>) {
    let Some(team_num) = extract_u64_from_keys(team, &["TeamNum", "team_num", "teamNum"]) else {
        return;
    };
    let Some(score) = extract_u64_from_keys(team, &["Score", "score"]) else {
        return;
    };

    match team_num {
        0 => *blue = Some(score),
        1 => *orange = Some(score),
        _ => {}
    }
}

pub fn extract_player_telemetry(raw: &Value) -> Value {
    let mut players = Map::new();

    if let Some(players_value) = find_value_by_keys(raw, &["Players", "players"])
        && let Value::Array(entries) = players_value
    {
        for entry in entries {
            collect_player_telemetry(entry, &mut players);
        }
    }

    Value::Object(players)
}

pub fn collect_player_telemetry(raw: &Value, players: &mut Map<String, Value>) {
    if let Value::Object(_) = raw {
        let primary_id = extract_string_from_keys(raw, &["PrimaryId", "primaryId"]);
        let player_name = extract_string_from_keys(raw, &["Name", "name"]);

        let final_player_id = match primary_id.as_deref() {
            Some(id) if id != "Unknown|0|0" => id.to_string(),
            _ => player_name.map_or_else(
                || "Unknown_Player".to_string(),
                |name| format!("Bot_{name}"),
            ),
        };

        let mut telemetry = Map::new();

        if let Some(boost) = extract_u64_from_keys(raw, &["boost", "Boost"]) {
            telemetry.insert("boost".to_string(), Value::from(boost));
        }
        if let Some(score) = extract_i64_from_keys(raw, &["score", "Score"]) {
            telemetry.insert("score".to_string(), Value::from(score));
        }
        if let Some(goals) = extract_u64_from_keys(raw, &["goals", "Goals"]) {
            telemetry.insert("goals".to_string(), Value::from(goals));
        }
        if let Some(shots) = extract_u64_from_keys(raw, &["Shots", "shots"]) {
            telemetry.insert("shots".to_string(), Value::from(shots));
        }
        if let Some(assists) = extract_u64_from_keys(raw, &["Assists", "assists"]) {
            telemetry.insert("assists".to_string(), Value::from(assists));
        }
        if let Some(saves) = extract_u64_from_keys(raw, &["Saves", "saves"]) {
            telemetry.insert("saves".to_string(), Value::from(saves));
        }
        if let Some(demos) = extract_u64_from_keys(raw, &["Demos", "demos"]) {
            telemetry.insert("demos".to_string(), Value::from(demos));
        }
        if let Some(touches) = extract_u64_from_keys(raw, &["Touches", "touches"]) {
            telemetry.insert("touches".to_string(), Value::from(touches));
        }
        if let Some(team) = extract_u64_from_keys(raw, &["TeamNum", "team_num", "teamNum"]) {
            telemetry.insert("team".to_string(), Value::from(team));
        }

        if !telemetry.is_empty() {
            players.insert(final_player_id, Value::Object(telemetry));
        }
    }
}

pub fn extract_details_object(raw: &Value) -> Value {
    if let Some(details) = raw.get("details")
        && details.is_object()
    {
        return details.clone();
    }

    let mut details = Map::new();

    if let Some(speed_kph) = extract_u64_from_keys(raw, &["speed_kph", "speedKph"]) {
        details.insert("speed_kph".to_string(), Value::from(speed_kph));
    }
    if let Some(goal_speed) = extract_u64_from_keys(raw, &["GoalSpeed", "goal_speed", "goalSpeed"])
    {
        details.insert("goal_speed".to_string(), Value::from(goal_speed));
    }
    if let Some(goal_time) = extract_u64_from_keys(raw, &["GoalTime", "goal_time", "goalTime"]) {
        details.insert("goal_time".to_string(), Value::from(goal_time));
    }

    Value::Object(details)
}

pub fn extract_string_from_keys(raw: &Value, keys: &[&str]) -> Option<String> {
    find_value_by_keys(raw, keys)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn extract_u64_from_keys(raw: &Value, keys: &[&str]) -> Option<u64> {
    find_value_by_keys(raw, keys).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
            .or_else(|| value.as_f64().and_then(parse_rounded_u64))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|text| text.trim().parse::<u64>().ok())
            })
    })
}

pub fn extract_i64_from_keys(raw: &Value, keys: &[&str]) -> Option<i64> {
    find_value_by_keys(raw, keys).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
            .or_else(|| value.as_f64().and_then(parse_rounded_i64))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|text| text.trim().parse::<i64>().ok())
            })
    })
}

fn parse_rounded_u64(number: f64) -> Option<u64> {
    if !number.is_finite() {
        return None;
    }

    let rounded = number.round();
    if rounded < 0.0 {
        return None;
    }

    format!("{rounded:.0}").parse::<u64>().ok()
}

fn parse_rounded_i64(number: f64) -> Option<i64> {
    if !number.is_finite() {
        return None;
    }

    format!("{:.0}", number.round()).parse::<i64>().ok()
}

pub fn find_value_by_keys<'a>(raw: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    match raw {
        Value::Object(object) => {
            for key in keys {
                if let Some(value) = object.get(*key) {
                    return Some(value);
                }
            }

            object
                .values()
                .find_map(|value| find_value_by_keys(value, keys))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|value| find_value_by_keys(value, keys)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn session_context() -> SessionContext {
        SessionContext::new(
            Some("match_cfg_1".to_string()),
            Some("session_cfg_1".to_string()),
        )
    }

    #[test]
    fn live_state_reads_spec_time_seconds_from_game_data() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Game": {
                    "TimeSeconds": 180.0,
                    "Teams": []
                }
            }
        });

        let normalized = normalize_live_state(&raw, &session_context());

        assert_eq!(
            normalized.get("time_remaining_seconds"),
            Some(&Value::from(180_u64))
        );
    }

    #[test]
    fn live_state_reads_spec_time_seconds_from_clock_event_data() {
        let raw = json!({
            "Event": "ClockUpdatedSeconds",
            "Data": {
                "TimeSeconds": 179.0,
                "bOvertime": false
            }
        });

        let normalized = normalize_live_state(&raw, &session_context());

        assert_eq!(
            normalized.get("time_remaining_seconds"),
            Some(&Value::from(179_u64))
        );
    }

    #[test]
    fn live_state_omits_missing_score_for_sparse_payloads() {
        let raw = json!({
            "Event": "ClockUpdatedSeconds",
            "Data": {
                "TimeSeconds": 179.0
            }
        });

        let normalized = normalize_live_state(&raw, &session_context());

        assert!(normalized.get("score").is_none());
        assert_eq!(
            normalized.get("time_remaining_seconds"),
            Some(&Value::from(179_u64))
        );
    }

    #[test]
    fn live_state_extracts_arena_replay_and_winner_metadata() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Game": {
                    "TimeSeconds": 95.0,
                    "Arena": "Champions Field"
                },
                "bOvertime": true,
                "bReplay": false,
                "bHasWinner": true,
                "Winner": {
                    "Name": "Blue"
                }
            }
        });

        let normalized = normalize_live_state(&raw, &session_context());

        assert_eq!(
            normalized.get("arena"),
            Some(&Value::String("Champions Field".to_string()))
        );
        assert_eq!(normalized.get("is_overtime"), Some(&Value::Bool(true)));
        assert_eq!(normalized.get("is_replay"), Some(&Value::Bool(false)));
        assert_eq!(normalized.get("has_winner"), Some(&Value::Bool(true)));
        assert_eq!(
            normalized.get("winner"),
            Some(&Value::String("Blue".to_string()))
        );
    }

    #[test]
    fn score_object_reads_spec_teams_array() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Game": {
                    "Teams": [
                        {"Name": "Orange", "TeamNum": 1, "Score": 3.0},
                        {"Name": "Blue", "TeamNum": 0, "Score": 2.0}
                    ]
                }
            }
        });

        let score = extract_score_object(&raw);
        assert!(score.is_some());
        let Some(score) = score else {
            return;
        };

        assert_eq!(score.get("blue"), Some(&Value::from(2_u64)));
        assert_eq!(score.get("orange"), Some(&Value::from(3_u64)));
    }

    #[test]
    fn score_object_is_omitted_when_no_score_fields_exist() {
        let raw = json!({
            "Event": "ClockUpdatedSeconds",
            "Data": {
                "TimeSeconds": 179.0
            }
        });

        assert!(extract_score_object(&raw).is_none());
    }

    #[test]
    fn player_telemetry_uses_primary_id_and_ignores_players_parent_key() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Players": [
                    {
                        "Name": "PlayerA",
                        "PrimaryId": "Steam|123|0",
                        "Score": 125,
                        "Goals": 1,
                        "Boost": 45,
                        "Shots": 2,
                        "Assists": 1,
                        "Saves": 3,
                        "Demos": 4,
                        "Touches": 5
                    },
                    {
                        "Name": "PlayerB",
                        "PrimaryId": "Epic|456|0",
                        "Score": 250,
                        "Goals": 2,
                        "Boost": 80,
                        "Shots": 6,
                        "Assists": 0,
                        "Saves": 1,
                        "Demos": 2,
                        "Touches": 7
                    }
                ],
                "Game": {
                    "Teams": [
                        {"TeamNum": 0, "Score": 1},
                        {"TeamNum": 1, "Score": 2}
                    ]
                }
            }
        });

        let telemetry = extract_player_telemetry(&raw);
        let Some(players) = telemetry.as_object() else {
            return;
        };

        assert!(players.contains_key("Steam|123|0"));
        assert!(players.contains_key("Epic|456|0"));
        assert!(!players.contains_key("Players"));
        assert!(!players.contains_key("Teams"));

        assert_eq!(players["Steam|123|0"]["boost"], json!(45));
        assert_eq!(players["Steam|123|0"]["shots"], json!(2));
        assert_eq!(players["Steam|123|0"]["assists"], json!(1));
        assert_eq!(players["Steam|123|0"]["saves"], json!(3));
        assert_eq!(players["Steam|123|0"]["demos"], json!(4));
        assert_eq!(players["Steam|123|0"]["touches"], json!(5));

        assert_eq!(players["Epic|456|0"]["boost"], json!(80));
        assert_eq!(players["Epic|456|0"]["shots"], json!(6));
        assert_eq!(players["Epic|456|0"]["assists"], json!(0));
        assert_eq!(players["Epic|456|0"]["saves"], json!(1));
        assert_eq!(players["Epic|456|0"]["demos"], json!(2));
        assert_eq!(players["Epic|456|0"]["touches"], json!(7));

        assert!(players["Steam|123|0"].get("team").is_none());
    }

    #[test]
    fn player_telemetry_uses_bot_name_when_primary_id_is_unknown() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Players": [
                    {
                        "Name": "Chipper",
                        "PrimaryId": "Unknown|0|0",
                        "Boost": 33,
                        "Score": 80
                    },
                    {
                        "Name": "Sultan",
                        "PrimaryId": "Unknown|0|0",
                        "Boost": 66,
                        "Score": 120
                    }
                ]
            }
        });

        let telemetry = extract_player_telemetry(&raw);
        let Some(players) = telemetry.as_object() else {
            return;
        };

        assert!(players.contains_key("Bot_Chipper"));
        assert!(players.contains_key("Bot_Sultan"));
        assert_eq!(players["Bot_Chipper"]["boost"], json!(33));
        assert_eq!(players["Bot_Sultan"]["boost"], json!(66));
        assert!(!players.contains_key("Unknown|0|0"));
    }

    #[test]
    fn player_telemetry_falls_back_to_unknown_player_without_id_or_name() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Players": [
                    {
                        "Boost": 50,
                        "Score": 10
                    }
                ]
            }
        });

        let telemetry = extract_player_telemetry(&raw);
        let Some(players) = telemetry.as_object() else {
            return;
        };

        assert!(players.contains_key("Unknown_Player"));
        assert_eq!(players["Unknown_Player"]["boost"], json!(50));
        assert_eq!(players["Unknown_Player"]["score"], json!(10));
    }

    #[test]
    fn player_telemetry_ignores_team_objects_with_name_and_score() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Game": {
                    "Teams": [
                        {"Name": "ECLIPSE TOTAL", "Score": 4},
                        {"Name": "NEO TOKYO", "Score": 1}
                    ]
                },
                "Players": [
                    {
                        "Name": "Chipper",
                        "PrimaryId": "Unknown|0|0",
                        "Boost": 42
                    }
                ]
            }
        });

        let telemetry = extract_player_telemetry(&raw);
        let Some(players) = telemetry.as_object() else {
            return;
        };

        assert!(players.contains_key("Bot_Chipper"));
        assert!(!players.contains_key("Bot_ECLIPSE TOTAL"));
        assert!(!players.contains_key("Bot_NEO TOKYO"));
    }

    #[test]
    fn extract_u64_from_keys_handles_fractional_float() {
        let raw = json!({
            "TimeSeconds": 300.45
        });

        let result = extract_u64_from_keys(&raw, &["TimeSeconds"]);
        assert_eq!(result, Some(300_u64));

        let raw_zero = json!({"value": 0.0});
        let result_zero = extract_u64_from_keys(&raw_zero, &["value"]);
        assert_eq!(result_zero, Some(0_u64));
    }

    #[test]
    fn normalize_event_feed_preserves_statfeed_analytics_fields() {
        let ctx = session_context();
        let raw = json!({
            "Event": "StatfeedEvent",
            "EventName": "Save",
            "MainTarget": {
                "Name": "PlayerA"
            },
            "game_seconds_remaining": 92
        });

        let normalized = normalize_event_feed(&raw, "StatfeedEvent", &ctx);

        assert_eq!(
            normalized.get("type"),
            Some(&Value::String("statfeedevent".to_string()))
        );
        assert_eq!(
            normalized.get("stat_type"),
            Some(&Value::String("Save".to_string()))
        );
        assert_eq!(
            normalized.get("primary_player"),
            Some(&Value::String("PlayerA".to_string()))
        );
        assert_eq!(
            normalized.get("match_id"),
            Some(&Value::String("match_cfg_1".to_string()))
        );
        assert_eq!(
            normalized.get("session_id"),
            Some(&Value::String("session_cfg_1".to_string()))
        );
    }

    #[test]
    fn normalize_event_feed_preserves_lifecycle_event_identity() {
        let ctx = session_context();
        let raw = json!({
            "Event": "MatchEnded",
            "game_seconds_remaining": 0
        });

        let normalized = normalize_event_feed(&raw, "MatchEnded", &ctx);

        assert_eq!(
            normalized.get("type"),
            Some(&Value::String("matchended".to_string()))
        );
        assert_eq!(
            normalized.get("game_seconds_remaining"),
            Some(&Value::from(0_u64))
        );
        assert_eq!(
            normalized.get("match_id"),
            Some(&Value::String("match_cfg_1".to_string()))
        );
        assert_eq!(
            normalized.get("session_id"),
            Some(&Value::String("session_cfg_1".to_string()))
        );
    }

    #[test]
    fn normalize_historical_extracts_scorer_and_team() {
        let ctx = session_context();
        let raw = json!({
            "Event": "GoalScored",
            "Data": {
                "MatchGuid": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6",
                "GoalSpeed": 87.3,
                "GoalTime": 127.5,
                "Scorer": {
                    "Name": "PlayerA",
                    "Shortcut": 1,
                    "TeamNum": 0
                }
            }
        });

        let normalized = normalize_historical(&raw, "GoalScored", &ctx);

        assert_eq!(
            normalized.get("team"),
            Some(&Value::String("blue".to_string()))
        );
        assert_eq!(
            normalized.get("scorer"),
            Some(&Value::String("PlayerA".to_string()))
        );
    }

    #[test]
    fn extract_details_object_captures_goal_speed_and_time() {
        let raw = json!({
            "Event": "GoalScored",
            "Data": {
                "GoalSpeed": 87.3,
                "GoalTime": 127.5
            }
        });

        let details = extract_details_object(&raw);
        let Some(details_map) = details.as_object() else {
            return;
        };

        assert_eq!(details_map.get("goal_speed"), Some(&Value::from(87_u64)));
        assert_eq!(details_map.get("goal_time"), Some(&Value::from(128_u64)));
    }

    #[test]
    fn player_telemetry_extracts_team_from_raw_player_data() {
        let raw = json!({
            "Event": "UpdateState",
            "Data": {
                "Players": [
                    {
                        "Name": "PlayerBlue",
                        "PrimaryId": "Steam|123|0",
                        "TeamNum": 0,
                        "Boost": 50
                    },
                    {
                        "Name": "PlayerOrange",
                        "PrimaryId": "Epic|456|0",
                        "TeamNum": 1,
                        "Boost": 60
                    }
                ]
            }
        });

        let telemetry = extract_player_telemetry(&raw);
        let Some(players) = telemetry.as_object() else {
            return;
        };

        assert_eq!(players["Steam|123|0"]["team"], json!(0));
        assert_eq!(players["Epic|456|0"]["team"], json!(1));
    }
}
