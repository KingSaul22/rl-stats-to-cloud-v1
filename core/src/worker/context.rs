use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct SessionContext {
    pub(crate) active_match_id: String,
    pub(crate) active_session_id: String,
    pub(crate) id_source: String,
    pub(crate) in_replay: bool,
    pub(crate) blue_team_id: Arc<RwLock<Option<(String, String)>>>,
    pub(crate) orange_team_id: Arc<RwLock<Option<(String, String)>>>,
    pub(crate) teams_resolved_for_match: Arc<RwLock<String>>,
}

impl SessionContext {
    #[must_use]
    pub(crate) fn new(config_match_id: Option<String>, config_session_id: Option<String>) -> Self {
        let mut context = Self {
            active_match_id: String::new(),
            active_session_id: String::new(),
            id_source: "Generated".to_string(),
            in_replay: false,
            blue_team_id: Arc::new(RwLock::new(None)),
            orange_team_id: Arc::new(RwLock::new(None)),
            teams_resolved_for_match: Arc::new(RwLock::new(String::new())),
        };

        if let Some(match_id) = config_match_id
            && !match_id.is_empty()
        {
            context.active_match_id = match_id;
            context.id_source = "Config".to_string();
        }

        if let Some(session_id) = config_session_id
            && !session_id.is_empty()
        {
            context.active_session_id = session_id;
            context.id_source = "Config".to_string();
        }

        context.ensure_fallback_ids();
        context
    }

    pub(crate) fn update_from_payload(&mut self, payload: &Value) {
        const MATCH_KEYS: &[&str] = &[
            "MatchGuid",
            "match_guid",
            "matchGuid",
            "match_id",
            "matchId",
            "MatchID",
            "MatchId",
            "game_id",
            "gameId",
            "GameID",
        ];
        const SESSION_KEYS: &[&str] = &[
            "session_id",
            "sessionId",
            "SessionID",
            "SessionId",
            "game_session_id",
            "gameSessionId",
            "GameSessionID",
        ];

        let telemetry_match_id = Self::extract_identifier(payload, MATCH_KEYS);
        let telemetry_session_id = Self::extract_identifier(payload, SESSION_KEYS);

        if let Some(match_id) = telemetry_match_id
            && !match_id.is_empty()
        {
            self.active_match_id = match_id;
            self.id_source = "Telemetry".to_string();
        }

        if let Some(session_id) = telemetry_session_id
            && !session_id.is_empty()
        {
            self.active_session_id = session_id;
            self.id_source = "Telemetry".to_string();
        }

        self.ensure_fallback_ids();
    }

    pub(crate) fn ensure_fallback_ids(&mut self) {
        if self.active_match_id.is_empty() {
            self.active_match_id = Self::generate_fallback_id("match");
            if self.id_source != "Config" {
                self.id_source = "Generated".to_string();
            }
        }

        if self.active_session_id.is_empty() {
            self.active_session_id = Self::generate_fallback_id("session");
            if self.id_source != "Config" {
                self.id_source = "Generated".to_string();
            }
        }
    }

    #[must_use]
    pub(crate) fn extract_identifier(payload: &Value, keys: &[&str]) -> Option<String> {
        Self::find_identifier(payload, keys)
    }

    fn find_identifier(payload: &Value, keys: &[&str]) -> Option<String> {
        match payload {
            Value::Object(object) => {
                let direct = keys
                    .iter()
                    .find_map(|key| object.get(*key).and_then(Value::as_str))
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string);

                direct.or_else(|| {
                    object
                        .values()
                        .find_map(|value| Self::find_identifier(value, keys))
                })
            }
            Value::Array(values) => values
                .iter()
                .find_map(|value| Self::find_identifier(value, keys)),
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn generate_fallback_id(prefix: &str) -> String {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_millis());
        format!("{prefix}_{timestamp_ms}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn update_from_payload_uses_nested_match_guid() {
        let mut context = SessionContext::new(None, None);
        let payload = json!({
            "Event": "UpdateState",
            "Data": {
                "MatchGuid": "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6"
            }
        });

        context.update_from_payload(&payload);

        assert_eq!(context.active_match_id, "A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6");
        assert_eq!(context.id_source, "Telemetry");
    }

    #[test]
    fn update_from_payload_uses_stripped_match_guid() {
        let mut context = SessionContext::new(None, None);
        let payload = json!({
            "MatchGuid": "STRIPPED_MATCH_GUID"
        });

        context.update_from_payload(&payload);

        assert_eq!(context.active_match_id, "STRIPPED_MATCH_GUID");
        assert_eq!(context.id_source, "Telemetry");
    }
}
