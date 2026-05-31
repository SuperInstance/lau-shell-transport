use serde::{Deserialize, Serialize};

use crate::message_type::MessageType;
use crate::priority::Priority;

/// The message container for inter-shell communication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    /// Unique message identifier (UUID).
    pub id: String,
    /// Source shell ID.
    pub from_shell: String,
    /// Target shell ID, or `"*"` for broadcast.
    pub to_shell: String,
    /// What kind of message this is.
    pub msg_type: MessageType,
    /// JSON payload.
    pub payload: String,
    /// Unix epoch milliseconds.
    pub timestamp: u64,
    /// Message priority.
    pub priority: Priority,
    /// Time-to-live in milliseconds; if `None` the message does not expire.
    pub ttl: Option<u64>,
    /// For request/response matching.
    pub correlation_id: Option<String>,
}

impl Envelope {
    /// Create a new envelope with sensible defaults.
    pub fn new(from_shell: impl Into<String>, to_shell: impl Into<String>, msg_type: MessageType, payload: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            from_shell: from_shell.into(),
            to_shell: to_shell.into(),
            msg_type,
            payload: payload.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            priority: Priority::default(),
            ttl: None,
            correlation_id: None,
        }
    }

    /// Builder: set priority.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Builder: set TTL.
    pub fn with_ttl(mut self, ttl_ms: u64) -> Self {
        self.ttl = Some(ttl_ms);
        self
    }

    /// Builder: set correlation ID.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Check if this message has expired based on the current time.
    pub fn is_expired(&self) -> bool {
        match self.ttl {
            None => false,
            Some(ttl) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now > self.timestamp + ttl
            }
        }
    }

    /// Check if this is a broadcast message.
    pub fn is_broadcast(&self) -> bool {
        self.to_shell == "*"
    }

    /// Serialize to JSON string.
    pub fn to_json(&self) -> Result<String, crate::error::TransportError> {
        serde_json::to_string(self).map_err(|e| crate::error::TransportError::Serialization(e.to_string()))
    }

    /// Deserialize from JSON string.
    pub fn from_json(json: &str) -> Result<Self, crate::error::TransportError> {
        serde_json::from_str(json).map_err(|e| crate::error::TransportError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_envelope_has_defaults() {
        let e = Envelope::new("shell-a", "shell-b", MessageType::Task, r#"{"action":"run"}"#);
        assert!(!e.id.is_empty());
        assert_eq!(e.from_shell, "shell-a");
        assert_eq!(e.to_shell, "shell-b");
        assert_eq!(e.msg_type, MessageType::Task);
        assert_eq!(e.priority, Priority::Normal);
        assert!(e.ttl.is_none());
        assert!(e.correlation_id.is_none());
        assert!(!e.is_broadcast());
    }

    #[test]
    fn broadcast() {
        let e = Envelope::new("a", "*", MessageType::Heartbeat, "{}");
        assert!(e.is_broadcast());
    }

    #[test]
    fn builder_chain() {
        let e = Envelope::new("a", "b", MessageType::Task, "{}")
            .with_priority(Priority::Critical)
            .with_ttl(5000)
            .with_correlation_id("corr-123");
        assert_eq!(e.priority, Priority::Critical);
        assert_eq!(e.ttl, Some(5000));
        assert_eq!(e.correlation_id.as_deref(), Some("corr-123"));
    }

    #[test]
    fn json_roundtrip() {
        let e = Envelope::new("a", "b", MessageType::Custom("foo".into()), "payload")
            .with_priority(Priority::High)
            .with_ttl(1000)
            .with_correlation_id("c1");
        let json = e.to_json().unwrap();
        let back = Envelope::from_json(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn not_expired_by_default() {
        let e = Envelope::new("a", "b", MessageType::Heartbeat, "{}");
        assert!(!e.is_expired());
    }

    #[test]
    fn ttl_expiration_check() {
        let mut e = Envelope::new("a", "b", MessageType::Heartbeat, "{}");
        e.timestamp = 1000;
        e.ttl = Some(500);
        // Current time is way past 1500
        assert!(e.is_expired());
    }

    #[test]
    fn unique_ids() {
        let a = Envelope::new("a", "b", MessageType::Task, "{}");
        let b = Envelope::new("a", "b", MessageType::Task, "{}");
        assert_ne!(a.id, b.id);
    }
}
