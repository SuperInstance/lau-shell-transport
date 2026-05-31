use serde::{Deserialize, Serialize};

/// Types of messages that can be sent between shells.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MessageType {
    /// Status update from one shell to another.
    Briefing,
    /// Work assignment.
    Task,
    /// Task completion result.
    Result,
    /// Escalate to Hermes (orchestrator).
    Escalation,
    /// Keep-alive ping.
    Heartbeat,
    /// Ask what an agent can do.
    CapabilityQuery,
    /// Response to a capability query.
    CapabilityResponse,
    /// Extensible custom message type.
    Custom(String),
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Briefing => write!(f, "briefing"),
            Self::Task => write!(f, "task"),
            Self::Result => write!(f, "result"),
            Self::Escalation => write!(f, "escalation"),
            Self::Heartbeat => write!(f, "heartbeat"),
            Self::CapabilityQuery => write!(f, "capability_query"),
            Self::CapabilityResponse => write!(f, "capability_response"),
            Self::Custom(s) => write!(f, "custom:{s}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_variants() {
        let variants = [
            MessageType::Briefing,
            MessageType::Task,
            MessageType::Result,
            MessageType::Escalation,
            MessageType::Heartbeat,
            MessageType::CapabilityQuery,
            MessageType::CapabilityResponse,
            MessageType::Custom("my_type".into()),
        ];
        for v in &variants {
            let json = serde_json::to_string(v).unwrap();
            let back: MessageType = serde_json::from_str(&json).unwrap();
            assert_eq!(v, &back);
        }
    }

    #[test]
    fn display_variants() {
        assert_eq!(MessageType::Task.to_string(), "task");
        assert_eq!(MessageType::Custom("foo".into()).to_string(), "custom:foo");
    }

    #[test]
    fn custom_preserves_value() {
        let mt = MessageType::Custom("special".into());
        let json = serde_json::to_string(&mt).unwrap();
        let back: MessageType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MessageType::Custom("special".into()));
    }
}
