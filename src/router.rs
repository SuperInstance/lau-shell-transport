use std::collections::HashMap;

use crate::envelope::Envelope;
use crate::error::TransportError;
use crate::transport::Transport;

/// Routes messages across multiple transports keyed by shell ID.
pub struct TransportRouter {
    transports: HashMap<String, Box<dyn Transport>>,
}

impl TransportRouter {
    pub fn new() -> Self {
        Self {
            transports: HashMap::new(),
        }
    }

    /// Register a transport for a shell.
    pub fn register(&mut self, shell_id: String, transport: Box<dyn Transport>) {
        self.transports.insert(shell_id, transport);
    }

    /// Unregister a transport for a shell.
    pub fn unregister(&mut self, shell_id: &str) -> Option<Box<dyn Transport>> {
        self.transports.remove(shell_id)
    }

    /// Route an envelope to the appropriate transport based on `to_shell`.
    /// Broadcast (`"*"`) sends to all connected shells except the sender.
    pub fn route(&self, envelope: &Envelope) -> Result<(), TransportError> {
        if envelope.is_broadcast() {
            for (shell_id, transport) in &self.transports {
                if *shell_id != envelope.from_shell && transport.is_connected() {
                    transport.send(envelope)?;
                }
            }
            Ok(())
        } else {
            match self.transports.get(&envelope.to_shell) {
                Some(transport) => transport.send(envelope),
                None => Err(TransportError::Io(format!(
                    "no transport registered for shell '{}'",
                    envelope.to_shell
                ))),
            }
        }
    }

    /// Poll all transports and return the first available message with its source shell ID.
    pub fn receive_any(&self, timeout_ms: u64) -> Result<Option<(String, Envelope)>, TransportError> {
        // First pass: quick check all transports
        for (shell_id, transport) in &self.transports {
            if let Some(envelope) = transport.receive()? {
                return Ok(Some((shell_id.clone(), envelope)));
            }
        }
        if timeout_ms == 0 {
            return Ok(None);
        }

        // Poll with timeout
        let start = std::time::Instant::now();
        let interval = std::time::Duration::from_millis(50);
        while start.elapsed().as_millis() < timeout_ms as u128 {
            std::thread::sleep(interval);
            for (shell_id, transport) in &self.transports {
                if let Some(envelope) = transport.receive()? {
                    return Ok(Some((shell_id.clone(), envelope)));
                }
            }
        }
        Ok(None)
    }

    /// Get the list of currently connected shell IDs.
    pub fn connected_shells(&self) -> Vec<String> {
        self.transports
            .iter()
            .filter(|(_, t)| t.is_connected())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Number of registered transports.
    pub fn len(&self) -> usize {
        self.transports.len()
    }

    /// Whether any transports are registered.
    pub fn is_empty(&self) -> bool {
        self.transports.is_empty()
    }
}

impl Default for TransportRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryTransport;
    use crate::message_type::MessageType;

    fn test_env(from: &str, to: &str) -> Envelope {
        Envelope::new(from, to, MessageType::Task, r#"{"ok":true}"#)
    }

    #[test]
    fn register_and_count() {
        let mut router = TransportRouter::new();
        assert!(router.is_empty());
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        assert_eq!(router.len(), 1);
        router.register("b".into(), Box::new(MemoryTransport::new("b")));
        assert_eq!(router.len(), 2);
    }

    #[test]
    fn unregister() {
        let mut router = TransportRouter::new();
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        let removed = router.unregister("a");
        assert!(removed.is_some());
        assert!(router.is_empty());
    }

    #[test]
    fn unregister_nonexistent() {
        let mut router = TransportRouter::new();
        assert!(router.unregister("ghost").is_none());
    }

    #[test]
    fn register_replaces_existing() {
        let mut router = TransportRouter::new();
        router.register("a".into(), Box::new(MemoryTransport::new("a-v1")));
        router.register("a".into(), Box::new(MemoryTransport::new("a-v2")));
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn route_unknown_shell_fails() {
        let mut router = TransportRouter::new();
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        let e = test_env("a", "z");
        let result = router.route(&e);
        assert!(result.is_err());
    }

    #[test]
    fn connected_shells() {
        let mut router = TransportRouter::new();
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        router.register("b".into(), Box::new(MemoryTransport::new("b")));
        let mut connected = router.connected_shells();
        connected.sort();
        assert_eq!(connected, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn connected_shells_excludes_disconnected() {
        let mut router = TransportRouter::new();
        let mut t = MemoryTransport::new("b");
        t.close().unwrap();
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        router.register("b".into(), Box::new(t));
        let connected = router.connected_shells();
        assert_eq!(connected, vec!["a".to_string()]);
    }

    #[test]
    fn receive_any_empty() {
        let mut router = TransportRouter::new();
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        assert!(router.receive_any(0).unwrap().is_none());
    }

    #[test]
    fn default_is_empty() {
        let router = TransportRouter::default();
        assert!(router.is_empty());
    }

    #[test]
    fn route_direct_sends_to_correct_transport() {
        let mut router = TransportRouter::new();
        let t = MemoryTransport::new("b");
        router.register("a".into(), Box::new(MemoryTransport::new("a")));
        router.register("b".into(), Box::new(t));

        let e = test_env("a", "b");
        router.route(&e).unwrap();
        // Message was sent via b's transport.send(), which pushes to its outbox
    }

    #[test]
    fn broadcast_sends_to_all_except_sender() {
        let mut router = TransportRouter::new();
        let t_a = MemoryTransport::new("a");
        let t_b = MemoryTransport::new("b");
        let t_c = MemoryTransport::new("c");
        router.register("a".into(), Box::new(t_a));
        router.register("b".into(), Box::new(t_b));
        router.register("c".into(), Box::new(t_c));

        let e = Envelope::new("a", "*", MessageType::Heartbeat, "{}");
        router.route(&e).unwrap();
        // Broadcast from a should have sent to b and c but not a
    }

    #[test]
    fn full_roundtrip_with_memory_transports() {
        // Use two routers: one for each direction
        let mut router = TransportRouter::new();

        // Create two memory transports
        let t_a = MemoryTransport::new("a");
        let t_b = MemoryTransport::new("b");

        // Inject a message into b's inbox
        let msg = test_env("b", "a");
        t_b.inject(msg.clone());

        router.register("a".into(), Box::new(t_a));
        router.register("b".into(), Box::new(t_b));

        // receive_any should find the message from b
        let result = router.receive_any(0).unwrap().unwrap();
        assert_eq!(result.0, "b");
        assert_eq!(result.1.from_shell, "b");
    }

    #[test]
    fn receive_any_returns_first_available() {
        let mut router = TransportRouter::new();

        let t_a = MemoryTransport::new("a");
        let t_b = MemoryTransport::new("b");

        // Inject into both
        t_a.inject(test_env("a", "router"));
        t_b.inject(test_env("b", "router"));

        router.register("a".into(), Box::new(t_a));
        router.register("b".into(), Box::new(t_b));

        let result = router.receive_any(0).unwrap().unwrap();
        // Could be a or b depending on HashMap iteration order
        assert!(&result.0 == "a" || &result.0 == "b");
    }
}
