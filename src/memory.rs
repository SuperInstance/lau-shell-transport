use std::collections::VecDeque;
use std::sync::Mutex;

use crate::envelope::Envelope;
use crate::error::TransportError;
use crate::transport::Transport;

/// In-memory transport for testing.
pub struct MemoryTransport {
    label: String,
    connected: Mutex<bool>,
    inbox: Mutex<VecDeque<Envelope>>,
    outbox: Mutex<VecDeque<Envelope>>,
}

impl MemoryTransport {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            connected: Mutex::new(true),
            inbox: Mutex::new(VecDeque::new()),
            outbox: Mutex::new(VecDeque::new()),
        }
    }

    /// Manually inject an envelope into the inbox (for testing).
    pub fn inject(&self, envelope: Envelope) {
        self.inbox.lock().unwrap().push_back(envelope);
    }

    /// Drain everything that was sent (for assertions).
    pub fn drain_outbox(&self) -> Vec<Envelope> {
        self.outbox.lock().unwrap().drain(..).collect()
    }

    /// Peek at inbox without consuming.
    pub fn peek_inbox(&self) -> Vec<Envelope> {
        self.inbox.lock().unwrap().iter().cloned().collect()
    }

    /// Current inbox length.
    pub fn inbox_len(&self) -> usize {
        self.inbox.lock().unwrap().len()
    }

    /// Current outbox length.
    pub fn outbox_len(&self) -> usize {
        self.outbox.lock().unwrap().len()
    }
}

impl Transport for MemoryTransport {
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError> {
        if !*self.connected.lock().unwrap() {
            return Err(TransportError::Disconnected);
        }
        self.outbox.lock().unwrap().push_back(envelope.clone());
        Ok(())
    }

    fn receive(&self) -> Result<Option<Envelope>, TransportError> {
        if !*self.connected.lock().unwrap() {
            return Err(TransportError::Disconnected);
        }
        Ok(self.inbox.lock().unwrap().pop_front())
    }

    fn poll(&self, timeout_ms: u64) -> Result<Option<Envelope>, TransportError> {
        if !*self.connected.lock().unwrap() {
            return Err(TransportError::Disconnected);
        }
        // Check immediately; for memory transport we don't actually wait.
        let result = self.inbox.lock().unwrap().pop_front();
        if result.is_some() {
            return Ok(result);
        }
        // If nothing and timeout is 0, return None immediately.
        if timeout_ms == 0 {
            return Ok(None);
        }
        // For a real timeout we'd sleep, but for tests just return None.
        // A more sophisticated implementation could use condvar, but that's overkill for testing.
        Ok(self.inbox.lock().unwrap().pop_front())
    }

    fn close(&mut self) -> Result<(), TransportError> {
        *self.connected.lock().unwrap() = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock().unwrap()
    }

    fn endpoint(&self) -> &str {
        &self.label
    }
}

#[cfg(test)]
mod tests {
    use crate::message_type::MessageType;

    use super::*;

    fn test_envelope(from: &str, to: &str) -> Envelope {
        Envelope::new(from, to, MessageType::Task, r#"{"test":true}"#)
    }

    #[test]
    fn send_and_drain() {
        let t = MemoryTransport::new("test");
        let e = test_envelope("a", "b");
        t.send(&e).unwrap();
        assert_eq!(t.outbox_len(), 1);
        let sent = t.drain_outbox();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].from_shell, "a");
        assert_eq!(t.outbox_len(), 0);
    }

    #[test]
    fn inject_and_receive() {
        let t = MemoryTransport::new("test");
        let e = test_envelope("x", "y");
        t.inject(e);
        assert_eq!(t.inbox_len(), 1);
        let received = t.receive().unwrap().unwrap();
        assert_eq!(received.from_shell, "x");
        assert_eq!(t.inbox_len(), 0);
    }

    #[test]
    fn receive_empty_returns_none() {
        let t = MemoryTransport::new("test");
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn poll_returns_available() {
        let t = MemoryTransport::new("test");
        t.inject(test_envelope("a", "b"));
        let result = t.poll(100).unwrap().unwrap();
        assert_eq!(result.from_shell, "a");
    }

    #[test]
    fn poll_empty_returns_none() {
        let t = MemoryTransport::new("test");
        assert!(t.poll(0).unwrap().is_none());
    }

    #[test]
    fn close_disconnects() {
        let mut t = MemoryTransport::new("test");
        assert!(t.is_connected());
        t.close().unwrap();
        assert!(!t.is_connected());
    }

    #[test]
    fn send_after_close_fails() {
        let mut t = MemoryTransport::new("test");
        t.close().unwrap();
        let result = t.send(&test_envelope("a", "b"));
        assert!(matches!(result, Err(TransportError::Disconnected)));
    }

    #[test]
    fn receive_after_close_fails() {
        let mut t = MemoryTransport::new("test");
        t.inject(test_envelope("a", "b"));
        t.close().unwrap();
        let result = t.receive();
        assert!(matches!(result, Err(TransportError::Disconnected)));
    }

    #[test]
    fn poll_after_close_fails() {
        let mut t = MemoryTransport::new("test");
        t.close().unwrap();
        let result = t.poll(100);
        assert!(matches!(result, Err(TransportError::Disconnected)));
    }

    #[test]
    fn endpoint_label() {
        let t = MemoryTransport::new("my-endpoint");
        assert_eq!(t.endpoint(), "my-endpoint");
    }

    #[test]
    fn multiple_messages_fifo() {
        let t = MemoryTransport::new("test");
        t.inject(test_envelope("a", "b"));
        t.inject(test_envelope("c", "d"));
        t.inject(test_envelope("e", "f"));

        let first = t.receive().unwrap().unwrap();
        assert_eq!(first.from_shell, "a");
        let second = t.receive().unwrap().unwrap();
        assert_eq!(second.from_shell, "c");
        let third = t.receive().unwrap().unwrap();
        assert_eq!(third.from_shell, "e");
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn drain_outbox_empties() {
        let t = MemoryTransport::new("test");
        t.send(&test_envelope("a", "b")).unwrap();
        t.send(&test_envelope("c", "d")).unwrap();
        let drained = t.drain_outbox();
        assert_eq!(drained.len(), 2);
        assert_eq!(t.drain_outbox().len(), 0);
    }

    #[test]
    fn peek_inbox_does_not_consume() {
        let t = MemoryTransport::new("test");
        t.inject(test_envelope("a", "b"));
        let peeked = t.peek_inbox();
        assert_eq!(peeked.len(), 1);
        assert_eq!(t.inbox_len(), 1); // still there
    }

    #[test]
    fn concurrent_send_receive() {
        let t = MemoryTransport::new("test");
        let t = std::sync::Arc::new(t);

        let sender = {
            let t = std::sync::Arc::clone(&t);
            std::thread::spawn(move || {
                for i in 0..100 {
                    let e = Envelope::new("a", "b", MessageType::Task, format!(r#"{{"i":{i}}}"#));
                    t.send(&e).unwrap();
                }
            })
        };

        sender.join().unwrap();
        assert_eq!(t.outbox_len(), 100);
    }
}
