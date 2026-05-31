use crate::error::TransportError;
use crate::Envelope;

/// Transport trait for inter-shell message passing.
pub trait Transport: Send + Sync {
    /// Send an envelope.
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError>;
    /// Try to receive an envelope (non-blocking).
    fn receive(&self) -> Result<Option<Envelope>, TransportError>;
    /// Poll for an envelope with a timeout in milliseconds.
    fn poll(&self, timeout_ms: u64) -> Result<Option<Envelope>, TransportError>;
    /// Close the transport.
    fn close(&mut self) -> Result<(), TransportError>;
    /// Whether the transport is still connected.
    fn is_connected(&self) -> bool;
    /// The endpoint identifier for this transport.
    fn endpoint(&self) -> &str;
}

#[cfg(test)]
mod tests {
    // Transport trait itself is tested through concrete implementations.
    // See memory.rs, file.rs, stdio.rs for integration tests.
}
