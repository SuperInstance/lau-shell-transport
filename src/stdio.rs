use std::io::{self, BufReader, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::envelope::Envelope;
use crate::error::TransportError;
use crate::transport::Transport;

/// Stdio-based transport using length-prefixed JSON frames.
///
/// Each message on the wire is: 4-byte LE length + JSON payload.
pub struct StdioTransport {
    reader: Mutex<BufReader<Box<dyn Read + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
    connected: AtomicBool,
    endpoint_label: String,
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl StdioTransport {
    /// Create a new StdioTransport wrapping stdin/stdout.
    pub fn new() -> Self {
        Self {
            reader: Mutex::new(BufReader::new(Box::new(io::stdin()))),
            writer: Mutex::new(Box::new(io::stdout())),
            connected: AtomicBool::new(true),
            endpoint_label: "stdio://stdin/stdout".to_string(),
        }
    }

    /// Create with custom reader/writer (for testing).
    pub fn with_io(reader: Box<dyn Read + Send>, writer: Box<dyn Write + Send>) -> Self {
        Self {
            reader: Mutex::new(BufReader::new(reader)),
            writer: Mutex::new(writer),
            connected: AtomicBool::new(true),
            endpoint_label: "stdio://custom".to_string(),
        }
    }

    /// Create with a labeled endpoint.
    pub fn with_endpoint(
        reader: Box<dyn Read + Send>,
        writer: Box<dyn Write + Send>,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            reader: Mutex::new(BufReader::new(reader)),
            writer: Mutex::new(writer),
            connected: AtomicBool::new(true),
            endpoint_label: endpoint.into(),
        }
    }

    fn write_frame(&self, data: &[u8]) -> Result<(), TransportError> {
        let len = data.len() as u32;
        let mut writer = self.writer.lock().unwrap();
        writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| TransportError::Io(e.to_string()))?;
        writer
            .write_all(data)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(())
    }

    fn read_frame(&self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut reader = self.reader.lock().unwrap();
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(TransportError::Io(e.to_string())),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let mut payload = vec![0u8; len];
        reader
            .read_exact(&mut payload)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Some(payload))
    }
}

impl Transport for StdioTransport {
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        let json = envelope.to_json()?;
        self.write_frame(json.as_bytes())
    }

    fn receive(&self) -> Result<Option<Envelope>, TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        match self.read_frame()? {
            Some(data) => {
                let json =
                    String::from_utf8(data).map_err(|e| TransportError::Serialization(e.to_string()))?;
                let envelope = Envelope::from_json(&json)?;
                Ok(Some(envelope))
            }
            None => Ok(None),
        }
    }

    fn poll(&self, _timeout_ms: u64) -> Result<Option<Envelope>, TransportError> {
        // For stdio, we can't easily do non-blocking I/O in a portable way.
        // In production, you'd use async I/O. For now, just try to receive.
        self.receive()
    }

    fn close(&mut self) -> Result<(), TransportError> {
        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn endpoint(&self) -> &str {
        &self.endpoint_label
    }
}

// Helpers for tests: shared buffer reader/writer
#[cfg(test)]
mod shared_buf {
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};

    pub fn pair() -> (SharedReader, SharedWriter) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        (
            SharedReader {
                buf: buf.clone(),
                pos: 0,
            },
            SharedWriter { buf },
        )
    }

    pub struct SharedWriter {
        pub buf: Arc<Mutex<Vec<u8>>>,
    }
    impl Write for SharedWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.buf.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    pub struct SharedReader {
        pub buf: Arc<Mutex<Vec<u8>>>,
        pub pos: usize,
    }
    impl Read for SharedReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let buf = self.buf.lock().unwrap();
            let remaining = &buf[self.pos..];
            let n = std::cmp::min(out.len(), remaining.len());
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
            }
            out[..n].copy_from_slice(&remaining[..n]);
            self.pos += n;
            Ok(n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;

    fn test_env(from: &str, to: &str) -> Envelope {
        Envelope::new(from, to, MessageType::Task, r#"{"ok":true}"#)
    }

    #[test]
    fn send_and_receive_via_shared_buffer() {
        let (reader, writer) = shared_buf::pair();
        let transport =
            StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test://pipe");

        let e = test_env("alpha", "beta");
        transport.send(&e).unwrap();

        let received = transport.receive().unwrap().unwrap();
        assert_eq!(received.from_shell, "alpha");
        assert_eq!(received.to_shell, "beta");
    }

    #[test]
    fn multiple_messages_roundtrip() {
        let (reader, writer) = shared_buf::pair();
        let transport = StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test");

        let msgs = vec![
            test_env("a", "b"),
            test_env("c", "d"),
            Envelope::new("e", "f", MessageType::Heartbeat, "{}"),
        ];
        for m in &msgs {
            transport.send(m).unwrap();
        }

        for expected in &msgs {
            let got = transport.receive().unwrap().unwrap();
            assert_eq!(got.from_shell, expected.from_shell);
            assert_eq!(got.to_shell, expected.to_shell);
            assert_eq!(got.msg_type, expected.msg_type);
        }

        assert!(transport.receive().unwrap().is_none());
    }

    #[test]
    fn close_disconnects() {
        let (reader, writer) = shared_buf::pair();
        let mut t =
            StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test");
        assert!(t.is_connected());
        t.close().unwrap();
        assert!(!t.is_connected());
    }

    #[test]
    fn send_after_close_fails() {
        let (reader, writer) = shared_buf::pair();
        let mut t =
            StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test");
        t.close().unwrap();
        assert!(matches!(
            t.send(&test_env("a", "b")),
            Err(TransportError::Disconnected)
        ));
    }

    #[test]
    fn endpoint_default() {
        let t = StdioTransport::new();
        assert_eq!(t.endpoint(), "stdio://stdin/stdout");
    }

    #[test]
    fn endpoint_custom() {
        let (reader, writer) = shared_buf::pair();
        let t = StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "my-endpoint");
        assert_eq!(t.endpoint(), "my-endpoint");
    }

    #[test]
    fn with_io_uses_custom_label() {
        let (reader, writer) = shared_buf::pair();
        let t = StdioTransport::with_io(Box::new(reader), Box::new(writer));
        assert_eq!(t.endpoint(), "stdio://custom");
    }

    #[test]
    fn empty_payload_frame() {
        let (reader, writer) = shared_buf::pair();
        let transport =
            StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test");

        // Send a message with empty payload
        let e = Envelope::new("a", "b", MessageType::Heartbeat, "");
        transport.send(&e).unwrap();
        let got = transport.receive().unwrap().unwrap();
        assert_eq!(got.payload, "");
    }

    #[test]
    fn large_payload() {
        let (reader, writer) = shared_buf::pair();
        let transport =
            StdioTransport::with_endpoint(Box::new(reader), Box::new(writer), "test");

        let big_payload = "x".repeat(10_000);
        let e = Envelope::new("a", "b", MessageType::Task, &big_payload);
        transport.send(&e).unwrap();
        let got = transport.receive().unwrap().unwrap();
        assert_eq!(got.payload, big_payload);
    }
}
