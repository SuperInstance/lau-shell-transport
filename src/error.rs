use serde::{Deserialize, Serialize};

/// Errors that can occur during transport operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransportError {
    /// I/O error (file, network, etc.).
    Io(String),
    /// Serialization/deserialization error.
    Serialization(String),
    /// The transport has been disconnected.
    Disconnected,
    /// Operation timed out.
    Timeout,
    /// File lock could not be acquired.
    FileLockFailed(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "I/O error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Self::Disconnected => write!(f, "transport disconnected"),
            Self::Timeout => write!(f, "operation timed out"),
            Self::FileLockFailed(msg) => write!(f, "file lock failed: {msg}"),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_variants() {
        assert!(TransportError::Io("broken".into()).to_string().contains("broken"));
        assert!(TransportError::Serialization("bad json".into()).to_string().contains("bad json"));
        assert_eq!(TransportError::Disconnected.to_string(), "transport disconnected");
        assert_eq!(TransportError::Timeout.to_string(), "operation timed out");
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let te: TransportError = io_err.into();
        assert!(matches!(te, TransportError::Io(_)));
    }

    #[test]
    fn serde_roundtrip() {
        let err = TransportError::FileLockFailed("locked".into());
        let json = serde_json::to_string(&err).unwrap();
        let back: TransportError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn is_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&TransportError::Disconnected);
    }
}
