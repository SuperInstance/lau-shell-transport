use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::envelope::Envelope;
use crate::error::TransportError;
use crate::transport::Transport;

/// File-based transport using JSONL files with flock for concurrency safety.
pub struct FileTransport {
    inbox_path: PathBuf,
    outbox_path: PathBuf,
    cursor: Mutex<u64>,
    connected: AtomicBool,
}

impl FileTransport {
    /// Create a new file transport. Auto-creates directories and files.
    pub fn new(inbox_path: PathBuf, outbox_path: PathBuf) -> Result<Self, TransportError> {
        if let Some(parent) = inbox_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| TransportError::Io(e.to_string()))?;
        }
        if let Some(parent) = outbox_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| TransportError::Io(e.to_string()))?;
        }
        // Ensure files exist
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&inbox_path)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&outbox_path)
            .map_err(|e| TransportError::Io(e.to_string()))?;

        Ok(Self {
            inbox_path,
            outbox_path,
            cursor: Mutex::new(0),
            connected: AtomicBool::new(true),
        })
    }

    /// Mark all current inbox messages as consumed by advancing the cursor.
    pub fn flush_inbox(&self) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        let metadata = std::fs::metadata(&self.inbox_path)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        *self.cursor.lock().unwrap() = metadata.len();
        Ok(())
    }

    /// Get the current cursor position (bytes read from inbox).
    pub fn cursor_pos(&self) -> u64 {
        *self.cursor.lock().unwrap()
    }

    fn read_new_line(&self) -> Result<Option<String>, TransportError> {
        let file = File::open(&self.inbox_path).map_err(|e| TransportError::Io(e.to_string()))?;

        // Acquire shared lock
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            if unsafe { libc::flock(fd, libc::LOCK_SH) } == -1 {
                return Err(TransportError::FileLockFailed(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }

        let mut cursor = self.cursor.lock().unwrap();
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(*cursor))
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let mut buf = String::new();
        let mut line = None;
        loop {
            buf.clear();
            let n = reader
                .read_line(&mut buf)
                .map_err(|e| TransportError::Io(e.to_string()))?;
            if n == 0 {
                break;
            }
            let trimmed = buf.trim().to_string();
            if !trimmed.is_empty() {
                line = Some(trimmed);
                break;
            }
        }

        *cursor = reader
            .stream_position()
            .map_err(|e| TransportError::Io(e.to_string()))?;

        // Release shared lock
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = reader.get_ref().as_raw_fd();
            unsafe { libc::flock(fd, libc::LOCK_UN) };
        }

        Ok(line)
    }
}

// File locking helpers using flock(2) on Unix.

impl Transport for FileTransport {
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.outbox_path)
            .map_err(|e| TransportError::Io(e.to_string()))?;

        // Exclusive lock for writing
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_EX) };
            if result == -1 {
                return Err(TransportError::FileLockFailed(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }

        let json = envelope.to_json()?;
        let result = writeln!(file, "{json}").map_err(|e| TransportError::Io(e.to_string()));

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            unsafe { libc::flock(fd, libc::LOCK_UN) };
        }

        result
    }

    fn receive(&self) -> Result<Option<Envelope>, TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        let line = self.read_new_line()?;
        if let Some(l) = line {
            if let Ok(envelope) = Envelope::from_json(&l) {
                return Ok(Some(envelope));
            }
            // Malformed line was consumed; try again for next valid line
            return self.receive();
        }
        Ok(None)
    }

    fn poll(&self, timeout_ms: u64) -> Result<Option<Envelope>, TransportError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(TransportError::Disconnected);
        }
        // Try immediately
        if let Some(e) = self.receive()? {
            return Ok(Some(e));
        }
        if timeout_ms == 0 {
            return Ok(None);
        }
        // Simple polling loop with 50ms intervals
        let start = std::time::Instant::now();
        let interval = std::time::Duration::from_millis(50);
        while start.elapsed().as_millis() < timeout_ms as u128 {
            std::thread::sleep(interval);
            if let Some(e) = self.receive()? {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), TransportError> {
        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn endpoint(&self) -> &str {
        self.inbox_path.to_str().unwrap_or("invalid-path")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;
    use tempfile::TempDir;

    fn test_env(from: &str, to: &str) -> Envelope {
        Envelope::new(from, to, MessageType::Task, r#"{"ok":true}"#)
    }

    fn setup() -> (TempDir, PathBuf, PathBuf) {
        let dir = TempDir::new().unwrap();
        let inbox = dir.path().join("inbox.jsonl");
        let outbox = dir.path().join("outbox.jsonl");
        (dir, inbox, outbox)
    }

    #[test]
    fn create_auto_makes_dirs() {
        let dir = TempDir::new().unwrap();
        let inbox = dir.path().join("sub/dir/inbox.jsonl");
        let outbox = dir.path().join("sub/dir/outbox.jsonl");
        let _t = FileTransport::new(inbox.clone(), outbox.clone()).unwrap();
        assert!(inbox.exists());
        assert!(outbox.exists());
    }

    #[test]
    fn send_appends_to_outbox() {
        let (_dir, inbox, outbox) = setup();
        let t = FileTransport::new(inbox, outbox.clone()).unwrap();
        t.send(&test_env("a", "b")).unwrap();
        t.send(&test_env("c", "d")).unwrap();
        let contents = std::fs::read_to_string(&outbox).unwrap();
        let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn receive_reads_from_inbox() {
        let (_dir, inbox, outbox) = setup();
        // Write a message directly to the inbox file
        let e = test_env("x", "y");
        let json = e.to_json().unwrap();
        std::fs::write(&inbox, format!("{json}\n")).unwrap();

        let t = FileTransport::new(inbox, outbox).unwrap();
        let received = t.receive().unwrap().unwrap();
        assert_eq!(received.from_shell, "x");
    }

    #[test]
    fn receive_returns_none_when_empty() {
        let (_dir, inbox, outbox) = setup();
        let t = FileTransport::new(inbox, outbox).unwrap();
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn cursor_advances() {
        let (_dir, inbox, outbox) = setup();
        let e = test_env("a", "b");
        let json = e.to_json().unwrap();
        std::fs::write(&inbox, format!("{json}\n")).unwrap();

        let t = FileTransport::new(inbox, outbox).unwrap();
        assert_eq!(t.cursor_pos(), 0);
        t.receive().unwrap().unwrap();
        assert!(t.cursor_pos() > 0);
        // Second receive returns None (cursor past the line)
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn flush_inbox_advances_cursor() {
        let (_dir, inbox, outbox) = setup();
        let e = test_env("a", "b");
        std::fs::write(&inbox, format!("{}\n", e.to_json().unwrap())).unwrap();

        let t = FileTransport::new(inbox, outbox).unwrap();
        assert_eq!(t.cursor_pos(), 0);
        t.flush_inbox().unwrap();
        assert!(t.cursor_pos() > 0);
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn close_disconnects() {
        let (_dir, inbox, outbox) = setup();
        let mut t = FileTransport::new(inbox, outbox).unwrap();
        assert!(t.is_connected());
        t.close().unwrap();
        assert!(!t.is_connected());
    }

    #[test]
    fn send_after_close_fails() {
        let (_dir, inbox, outbox) = setup();
        let mut t = FileTransport::new(inbox, outbox).unwrap();
        t.close().unwrap();
        assert!(matches!(t.send(&test_env("a", "b")), Err(TransportError::Disconnected)));
    }

    #[test]
    fn receive_after_close_fails() {
        let (_dir, inbox, outbox) = setup();
        let mut t = FileTransport::new(inbox, outbox).unwrap();
        t.close().unwrap();
        assert!(matches!(t.receive(), Err(TransportError::Disconnected)));
    }

    #[test]
    fn endpoint_is_inbox_path() {
        let (_dir, inbox, outbox) = setup();
        let t = FileTransport::new(inbox.clone(), outbox).unwrap();
        assert!(t.endpoint().contains("inbox.jsonl"));
    }

    #[test]
    fn multiple_messages_sequential() {
        let (_dir, inbox, outbox) = setup();
        let e1 = test_env("a", "b");
        let e2 = test_env("c", "d");
        let e3 = test_env("e", "f");
        let mut contents = String::new();
        contents.push_str(&format!("{}\n", e1.to_json().unwrap()));
        contents.push_str(&format!("{}\n", e2.to_json().unwrap()));
        contents.push_str(&format!("{}\n", e3.to_json().unwrap()));
        std::fs::write(&inbox, contents).unwrap();

        let t = FileTransport::new(inbox, outbox).unwrap();
        let r1 = t.receive().unwrap().unwrap();
        assert_eq!(r1.from_shell, "a");
        let r2 = t.receive().unwrap().unwrap();
        assert_eq!(r2.from_shell, "c");
        let r3 = t.receive().unwrap().unwrap();
        assert_eq!(r3.from_shell, "e");
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn malformed_lines_skipped() {
        let (_dir, inbox, outbox) = setup();
        let e = test_env("a", "b");
        let mut contents = String::new();
        contents.push_str("not json\n");
        contents.push_str(&format!("{}\n", e.to_json().unwrap()));
        std::fs::write(&inbox, contents).unwrap();

        let t = FileTransport::new(inbox, outbox).unwrap();
        // First receive skips malformed, gets valid
        let r = t.receive().unwrap().unwrap();
        assert_eq!(r.from_shell, "a");
        assert!(t.receive().unwrap().is_none());
    }

    #[test]
    fn poll_returns_immediately_if_available() {
        let (_dir, inbox, outbox) = setup();
        let e = test_env("a", "b");
        std::fs::write(&inbox, format!("{}\n", e.to_json().unwrap())).unwrap();
        let t = FileTransport::new(inbox, outbox).unwrap();
        let r = t.poll(0).unwrap().unwrap();
        assert_eq!(r.from_shell, "a");
    }

    #[test]
    fn concurrent_writes() {
        let (_dir, inbox, outbox) = setup();
        let t = std::sync::Arc::new(FileTransport::new(inbox, outbox.clone()).unwrap());

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let t = std::sync::Arc::clone(&t);
                std::thread::spawn(move || {
                    let e = Envelope::new(format!("shell-{i}"), "target", MessageType::Task, "{}");
                    t.send(&e).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let contents = std::fs::read_to_string(&outbox).unwrap();
        let count = contents.lines().filter(|l| !l.trim().is_empty()).count();
        assert_eq!(count, 4);
    }
}
