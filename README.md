# lau-shell-transport

**Transport layer for inter-shell communication — message passing via files, stdio, and memory.**

A Rust library providing a unified `Transport` trait and three concrete implementations (in-memory, stdio, file-based) for routing structured `Envelope` messages between shell instances. Includes a `TransportRouter` for multiplexing, TTL-based expiry, broadcast support, priority queuing, and JSONL file locking with `flock(2)`. 69 tests, full serde support.

---

## What This Does

`lau-shell-transport` solves the problem of **how shell instances talk to each other**. It provides:

1. **A `Transport` trait** — a unified interface for `send`, `receive`, `poll`, `close`, and `is_connected` that works across any I/O backend.
2. **`MemoryTransport`** — in-memory inbox/outbox queues for testing and same-process communication.
3. **`StdioTransport`** — length-prefixed binary frames over stdin/stdout (or any `Read + Write` pair) for subprocess communication.
4. **`FileTransport`** — JSONL files with `flock(2)` locking for cross-process, disk-based message passing with cursor tracking.
5. **`TransportRouter`** — a multiplexer that routes messages to the correct transport by shell ID, with broadcast (`"*"`) support.
6. **`Envelope`** — a rich message container with UUID IDs, priority levels, TTL expiry, correlation IDs, and typed message categories.

---

## Key Idea

Shells need to communicate across different substrates — sometimes in the same process (testing), sometimes over pipes (subprocesses), sometimes via files (cross-process without a broker). This crate provides **one trait, three backends, zero lock-in**:

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Shell A    │────►│  TransportRouter │────►│  Shell B    │
│  (Hermes)   │     │  route(envelope) │     │  (ZeroClaw) │
└─────────────┘     └──────────────────┘     └─────────────┘
                           │
                    ┌──────┼──────┐
                    ▼      ▼      ▼
               Memory   Stdio   File
             (testing) (pipes) (disk)
```

Every message is an `Envelope` — a self-contained JSON-serializable unit with metadata for routing, expiry, and correlation.

---

## Install

```toml
[dependencies]
lau-shell-transport = "0.1.0"
```

```sh
cargo add lau-shell-transport
```

### Requirements

- Rust 2021 edition
- `serde` 1.x + `serde_json` 1.x
- `uuid` 1.x (v4)
- `libc` 0.2 (for `flock` on Unix)

---

## Quick Start

### Create and send an envelope

```rust
use lau_shell_transport::*;

let env = Envelope::new("shell-a", "shell-b", MessageType::Task, r#"{"action":"compute"}"#)
    .with_priority(Priority::High)
    .with_ttl(30_000)           // expires after 30 seconds
    .with_correlation_id("req-42");

assert!(!env.is_expired());
assert!(!env.is_broadcast());
```

### In-memory transport (testing)

```rust
let t = MemoryTransport::new("test-endpoint");

// Inject a message into the inbox
t.inject(Envelope::new("other", "test", MessageType::Heartbeat, "{}"));

// Receive it
let received = t.receive().unwrap().unwrap();
assert_eq!(received.msg_type, MessageType::Heartbeat);

// Send a message
t.send(&Envelope::new("test", "other", MessageType::Result, "{}")).unwrap();
let sent = t.drain_outbox();
assert_eq!(sent.len(), 1);
```

### Stdio transport (subprocess communication)

```rust
// Wraps stdin/stdout with length-prefixed JSON framing
let transport = StdioTransport::new();

// Or with custom I/O for testing:
let transport = StdioTransport::with_io(reader, writer);

// Each frame: [4-byte LE length][JSON payload]
transport.send(&envelope).unwrap();
let response = transport.receive().unwrap();
```

### File transport (cross-process via disk)

```rust
let inbox = PathBuf::from("/tmp/shell-b-inbox.jsonl");
let outbox = PathBuf::from("/tmp/shell-b-outbox.jsonl");

let transport = FileTransport::new(inbox, outbox).unwrap();

// Send (appends JSONL line with exclusive flock)
transport.send(&envelope).unwrap();

// Receive (reads new lines since last cursor position with shared flock)
let msg = transport.receive().unwrap();

// Poll with timeout (50ms intervals)
let msg = transport.poll(5000).unwrap();
```

### Router (multiplexing)

```rust
let mut router = TransportRouter::new();

router.register("shell-a".into(), Box::new(MemoryTransport::new("a")));
router.register("shell-b".into(), Box::new(MemoryTransport::new("b")));
router.register("shell-c".into(), Box::new(MemoryTransport::new("c")));

// Direct message
router.route(&Envelope::new("a", "b", MessageType::Task, "{}")).unwrap();

// Broadcast (sends to all except sender)
router.route(&Envelope::new("a", "*", MessageType::Heartbeat, "{}")).unwrap();

// Receive from any transport
if let Some((source_id, envelope)) = router.receive_any(1000).unwrap() {
    println!("Got message from {}", source_id);
}
```

---

## API Reference

### Core Trait: `Transport`

```rust
pub trait Transport: Send + Sync {
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError>;
    fn receive(&self) -> Result<Option<Envelope>, TransportError>;
    fn poll(&self, timeout_ms: u64) -> Result<Option<Envelope>, TransportError>;
    fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn endpoint(&self) -> &str;
}
```

### `Envelope`

| Field | Type | Description |
|---|---|---|
| `id` | `String` | UUID v4, auto-generated |
| `from_shell` | `String` | Source shell ID |
| `to_shell` | `String` | Target shell ID, or `"*"` for broadcast |
| `msg_type` | `MessageType` | Semantic category |
| `payload` | `String` | JSON payload body |
| `timestamp` | `u64` | Unix epoch milliseconds |
| `priority` | `Priority` | `Background < Low < Normal < High < Critical` |
| `ttl` | `Option<u64>` | Time-to-live in milliseconds |
| `correlation_id` | `Option<String>` | Request/response matching |

Methods: `new()`, `with_priority()`, `with_ttl()`, `with_correlation_id()`, `is_expired()`, `is_broadcast()`, `to_json()`, `from_json()`.

### `MessageType`

Variants: `Briefing`, `Task`, `Result`, `Escalation`, `Heartbeat`, `CapabilityQuery`, `CapabilityResponse`, `Custom(String)`.

### `Priority`

`Background(0) < Low(1) < Normal(2, default) < High(3) < Critical(4)`. Implements `Ord` for sorting.

### `MemoryTransport`

Thread-safe in-memory queues. Extras: `inject()`, `drain_outbox()`, `peek_inbox()`, `inbox_len()`, `outbox_len()`.

### `StdioTransport`

Length-prefixed binary framing over `Read + Write`. Frame format: `[4-byte LE u32 length][JSON bytes]`.

### `FileTransport`

JSONL file transport with `flock(2)` (Unix only). Tracks a byte cursor for incremental reads. `flush_inbox()` advances the cursor to end-of-file. `poll()` loops at 50ms intervals.

### `TransportRouter`

`register(id, transport)`, `unregister(id)`, `route(envelope)`, `receive_any(timeout_ms)`, `connected_shells()`, `len()`, `is_empty()`.

### `TransportError`

Variants: `Io(String)`, `Serialization(String)`, `Disconnected`, `Timeout`, `FileLockFailed(String)`. Implements `std::error::Error`.

---

## How It Works

### Envelope Routing

The `TransportRouter` maintains a `HashMap<String, Box<dyn Transport>>`. When `route()` is called:

- **Direct message** (`to_shell != "*"`): Looks up the transport by shell ID and calls `send()`. Returns `Err` if no transport is registered for that shell.
- **Broadcast** (`to_shell == "*"`): Iterates all transports, sending to every connected transport except the one matching `from_shell`.

### Stdio Framing

Messages are encoded as length-prefixed frames:

```
┌──────────────┬────────────────────┐
│  u32 LE len  │   JSON payload     │
│  (4 bytes)   │   (len bytes)      │
└──────────────┴────────────────────┘
```

Reading uses `read_exact` for the 4-byte header, then `read_exact` for the payload. `UnexpectedEof` is treated as a clean stream close.

### File Transport Concurrency

On Unix, file operations use `flock(2)`:
- **Sending**: Acquires `LOCK_EX` (exclusive lock), appends a JSONL line, releases.
- **Receiving**: Acquires `LOCK_SH` (shared lock), reads from cursor position, advances cursor, releases.

The cursor is tracked in a `Mutex<u64>` so multiple `receive()` calls don't re-read the same lines.

### TTL Expiry

`Envelope::is_expired()` compares `timestamp + ttl` against the current time. Expired envelopes are **not automatically filtered** — the caller is responsible for checking. This keeps the transport layer simple and composable.

---

## The Math

### Priority Ordering

Priority is an enum with integer discriminants implementing `Ord`:

```
Background = 0 < Low = 1 < Normal = 2 < High = 3 < Critical = 4
```

When sorted ascending, higher-priority items appear last. A max-heap or reverse sort surfaces critical messages first.

### TTL Expiry Condition

A message expires when:

```
now > timestamp + ttl
```

Where `now`, `timestamp`, and `ttl` are all in milliseconds. The probability of expiry increases linearly with time — no exponential decay.

### File Cursor Arithmetic

The cursor tracks bytes read from the inbox file. After each `receive()`:

```
cursor_new = cursor_old + len(json_line) + 1  // +1 for newline
```

This ensures each line is read exactly once, even across multiple `receive()` calls.

---

## License

MIT
