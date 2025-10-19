<div align="center">
  <img src="./figures/walrus1.png"
       alt="walrus"
       width="30%">
    <div>Walrus: A high performance Write Ahead Log (WAL) in Rust</div>

[![Crates.io](https://img.shields.io/crates/v/walrus-rust.svg)](https://crates.io/crates/walrus-rust)
[![Documentation](https://docs.rs/walrus-rust/badge.svg)](https://docs.rs/walrus-rust)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)


</div>

## Features

- **High Performance**: Optimized for concurrent writes and reads
- **Topic-based Organization**: Separate read/write streams per topic
- **Configurable Consistency**: Choose between strict and relaxed consistency models
- **Batched I/O**: Atomic batch append and capped batch read APIs with io_uring acceleration on Linux
- **Memory-mapped I/O**: Efficient file operations using memory mapping
- **Persistent Read Offsets**: Read positions survive process restarts
- **Coordination-free Deletion**: Atomic file cleanup without blocking operations
- **Comprehensive Benchmarking**: Built-in performance testing suite

## Benchmarks

Run the supplied load tests straight from the repo:

```bash
make bench-writes    # sustained write throughput
make bench-reads     # write phase + read phase
make bench-scaling   # threads vs throughput sweep
```

Each target honours the environment variables documented in `Makefile`. Tweak
things like `FSYNC`, `THREADS`, or `WALRUS_DURATION` to explore other scenarios.

## Quick Start

Add Walrus to your `Cargo.toml`:

```toml
[dependencies]
walrus-rust = "0.1.0"
```

### Basic Usage

```rust
use walrus_rust::{Walrus, ReadConsistency};

// Create a new WAL instance with default settings
let wal = Walrus::new()?;

// Write data to a topic
let data = b"Hello, Walrus!";
wal.append_for_topic("my-topic", data)?;

// Read data from the topic
if let Some(entry) = wal.read_next("my-topic", true)? {
    println!("Read: {:?}", String::from_utf8_lossy(&entry.data));
}
```

To peek without consuming an entry, call `read_next("my-topic", false)`; the cursor only advances
when you pass `true`.

### Advanced Configuration

```rust
use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule, enable_fd_backend};

// Configure with custom consistency and fsync behavior
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::Milliseconds(500)
)?;

// Write and read operations work the same way
wal.append_for_topic("events", b"event data")?;

// Explicitly select the fd + io_uring backend (Linux; enabled by default)
enable_fd_backend();
```

## Configuration Basics

- **Read consistency**: `StrictlyAtOnce` persists every checkpoint; `AtLeastOnce { persist_every }` favours throughput and tolerates replays.
- **Fsync schedule**: choose `SyncEach`, `Milliseconds(n)`, or `NoFsync` when constructing `Walrus` to balance durability vs latency.
- **Storage backend**: `enable_fd_backend()` switches to the io_uring fast-path on Linux; `disable_fd_backend()` forces the mmap fallback.
- **Namespacing & data dir**: set `WALRUS_INSTANCE_KEY` or use the `_for_key` constructors to isolate workloads; `WALRUS_DATA_DIR` relocates the entire tree.
- **Noise control**: `WALRUS_QUIET=1` mutes debug logging from internal helpers.

Benchmark targets (`make bench-writes`, etc.) honour flags like `FSYNC`, `THREADS`, `WALRUS_DURATION`, and `WALRUS_BATCH_SIZE`, check the `Makefile` for the full list.

## API Reference

### Constructors

- `Walrus::new() -> io::Result<Self>` – StrictlyAtOnce reads, 1 s fsync cadence.
- `Walrus::with_consistency(mode: ReadConsistency) -> io::Result<Self>` – Pick the read checkpoint model.
- `Walrus::with_consistency_and_schedule(mode: ReadConsistency, schedule: FsyncSchedule) -> io::Result<Self>` – Set both read consistency and fsync policy explicitly.
- `Walrus::new_for_key(key: &str) -> io::Result<Self>` – Namespace files under `wal_files/<sanitized-key>/`.
- `Walrus::with_consistency_for_key(...)` / `with_consistency_and_schedule_for_key(...)` – Combine per-key isolation with custom consistency/fsync choices.

Set `WALRUS_INSTANCE_KEY=<key>` to make the default constructors pick the same namespace without changing call-sites.

### Topic Writes

- `append_for_topic(&self, topic: &str, data: &[u8]) -> io::Result<()>` – Appends a single payload. Topics are created lazily. Returns `ErrorKind::WouldBlock` if a batch is currently running for the topic.
- `batch_append_for_topic(&self, topic: &str, batch: &[&[u8]]) -> io::Result<()>` – Writes up to 2 000 entries (~10 GB including metadata) atomically. On Linux with the fd backend enabled the batch is submitted via io_uring; other platforms fall back to sequential writes. Failures roll back offsets and release provisional blocks.

### Topic Reads

- `read_next(&self, topic: &str, checkpoint: bool) -> io::Result<Option<Entry>>` – Returns the next entry, advancing the persisted cursor when `checkpoint` is `true`. Passing `false` lets you peek without consuming the entry.
- `batch_read_for_topic(&self, topic: &str, max_bytes: usize, checkpoint: bool) -> io::Result<Vec<Entry>>` – Streams entries in commit order until either `max_bytes` of payload or the 2 000-entry ceiling is reached (always yields at least one entry when data is available). Respects the same checkpoint semantics as `read_next`.

### Types

```rust
pub struct Entry {
    pub data: Vec<u8>,
}
```

## Further Reading

Older deep dives live under `docs/` (architecture, batch design notes, etc.) if
you need more than the basics above.

## Contributing

We welcome patches, check [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow.

## License

This project is licensed under the MIT License, see the [LICENSE](LICENSE) file for details.

## Changelog

### Version 0.1.0
- Initial release
- Core WAL functionality
- Topic-based organization
- Configurable consistency modes
- Comprehensive benchmark suite
- Memory-mapped I/O implementation
- Persistent read offset tracking

---
