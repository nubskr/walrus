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

## Benchmark Quick Start

Run quick benchmarks with:

```bash
pip install pandas matplotlib # we need these to show graphs
make bench-and-show-reads
```

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

## Configuration Options

### Read Consistency Modes

Walrus supports two consistency models:

#### `ReadConsistency::StrictlyAtOnce`
- **Behavior**: Read offsets are persisted after every read operation
- **Guarantees**: No message will be read more than once, even after crashes
- **Performance**: Higher I/O overhead due to frequent persistence
- **Use Case**: Critical systems where duplicate processing must be avoided

```rust
let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce)?;
```

#### `ReadConsistency::AtLeastOnce { persist_every: u32 }`
- **Behavior**: Read offsets are persisted every N read operations
- **Guarantees**: Messages may be re-read after crashes (at-least-once delivery)
- **Performance**: Better throughput with configurable persistence frequency
- **Use Case**: High-throughput systems that can handle duplicate processing

```rust
let wal = Walrus::with_consistency(
    ReadConsistency::AtLeastOnce { persist_every: 5000 }
)?;
```

### Fsync Scheduling

Control when data is flushed to disk:

#### `FsyncSchedule::Milliseconds(u64)`
- **Behavior**: Background thread flushes data every N milliseconds
- **Default**: 1000ms (1 second)
- **Range**: Minimum 1ms, recommended 100-5000ms

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::Milliseconds(2000)  // Flush every 2 seconds
)?;
```

#### `FsyncSchedule::SyncEach`
- **Behavior**: Fsync immediately after every write operation
- **Guarantees**: Maximum durability - data is guaranteed on disk before write returns
- **Performance**: Slowest option due to synchronous disk operations
- **Use Case**: Critical systems requiring immediate persistence

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::SyncEach  // Fsync after every write
)?;
```

#### `FsyncSchedule::NoFsync`
- **Behavior**: Disable fsyncing entirely - data stays in OS buffers
- **Guarantees**: No durability guarantees - data may be lost on crash/power failure
- **Performance**: Fastest option - maximum throughput with no disk sync overhead
- **Use Case**: Performance testing, non-critical data, or when external durability is handled

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::NoFsync  // No fsyncing - maximum performance
)?;
```

### I/O Backend Selection

Walrus ships with two storage backends:

- **FD backend (default on Linux)**: Uses file descriptors for I/O and enables
  the `io_uring` fast-path for batch appends and reads.
- **Mmap backend**: Falls back to memory-mapped I/O. Batch append and read
  helpers execute sequentially without `io_uring`.

Switching between backends is a process-wide toggle:

```rust
use walrus_rust::{enable_fd_backend, disable_fd_backend};

disable_fd_backend(); // choose mmap backend explicitly
// ... work with Walrus::new() instances ...
enable_fd_backend();  // revert to fd + io_uring backend
```

You can also set `WALRUS_QUIET=1` to silence debug output when flipping
backends.

### Environment Variables

- **`WALRUS_QUIET`**: Set to any value to suppress debug output during operations
- **`WALRUS_DATA_DIR`**: Override the default `wal_files/` directory used for log and index storage
- **`WALRUS_INSTANCE_KEY`**: Force all instances created via default constructors to namespace their files under `wal_files/<sanitized-key>/`
- **`WALRUS_FSYNC`**: Configure fsync schedule for benchmarks (`sync-each`, `no-fsync`, `async`, `<number>ms`)
- **`WALRUS_THREADS`**: Configure thread range for scaling benchmark (`<number>` or `<start-end>`)
- **`WALRUS_DURATION`**: Configure benchmark duration for both write and read phases (`30s`, `2m`, `1h`)
- **`WALRUS_WRITE_DURATION`**: Configure write phase duration specifically (`1m`, `120s`)
- **`WALRUS_READ_DURATION`**: Configure read phase duration specifically (`2m`, `180s`)
- **`WALRUS_BATCH_SIZE`**: Override entries per batch for batch benchmarks (default 2000 for `multithreaded_benchmark_batch`, 256 for `batch_scaling_benchmark`)

## File Structure and Storage

Walrus organizes data in the following structure:

```
wal_files/
├── 1700000000                  # Default instance log file (10MB blocks, 100 per file)
├── read_offset_idx_index.db    # Default instance read-offset index
├── analytics/                  # Keyed instance for "analytics"
│   ├── 1700000100
│   └── read_offset_idx_index.db
└── transactions/               # Keyed instance for "transactions"
    ├── 1700000200
    └── read_offset_idx_index.db
```

Each `Walrus` instance writes under `wal_files/`. Instances created with the new
key-aware constructors store their files inside a sanitized subdirectory named
after the key, keeping topics with different durability requirements isolated.
Set `WALRUS_DATA_DIR=/path/to/data` before constructing an instance to relocate
the entire tree elsewhere.

### Storage Configuration

- **Block Size**: 10MB per block (configurable via `DEFAULT_BLOCK_SIZE`)
- **Blocks Per File**: 100 blocks per file (1GB total per file)
- **Max File Size**: 1GB per log file
- **Index Persistence**: Read offsets stored in separate index files

## API Reference

### Core Methods

#### `Walrus::new() -> std::io::Result<Self>`
Creates a new WAL instance with default settings (`StrictlyAtOnce` consistency).

#### `Walrus::with_consistency(mode: ReadConsistency) -> std::io::Result<Self>`
Creates a WAL with custom consistency mode and default fsync schedule (1000ms).

#### `Walrus::with_consistency_and_schedule(mode: ReadConsistency, schedule: FsyncSchedule) -> std::io::Result<Self>`
Creates a WAL with full configuration control.

#### `Walrus::new_for_key(key: &str) -> std::io::Result<Self>`
Creates a new WAL instance that is namespaced under `wal_files/<sanitized-key>/`,
allowing multiple configurations to coexist without sharing log or index files.

#### `Walrus::with_consistency_for_key(key: &str, mode: ReadConsistency) -> std::io::Result<Self>`
Same as `with_consistency`, but stores all files inside the directory that
corresponds to the provided key.

#### `Walrus::with_consistency_and_schedule_for_key(key: &str, mode: ReadConsistency, schedule: FsyncSchedule) -> std::io::Result<Self>`
Full configuration control plus per-key isolation, ideal when different topics
need separate fsync guarantees or tuning.

You can also set the environment variable `WALRUS_INSTANCE_KEY` to force the
default constructors (`Walrus::new`, `Walrus::with_consistency`, etc.) to use a
sanitized subdirectory under `wal_files/` that matches the supplied key. This is
handy when you want multiple independent WAL instances in the same process
without switching APIs.

#### `append_for_topic(&self, topic: &str, data: &[u8]) -> std::io::Result<()>`
Appends data to the specified topic. Topics are created automatically on first write.
If a batch write is currently in-flight for the topic the call fails fast with
`ErrorKind::WouldBlock`; retry once the batch completes.

#### `read_next(&self, topic: &str, checkpoint: bool) -> std::io::Result<Option<Entry>>`
Reads the next entry from the topic. Pass `checkpoint = true` to advance the cursor (persisting
progress according to the configured consistency mode). Passing `false` returns the entry without
moving the cursor so the same data can be re-read later. Returns `None` if no more data is available.

### Batch Operations

#### `batch_append_for_topic(&self, topic: &str, batch: &[&[u8]]) -> std::io::Result<()>`
- Writes up to **2,000** entries atomically (shared with batch reads). Batches larger than this limit or exceeding 10 GB (payload + metadata) return `ErrorKind::InvalidInput`.
- Acquires a per-topic batch flag; concurrent single writes/batches receive `ErrorKind::WouldBlock` while a batch is active.
- Uses `io_uring` on Linux when the fd backend is enabled (default); otherwise it falls back to sequential writes through the mmap backend.
- Any failure (write error, fsync failure, checksum mismatch) rolls back the batch before propagating the error.

```rust
let wal = Walrus::new()?;
let batch: Vec<Vec<u8>> = (0..2000).map(|i| format!("msg-{i}").into_bytes()).collect();
let batch_refs: Vec<&[u8]> = batch.iter().map(|m| m.as_slice()).collect();
wal.batch_append_for_topic("events", &batch_refs)?;
```

#### `batch_read_for_topic(&self, topic: &str, max_bytes: usize, checkpoint: bool) -> std::io::Result<Vec<Entry>>`
- Returns entries in commit order up to `max_bytes` of payload **or** the 2,000-entry ceiling, whichever comes first (the first entry is always returned, even if it exceeds `max_bytes`).
- On Linux with the fd backend enabled the reader issues one `io_uring` read per contiguous range; other configurations fall back to mmap reads.
- When `checkpoint = true`, the reader advances its cursor (respecting the configured persistence cadence). Passing `false` leaves the cursor unchanged so callers can peek without consuming data.
- Subsequent invocations continue from the last checkpointed cursor so larger batches are streamed across multiple calls.

```rust
let chunk = wal.batch_read_for_topic("events", 512 * 1024, true)?; // ~512KB budget
println!("read {} entries", chunk.len());
```

### Data Types

#### `Entry`
```rust
pub struct Entry {
    pub data: Vec<u8>,
}
```

## Benchmarks

Walrus includes a comprehensive benchmarking suite to measure performance across different scenarios.

### Available Benchmarks

#### 1. Write Benchmark (`multithreaded_benchmark_writes`)
- **Duration**: 2 minutes (configurable via `WALRUS_DURATION`)
- **Threads**: 10 concurrent writers
- **Data Size**: Random entries between 500B and 1KB
- **Topics**: One topic per thread (`topic_0` through `topic_9`)
- **Configuration**: `AtLeastOnce { persist_every: 50 }`
- **Monitoring**: Real-time throughput, dirty pages ratio, memory usage
- **Output**: `benchmark_throughput.csv` (includes dirty pages data)

#### 2. Read Benchmark (`multithreaded_benchmark_reads`)
- **Phases**: 
  - Write Phase: 1 minute (configurable via `WALRUS_WRITE_DURATION`)
  - Read Phase: 2 minutes (configurable via `WALRUS_READ_DURATION`)
- **Threads**: 10 concurrent reader/writers
- **Data Size**: Random entries between 500B and 1KB
- **Configuration**: `AtLeastOnce { persist_every: 5000 }`
- **Monitoring**: Real-time throughput for both phases, dirty pages ratio, memory usage
- **Output**: `read_benchmark_throughput.csv` (includes dirty pages data)

#### 3. Scaling Benchmark (`scaling_benchmark`)
- **Thread Counts**: 1 to 10 threads by default (configurable via `WALRUS_THREADS`)
- **Duration**: 30 seconds per thread count
- **Data Size**: Random entries between 500B and 1KB
- **Configuration**: `AtLeastOnce { persist_every: 50 }`
- **Fsync Schedule**: Configurable via `WALRUS_FSYNC` (default: async 1000ms)
- **Output**: `scaling_results.csv` and `scaling_results_live.csv`

### Running Benchmarks

- `make bench-writes`, `make bench-reads`, and `make bench-scaling` cover the main scenarios; tweak them with env vars such as `FSYNC`, `THREADS`, `WALRUS_DURATION`, and `BATCH` to explore different settings.
- Run `cargo test --test <bench> -- --nocapture` for ad-hoc runs (e.g. `multithreaded_benchmark_writes`, `multithreaded_benchmark_reads`, `scaling_benchmark`); append the same env vars or pass CLI flags like `-- --batch-size 512`.
- Plot CSV output with `make show-writes`, `make show-reads`, or `make show-scaling`; `scripts/` contains the underlying Python helpers that require `pandas` and `matplotlib`.
- Remove generated artefacts with `make clean`.

### Benchmark Configuration Options

#### Fsync Schedule Configuration

`WALRUS_FSYNC`/`FSYNC` accepts `sync-each`, `no-fsync` (`none`), `async` (1000 ms cadence), or a custom millisecond interval such as `100ms`/`500`.

#### Duration Configuration

Set overall runtime with `WALRUS_DURATION` (or `--duration`). For asymmetric phases use `WALRUS_WRITE_DURATION` / `WALRUS_READ_DURATION` and their CLI equivalents. Durations accept `s`, `m`, `h`, or raw seconds; defaults are 2 min for writes, 1 min write + 1 min read, and 30 s per scaling step.

#### Thread Count Configuration (Scaling Benchmark Only)

`WALRUS_THREADS` (or `THREADS` for Make) accepts either a single number—testing `1..=N`—or a `start-end` range; defaults to `1-10` and caps at 128.

#### Batch Size Configuration (Batch Benchmarks)

Set `WALRUS_BATCH_SIZE`/`BATCH` to control entries per batch (2000 for `multithreaded_benchmark_batch`, 256 for `batch_scaling_benchmark` by default); cargo users can also pass `-- --batch-size <entries>`.

#### Configuration Methods

- Environment variables pair naturally with the Make targets, e.g. `FSYNC=sync-each THREADS=16 make bench-scaling`.
- When using cargo directly, append the same switches after `--`, for example `cargo test --test scaling_benchmark -- --nocapture --fsync sync-each --threads 16`.

#### Practical Duration Examples

- Use short runs (`WALRUS_DURATION=30s`) to sanity-check performance.
- Run for minutes (`WALRUS_DURATION=5m`) when evaluating durability modes like `sync-each`.
- Mix and match `WALRUS_WRITE_DURATION` / `WALRUS_READ_DURATION` to emphasise a particular phase.

#### Machine-Specific Tips

- **Laptops/small systems**: keep `THREADS` in the 1–6 range.
- **Workstations**: 1–16 threads covers most desktop workloads.
- **Servers**: push to 32+ threads to stress the pipeline.

### Benchmark Data Generation

Benchmarks emit pseudo-random payloads between 500 B and 1 KB using deterministic byte patterns so verification stays cheap.

### Dirty Pages Monitoring

Benchmarks log dirty-page counts and ratios to highlight memory pressure, pulling metrics from `vm_stat` on macOS and `/proc/meminfo` on Linux. Expect `sync-each` runs to stay low, `no-fsync` to spike, and `async` to oscillate as the flusher catches up.

### Visualization Scripts

`scripts/` contains plotting helpers (`visualize_throughput.py`, `show_reads_graph.py`, `show_scaling_graph_writes.py`, `live_scaling_plot.py`, `compare_walrus_rocksdb.py`); install `pandas` and `matplotlib` to use them.

## Architecture

### Key Design Principles

1. **Coordination-free Operations**: Writers don't block readers, minimal locking
2. **Memory-mapped I/O**: Efficient file operations with OS-level optimizations
3. **Topic Isolation**: Each topic maintains independent read/write positions
4. **Persistent State**: Read offsets survive process restarts
5. **Background Maintenance**: Async fsync and cleanup operations

### Read Offset Persistence

**Important**: Read offsets are decoupled from write offsets. This means:

- Each topic maintains its own read position independently
- Read positions are persisted to disk based on consistency configuration
- After restart, readers continue from their last persisted position
- Write operations don't affect existing read positions

This design enables multiple readers per topic and supports replay scenarios.

## Performance Tuning

### For Maximum Throughput (No Durability)

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 10000 },
    FsyncSchedule::NoFsync  // No fsyncing - fastest possible
)?;
```

### For High Throughput (Some Durability)

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 10000 },
    FsyncSchedule::Milliseconds(5000)  // Infrequent fsyncing
)?;
```

### For Maximum Durability

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::SyncEach  // Fsync after every write
)?;
```

### For Balanced Performance

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::Milliseconds(1000)  // Regular fsyncing
)?;
```

### Performance vs Durability Trade-offs

| Configuration | Throughput | Durability | Use Case |
|---------------|------------|------------|----------|
| `NoFsync` | Highest | None | Performance testing, non-critical data |
| `Milliseconds(5000)` | High | Low | High-throughput logging |
| `Milliseconds(1000)` | Medium | Medium | Balanced production use |
| `Milliseconds(100)` | Lower | High | Near real-time requirements |
| `SyncEach` | Lowest | Highest | Critical financial/safety systems |

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone <repo-url>
cd walrus-unstable
cargo build
cargo test
```

### Running Tests

```bash
# Unit tests
cargo test --test unit

# Integration tests  
cargo test --test integration

# End-to-end tests
cargo test --test e2e_longrunning

# All tests
cargo test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

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
