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
- **Memory-mapped I/O**: Efficient file operations using memory mapping
- **Persistent Read Offsets**: Read positions survive process restarts
- **Coordination-free Deletion**: Atomic file cleanup without blocking operations
- **Comprehensive Benchmarking**: Built-in performance testing suite

## Benchmarks

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
if let Some(entry) = wal.read_next("my-topic")? {
    println!("Read: {:?}", String::from_utf8_lossy(&entry.data));
}
```

### Advanced Configuration

```rust
use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};

// Configure with custom consistency and fsync behavior
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::Milliseconds(500)
)?;

// Write and read operations work the same way
wal.append_for_topic("events", b"event data")?;
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

### Environment Variables

- **`WALRUS_QUIET`**: Set to any value to suppress debug output during operations
- **`WALRUS_FSYNC`**: Configure fsync schedule for benchmarks (`sync-each`, `no-fsync`, `async`, `<number>ms`)
- **`WALRUS_THREADS`**: Configure thread range for scaling benchmark (`<number>` or `<start-end>`)
- **`WALRUS_DURATION`**: Configure benchmark duration for both write and read phases (`30s`, `2m`, `1h`)
- **`WALRUS_WRITE_DURATION`**: Configure write phase duration specifically (`1m`, `120s`)
- **`WALRUS_READ_DURATION`**: Configure read phase duration specifically (`2m`, `180s`)

```bash
export WALRUS_QUIET=1                    # Suppress debug messages
export WALRUS_FSYNC=sync-each            # Use sync-each fsync for benchmarks
export WALRUS_FSYNC=no-fsync             # Disable fsyncing for max performance
export WALRUS_THREADS=16                 # Test scaling up to 16 threads
export WALRUS_THREADS=2-8                # Test scaling from 2 to 8 threads
export WALRUS_DURATION=30s               # Run benchmarks for 30 seconds
export WALRUS_WRITE_DURATION=2m          # 2 minute write phase
export WALRUS_READ_DURATION=1m           # 1 minute read phase
```

## File Structure and Storage

Walrus organizes data in the following structure:

```
wal_files/
├── wal_1234567890.log          # Log files (10MB blocks, 100 blocks per file)
├── wal_1234567891.log
├── read_offset_idx_index.db    # Persistent read offset index
└── read_offset_idx_index.db.tmp # Temporary file for atomic updates
```

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

#### `append_for_topic(&self, topic: &str, data: &[u8]) -> std::io::Result<()>`
Appends data to the specified topic. Topics are created automatically on first write.

#### `read_next(&self, topic: &str) -> std::io::Result<Option<Entry>>`
Reads the next entry from the topic. Returns `None` if no more data is available.

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

#### Using Make (Recommended)

```bash
# Run individual benchmarks (default settings)
make bench-writes      # Write benchmark (async 1000ms fsync)
make bench-reads       # Read benchmark (async 1000ms fsync)  
make bench-scaling     # Scaling benchmark (1-10 threads, async 1000ms fsync)

# Run with different fsync schedules
make bench-writes-sync    # Fsync after every write (most durable, slowest)
make bench-reads-sync     # Fsync after every write
make bench-scaling-sync   # Fsync after every write

make bench-writes-fast    # 100ms fsync interval (faster)
make bench-reads-fast     # 100ms fsync interval
make bench-scaling-fast   # 100ms fsync interval

# Custom configurations
FSYNC=sync-each make bench-writes           # Custom fsync schedule
FSYNC=500ms make bench-reads               # Custom fsync interval
THREADS=16 make bench-scaling              # Test up to 16 threads
THREADS=2-8 make bench-scaling-sync        # Test 2-8 threads with sync-each
FSYNC=250ms THREADS=32 make bench-scaling  # Combined custom settings

# Show results
make show-writes       # Visualize write results
make show-reads        # Visualize read results
make show-scaling      # Visualize scaling results

# Live monitoring (run in separate terminal)
make live-writes       # Live write throughput
make live-scaling      # Live scaling progress

# Cleanup
make clean            # Remove CSV files
```

#### Using Cargo Directly

```bash
# Default benchmarks
cargo test --test multithreaded_benchmark_writes -- --nocapture
cargo test --test multithreaded_benchmark_reads -- --nocapture
cargo test --test scaling_benchmark -- --nocapture

# With environment variables
WALRUS_FSYNC=sync-each cargo test --test multithreaded_benchmark_writes -- --nocapture
WALRUS_FSYNC=500ms cargo test --test multithreaded_benchmark_reads -- --nocapture
WALRUS_THREADS=16 cargo test --test scaling_benchmark -- --nocapture
WALRUS_FSYNC=sync-each WALRUS_THREADS=2-8 cargo test --test scaling_benchmark -- --nocapture
```

### Benchmark Configuration Options

#### Fsync Schedule Configuration

Control when data is flushed to disk during benchmarks:

- **`sync-each`**: Fsync after every write (slowest, most durable)
- **`no-fsync`**: Disable fsyncing entirely (fastest, no durability)
- **`none`**: Same as `no-fsync`
- **`async`**: Async fsync every 1000ms (default)
- **`<number>ms`**: Custom millisecond intervals (e.g., `100ms`, `500ms`, `2000ms`)
- **`<number>`**: Same as above without "ms" suffix (e.g., `100`, `500`, `2000`)

#### Duration Configuration

Control how long benchmarks run:

- **`WALRUS_DURATION`**: Set both write and read phase duration (e.g., `30s`, `2m`, `1h`)
- **`WALRUS_WRITE_DURATION`**: Set write phase duration specifically
- **`WALRUS_READ_DURATION`**: Set read phase duration specifically (reads benchmark only)
- **`--duration <time>`**: Command line equivalent of `WALRUS_DURATION`
- **`--write-duration <time>`**: Command line equivalent of `WALRUS_WRITE_DURATION`
- **`--read-duration <time>`**: Command line equivalent of `WALRUS_READ_DURATION`

**Duration Format:**
- **`<number>s`**: Seconds (e.g., `30s`, `120s`)
- **`<number>m`**: Minutes (e.g., `2m`, `5m`)
- **`<number>h`**: Hours (e.g., `1h`, `2h`)
- **`<number>`**: Raw seconds (e.g., `120`, `300`)

**Defaults:**
- Write benchmark: 2 minutes
- Read benchmark: 1 minute write + 1 minute read
- Scaling benchmark: 30 seconds per thread count

#### Thread Count Configuration (Scaling Benchmark Only)

Configure how many threads to test in the scaling benchmark:

- **`<number>`**: Test from 1 to N threads (e.g., `16` means test 1-16 threads)
- **`<start-end>`**: Test specific range (e.g., `2-8`, `4-12`, `1-32`)
- **Default**: `1-10` threads
- **Maximum**: 128 threads

#### Configuration Methods

**Environment Variables (Recommended for Makefile):**
```bash
export WALRUS_FSYNC=sync-each        # Set fsync schedule
export WALRUS_FSYNC=no-fsync         # Disable fsyncing for max performance
export WALRUS_THREADS=16             # Set thread range
export WALRUS_DURATION=30s           # Set benchmark duration
export WALRUS_WRITE_DURATION=2m      # Set write phase duration
export WALRUS_READ_DURATION=1m       # Set read phase duration
```

**Makefile Parameters:**
```bash
FSYNC=sync-each make bench-writes
FSYNC=no-fsync make bench-writes
THREADS=16 make bench-scaling
FSYNC=500ms THREADS=2-8 make bench-scaling-sync
```

**Command Line Arguments:**
```bash
# Scaling benchmark with custom fsync and threads
cargo test --test scaling_benchmark -- --nocapture --fsync sync-each --threads 16

# Write benchmark with no-fsync and custom duration
cargo test --test multithreaded_benchmark_writes -- --nocapture --fsync no-fsync --duration 30s

# Read benchmark with separate write/read durations
cargo test --test multithreaded_benchmark_reads -- --nocapture --write-duration 2m --read-duration 1m
```

#### Practical Duration Examples

**Quick Performance Test (30 seconds):**
```bash
# Environment variable approach
WALRUS_DURATION=30s WALRUS_FSYNC=no-fsync cargo test --test multithreaded_benchmark_writes -- --nocapture

# Command line approach  
cargo test --test multithreaded_benchmark_writes -- --nocapture --duration 30s --fsync no-fsync
```

**Extended Durability Test (5 minutes):**
```bash
# Test sync-each performance over longer period
WALRUS_DURATION=5m WALRUS_FSYNC=sync-each cargo test --test multithreaded_benchmark_writes -- --nocapture

# Command line equivalent
cargo test --test multithreaded_benchmark_writes -- --nocapture --duration 5m --fsync sync-each
```

**Read Benchmark with Different Phase Durations:**
```bash
# Long write phase, short read phase
WALRUS_WRITE_DURATION=3m WALRUS_READ_DURATION=1m cargo test --test multithreaded_benchmark_reads -- --nocapture

# Command line equivalent
cargo test --test multithreaded_benchmark_reads -- --nocapture --write-duration 3m --read-duration 1m

# Equal phases
WALRUS_DURATION=2m cargo test --test multithreaded_benchmark_reads -- --nocapture
```

**Performance Comparison Suite:**
```bash
# Test different fsync modes with same duration
WALRUS_DURATION=1m WALRUS_FSYNC=no-fsync cargo test --test multithreaded_benchmark_writes -- --nocapture
WALRUS_DURATION=1m WALRUS_FSYNC=async cargo test --test multithreaded_benchmark_writes -- --nocapture  
WALRUS_DURATION=1m WALRUS_FSYNC=sync-each cargo test --test multithreaded_benchmark_writes -- --nocapture
```

#### Machine-Specific Recommendations

**Laptops/Small Systems:**
```bash
THREADS=4 make bench-scaling        # Test 1-4 threads
THREADS=1-6 make bench-scaling-fast # Test 1-6 threads with fast fsync
```

**Workstations:**
```bash
THREADS=16 make bench-scaling       # Test 1-16 threads
THREADS=2-12 make bench-scaling     # Skip single-thread test
```

**Servers/High-end Systems:**
```bash
THREADS=32 make bench-scaling       # Test 1-32 threads
THREADS=4-24 make bench-scaling     # Focus on multi-thread performance
```

### Benchmark Data Generation

All benchmarks use the following data generation strategy:

```rust
// Random entry size between 500B and 1KB
let size = rng.gen_range(500..=1024);
let data = vec![(counter % 256) as u8; size];
```

This creates realistic variable-sized entries with predictable content for verification.

### Dirty Pages Monitoring

All benchmarks now include real-time monitoring of system dirty pages to understand memory pressure and I/O behavior:

**What is monitored:**
- **Dirty Pages**: Memory pages that have been modified but not yet written to disk
- **Dirty Ratio**: Percentage of total system memory that is dirty
- **Platform Support**: macOS (via `vm_stat`) and Linux (via `/proc/meminfo`)

**CSV Output includes:**
- `dirty_pages_kb`: Current dirty pages in kilobytes
- `dirty_ratio_percent`: Percentage of total memory that is dirty

**Console Output example:**
```
[Monitor] 15.0s: 45230 writes/sec, 43.2 MB/sec, total: 678450 writes, dirty: 2.1% (524288 KB)
```

**Why this matters:**
- High dirty ratios indicate memory pressure and potential I/O bottlenecks
- `sync-each` mode keeps dirty pages low (immediate flushing)
- `no-fsync` mode may show higher dirty ratios (data stays in memory)
- `async` mode shows periodic spikes when background flusher runs

**Platform-specific details:**
- **macOS**: Uses `sysctl hw.memsize` and `vm_stat` to get memory info
- **Linux**: Reads `/proc/meminfo` for `MemTotal` and `Dirty` fields
- **Other OS**: Reports zeros (monitoring disabled)

### Visualization Scripts

The `scripts/` directory contains Python visualization tools:

- `visualize_throughput.py` - Write benchmark graphs
- `show_reads_graph.py` - Read benchmark graphs  
- `show_scaling_graph_writes.py` - Scaling results
- `live_scaling_plot.py` - Live scaling monitoring

Requirements: `pandas`, `matplotlib`

```bash
pip install pandas matplotlib
```

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
git clone https://github.com/your-username/walrus.git
cd walrus
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

