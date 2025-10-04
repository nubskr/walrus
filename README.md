<div align="center">
  <img src="./figures/walrus1.png"
       alt="walrus"
       width="30%">
    <div>Walrus: A high performance Write Ahead Log (WAL) in Rust</div>

[![Crates.io](https://img.shields.io/crates/v/walrus-rust.svg)](https://crates.io/crates/walrus-rust)
[![Documentation](https://docs.rs/walrus-rust/badge.svg)](https://docs.rs/walrus-rust)
[![License](https://img.shields.io/crates/l/walrus-rust.svg)](LICENSE)


</div>

## Features

- **High Performance**: Optimized for concurrent writes and reads
- **Topic-based Organization**: Separate read/write streams per topic
- **Configurable Consistency**: Choose between strict and relaxed consistency models
- **Memory-mapped I/O**: Efficient file operations using memory mapping
- **Persistent Read Offsets**: Read positions survive process restarts
- **Coordination-free Deletion**: Atomic file cleanup without blocking operations
- **Comprehensive Benchmarking**: Built-in performance testing suite

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

### Environment Variables

- **`WALRUS_QUIET`**: Set to any value to suppress debug output during operations

```bash
export WALRUS_QUIET=1  # Suppress debug messages
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
- **Duration**: 2 minutes
- **Threads**: 10 concurrent writers
- **Data Size**: Random entries between 500B and 1KB
- **Topics**: One topic per thread (`topic_0` through `topic_9`)
- **Configuration**: `AtLeastOnce { persist_every: 50 }`
- **Output**: `benchmark_throughput.csv`

#### 2. Read Benchmark (`multithreaded_benchmark_reads`)
- **Phases**: 
  - Write Phase: 1 minute (populate data)
  - Read Phase: 2 minutes (consume data)
- **Threads**: 10 concurrent reader/writers
- **Data Size**: Random entries between 500B and 1KB
- **Configuration**: `AtLeastOnce { persist_every: 5000 }`
- **Output**: `read_benchmark_throughput.csv`

#### 3. Scaling Benchmark (`scaling_benchmark`)
- **Thread Counts**: 1 to 10 threads (tested sequentially)
- **Duration**: 30 seconds per thread count
- **Data Size**: Random entries between 500B and 1KB
- **Configuration**: `AtLeastOnce { persist_every: 50 }`
- **Output**: `scaling_results.csv` and `scaling_results_live.csv`

### Running Benchmarks

#### Using Make (Recommended)

```bash
# Run individual benchmarks
make bench-writes      # Write benchmark
make bench-reads       # Read benchmark  
make bench-scaling     # Scaling benchmark
make bench-all         # All benchmarks

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
# Write benchmark
cargo test --test multithreaded_benchmark_writes -- --nocapture

# Read benchmark  
cargo test --test multithreaded_benchmark_reads -- --nocapture

# Scaling benchmark
cargo test --test scaling_benchmark -- --nocapture
```

### Benchmark Data Generation

All benchmarks use the following data generation strategy:

```rust
// Random entry size between 500B and 1KB
let size = rng.gen_range(500..=1024);
let data = vec![(counter % 256) as u8; size];
```

This creates realistic variable-sized entries with predictable content for verification.
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

### For Maximum Throughput

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 10000 },
    FsyncSchedule::Milliseconds(5000)
)?;
```

### For Maximum Durability

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::Milliseconds(100)
)?;
```

### For Balanced Performance

```rust
let wal = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 1000 },
    FsyncSchedule::Milliseconds(1000)
)?;
```

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

