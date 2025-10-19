//! # Walrus 🦭
//!
//! A high-performance Write-Ahead Log (WAL) implementation in Rust designed for concurrent
//! workloads with topic-based organization, configurable consistency models, and dual storage backends.
//!
//! ## Features
//!
//! - **High Performance**: Optimized for concurrent writes and reads
//! - **Topic-based Organization**: Separate read/write streams per topic
//! - **Configurable Consistency**: Choose between strict and relaxed consistency models
//! - **Batched I/O**: Atomic batch append and read APIs (uses io_uring on Linux with FD backend)
//! - **Dual Storage Backends**: FD backend with pread/pwrite (default) or mmap backend
//! - **Persistent Read Offsets**: Read positions survive process restarts
//! - **Namespace Isolation**: Separate WAL instances with per-key directories
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use walrus_rust::{Walrus, ReadConsistency};
//!
//! # fn main() -> std::io::Result<()> {
//! // Create a new WAL instance
//! let wal = Walrus::new()?;
//!
//! // Write data to a topic
//! wal.append_for_topic("my-topic", b"Hello, Walrus!")?;
//!
//! // Read data from the topic (checkpoint=true consumes the entry)
//! if let Some(entry) = wal.read_next("my-topic", true)? {
//!     println!("Read: {:?}", String::from_utf8_lossy(&entry.data));
//! }
//!
//! // Peek at the next entry without consuming it (checkpoint=false)
//! if let Some(entry) = wal.read_next("my-topic", false)? {
//!     println!("Peeking: {:?}", String::from_utf8_lossy(&entry.data));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Operations
//!
//! Walrus supports efficient batch writes and reads. On Linux with the FD backend (default),
//! batch operations automatically use io_uring for parallel I/O submission. On other platforms
//! or with the mmap backend, batches fall back to sequential operations.
//!
//! **Limits:**
//! - Maximum 2,000 entries per batch
//! - Maximum ~10GB total payload per batch
//!
//! ```rust,no_run
//! use walrus_rust::Walrus;
//!
//! # fn main() -> std::io::Result<()> {
//! let wal = Walrus::new()?;
//!
//! // Atomic batch write (all-or-nothing)
//! let batch = vec![
//!     b"entry 1".as_slice(),
//!     b"entry 2".as_slice(),
//!     b"entry 3".as_slice(),
//! ];
//! wal.batch_append_for_topic("events", &batch)?;
//!
//! // Batch read with byte limit (returns at least 1 entry if available)
//! let max_bytes = 1024 * 1024; // 1MB
//! let entries = wal.batch_read_for_topic("events", max_bytes, true)?;
//! for entry in entries {
//!     println!("Read: {} bytes", entry.data.len());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Consistency Models
//!
//! Control the trade-off between durability and performance:
//!
//! ```rust,no_run
//! use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};
//!
//! # fn main() -> std::io::Result<()> {
//! // Strict consistency - every read checkpoint is persisted immediately
//! let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce)?;
//!
//! // At-least-once delivery - persist every N reads (higher throughput)
//! // This allows replaying up to N entries after a crash
//! let wal = Walrus::with_consistency(
//!     ReadConsistency::AtLeastOnce { persist_every: 1000 }
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Fsync Scheduling
//!
//! Configure when data is flushed to disk:
//!
//! ```rust,no_run
//! use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};
//!
//! # fn main() -> std::io::Result<()> {
//! // Fsync every 500ms (default is 200ms)
//! let wal = Walrus::with_consistency_and_schedule(
//!     ReadConsistency::StrictlyAtOnce,
//!     FsyncSchedule::Milliseconds(500)
//! )?;
//!
//! // Fsync after every single write (maximum durability, lower throughput)
//! let wal = Walrus::with_consistency_and_schedule(
//!     ReadConsistency::StrictlyAtOnce,
//!     FsyncSchedule::SyncEach
//! )?;
//!
//! // Never fsync (maximum throughput, no durability guarantees)
//! let wal = Walrus::with_consistency_and_schedule(
//!     ReadConsistency::StrictlyAtOnce,
//!     FsyncSchedule::NoFsync
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Namespace Isolation
//!
//! Create isolated WAL instances with separate storage directories:
//!
//! ```rust,no_run
//! use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};
//!
//! # fn main() -> std::io::Result<()> {
//! // Create a namespaced WAL (stored in wal_files/<sanitized-key>/)
//! let wal1 = Walrus::new_for_key("tenant-123")?;
//! let wal2 = Walrus::new_for_key("tenant-456")?;
//!
//! // With custom consistency
//! let wal = Walrus::with_consistency_for_key(
//!     "my-app",
//!     ReadConsistency::AtLeastOnce { persist_every: 100 }
//! )?;
//!
//! // With full configuration
//! let wal = Walrus::with_consistency_and_schedule_for_key(
//!     "my-app",
//!     ReadConsistency::AtLeastOnce { persist_every: 1000 },
//!     FsyncSchedule::Milliseconds(500)
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Storage Backends
//!
//! Walrus supports two storage backends that can be selected at runtime:
//!
//! ### FD Backend (File Descriptor) - Default
//!
//! Uses file descriptors with `pread`/`pwrite` syscalls for I/O operations. This is the
//! default backend and requires Unix-specific APIs.
//!
//! **Batch Operations on Linux:**
//! - When running on Linux, batch operations (`batch_append_for_topic` and `batch_read_for_topic`)
//!   automatically use io_uring for high-performance parallel I/O submission
//! - Regular single-entry operations use standard `pread`/`pwrite` syscalls
//!
//! **O_SYNC Mode:**
//! - When `FsyncSchedule::SyncEach` is configured, files are opened with the `O_SYNC` flag,
//!   making every write synchronous
//!
//! - **Works on**: Unix systems (Linux, macOS, BSD)
//! - **Best for**: Batch operations on Linux (io_uring), general-purpose workloads
//! - **Default**: Enabled
//!
//! ### Mmap Backend (Memory-Mapped Files)
//!
//! Uses memory-mapped files for direct memory access. Batch operations fall back to
//! sequential reads/writes without io_uring acceleration.
//!
//! - **Works on**: All platforms
//! - **Best for**: Windows, or when FD backend is incompatible
//! - **Default**: Disabled (use `disable_fd_backend()` to enable)
//!
//! ### Selecting a Backend
//!
//! ```rust,no_run
//! use walrus_rust::{enable_fd_backend, disable_fd_backend};
//!
//! // Use FD backend (default - uses io_uring for batches on Linux)
//! enable_fd_backend();
//!
//! // Use mmap backend (no io_uring, sequential batch operations)
//! disable_fd_backend();
//! ```
//!
//! **Important**: Backend selection must be done before creating any `Walrus` instances.
//!
//! ## Environment Variables
//!
//! - `WALRUS_DATA_DIR`: Change storage location (default: `./wal_files`)
//! - `WALRUS_INSTANCE_KEY`: Default namespace for all instances
//! - `WALRUS_QUIET=1`: Suppress debug output
//!
//! ```bash
//! # Example: Use a custom data directory
//! export WALRUS_DATA_DIR=/var/lib/myapp/wal
//!
//! # Example: Set default namespace
//! export WALRUS_INSTANCE_KEY=production
//!
//! # Example: Quiet mode
//! export WALRUS_QUIET=1
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Block size**: 10MB per block
//! - **Batch limits**: Up to 2,000 entries or ~10GB payload per batch
//! - **Default fsync interval**: 200ms
//! - **File organization**: 100 blocks per file (1GB files)
//!
//! ## Types
//!
//! ```rust
//! # use walrus_rust::Entry;
//! // Entry returned by read operations
//! pub struct Entry {
//!     pub data: Vec<u8>,
//! }
//! ```
//!
//! ## API Reference
//!
//! ### Constructors
//!
//! - [`Walrus::new()`]: Default constructor (StrictlyAtOnce, 200ms fsync)
//! - [`Walrus::with_consistency()`]: Set read consistency model
//! - [`Walrus::with_consistency_and_schedule()`]: Set consistency and fsync policy
//! - [`Walrus::new_for_key()`]: Create namespaced instance
//! - [`Walrus::with_consistency_for_key()`]: Namespaced with consistency
//! - [`Walrus::with_consistency_and_schedule_for_key()`]: Full namespaced configuration
//!
//! ### Write Operations
//!
//! - [`Walrus::append_for_topic()`]: Append single entry to topic
//! - [`Walrus::batch_append_for_topic()`]: Atomic batch write (up to 2,000 entries)
//!
//! ### Read Operations
//!
//! - [`Walrus::read_next()`]: Read next entry (checkpoint=true consumes, false peeks)
//! - [`Walrus::batch_read_for_topic()`]: Read multiple entries up to byte limit

#![recursion_limit = "256"]
pub mod wal;
pub use wal::{
    Entry, FsyncSchedule, ReadConsistency, WalIndex, Walrus, disable_fd_backend, enable_fd_backend,
};
