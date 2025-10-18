//! # Walrus 🦭
//!
//! A high-performance Write-Ahead Log (WAL) implementation in Rust designed for concurrent
//! workloads
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
//! // Read data from the topic
//! if let Some(entry) = wal.read_next("my-topic")? {
//!     println!("Read: {:?}", String::from_utf8_lossy(&entry.data));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Configuration
//!
//! Walrus supports different consistency models:
//!
//! ```rust,no_run
//! use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};
//!
//! # fn main() -> std::io::Result<()> {
//! // Strict consistency - every read is persisted
//! let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce)?;
//!
//! // At-least-once delivery - persist every N reads
//! let wal = Walrus::with_consistency(
//!     ReadConsistency::AtLeastOnce { persist_every: 1000 }
//! )?;
//!
//! // Full configuration control
//! let wal = Walrus::with_consistency_and_schedule(
//!     ReadConsistency::AtLeastOnce { persist_every: 1000 },
//!     FsyncSchedule::Milliseconds(500)
//! )?;
//! # Ok(())
//! # }
//! ```

#![recursion_limit = "256"]
pub mod wal;
pub use wal::{Entry, FsyncSchedule, ReadConsistency, Walrus, enable_fd_backend, disable_fd_backend};
