#![recursion_limit = "256"]
pub mod wal;
pub use wal::{Entry, ReadConsistency, Walrus};
