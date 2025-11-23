#![cfg(feature = "openraft")]

use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Application entry payload for OpenRaft.
/// Keep it simple: raw bytes of a command.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppEntry(pub Vec<u8>);

impl std::fmt::Display for AppEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppEntry({} bytes)", self.0.len())
    }
}

/// Application response for writes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppResponse(pub Vec<u8>);

/// Snapshot bytes container.
pub type AppSnapshot = Vec<u8>;

/// Node ID type.
pub type AppNodeId = u64;

/// OpenRaft type configuration for Octopii.
openraft::declare_raft_types!(
    pub AppTypeConfig:
        D = AppEntry,
        R = AppResponse,
        NodeId = AppNodeId,
);
