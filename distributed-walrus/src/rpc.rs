use serde::{Deserialize, Serialize};

/// Internal data-plane operations forwarded between nodes.
#[derive(Debug, Serialize, Deserialize)]
pub enum InternalOp {
    ForwardAppend { wal_key: String, data: Vec<u8> },
    ForwardRead { wal_key: String, max_bytes: usize },
    JoinCluster { node_id: u64, addr: String },
}

/// Responses for internal operations.
#[derive(Debug, Serialize, Deserialize)]
pub enum InternalResp {
    Ok,
    ReadResult(Vec<Vec<u8>>),
    Error(String),
}
