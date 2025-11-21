use serde::{Deserialize, Serialize};

/// Internal data-plane operations forwarded between nodes.
#[derive(Debug, Serialize, Deserialize)]
pub enum InternalOp {
    ForwardAppend { wal_key: String, data: Vec<u8> },
    ForwardRead { wal_key: String, max_bytes: usize },
    JoinCluster { node_id: u64, addr: String },
    TestControl(TestControl),
}

/// Responses for internal operations.
#[derive(Debug, Serialize, Deserialize)]
pub enum InternalResp {
    Ok,
    ReadResult {
        data: Vec<Vec<u8>>,
        high_watermark: u64,
    },
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TestControl {
    ForceForwardReadError(bool),
    RevokeLeases { topic: String, partition: u32 },
    SyncLeases,
    TriggerJoin { node_id: u64, addr: String },
    ForceMonitorError,
    ForceDirSizeError,
    ForceGcError,
}
