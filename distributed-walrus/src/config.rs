use clap::Parser;
use std::path::PathBuf;

/// Command-line configuration for a single node instance.
#[derive(Parser, Debug, Clone)]
pub struct NodeConfig {
    /// Unique ID for this node.
    #[arg(long, default_value = "1")]
    pub node_id: u64,

    /// Root directory for all on-disk storage.
    #[arg(long = "data-dir", default_value = "./data")]
    pub data_root: PathBuf,

    /// Address of a peer to join (e.g., "127.0.0.1:5001").
    #[arg(long = "join")]
    pub join_addr: Option<String>,

    /// List of initial peer addresses for the bootstrapping leader (e.g., "127.0.0.1:5001").
    #[arg(long = "initial-peer")]
    pub initial_peers: Vec<String>,

    /// Port for the Kafka facade listener.
    #[arg(long = "port", default_value = "9092")]
    pub kafka_port: u16,

    /// Port for the Raft/Internal RPC listener.
    #[arg(long = "raft-port", default_value = "6000")]
    pub raft_port: u16,

    /// Optional file to write logs to. If not specified, logs go to stdout.
    #[arg(long = "log-file")]
    pub log_file: Option<PathBuf>,
}

impl NodeConfig {
    /// Directory for user data (bucket Walrus files).
    pub fn data_wal_dir(&self) -> PathBuf {
        self.data_root
            .join(format!("node_{}", self.node_id))
            .join("user_data")
    }

    /// Directory for future control-plane (Raft) metadata.
    pub fn meta_wal_dir(&self) -> PathBuf {
        self.data_root
            .join(format!("node_{}", self.node_id))
            .join("raft_meta")
    }
}
