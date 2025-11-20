use thiserror::Error;

#[derive(Error, Debug)]
pub enum OctopiiError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("QUIC connection error: {0}")]
    QuicConnection(#[from] quinn::ConnectionError),

    #[error("QUIC write error: {0}")]
    QuicWrite(#[from] quinn::WriteError),

    #[error("QUIC read error: {0}")]
    QuicRead(#[from] quinn::ReadError),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Node not found: {0}")]
    NodeNotFound(u64),
}

pub type Result<T> = std::result::Result<T, OctopiiError>;
