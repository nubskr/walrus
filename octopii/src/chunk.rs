use bytes::Bytes;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Source of chunk data for transfer
#[derive(Debug, Clone)]
pub enum ChunkSource {
    /// Read chunk from a file on disk
    File(PathBuf),
    /// Chunk data already in memory
    Memory(Bytes),
}

/// Result of transferring a chunk to a single peer
#[derive(Debug, Clone)]
pub struct TransferResult {
    /// Address of the peer
    pub peer: SocketAddr,
    /// Whether the transfer succeeded
    pub success: bool,
    /// Number of bytes transferred
    pub bytes_transferred: u64,
    /// Whether checksum was verified by peer
    pub checksum_verified: bool,
    /// How long the transfer took
    pub duration: Duration,
    /// Error message if transfer failed
    pub error: Option<String>,
}

impl TransferResult {
    /// Create a successful transfer result
    pub fn success(peer: SocketAddr, bytes: u64, duration: Duration) -> Self {
        Self {
            peer,
            success: true,
            bytes_transferred: bytes,
            checksum_verified: true,
            duration,
            error: None,
        }
    }

    /// Create a failed transfer result
    pub fn failure(peer: SocketAddr, error: String) -> Self {
        Self {
            peer,
            success: false,
            bytes_transferred: 0,
            checksum_verified: false,
            duration: Duration::ZERO,
            error: Some(error),
        }
    }
}
