use crate::chunk::{ChunkSource, TransferResult};
use crate::error::Result;
use crate::transport::QuicTransport;
use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::time::Instant;

/// High-level helper for orchestrating chunk transfers between peers.
pub struct ShippingLane {
    transport: Arc<QuicTransport>,
}

impl ShippingLane {
    pub fn new(transport: Arc<QuicTransport>) -> Self {
        Self { transport }
    }

    /// Send a file to a peer, returning a `TransferResult`.
    pub async fn send_file<P: AsRef<Path>>(
        &self,
        addr: SocketAddr,
        path: P,
    ) -> Result<TransferResult> {
        let peer = self.transport.connect(addr).await?;
        let chunk = ChunkSource::File(path.as_ref().to_path_buf());
        let start = Instant::now();
        match peer.send_chunk_verified(&chunk).await {
            Ok(bytes) => Ok(TransferResult::success(addr, bytes, start.elapsed())),
            Err(err) => Ok(TransferResult::failure(addr, err.to_string())),
        }
    }

    /// Receive a file from a peer and write it to `dest`.
    pub async fn receive_file<P: AsRef<Path>>(
        &self,
        addr: SocketAddr,
        dest: P,
    ) -> Result<TransferResult> {
        let peer = self.transport.connect(addr).await?;
        let start = Instant::now();
        match peer.recv_chunk_to_path(dest).await? {
            Some(bytes) => Ok(TransferResult::success(addr, bytes, start.elapsed())),
            None => Ok(TransferResult::failure(
                addr,
                "connection closed before data transfer".to_string(),
            )),
        }
    }

    /// Send an in-memory payload to a peer.
    pub async fn send_memory(&self, addr: SocketAddr, payload: Bytes) -> Result<TransferResult> {
        let peer = self.transport.connect(addr).await?;
        let chunk = ChunkSource::Memory(payload);
        let start = Instant::now();
        match peer.send_chunk_verified(&chunk).await {
            Ok(bytes) => Ok(TransferResult::success(addr, bytes, start.elapsed())),
            Err(err) => Ok(TransferResult::failure(addr, err.to_string())),
        }
    }

    /// Receive a chunk into memory.
    pub async fn receive_memory(
        &self,
        addr: SocketAddr,
    ) -> Result<(TransferResult, Option<Bytes>)> {
        let peer = self.transport.connect(addr).await?;
        let start = Instant::now();
        match peer.recv_chunk_verified().await {
            Ok(Some(bytes)) => {
                let result = TransferResult::success(addr, bytes.len() as u64, start.elapsed());
                Ok((result, Some(bytes)))
            }
            Ok(None) => Ok((
                TransferResult::failure(addr, "connection closed before data".to_string()),
                None,
            )),
            Err(err) => Ok((TransferResult::failure(addr, err.to_string()), None)),
        }
    }
}
