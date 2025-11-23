# Shipping Lane: P2P File Transfer in Octopii

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [The Chunk Transfer Protocol](#the-chunk-transfer-protocol)
4. [Building Custom Protocols](#building-custom-protocols)
5. [Transport Layer API](#transport-layer-api)
6. [Internal Implementation](#internal-implementation)
7. [Performance Optimization](#performance-optimization)
8. [Security Considerations](#security-considerations)
9. [Complete Examples](#complete-examples)
10. [Gotchas and Best Practices](#gotchas-and-best-practices)

---

## Overview

The **Shipping Lane** is Octopii's P2P file transfer system, designed for transferring data that's too large for the RPC layer. It provides:

- **Direct peer-to-peer transfer**: No routing through Raft leader
- **Checksum verification**: SHA-256 integrity checks
- **Streaming**: Efficient memory usage for large files (see note below)
- **QUIC-based**: Fast, reliable, multiplexed transport
- **Composable**: Build custom protocols combining RPC + bulk transfer

> **Streaming note:** `PeerConnection::recv_chunk_verified()` currently buffers
> the entire payload in memory. Use the disk-streaming helper
> `recv_chunk_to_path()` (documented in the API section) for truly constant
> memory usage when pulling multi-GB snapshots or files.

### When to Use Shipping Lane

```
┌────────────────────────────────────────────────────────────┐
│                    Data Size Guide                         │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  0 - 1 KB        →  RPC (optimal)                         │
│  1 KB - 64 KB    →  RPC (acceptable)                      │
│  64 KB - 1 MB    →  RPC or Shipping Lane                  │
│  1 MB - 100 MB   →  Shipping Lane (recommended)           │
│  100 MB+         →  Shipping Lane (required)              │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

**Use RPC for:**
- Small commands/responses
- Request/response patterns
- Control messages
- Metadata

**Use Shipping Lane for:**
- Large files
- Snapshots
- Media content
- Database dumps
- Log archives

---

## Architecture

### High-Level Design

```
┌────────────────────────────────────────────────────────────┐
│                      Application                           │
│                                                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │      Custom Protocol Logic                       │     │
│  │                                                   │     │
│  │  1. RPC: Negotiate transfer                      │     │
│  │  2. Shipping Lane: Send bulk data                │     │
│  │  3. RPC: Confirm completion                      │     │
│  └────────┬────────────────────────┬──────────────┘     │
└───────────┼────────────────────────┼────────────────────┘
            │                        │
            ▼                        ▼
┌────────────────────┐    ┌────────────────────┐
│    RpcHandler      │    │  PeerConnection    │
│                    │    │  (Chunk Transfer)  │
│  • Request/Response│    │  • send_chunk_     │
│  • Timeouts        │    │    verified()      │
│  • Correlation     │    │  • recv_chunk_     │
└─────────┬──────────┘    │    verified()      │
          │               └──────────┬─────────┘
          │                          │
          └───────────┬──────────────┘
                      │
                      ▼
          ┌────────────────────┐
          │  QuicTransport     │
          │  (Connection Pool) │
          └──────────┬─────────┘
                     │
                     ▼
          ┌────────────────────┐
          │   QUIC Protocol    │
          │   (UDP + TLS)      │
          └────────────────────┘
```

### Protocol Layers

```
┌────────────────────────────────────────────────────────────┐
│ Layer 5: Application Protocol                              │
│          (Your custom logic: negotiate, transfer, confirm) │
├────────────────────────────────────────────────────────────┤
│ Layer 4: Chunk Protocol                                    │
│          (Framing, checksum, ACK)                          │
├────────────────────────────────────────────────────────────┤
│ Layer 3: RPC Framework                                     │
│          (Request/response correlation, routing)           │
├────────────────────────────────────────────────────────────┤
│ Layer 2: QUIC Transport                                    │
│          (Streams, flow control, reliability)              │
├────────────────────────────────────────────────────────────┤
│ Layer 1: UDP + TLS                                         │
│          (Network, encryption)                             │
└────────────────────────────────────────────────────────────┘
```

---

## The Chunk Transfer Protocol

### Protocol Specification

#### Send Path

```
Sender                                          Receiver
  │                                                │
  ├─────── Open bidirectional QUIC stream ───────►│
  │                                                │
  ├─────── [8 bytes: chunk size] ────────────────►│
  │                                                │
  ├─────── [N bytes: chunk data] ────────────────►│
  │         (streamed in 64KB buffers)            │
  │                                                │
  ├─────── [32 bytes: SHA-256 checksum] ─────────►│
  │                                                │
  │         Receiver computes checksum             │
  │         and verifies                           │
  │                                                │
  │◄─────── [1 byte: status] ─────────────────────┤
  │         0 = OK                                 │
  │         1 = checksum failed                    │
  │         2 = error                              │
  │                                                │
  └─────── Close stream ─────────────────────────►│
```

#### Wire Format

```
┌─────────────────────────────────────────────────────────┐
│                    Chunk Transfer                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Size (8 bytes, little-endian u64)               │  │
│  │  Value: total data bytes                         │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Data (N bytes)                                  │  │
│  │  Streamed in 64KB buffers                       │  │
│  │  (no intermediate framing)                       │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Checksum (32 bytes)                             │  │
│  │  SHA-256 hash of data                            │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ACK (1 byte)                                    │  │
│  │  0 = success, 1 = checksum_fail, 2 = error      │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### API Reference

**Location:** `src/transport/peer.rs`

```rust
impl PeerConnection {
    /// Send a chunk with checksum verification
    ///
    /// Returns the number of bytes transferred
    pub async fn send_chunk_verified(
        &self,
        chunk: &ChunkSource
    ) -> Result<u64>

    /// Receive a chunk with checksum verification
    ///
    /// Returns the received chunk data
    pub async fn recv_chunk_verified(
        &self
    ) -> Result<Option<Bytes>>
}

pub enum ChunkSource {
    /// Read from a file on disk
    File(PathBuf),

    /// Data already in memory
    Memory(Bytes),
}
```

### Example Usage

#### Basic Transfer

```rust
use octopii::transport::{QuicTransport, ChunkSource};
use bytes::Bytes;

// Sender
async fn send_file(transport: &QuicTransport, peer_addr: SocketAddr) -> Result<()> {
    let peer = transport.connect(peer_addr).await?;

    // Option 1: Send from memory
    let data = Bytes::from(vec![0u8; 10_000_000]); // 10MB
    let chunk = ChunkSource::Memory(data);
    let bytes_sent = peer.send_chunk_verified(&chunk).await?;
    println!("Sent {} bytes", bytes_sent);

    // Option 2: Send from file
    let chunk = ChunkSource::File(PathBuf::from("large_file.dat"));
    let bytes_sent = peer.send_chunk_verified(&chunk).await?;
    println!("Sent {} bytes from file", bytes_sent);

    Ok(())
}

// Receiver
async fn receive_file(transport: &QuicTransport) -> Result<()> {
    let (peer_addr, peer) = transport.accept().await?;
    println!("Connection from {}", peer_addr);

match peer.recv_chunk_verified().await? {
    Some(data) => {
        println!("Received {} bytes", data.len());
        // Process data...
        Ok(())
        }
        None => {
            println!("Connection closed");
            Ok(())
        }
    }
}
```

#### Streaming Directly to Disk

```rust
/// Receive a chunk and write it to the provided path.
pub async fn recv_chunk_to_path<P: AsRef<Path>>(
    &self,
    path: P,
) -> Result<Option<u64>>;
```

`recv_chunk_to_path()` streams the payload into the destination file while
computing the checksum, keeping memory usage bounded by the internal 64KB
buffer. Prefer this API for large files or snapshots:

```rust
let Some(bytes_written) = peer.recv_chunk_to_path("/tmp/snapshot.bin").await? else {
    anyhow::bail!("connection closed before data arrived");
};
println!("Wrote {} bytes to disk", bytes_written);
```

---

## Building Custom Protocols

### Pattern 1: Request-Transfer-Confirm

**Use Case:** Client requests a file, server sends it

```
Client                                    Server
  │                                          │
  ├───── RPC: "GET /file/abc.mp4" ─────────►│
  │                                          │
  │                                          │ Prepare file
  │                                          │
  │◄─── RPC Response: "OK, ready, 100MB" ───┤
  │                                          │
  │                                          │
  │◄───── Chunk Transfer: file data ────────┤
  │         (via Shipping Lane)              │
  │                                          │
  ├───── RPC: "CONFIRM, checksum=..." ──────►│
  │                                          │
  │◄─── RPC Response: "COMPLETE" ────────────┤
```

**Implementation:**

```rust
use octopii::{OctopiiNode, transport::{QuicTransport, ChunkSource}};
use bytes::Bytes;
use std::path::PathBuf;

struct FileTransferProtocol {
    node: Arc<OctopiiNode>,
    transport: Arc<QuicTransport>,
}

impl FileTransferProtocol {
    // Client side
    async fn get_file(&self, peer_addr: SocketAddr, path: &str) -> Result<Bytes> {
        // Step 1: Request via RPC
        let request = format!("GET {}", path);
        let response = self.node.query(request.as_bytes()).await?;

        let response_str = String::from_utf8(response.to_vec())?;
        if !response_str.starts_with("OK") {
            return Err("File not available".into());
        }

        // Step 2: Receive via Shipping Lane
        let peer = self.transport.connect(peer_addr).await?;
        let data = peer.recv_chunk_verified().await?
            .ok_or("No data received")?;

        // Step 3: Confirm via RPC
        let checksum = sha256(&data);
        let confirm = format!("CONFIRM {} {}", path, hex::encode(checksum));
        self.node.query(confirm.as_bytes()).await?;

        Ok(data)
    }

    // Server side
    async fn serve_file(&self, request: &[u8]) -> Result<Bytes> {
        let request_str = std::str::from_utf8(request)?;

        if request_str.starts_with("GET ") {
            let path = &request_str[4..];

            // Validate and prepare file
            let file_path = self.validate_path(path)?;
            let metadata = tokio::fs::metadata(&file_path).await?;

            // Send metadata response
            let response = format!("OK ready {}", metadata.len());

            // Schedule Shipping Lane transfer
            let transport = self.transport.clone();
            let peer_addr = self.current_peer_addr; // Track this
            tokio::spawn(async move {
                let peer = transport.connect(peer_addr).await?;
                let chunk = ChunkSource::File(file_path);
                peer.send_chunk_verified(&chunk).await?;
                Ok::<_, Error>(())
            });

            Ok(Bytes::from(response))
        } else if request_str.starts_with("CONFIRM ") {
            // Handle confirmation
            Ok(Bytes::from("COMPLETE"))
        } else {
            Err("Unknown command".into())
        }
    }
}
```

---

### Pattern 2: Push Notification

**Use Case:** Server proactively sends data to client

```
Server                                    Client
  │                                          │
  ├───── RPC: "PUSH_NOTIFY /updates" ───────►│
  │                                          │
  │◄──── RPC Response: "READY" ──────────────┤
  │                                          │
  ├───── Chunk Transfer: update data ───────►│
  │         (via Shipping Lane)              │
  │                                          │
  │◄──── RPC: "ACK" ─────────────────────────┤
```

**Implementation:**

```rust
struct PushProtocol {
    node: Arc<OctopiiNode>,
    transport: Arc<QuicTransport>,
}

impl PushProtocol {
    // Server side
    async fn push_update(&self, peer_addr: SocketAddr, data: Bytes) -> Result<()> {
        // Step 1: Notify via RPC
        let notification = format!("PUSH_NOTIFY size={}", data.len());
        let response = self.send_rpc(peer_addr, notification.as_bytes()).await?;

        if response != b"READY" {
            return Err("Client not ready".into());
        }

        // Step 2: Send via Shipping Lane
        let peer = self.transport.connect(peer_addr).await?;
        let chunk = ChunkSource::Memory(data);
        peer.send_chunk_verified(&chunk).await?;

        // Step 3: Wait for ACK
        let ack = self.wait_for_rpc_ack(peer_addr).await?;
        if ack != b"ACK" {
            return Err("Client did not ACK".into());
        }

        Ok(())
    }

    // Client side
    async fn handle_push_notification(&self, notification: &[u8]) -> Result<Bytes> {
        let notif_str = std::str::from_utf8(notification)?;

        if notif_str.starts_with("PUSH_NOTIFY") {
            // Accept notification
            let response = Bytes::from("READY");

            // Prepare to receive
            let transport = self.transport.clone();
            tokio::spawn(async move {
                let (_peer_addr, peer) = transport.accept().await?;
                let data = peer.recv_chunk_verified().await?
                    .ok_or("No data received")?;

                // Process data
                process_update(data);

                // Send ACK
                // (via RPC in separate channel)
                Ok::<_, Error>(())
            });

            Ok(response)
        } else {
            Err("Unknown notification".into())
        }
    }
}
```

---

### Pattern 3: Bidirectional Sync

**Use Case:** Synchronize large datasets between peers

```
Peer A                                    Peer B
  │                                          │
  ├───── RPC: "SYNC_REQUEST hash=..." ──────►│
  │                                          │
  │◄─── RPC Response: "NEED_CHUNKS [1,5]" ──┤
  │                                          │
  ├───── Chunk Transfer: chunks [1,5] ──────►│
  │                                          │
  │◄─── RPC: "NEED_FROM_YOU [3,7]" ──────────┤
  │                                          │
  │◄─── Chunk Transfer: chunks [3,7] ────────┤
  │                                          │
  ├───── RPC: "SYNC_COMPLETE" ───────────────►│
```

**Implementation:**

```rust
struct SyncProtocol {
    node: Arc<OctopiiNode>,
    transport: Arc<QuicTransport>,
}

#[derive(Serialize, Deserialize)]
struct SyncRequest {
    dataset_id: String,
    local_hash: String,
    chunk_hashes: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct SyncResponse {
    need_chunks: Vec<usize>,
    can_provide: Vec<usize>,
}

impl SyncProtocol {
    async fn sync_dataset(
        &self,
        peer_addr: SocketAddr,
        dataset_id: &str
    ) -> Result<()> {
        // Step 1: Negotiate via RPC
        let local_state = self.get_local_state(dataset_id).await?;
        let request = SyncRequest {
            dataset_id: dataset_id.to_string(),
            local_hash: local_state.hash.clone(),
            chunk_hashes: local_state.chunk_hashes.clone(),
        };

        let request_bytes = bincode::serialize(&request)?;
        let response_bytes = self.send_rpc(peer_addr, &request_bytes).await?;
        let response: SyncResponse = bincode::deserialize(&response_bytes)?;

        // Step 2: Send chunks they need via Shipping Lane
        for chunk_idx in response.need_chunks {
            let chunk_data = self.get_chunk_data(dataset_id, chunk_idx).await?;
            let peer = self.transport.connect(peer_addr).await?;
            let chunk = ChunkSource::Memory(chunk_data);
            peer.send_chunk_verified(&chunk).await?;
        }

        // Step 3: Receive chunks we need via Shipping Lane
        for chunk_idx in response.can_provide {
            let (_addr, peer) = self.transport.accept().await?;
            let chunk_data = peer.recv_chunk_verified().await?
                .ok_or("No chunk data")?;
            self.store_chunk(dataset_id, chunk_idx, chunk_data).await?;
        }

        // Step 4: Confirm completion via RPC
        let confirm = format!("SYNC_COMPLETE {}", dataset_id);
        self.send_rpc(peer_addr, confirm.as_bytes()).await?;

        Ok(())
    }
}
```

---

## Transport Layer API

### QuicTransport

**Location:** `src/transport/mod.rs`

```rust
pub struct QuicTransport {
    endpoint: Arc<Endpoint>,
    connections: Arc<RwLock<HashMap<SocketAddr, PeerConnection>>>,
    bind_addr: SocketAddr,
}

impl QuicTransport {
    /// Create a new QUIC transport
    pub async fn new(bind_addr: SocketAddr) -> Result<Self>

    /// Connect to a peer (or return existing connection)
    pub async fn connect(
        &self,
        addr: SocketAddr
    ) -> Result<Arc<PeerConnection>>

    /// Accept an incoming connection
    pub async fn accept(
        &self
    ) -> Result<(SocketAddr, Arc<PeerConnection>)>

    /// Send data to a peer (simplified API)
    pub async fn send(
        &self,
        addr: SocketAddr,
        data: Bytes
    ) -> Result<()>

    /// Get local listening address
    pub fn local_addr(&self) -> Result<SocketAddr>

    /// Close the transport
    pub fn close(&self)

    /// Check if peer connection is active
    pub async fn has_active_peer(
        &self,
        addr: SocketAddr
    ) -> bool
}
```

### PeerConnection

```rust
pub struct PeerConnection {
    connection: Connection,
}

impl PeerConnection {
    /// Send data (length-prefixed, with ACK)
    pub async fn send(&self, data: Bytes) -> Result<()>

    /// Receive data (length-prefixed)
    pub async fn recv(&self) -> Result<Option<Bytes>>

    /// Check if connection is closed
    pub fn is_closed(&self) -> bool

    /// Get connection statistics
    pub fn stats(&self) -> quinn::ConnectionStats

    /// Send a chunk with checksum verification
    pub async fn send_chunk_verified(
        &self,
        chunk: &ChunkSource
    ) -> Result<u64>

    /// Receive a chunk with checksum verification
    pub async fn recv_chunk_verified(
        &self
    ) -> Result<Option<Bytes>>
}
```

### ChunkSource

```rust
#[derive(Debug, Clone)]
pub enum ChunkSource {
    /// Read chunk from a file on disk
    File(PathBuf),

    /// Chunk data already in memory
    Memory(Bytes),
}
```

### TransferResult

```rust
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
    pub fn success(peer: SocketAddr, bytes: u64, duration: Duration) -> Self
    pub fn failure(peer: SocketAddr, error: String) -> Self
}
```

---

## Internal Implementation

### Connection Pooling

```
┌────────────────────────────────────────────────────────────┐
│                  QuicTransport                             │
│                                                            │
│  connections: HashMap<SocketAddr, PeerConnection>         │
│                                                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │  192.168.1.100:5001  →  PeerConnection           │     │
│  │  192.168.1.101:5001  →  PeerConnection           │     │
│  │  192.168.1.102:5001  →  PeerConnection           │     │
│  └──────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────┘

connect(addr) flow:
  1. Check if connection exists in pool
  2. If exists and alive: return existing
  3. If not exists: create new QUIC connection
  4. Store in pool and return
```

### Stream Multiplexing

```
Single QUIC Connection
        │
        ├─── Stream 1: RPC request/response
        │
        ├─── Stream 2: Chunk transfer (file 1)
        │
        ├─── Stream 3: Chunk transfer (file 2)
        │
        ├─── Stream 4: RPC request/response
        │
        └─── Stream 5: Chunk transfer (snapshot)

Benefits:
  • No head-of-line blocking between streams
  • Efficient use of network resources
  • Single TLS handshake
  • Automatic flow control per stream
```

### Checksum Computation

```rust
// Sender side (from src/transport/peer.rs)
async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64> {
    let (data, size) = match chunk {
        ChunkSource::Memory(bytes) => {
            // Compute checksum for memory data
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let hash = hasher.finalize();
            (Some(bytes.clone()), bytes.len() as u64, hash.to_vec())
        }
        ChunkSource::File(path) => {
            // Stream file and compute checksum incrementally
            let mut file = File::open(path).await?;
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 64 * 1024];

            loop {
                let n = file.read(&mut buffer).await?;
                if n == 0 { break; }
                hasher.update(&buffer[..n]);
            }

            let hash = hasher.finalize();
            (None, metadata.len(), hash.to_vec())
        }
    };

    // Send: size + data + checksum
    // Wait for ACK
}

// Receiver side
async fn recv_chunk_verified(&self) -> Result<Option<Bytes>> {
    // Read size
    let total_size = read_u64().await?;

    // Stream data and compute checksum
    let mut hasher = Sha256::new();
    let mut data = BytesMut::new();
    let mut received = 0u64;

    while received < total_size {
        let chunk = read_chunk().await?;
        hasher.update(&chunk);
        data.extend_from_slice(&chunk);
        received += chunk.len() as u64;
    }

    // Read and verify checksum
    let received_checksum = read_checksum().await?;
    let computed_checksum = hasher.finalize();

    if computed_checksum[..] != received_checksum {
        send_ack(1).await?; // Checksum failed
        return Err("Checksum mismatch");
    }

    send_ack(0).await?; // Success
    Ok(Some(data.freeze()))
}
```

---

## Performance Optimization

### 1. Buffer Size Tuning

```rust
const BUFFER_SIZE: usize = 64 * 1024; // 64KB default

// For high-bandwidth, high-latency networks (increase):
const BUFFER_SIZE: usize = 256 * 1024; // 256KB

// For low-memory environments (decrease):
const BUFFER_SIZE: usize = 16 * 1024; // 16KB
```

### 2. Memory Management

```rust
// Bad: Load entire file into memory
async fn send_file_bad(path: PathBuf) -> Result<()> {
    let data = tokio::fs::read(&path).await?; // ❌ 1GB file = 1GB RAM!
    let chunk = ChunkSource::Memory(Bytes::from(data));
    peer.send_chunk_verified(&chunk).await?;
    Ok(())
}

// Good: Stream from disk
async fn send_file_good(path: PathBuf) -> Result<()> {
    let chunk = ChunkSource::File(path); // ✓ ~64KB RAM usage
    peer.send_chunk_verified(&chunk).await?;
    Ok(())
}
```

### 3. Parallel Transfers

```rust
// Transfer multiple files in parallel
async fn transfer_batch(
    transport: &QuicTransport,
    peer_addr: SocketAddr,
    files: Vec<PathBuf>
) -> Result<Vec<TransferResult>> {
    let mut handles = vec![];

    for file in files {
        let transport = transport.clone();
        let peer_addr = peer_addr.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let peer = transport.connect(peer_addr).await?;
            let chunk = ChunkSource::File(file.clone());

            match peer.send_chunk_verified(&chunk).await {
                Ok(bytes) => {
                    TransferResult::success(peer_addr, bytes, start.elapsed())
                }
                Err(e) => {
                    TransferResult::failure(peer_addr, e.to_string())
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all transfers
    let results = futures::future::join_all(handles).await;
    Ok(results.into_iter().filter_map(|r| r.ok()).collect())
}
```

### 4. Connection Reuse

```rust
// Bad: Create new connection per transfer
for file in files {
    let peer = transport.connect(peer_addr).await?; // New connection each time
    send_file(peer, file).await?;
}

// Good: Reuse connection
let peer = transport.connect(peer_addr).await?; // Single connection
for file in files {
    send_file(&peer, file).await?; // Reuse via multiplexing
}
```

### 5. Compression

```rust
use flate2::Compression;
use flate2::write::GzEncoder;

async fn send_compressed(
    peer: &PeerConnection,
    data: Bytes
) -> Result<u64> {
    // Compress data
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&data)?;
    let compressed = encoder.finish()?;

    // Send compressed data
    let chunk = ChunkSource::Memory(Bytes::from(compressed));
    peer.send_chunk_verified(&chunk).await
}
```

---

## Security Considerations

### 1. TLS Encryption

All Shipping Lane transfers use TLS 1.3 via QUIC:

```
┌────────────────────────────────────────────────────────────┐
│                  Security Layers                           │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  Application Data                                          │
│         │                                                  │
│         ▼                                                  │
│  SHA-256 Checksum (integrity)                             │
│         │                                                  │
│         ▼                                                  │
│  QUIC Stream (framing, flow control)                      │
│         │                                                  │
│         ▼                                                  │
│  TLS 1.3 (encryption, authentication)                     │
│         │                                                  │
│         ▼                                                  │
│  UDP (network layer)                                       │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### 2. Checksum Verification

**Always enabled** - cannot be bypassed:

```rust
// Automatic verification in recv_chunk_verified()
let data = peer.recv_chunk_verified().await?; // ✓ Verified
// If checksum fails, returns error automatically
```

### 3. Size Limits

Implement application-level limits:

```rust
const MAX_CHUNK_SIZE: u64 = 1024 * 1024 * 1024; // 1GB

async fn recv_with_limit(peer: &PeerConnection) -> Result<Bytes> {
    // Read size first
    let size = read_size_from_stream().await?;

    if size > MAX_CHUNK_SIZE {
        return Err("Chunk too large".into());
    }

    peer.recv_chunk_verified().await?
        .ok_or("No data".into())
}
```

### 4. Rate Limiting

```rust
use tokio::time::{interval, Duration};

struct RateLimiter {
    bytes_per_second: u64,
    last_reset: Instant,
    bytes_sent: AtomicU64,
}

impl RateLimiter {
    async fn send_with_limit(
        &self,
        peer: &PeerConnection,
        chunk: &ChunkSource
    ) -> Result<u64> {
        // Check rate limit
        let sent = self.bytes_sent.load(Ordering::Relaxed);
        let elapsed = self.last_reset.elapsed().as_secs();

        if sent / elapsed.max(1) > self.bytes_per_second {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Send
        let bytes = peer.send_chunk_verified(chunk).await?;
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);

        Ok(bytes)
    }
}
```

---

## Complete Examples

### Example 1: Snapshot Transfer System

> **Note:** The following snapshot protocol is illustrative. The current OpenRaft
> integration does not automatically invoke Shipping Lane for snapshot installs,
> so you would need to wire these steps into your own control plane.

```rust
use octopii::{OctopiiNode, StateMachineTrait, transport::*};
use bytes::Bytes;
use std::sync::Arc;

/// Custom protocol for transferring snapshots between nodes
pub struct SnapshotTransferProtocol {
    node: Arc<OctopiiNode>,
    transport: Arc<QuicTransport>,
}

impl SnapshotTransferProtocol {
    pub fn new(node: Arc<OctopiiNode>, transport: Arc<QuicTransport>) -> Self {
        Self { node, transport }
    }

    /// Request a snapshot from a peer
    pub async fn request_snapshot(
        &self,
        peer_addr: SocketAddr,
        snapshot_id: &str
    ) -> Result<Bytes> {
        // Step 1: Request via RPC
        let request = format!("SNAPSHOT_REQUEST {}", snapshot_id);
        let response = self.send_rpc(peer_addr, request.as_bytes()).await?;

        let response_str = String::from_utf8(response.to_vec())?;
        if !response_str.starts_with("SNAPSHOT_READY") {
            return Err(format!("Snapshot not ready: {}", response_str).into());
        }

        // Parse snapshot size
        let parts: Vec<&str> = response_str.split_whitespace().collect();
        let size: u64 = parts.get(1)
            .ok_or("Missing size")?
            .parse()
            .map_err(|_| "Invalid size")?;

        println!("Receiving snapshot: {} bytes", size);

        // Step 2: Receive snapshot via Shipping Lane
        let peer = self.transport.connect(peer_addr).await?;
        let snapshot_data = peer.recv_chunk_verified().await?
            .ok_or("No snapshot data received")?;

        // Step 3: Verify size
        if snapshot_data.len() as u64 != size {
            return Err("Size mismatch".into());
        }

        // Step 4: Confirm via RPC
        let confirm = format!("SNAPSHOT_RECEIVED {}", snapshot_id);
        self.send_rpc(peer_addr, confirm.as_bytes()).await?;

        Ok(snapshot_data)
    }

    /// Serve snapshot requests
    pub async fn serve_snapshot(
        &self,
        request: &[u8]
    ) -> Result<Bytes> {
        let request_str = std::str::from_utf8(request)?;

        if request_str.starts_with("SNAPSHOT_REQUEST ") {
            let snapshot_id = &request_str[17..];

            // Generate snapshot from state machine
            let snapshot_data = self.node.state_machine.snapshot();
            let size = snapshot_data.len();

            // Send metadata response
            let response = format!("SNAPSHOT_READY {} {}", size, snapshot_id);

            // Schedule Shipping Lane transfer
            let transport = self.transport.clone();
            let peer_addr = self.get_current_peer_addr()?;
            tokio::spawn(async move {
                let peer = transport.connect(peer_addr).await?;
                let chunk = ChunkSource::Memory(Bytes::from(snapshot_data));
                peer.send_chunk_verified(&chunk).await?;
                Ok::<_, Error>(())
            });

            Ok(Bytes::from(response))
        } else if request_str.starts_with("SNAPSHOT_RECEIVED ") {
            Ok(Bytes::from("OK"))
        } else {
            Err("Unknown command".into())
        }
    }

    // Helper to send RPC
    async fn send_rpc(
        &self,
        peer_addr: SocketAddr,
        data: &[u8]
    ) -> Result<Bytes> {
        // Use your RPC mechanism here
        // This is a simplified example
        self.node.query(data).await
    }

    fn get_current_peer_addr(&self) -> Result<SocketAddr> {
        // Track the current peer making the request
        // Implementation depends on your setup
        Ok("127.0.0.1:5001".parse()?)
    }
}
```

---

### Example 2: Media Streaming Service

```rust
use octopii::transport::*;
use tokio::io::AsyncWriteExt;
use std::path::PathBuf;

/// Stream large media files to clients
pub struct MediaStreamingService {
    transport: Arc<QuicTransport>,
    media_dir: PathBuf,
}

impl MediaStreamingService {
    pub fn new(bind_addr: SocketAddr, media_dir: PathBuf) -> Result<Self> {
        let transport = QuicTransport::new(bind_addr).await?;
        Ok(Self {
            transport: Arc::new(transport),
            media_dir,
        })
    }

    /// Stream a media file to a client
    pub async fn stream_media(
        &self,
        client_addr: SocketAddr,
        filename: &str
    ) -> Result<TransferResult> {
        let start = Instant::now();

        // Validate filename (prevent directory traversal)
        let safe_filename = PathBuf::from(filename)
            .file_name()
            .ok_or("Invalid filename")?
            .to_str()
            .ok_or("Invalid UTF-8")?;

        let file_path = self.media_dir.join(safe_filename);

        // Check if file exists
        if !file_path.exists() {
            return Ok(TransferResult::failure(
                client_addr,
                "File not found".to_string()
            ));
        }

        // Connect to client
        let peer = self.transport.connect(client_addr).await
            .map_err(|e| TransferResult::failure(
                client_addr,
                format!("Connection failed: {}", e)
            ))?;

        // Stream file via Shipping Lane
        let chunk = ChunkSource::File(file_path);
        match peer.send_chunk_verified(&chunk).await {
            Ok(bytes_sent) => {
                println!("Streamed {} bytes to {} in {:?}",
                    bytes_sent, client_addr, start.elapsed());
                Ok(TransferResult::success(
                    client_addr,
                    bytes_sent,
                    start.elapsed()
                ))
            }
            Err(e) => {
                Ok(TransferResult::failure(
                    client_addr,
                    e.to_string()
                ))
            }
        }
    }

    /// Serve multiple concurrent streams
    pub async fn serve(self: Arc<Self>) -> Result<()> {
        loop {
            let (client_addr, peer) = self.transport.accept().await?;
            let service = self.clone();

            tokio::spawn(async move {
                // Receive media request
                match peer.recv().await {
                    Ok(Some(request)) => {
                        let filename = String::from_utf8_lossy(&request);
                        println!("Streaming {} to {}", filename, client_addr);

                        if let Err(e) = service.stream_media(client_addr, &filename).await {
                            eprintln!("Stream error: {}", e);
                        }
                    }
                    Ok(None) => println!("Client {} disconnected", client_addr),
                    Err(e) => eprintln!("Receive error: {}", e),
                }
            });
        }
    }
}

// Client side
pub struct MediaClient {
    transport: Arc<QuicTransport>,
}

impl MediaClient {
    pub async fn download_media(
        &self,
        server_addr: SocketAddr,
        filename: &str,
        output_path: PathBuf
    ) -> Result<()> {
        // Connect to server
        let peer = self.transport.connect(server_addr).await?;

        // Send request
        peer.send(Bytes::from(filename.to_string())).await?;

        // Receive media via Shipping Lane
        let data = peer.recv_chunk_verified().await?
            .ok_or("No data received")?;

        // Save to disk
        let mut file = tokio::fs::File::create(output_path).await?;
        file.write_all(&data).await?;
        file.flush().await?;

        println!("Downloaded {} ({} bytes)", filename, data.len());
        Ok(())
    }
}

// Usage
#[tokio::main]
async fn main() -> Result<()> {
    // Start server
    let service = Arc::new(MediaStreamingService::new(
        "0.0.0.0:8080".parse()?,
        PathBuf::from("./media")
    )?);

    tokio::spawn(service.clone().serve());

    // Client downloads a file
    let client = MediaClient {
        transport: Arc::new(QuicTransport::new("0.0.0.0:0".parse()?).await?),
    };

    client.download_media(
        "127.0.0.1:8080".parse()?,
        "video.mp4",
        PathBuf::from("downloaded_video.mp4")
    ).await?;

    Ok(())
}
```

---

## Gotchas and Best Practices

### Gotcha #1: Connection Pooling Assumption

**Problem:**
```rust
// Assumes connection stays open
let peer = transport.connect(addr).await?;
tokio::time::sleep(Duration::from_secs(3600)).await; // 1 hour
peer.send_chunk_verified(&chunk).await?; // ❌ May fail if connection closed
```

**Solution:**
```rust
// Always check connection state
let peer = transport.connect(addr).await?; // Reconnects if needed
if peer.is_closed() {
    peer = transport.connect(addr).await?;
}
peer.send_chunk_verified(&chunk).await?;
```

---

### Gotcha #2: Backpressure

**Problem:**
```rust
// Sending faster than receiver can process
for file in huge_file_list {
    peer.send_chunk_verified(&ChunkSource::File(file)).await?; // No backpressure!
}
```

**Solution:**
```rust
// Limit concurrent transfers
use tokio::sync::Semaphore;

let semaphore = Arc::new(Semaphore::new(5)); // Max 5 concurrent

for file in huge_file_list {
    let permit = semaphore.clone().acquire_owned().await?;
    let peer = peer.clone();
    let file = file.clone();

    tokio::spawn(async move {
        let _permit = permit; // Drop when done
        peer.send_chunk_verified(&ChunkSource::File(file)).await?;
        Ok::<_, Error>(())
    });
}
```

---

### Gotcha #3: Error Recovery

**Problem:**
```rust
// No retry on failure
peer.send_chunk_verified(&chunk).await?; // Fails and gives up
```

**Solution:**
```rust
// Implement retry logic
async fn send_with_retry(
    transport: &QuicTransport,
    peer_addr: SocketAddr,
    chunk: &ChunkSource,
    max_retries: usize
) -> Result<u64> {
    let mut retries = 0;

    loop {
        let peer = transport.connect(peer_addr).await?;

        match peer.send_chunk_verified(chunk).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) if retries < max_retries => {
                retries += 1;
                eprintln!("Transfer failed (attempt {}): {}", retries, e);
                tokio::time::sleep(Duration::from_secs(2u64.pow(retries as u32))).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

---

### Best Practice #1: Progress Tracking

```rust
use indicatif::{ProgressBar, ProgressStyle};

async fn send_with_progress(
    peer: &PeerConnection,
    file_path: PathBuf
) -> Result<u64> {
    let metadata = tokio::fs::metadata(&file_path).await?;
    let total_size = metadata.len();

    // Create progress bar
    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{bar:40} {bytes}/{total_bytes} ({eta})")
        .unwrap());

    // We can't directly hook into send_chunk_verified,
    // but we can wrap it with monitoring
    let start = Instant::now();
    let chunk = ChunkSource::File(file_path);
    let bytes_sent = peer.send_chunk_verified(&chunk).await?;

    pb.finish_with_message("Transfer complete");

    let duration = start.elapsed();
    let speed = bytes_sent as f64 / duration.as_secs_f64();
    println!("Average speed: {:.2} MB/s", speed / 1_000_000.0);

    Ok(bytes_sent)
}
```

---

### Best Practice #2: Graceful Shutdown

```rust
struct TransferManager {
    transport: Arc<QuicTransport>,
    active_transfers: Arc<RwLock<HashSet<Uuid>>>,
    shutdown: Arc<AtomicBool>,
}

impl TransferManager {
    async fn shutdown_gracefully(&self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // Wait for active transfers to complete
        let timeout = Duration::from_secs(30);
        let start = Instant::now();

        loop {
            let active = self.active_transfers.read().await.len();
            if active == 0 || start.elapsed() > timeout {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Close transport
        self.transport.close();
    }

    async fn transfer_with_tracking(
        &self,
        peer_addr: SocketAddr,
        chunk: ChunkSource
    ) -> Result<u64> {
        let transfer_id = Uuid::new_v4();
        self.active_transfers.write().await.insert(transfer_id);

        let result = async {
            let peer = self.transport.connect(peer_addr).await?;
            peer.send_chunk_verified(&chunk).await
        }.await;

        self.active_transfers.write().await.remove(&transfer_id);
        result
    }
}
```

---

### Best Practice #3: Metrics Collection

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TransferMetrics {
    total_bytes_sent: AtomicU64,
    total_bytes_received: AtomicU64,
    transfers_completed: AtomicU64,
    transfers_failed: AtomicU64,
    total_duration_ms: AtomicU64,
}

impl TransferMetrics {
    pub fn record_transfer(&self, result: &TransferResult) {
        if result.success {
            self.total_bytes_sent.fetch_add(
                result.bytes_transferred,
                Ordering::Relaxed
            );
            self.transfers_completed.fetch_add(1, Ordering::Relaxed);
            self.total_duration_ms.fetch_add(
                result.duration.as_millis() as u64,
                Ordering::Relaxed
            );
        } else {
            self.transfers_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn report(&self) {
        let completed = self.transfers_completed.load(Ordering::Relaxed);
        let failed = self.transfers_failed.load(Ordering::Relaxed);
        let bytes = self.total_bytes_sent.load(Ordering::Relaxed);
        let duration_ms = self.total_duration_ms.load(Ordering::Relaxed);

        let avg_speed = if duration_ms > 0 {
            (bytes as f64 / (duration_ms as f64 / 1000.0)) / 1_000_000.0
        } else {
            0.0
        };

        println!("Transfer Statistics:");
        println!("  Completed: {}", completed);
        println!("  Failed: {}", failed);
        println!("  Total bytes: {} MB", bytes / 1_000_000);
        println!("  Average speed: {:.2} MB/s", avg_speed);
    }
}
```

---

**Version:** 0.1.0
**Last Updated:** 2025-11-13
