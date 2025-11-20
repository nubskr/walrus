# Octopii External API Documentation

## Overview

Octopii is a Rust-based distributed consensus and replication system built on top of the Raft consensus algorithm. This document covers every aspect of the external API that users interact with.

**Core Features:**
- Raft-based consensus using `raft-rs` and OpenRaft
- QUIC transport layer via Quinn
- Write-Ahead Log (WAL) for durability
- RPC framework for request/response messaging
- Pluggable state machines for custom replication logic

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [OctopiiRuntime](#octopiiruntime)
3. [OctopiiNode](#octopiinode)
4. [Configuration](#configuration)
5. [State Machine Interface](#state-machine-interface)
6. [Error Handling](#error-handling)
7. [RPC Framework](#rpc-framework)
8. [Transport Layer](#transport-layer)
9. [Data Transfer](#data-transfer)
10. [Write-Ahead Log](#write-ahead-log)
11. [Usage Examples](#usage-examples)

---

## Getting Started

### Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
octopii = { version = "*", features = ["openraft"] }
tokio = { version = "1", features = ["full"] }
```

### Quick Example

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create runtime
    let runtime = OctopiiRuntime::new(4);

    // Configure node
    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5001".parse()?,
        peers: vec!["127.0.0.1:5002".parse()?],
        wal_dir: "./data/node1".into(),
        is_initial_leader: true,
        ..Default::default()
    };

    // Create and start node
    let node = OctopiiNode::new(config, runtime).await?;
    node.start().await?;

    // Propose a command
    let response = node.propose(b"SET key value".to_vec()).await?;
    println!("Response: {:?}", response);

    Ok(())
}
```

---

## OctopiiRuntime

### Overview

`OctopiiRuntime` manages an isolated Tokio runtime for Octopii operations. It provides a wrapper around Tokio's runtime with support for both owned and handle-based runtimes.

**Location:** `src/runtime.rs`

### API Reference

#### Constructor Methods

```rust
pub fn new(worker_threads: usize) -> Self
```

Creates a new runtime with a dedicated thread pool.

**Parameters:**
- `worker_threads`: Number of worker threads for the Tokio runtime

**Example:**
```rust
let runtime = OctopiiRuntime::new(4); // 4-thread runtime
```

---

```rust
pub fn from_handle(handle: Handle) -> Self
```

Creates a runtime from an existing Tokio handle.

**Parameters:**
- `handle`: Tokio runtime handle

**Use Cases:**
- Sharing runtime across multiple nodes
- Testing scenarios
- Integration with existing Tokio applications

**Example:**
```rust
let handle = tokio::runtime::Handle::current();
let runtime = OctopiiRuntime::from_handle(handle);
```

---

```rust
pub fn default() -> Self
```

Creates a runtime with default settings (4 worker threads).

**Example:**
```rust
let runtime = OctopiiRuntime::default();
```

---

#### Methods

```rust
pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
```

Spawns a future on the runtime.

**Parameters:**
- `future`: The async task to execute

**Returns:** Join handle for the spawned task

**Example:**
```rust
let handle = runtime.spawn(async {
    println!("Running on Octopii runtime");
});
```

---

```rust
pub fn handle(&self) -> tokio::runtime::Handle
```

Returns the underlying Tokio runtime handle.

**Example:**
```rust
let handle = runtime.handle();
```

---

### Thread Safety

`OctopiiRuntime` is `Clone`, allowing it to be shared across multiple components safely.

---

## OctopiiNode

### Overview

`OctopiiNode` is the main entry point for creating and managing a Raft node. It provides high-level APIs for consensus operations, membership management, and data replication.

**Location:** `src/openraft/node.rs`
**Feature:** Requires `openraft` feature flag

### API Reference

#### Initialization

```rust
pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self>
```

Creates a new Raft node with default in-memory state machine.

**Parameters:**
- `config`: Node configuration
- `runtime`: Octopii runtime

**Returns:** `Result<OctopiiNode>`

**Example:**
```rust
let node = OctopiiNode::new(config, runtime).await?;
```

---

```rust
pub fn new_blocking(config: Config) -> Result<Self>
```

Creates a new node using a blocking call (creates its own runtime).

**Parameters:**
- `config`: Node configuration

**Returns:** `Result<OctopiiNode>`

**Example:**
```rust
let node = OctopiiNode::new_blocking(config)?;
```

---

```rust
pub async fn new_with_state_machine(
    config: Config,
    runtime: OctopiiRuntime,
    state_machine: StateMachine,
) -> Result<Self>
```

Creates a new node with a custom state machine.

**Parameters:**
- `config`: Node configuration
- `runtime`: Octopii runtime
- `state_machine`: Custom state machine implementation

**Returns:** `Result<OctopiiNode>`

**Example:**
```rust
let state_machine = Arc::new(MyStateMachine::new());
let node = OctopiiNode::new_with_state_machine(config, runtime, state_machine).await?;
```

---

#### Lifecycle Management

```rust
pub async fn start(&self) -> Result<()>
```

Starts the Raft node and begins consensus protocol.

**Returns:** `Result<()>`

**Example:**
```rust
node.start().await?;
```

---

```rust
pub fn shutdown(&self)
```

Gracefully shuts down the node.

**Example:**
```rust
node.shutdown();
```

---

#### Write Operations

```rust
pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes>
```

Proposes a command to the Raft cluster. This command will be replicated and applied once committed.

**Parameters:**
- `command`: Byte array representing the command

**Returns:** `Result<Bytes>` - Response from state machine after command is applied

**Behavior:**
- If not leader: Returns error
- If leader: Replicates to majority, applies to state machine, returns result

**Example:**
```rust
let response = node.propose(b"SET key value".to_vec()).await?;
```

---

#### Read Operations

```rust
pub async fn query(&self, command: &[u8]) -> Result<Bytes>
```

Executes a read-only query against the state machine.

**Parameters:**
- `command`: Query command

**Returns:** `Result<Bytes>` - Query result

**Note:** For linearizable reads, consider using `read_index()` first.

**Example:**
```rust
let value = node.query(b"GET key").await?;
```

---

#### Leadership Management

```rust
pub async fn is_leader(&self) -> bool
```

Checks if this node is currently the leader.

**Returns:** `true` if leader, `false` otherwise

**Example:**
```rust
if node.is_leader().await {
    println!("This node is the leader");
}
```

---

```rust
pub async fn has_leader(&self) -> bool
```

Checks if the cluster has an active leader.

**Returns:** `true` if cluster has a leader, `false` otherwise

**Example:**
```rust
if !node.has_leader().await {
    println!("No leader elected");
}
```

---

```rust
pub async fn campaign(&self) -> Result<()>
```

Starts a leader election campaign.

**Returns:** `Result<()>`

**Use Cases:**
- Forcing an election
- Testing leader election
- Manual failover

**Example:**
```rust
node.campaign().await?;
```

---

> **Note:** Direct leader transfer is not yet supported. OpenRaft 0.10 does not
> expose an API for forcing a leader transfer, so Octopii currently relies on
> elections via `campaign()`.

---

#### Membership Management

```rust
pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<()>
```

Adds a new node as a learner (non-voting member).

**Parameters:**
- `peer_id`: Unique node identifier
- `addr`: Network address of the peer

**Returns:** `Result<()>`

**Workflow:**
1. Add as learner
2. Wait for catch-up
3. Promote to voter

**Example:**
```rust
let addr: SocketAddr = "127.0.0.1:5003".parse()?;
node.add_learner(3, addr).await?;
```

---

```rust
pub async fn promote_learner(&self, peer_id: u64) -> Result<()>
```

Promotes a learner to a voting member.

**Parameters:**
- `peer_id`: Node ID of the learner

**Returns:** `Result<()>`

**Requirements:**
- Node must be a learner
- Node should be caught up (check with `is_learner_caught_up`)

**Example:**
```rust
if node.is_learner_caught_up(3).await? {
    node.promote_learner(3).await?;
}
```

---

```rust
pub async fn is_learner_caught_up(&self, peer_id: u64) -> Result<bool>
```

Checks if a learner has caught up with the leader's log.

**Parameters:**
- `peer_id`: Node ID of the learner

**Returns:** `Result<bool>` - `true` if caught up

**Example:**
```rust
let caught_up = node.is_learner_caught_up(3).await?;
```

---

#### Snapshot Management

```rust
pub async fn force_snapshot_to_peer(&self, peer_id: u64) -> Result<()>
```

Triggers OpenRaft's snapshot mechanism. The `peer_id` argument is currently
ignored; snapshots are created locally and distributed by the Raft core based on
its own lag detection. Targeted snapshot streaming is not yet implemented.

**Parameters:**
- `peer_id`: Reserved for future use

**Returns:** `Result<()>`

**Use Cases:**
- Force log compaction on the leader
- Help lagging peers indirectly (Raft decides when to install snapshots)

**Example:**
```rust
node.force_snapshot_to_peer(3).await?;
```

---

#### Utility Methods

```rust
pub async fn read_index(&self, ctx: Vec<u8>) -> Result<()>
```

Performs a lightweight leadership check. This helper verifies the current node
still believes it is leader before a read, but it does **not** execute
OpenRaft's `client_read`/`read_index` flow yet. Treat it as a guard for cached
reads rather than a full linearizability fence.

**Parameters:**
- `ctx`: Context for the read (arbitrary bytes)

**Returns:** `Result<()>`

**Example:**
```rust
node.read_index(b"read-ctx".to_vec()).await?;
let value = node.query(b"GET key").await?;
```

> **Tip:** For strict linearizable reads, issue a Raft write or implement a
> custom RPC that uses `client_read` once OpenRaft exposes it.

---

```rust
pub async fn conf_state(&self) -> ConfStateCompat
```

Returns the current cluster configuration.

**Returns:** `ConfStateCompat` with voters and learners

**Example:**
```rust
let conf = node.conf_state().await;
println!("Voters: {:?}", conf.voters);
println!("Learners: {:?}", conf.learners);
```

---

```rust
pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)>
```

Gets replication progress for a peer.

**Parameters:**
- `peer_id`: Peer node ID

**Returns:** `Option<(matched, next)>` where:
- `matched`: Last log index known to be replicated
- `next`: Next log index to send

**Example:**
```rust
if let Some((matched, next)) = node.peer_progress(2).await {
    println!("Peer 2: matched={}, next={}", matched, next);
}
```

---

```rust
pub async fn update_peer_addr(&self, peer_id: u64, addr: SocketAddr)
```

Updates the network address for a peer.

**Parameters:**
- `peer_id`: Peer node ID
- `addr`: New network address

**Use Cases:**
- IP address changes
- Port changes
- Network reconfiguration

**Example:**
```rust
let new_addr: SocketAddr = "127.0.0.1:6000".parse()?;
node.update_peer_addr(2, new_addr).await;
```

---

```rust
pub fn id(&self) -> u64
```

Returns this node's ID.

**Returns:** Node identifier

**Example:**
```rust
let node_id = node.id();
```

---

## Configuration

### Overview

The `Config` struct provides comprehensive configuration for Octopii nodes and clusters.

**Location:** `src/config.rs`

### API Reference

```rust
pub struct Config {
    pub node_id: u64,
    pub bind_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub wal_dir: PathBuf,
    pub worker_threads: usize,
    pub wal_batch_size: usize,
    pub wal_flush_interval_ms: u64,
    pub is_initial_leader: bool,
    pub snapshot_lag_threshold: u64,
}
```

#### Field Descriptions

##### `node_id: u64`

Unique identifier for this node in the cluster.

**Requirements:**
- Must be unique across all nodes
- Typically starts from 1

**Example:**
```rust
config.node_id = 1;
```

---

##### `bind_addr: SocketAddr`

Network address for this node to listen on.

**Format:** IP:port

**Example:**
```rust
config.bind_addr = "127.0.0.1:5001".parse()?;
```

---

##### `peers: Vec<SocketAddr>`

List of network addresses for other nodes in the cluster.

**Notes:**
- Should not include this node's address
- Can be empty for single-node clusters
- Can be updated dynamically via membership API
- **Current limitation:** Node IDs are inferred from the last digit of the peer's
  port (e.g., `127.0.0.1:5002` → ID `2`). Avoid reusing the same port suffix or
  exceeding 9 peers until explicit `(id, addr)` mapping is added.

**Example:**
```rust
config.peers = vec![
    "127.0.0.1:5002".parse()?,
    "127.0.0.1:5003".parse()?,
];
```

---

##### `wal_dir: PathBuf`

Directory for Write-Ahead Log storage.

**Requirements:**
- Must be writable
- Should be on persistent storage
- Each node should have its own directory

**Example:**
```rust
config.wal_dir = PathBuf::from("./data/node1");
```

---

##### `worker_threads: usize`

Number of threads in the Tokio runtime.

**Default:** 4

**Recommendations:**
- Production: Number of CPU cores
- Development: 2-4

**Example:**
```rust
config.worker_threads = 8;
```

---

##### `wal_batch_size: usize`

Number of entries to accumulate before forcing an fsync.

**Default:** 100

**Trade-offs:**
- Larger: Better throughput, higher latency
- Smaller: Lower latency, lower throughput

**Example:**
```rust
config.wal_batch_size = 50;
```

---

##### `wal_flush_interval_ms: u64`

Maximum milliseconds to wait before forcing an fsync.

**Default:** 100ms

**Purpose:** Ensures bounded latency even with low write rates

**Example:**
```rust
config.wal_flush_interval_ms = 50; // 50ms flush interval
```

---

##### `is_initial_leader: bool`

Whether this node should start as the leader.

**Default:** false

**Requirements:**
- Only ONE node in cluster should set to `true`
- Used for cluster bootstrap

**Example:**
```rust
config.is_initial_leader = true; // Only on node 1
```

---

##### `snapshot_lag_threshold: u64`

Number of log entries a peer can lag behind before triggering snapshot transfer.

**Default:** 1000

**Purpose:** Faster catch-up for lagging peers

**Example:**
```rust
config.snapshot_lag_threshold = 500;
```

---

### Default Configuration

```rust
impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            bind_addr: "127.0.0.1:5000".parse().unwrap(),
            peers: vec![],
            wal_dir: PathBuf::from("./octopii_data"),
            worker_threads: 4,
            wal_batch_size: 100,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 1000,
        }
    }
}
```

### Configuration Patterns

#### Single-Node Development

```rust
let config = Config {
    node_id: 1,
    bind_addr: "127.0.0.1:5000".parse()?,
    peers: vec![],
    is_initial_leader: true,
    ..Default::default()
};
```

#### Three-Node Cluster

**Node 1 (Initial Leader):**
```rust
Config {
    node_id: 1,
    bind_addr: "127.0.0.1:5001".parse()?,
    peers: vec![
        "127.0.0.1:5002".parse()?,
        "127.0.0.1:5003".parse()?,
    ],
    wal_dir: PathBuf::from("./data/node1"),
    is_initial_leader: true,
    ..Default::default()
}
```

**Node 2:**
```rust
Config {
    node_id: 2,
    bind_addr: "127.0.0.1:5002".parse()?,
    peers: vec![
        "127.0.0.1:5001".parse()?,
        "127.0.0.1:5003".parse()?,
    ],
    wal_dir: PathBuf::from("./data/node2"),
    is_initial_leader: false,
    ..Default::default()
}
```

**Node 3:**
```rust
Config {
    node_id: 3,
    bind_addr: "127.0.0.1:5003".parse()?,
    peers: vec![
        "127.0.0.1:5001".parse()?,
        "127.0.0.1:5002".parse()?,
    ],
    wal_dir: PathBuf::from("./data/node3"),
    is_initial_leader: false,
    ..Default::default()
}
```

#### High-Throughput Configuration

```rust
Config {
    worker_threads: 16,
    wal_batch_size: 1000,
    wal_flush_interval_ms: 500,
    snapshot_lag_threshold: 5000,
    ..Default::default()
}
```

#### Low-Latency Configuration

```rust
Config {
    wal_batch_size: 10,
    wal_flush_interval_ms: 10,
    ..Default::default()
}
```

---

## State Machine Interface

### Overview

The `StateMachineTrait` allows you to implement custom replication logic. All operations must be deterministic to maintain consistency across replicas.

**Location:** `src/state_machine.rs`

### API Reference

```rust
pub trait StateMachineTrait: Send + Sync {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&self, data: &[u8]) -> Result<(), String>;
    fn compact(&self) -> Result<(), String> { Ok(()) }
}

pub type StateMachine = Arc<dyn StateMachineTrait>;
```

#### Methods

##### `apply(&self, command: &[u8]) -> Result<Bytes, String>`

Applies a committed command to the state machine.

**Parameters:**
- `command`: Command bytes to execute

**Returns:**
- `Ok(Bytes)`: Success with response data
- `Err(String)`: Error message

**Requirements:**
- MUST be deterministic (same input → same output)
- MUST be thread-safe
- Should be fast (runs in consensus loop)

**Example:**
```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let cmd = String::from_utf8(command.to_vec())
        .map_err(|e| e.to_string())?;

    if cmd.starts_with("SET ") {
        // Parse and execute SET command
        Ok(Bytes::from("OK"))
    } else {
        Err("Unknown command".into())
    }
}
```

---

##### `snapshot(&self) -> Vec<u8>`

Creates a snapshot of the current state.

**Returns:** Serialized state as bytes

**Purpose:**
- Log compaction
- Fast catch-up for new/lagging nodes

**Example:**
```rust
fn snapshot(&self) -> Vec<u8> {
    let state = self.data.read().unwrap();
    bincode::serialize(&*state).unwrap()
}
```

---

##### `restore(&self, data: &[u8]) -> Result<(), String>`

Restores state from a snapshot.

**Parameters:**
- `data`: Snapshot bytes

**Returns:**
- `Ok(())`: Success
- `Err(String)`: Error message

**Example:**
```rust
fn restore(&self, data: &[u8]) -> Result<(), String> {
    let state = bincode::deserialize(data)
        .map_err(|e| e.to_string())?;
    *self.data.write().unwrap() = state;
    Ok(())
}
```

---

##### `compact(&self) -> Result<(), String>`

Optional method for performing compaction or cleanup.

**Default Implementation:** No-op

**Use Cases:**
- Clearing old tombstones
- Optimizing internal data structures

**Example:**
```rust
fn compact(&self) -> Result<(), String> {
    let mut state = self.data.write().unwrap();
    state.retain(|k, _| !k.starts_with(b"tmp_"));
    Ok(())
}
```

---

### Built-in State Machine

#### KvStateMachine

Simple key-value store implementation.

**Location:** `src/state_machine.rs`

```rust
pub struct KvStateMachine {
    map: StdMutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvStateMachine {
    pub fn in_memory() -> Self
}
```

**Commands:**

| Command | Format | Response |
|---------|--------|----------|
| SET | `SET <key> <value>` | `"OK"` |
| GET | `GET <key>` | Value or `"NOT_FOUND"` |
| DELETE | `DELETE <key>` | `"OK"` |

**Example:**
```rust
let sm = Arc::new(KvStateMachine::in_memory());
let node = OctopiiNode::new_with_state_machine(config, runtime, sm).await?;

// Use the state machine
node.propose(b"SET mykey myvalue".to_vec()).await?;
let value = node.query(b"GET mykey").await?;
```

---

### Custom State Machine Example

#### Counter State Machine

```rust
use octopii::StateMachineTrait;
use bytes::Bytes;
use std::sync::RwLock;

pub struct CounterStateMachine {
    counter: RwLock<i64>,
}

impl CounterStateMachine {
    pub fn new() -> Self {
        Self {
            counter: RwLock::new(0),
        }
    }
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd = String::from_utf8(command.to_vec())
            .map_err(|e| e.to_string())?;

        match cmd.as_str() {
            "INCREMENT" => {
                let mut counter = self.counter.write().unwrap();
                *counter += 1;
                Ok(Bytes::from(counter.to_string()))
            }
            "DECREMENT" => {
                let mut counter = self.counter.write().unwrap();
                *counter -= 1;
                Ok(Bytes::from(counter.to_string()))
            }
            "GET" => {
                let counter = self.counter.read().unwrap();
                Ok(Bytes::from(counter.to_string()))
            }
            _ => Err("Unknown command".into()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let counter = self.counter.read().unwrap();
        counter.to_le_bytes().to_vec()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        if data.len() != 8 {
            return Err("Invalid snapshot data".into());
        }
        let bytes: [u8; 8] = data.try_into()
            .map_err(|_| "Invalid snapshot data")?;
        *self.counter.write().unwrap() = i64::from_le_bytes(bytes);
        Ok(())
    }
}
```

**Usage:**
```rust
let sm = Arc::new(CounterStateMachine::new());
let node = OctopiiNode::new_with_state_machine(config, runtime, sm).await?;

node.propose(b"INCREMENT".to_vec()).await?; // Returns "1"
node.propose(b"INCREMENT".to_vec()).await?; // Returns "2"
let count = node.query(b"GET").await?; // Returns "2"
```

---

## Error Handling

### Overview

Octopii uses a comprehensive error type that covers all failure modes.

**Location:** `src/error.rs`

### API Reference

```rust
pub enum OctopiiError {
    Io(std::io::Error),
    Serialization(bincode::Error),
    QuicConnection(quinn::ConnectionError),
    QuicWrite(quinn::WriteError),
    QuicRead(quinn::ReadError),
    Wal(String),
    Transport(String),
    Rpc(String),
    NodeNotFound(u64),
}

pub type Result<T> = std::result::Result<T, OctopiiError>;
```

### Error Variants

#### `Io(std::io::Error)`

File system or network I/O errors.

**Common Causes:**
- File access errors
- Directory creation failures
- Network socket errors

---

#### `Serialization(bincode::Error)`

Serialization/deserialization errors.

**Common Causes:**
- Corrupted data
- Version mismatches
- Invalid message formats

---

#### `QuicConnection(quinn::ConnectionError)`

QUIC connection-level errors.

**Common Causes:**
- Connection timeout
- Connection closed by peer
- TLS handshake failure

---

#### `QuicWrite(quinn::WriteError)`

QUIC stream write errors.

**Common Causes:**
- Stream closed
- Connection lost during write

---

#### `QuicRead(quinn::ReadError)`

QUIC stream read errors.

**Common Causes:**
- Stream closed
- Connection lost during read
- Reset by peer

---

#### `Wal(String)`

Write-Ahead Log errors.

**Common Causes:**
- Disk full
- Corruption detected
- Fsync failure

---

#### `Transport(String)`

Transport layer errors.

**Common Causes:**
- Peer unreachable
- Connection pool exhaustion

---

#### `Rpc(String)`

RPC framework errors.

**Common Causes:**
- Request timeout
- Invalid response
- Peer not responding

---

#### `NodeNotFound(u64)`

Node with specified ID not found.

**Common Causes:**
- Invalid peer ID
- Peer not in cluster configuration

---

### Error Handling Patterns

#### Basic Error Handling

```rust
match node.propose(command).await {
    Ok(response) => println!("Success: {:?}", response),
    Err(OctopiiError::Rpc(msg)) => eprintln!("RPC failed: {}", msg),
    Err(OctopiiError::Wal(msg)) => eprintln!("WAL error: {}", msg),
    Err(e) => eprintln!("Other error: {:?}", e),
}
```

#### Retry Logic

```rust
use tokio::time::{sleep, Duration};

async fn propose_with_retry(
    node: &OctopiiNode,
    command: Vec<u8>,
    max_retries: usize,
) -> Result<Bytes> {
    for attempt in 0..max_retries {
        match node.propose(command.clone()).await {
            Ok(response) => return Ok(response),
            Err(e) => {
                eprintln!("Attempt {} failed: {:?}", attempt + 1, e);
                if attempt + 1 < max_retries {
                    sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    Err(OctopiiError::Rpc("Max retries exceeded".into()))
}
```

---

## RPC Framework

### Overview

The RPC framework provides request/response and one-way messaging over QUIC.

**Location:** `src/rpc/`

### API Reference

#### RpcHandler

```rust
pub struct RpcHandler {
    pub async fn new(transport: Arc<QuicTransport>) -> Self

    pub async fn set_request_handler<F>(&self, handler: F)
    where F: Fn(RpcRequest) -> ResponsePayload + Send + Sync + 'static

    pub async fn request(
        &self,
        addr: SocketAddr,
        payload: RequestPayload,
        timeout: Duration,
    ) -> Result<RpcResponse>

    pub async fn send_one_way(
        &self,
        addr: SocketAddr,
        message: OneWayMessage,
    ) -> Result<()>
}
```

---

### Message Types

#### RpcMessage

```rust
pub enum RpcMessage {
    Request(RpcRequest),
    Response(RpcResponse),
    OneWay(OneWayMessage),
}
```

---

#### RpcRequest

```rust
pub struct RpcRequest {
    pub message_id: MessageId,
    pub payload: RequestPayload,
}

pub enum RequestPayload {
    RaftMessage {
        message: Bytes,
    },
    OpenRaft {
        kind: String,
        data: Bytes,
    },
    Custom {
        operation: String,
        data: Bytes,
    },
}
```

**Usage:**
```rust
let payload = RequestPayload::Custom {
    operation: "my_operation".into(),
    data: Bytes::from("data"),
};

let response = rpc_handler.request(
    peer_addr,
    payload,
    Duration::from_secs(5),
).await?;
```

---

#### RpcResponse

```rust
pub struct RpcResponse {
    pub message_id: MessageId,
    pub payload: ResponsePayload,
}

pub enum ResponsePayload {
    AppendEntriesResponse {
        term: u64,
        success: bool,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    SnapshotResponse {
        term: u64,
        success: bool,
    },
    OpenRaft {
        kind: String,
        data: Bytes,
    },
    CustomResponse {
        success: bool,
        data: Bytes,
    },
    Error {
        message: String,
    },
}
```

---

#### OneWayMessage

```rust
pub enum OneWayMessage {
    Heartbeat {
        node_id: u64,
        timestamp: u64,
    },
    Custom {
        operation: String,
        data: Bytes,
    },
}
```

**Usage:**
```rust
let heartbeat = OneWayMessage::Heartbeat {
    node_id: 1,
    timestamp: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs(),
};

rpc_handler.send_one_way(peer_addr, heartbeat).await?;
```

---

### Utility Functions

```rust
pub fn serialize<T: Serialize>(msg: &T) -> Result<Bytes>
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T>
```

**Example:**
```rust
use octopii::rpc::{serialize, deserialize};

let data = serialize(&my_struct)?;
let recovered: MyStruct = deserialize(&data)?;
```

---

## Transport Layer

### Overview

QUIC-based transport with connection pooling and automatic reconnection.

**Location:** `src/transport/`

### API Reference

#### QuicTransport

```rust
pub struct QuicTransport {
    pub async fn new(bind_addr: SocketAddr) -> Result<Self>

    pub async fn connect(&self, addr: SocketAddr) -> Result<Arc<PeerConnection>>
    pub async fn accept(&self) -> Result<(SocketAddr, Arc<PeerConnection>)>
    pub async fn send(&self, addr: SocketAddr, data: Bytes) -> Result<()>

    pub fn local_addr(&self) -> Result<SocketAddr>
    pub fn close(&self)
    pub async fn has_active_peer(&self, addr: SocketAddr) -> bool
}
```

##### Methods

**`new(bind_addr: SocketAddr) -> Result<Self>`**

Creates a new QUIC transport.

**Example:**
```rust
let transport = QuicTransport::new("127.0.0.1:5000".parse()?).await?;
```

---

**`connect(&self, addr: SocketAddr) -> Result<Arc<PeerConnection>>`**

Establishes a connection to a peer (or returns existing connection).

**Example:**
```rust
let peer = transport.connect("127.0.0.1:5001".parse()?).await?;
```

---

**`accept(&self) -> Result<(SocketAddr, Arc<PeerConnection>)>`**

Accepts an incoming connection.

**Example:**
```rust
let (peer_addr, connection) = transport.accept().await?;
println!("Connection from: {}", peer_addr);
```

---

**`send(&self, addr: SocketAddr, data: Bytes) -> Result<()>`**

Sends data to a peer.

**Example:**
```rust
transport.send(peer_addr, Bytes::from("hello")).await?;
```

---

#### PeerConnection

```rust
pub struct PeerConnection {
    pub async fn send(&self, data: Bytes) -> Result<()>
    pub async fn recv(&self) -> Result<Option<Bytes>>

    pub fn is_closed(&self) -> bool
    pub fn stats(&self) -> quinn::ConnectionStats

    pub async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64>
    pub async fn recv_chunk_verified(&self) -> Result<Option<Bytes>>
}
```

##### Methods

**`send(&self, data: Bytes) -> Result<()>`**

Sends data over this connection.

**Example:**
```rust
peer.send(Bytes::from("message")).await?;
```

---

**`recv(&self) -> Result<Option<Bytes>>`**

Receives data from this connection.

**Returns:**
- `Ok(Some(Bytes))`: Data received
- `Ok(None)`: Connection closed
- `Err(...)`: Error occurred

**Example:**
```rust
match peer.recv().await? {
    Some(data) => println!("Received: {:?}", data),
    None => println!("Connection closed"),
}
```

---

**`is_closed(&self) -> bool`**

Checks if connection is closed.

**Example:**
```rust
if peer.is_closed() {
    println!("Connection is closed");
}
```

---

**`stats(&self) -> quinn::ConnectionStats`**

Returns connection statistics.

**Example:**
```rust
let stats = peer.stats();
println!("RTT: {:?}", stats.path.rtt);
```

---

## Data Transfer

### Overview

Efficient transfer of large data blocks with checksum verification.

**Location:** `src/chunk.rs`

### API Reference

#### ChunkSource

```rust
pub enum ChunkSource {
    File(PathBuf),
    Memory(Bytes),
}
```

**Usage:**
```rust
// From file
let chunk = ChunkSource::File(PathBuf::from("large_file.dat"));

// From memory
let chunk = ChunkSource::Memory(Bytes::from("data"));
```

---

#### TransferResult

```rust
pub struct TransferResult {
    pub peer: SocketAddr,
    pub success: bool,
    pub bytes_transferred: u64,
    pub checksum_verified: bool,
    pub duration: Duration,
    pub error: Option<String>,
}
```

---

#### Chunk Transfer Methods

**`send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64>`**

Sends a chunk with SHA-256 checksum verification.

**Parameters:**
- `chunk`: Data to send (file or memory)

**Returns:** Number of bytes transferred

**Protocol:**
1. Send: [8 bytes: size] [N bytes: data] [32 bytes: SHA256]
2. Receive: [1 byte: status] (0=OK, 1=checksum_fail, 2=error)

**Example:**
```rust
let chunk = ChunkSource::Memory(Bytes::from(vec![0u8; 1_000_000]));
let bytes_sent = peer.send_chunk_verified(&chunk).await?;
println!("Transferred {} bytes", bytes_sent);
```

---

**`recv_chunk_verified(&self) -> Result<Option<Bytes>>`**

Receives a chunk and verifies checksum.

**Returns:**
- `Ok(Some(Bytes))`: Data received and verified
- `Ok(None)`: Connection closed
- `Err(...)`: Transfer or verification failed

**Example:**
```rust
match peer.recv_chunk_verified().await? {
    Some(data) => println!("Received {} bytes", data.len()),
    None => println!("No more data"),
}
```

---

## Write-Ahead Log

### Overview

Durable persistence using the Walrus backend.

**Location:** `src/wal/`

### API Reference

```rust
pub struct WriteAheadLog {
    pub async fn new(
        path: PathBuf,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Result<Self>

    pub async fn append(&self, data: Bytes) -> Result<u64>
    pub async fn flush(&self) -> Result<()>
    pub async fn read_all(&self) -> Result<Vec<Bytes>>
}
```

### Methods

**`new(path, batch_size, flush_interval) -> Result<Self>`**

Creates a new WAL.

**Parameters:**
- `path`: Directory for WAL files
- `batch_size`: Entries to batch before fsync
- `flush_interval`: Maximum time between fsyncs

**Example:**
```rust
let wal = WriteAheadLog::new(
    PathBuf::from("./wal"),
    100,
    Duration::from_millis(100),
).await?;
```

---

**`append(&self, data: Bytes) -> Result<u64>`**

Appends an entry to the log.

**Returns:** Entry offset

**Example:**
```rust
let offset = wal.append(Bytes::from("entry")).await?;
```

---

**`flush(&self) -> Result<()>`**

Forces an fsync.

**Example:**
```rust
wal.flush().await?;
```

---

**`read_all(&self) -> Result<Vec<Bytes>>`**

Reads all entries from the log.

**Example:**
```rust
let entries = wal.read_all().await?;
for entry in entries {
    println!("Entry: {:?}", entry);
}
```

---

## Usage Examples

### Example 1: Single-Node Setup

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);

    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5000".parse()?,
        peers: vec![],
        is_initial_leader: true,
        ..Default::default()
    };

    let node = OctopiiNode::new(config, runtime).await?;
    node.start().await?;

    // Write
    node.propose(b"SET key1 value1".to_vec()).await?;

    // Read
    let value = node.query(b"GET key1").await?;
    println!("Value: {:?}", String::from_utf8(value.to_vec()));

    Ok(())
}
```

### Example 2: Three-Node Cluster

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::path::PathBuf;

async fn start_node(node_id: u64, port: u16, is_leader: bool)
    -> Result<OctopiiNode, Box<dyn std::error::Error>>
{
    let runtime = OctopiiRuntime::new(4);

    let mut peers = vec![
        "127.0.0.1:5001".parse()?,
        "127.0.0.1:5002".parse()?,
        "127.0.0.1:5003".parse()?,
    ];

    // Remove self from peers
    let self_addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    peers.retain(|&addr| addr != self_addr);

    let config = Config {
        node_id,
        bind_addr: self_addr,
        peers,
        wal_dir: PathBuf::from(format!("./data/node{}", node_id)),
        is_initial_leader: is_leader,
        ..Default::default()
    };

    let node = OctopiiNode::new(config, runtime).await?;
    node.start().await?;

    Ok(node)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start three nodes
    let node1 = start_node(1, 5001, true).await?;  // Leader
    let node2 = start_node(2, 5002, false).await?;
    let node3 = start_node(3, 5003, false).await?;

    // Wait for cluster to stabilize
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Propose on leader
    node1.propose(b"SET key value".to_vec()).await?;

    // Query from follower
    let value = node2.query(b"GET key").await?;
    println!("Value from follower: {:?}", String::from_utf8(value.to_vec()));

    Ok(())
}
```

### Example 3: Dynamic Membership

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start initial cluster (nodes 1 and 2)
    let node1 = create_node(1, 5001, true).await?;
    let node2 = create_node(2, 5002, false).await?;

    sleep(Duration::from_secs(1)).await;

    // Add node 3 as learner
    let node3_addr: SocketAddr = "127.0.0.1:5003".parse()?;
    node1.add_learner(3, node3_addr).await?;

    // Start node 3
    let node3 = create_node(3, 5003, false).await?;

    // Wait for node 3 to catch up
    loop {
        if node1.is_learner_caught_up(3).await? {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Promote node 3 to voter
    node1.promote_learner(3).await?;
    println!("Node 3 is now a voting member");

    Ok(())
}
```

### Example 4: Custom State Machine

```rust
use octopii::{StateMachineTrait, Config, OctopiiNode, OctopiiRuntime};
use bytes::Bytes;
use std::sync::RwLock;
use std::collections::HashMap;

struct RegistryStateMachine {
    registry: RwLock<HashMap<String, String>>,
}

impl StateMachineTrait for RegistryStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd_str = String::from_utf8(command.to_vec())
            .map_err(|e| e.to_string())?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.get(0).map(|s| *s) {
            Some("REGISTER") if parts.len() == 3 => {
                let key = parts[1].to_string();
                let value = parts[2].to_string();
                self.registry.write().unwrap().insert(key, value);
                Ok(Bytes::from("REGISTERED"))
            }
            Some("LOOKUP") if parts.len() == 2 => {
                let key = parts[1];
                match self.registry.read().unwrap().get(key) {
                    Some(value) => Ok(Bytes::from(value.clone())),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            _ => Err("Invalid command".into()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let registry = self.registry.read().unwrap();
        serde_json::to_vec(&*registry).unwrap()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let registry: HashMap<String, String> = serde_json::from_slice(data)
            .map_err(|e| e.to_string())?;
        *self.registry.write().unwrap() = registry;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);
    let config = Config::default();

    let sm = Arc::new(RegistryStateMachine {
        registry: RwLock::new(HashMap::new()),
    });

    let node = OctopiiNode::new_with_state_machine(config, runtime, sm).await?;
    node.start().await?;

    // Register a service
    node.propose(b"REGISTER api-server 192.168.1.100".to_vec()).await?;

    // Lookup a service
    let addr = node.query(b"LOOKUP api-server").await?;
    println!("API Server: {}", String::from_utf8(addr.to_vec())?);

    Ok(())
}
```

### Example 5: Error Handling and Retries

```rust
use octopii::{OctopiiNode, OctopiiError, Result};
use tokio::time::{sleep, Duration};

async fn propose_with_retry(
    node: &OctopiiNode,
    command: Vec<u8>,
    max_retries: usize,
) -> Result<Bytes> {
    let mut retry_delay = Duration::from_millis(100);

    for attempt in 0..max_retries {
        match node.propose(command.clone()).await {
            Ok(response) => return Ok(response),
            Err(OctopiiError::Rpc(msg)) if attempt + 1 < max_retries => {
                eprintln!("RPC error (attempt {}): {}", attempt + 1, msg);
                sleep(retry_delay).await;
                retry_delay *= 2; // Exponential backoff
            }
            Err(e) => return Err(e),
        }
    }

    Err(OctopiiError::Rpc("Max retries exceeded".into()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = create_node().await?;

    match propose_with_retry(&node, b"SET key value".to_vec(), 3).await {
        Ok(response) => println!("Success: {:?}", response),
        Err(e) => eprintln!("Failed after retries: {:?}", e),
    }

    Ok(())
}
```

---

## Feature Flags

Octopii supports optional features via Cargo feature flags.

### Available Features

#### `openraft` (default)

Enables OpenRaft integration and the `OctopiiNode` API.

```toml
[dependencies]
octopii = { version = "*", features = ["openraft"] }
```

#### `openraft-filters`

Enables network failure simulation for testing.

```toml
[dependencies]
octopii = { version = "*", features = ["openraft", "openraft-filters"] }
```

**Usage:**
- Simulate network delays
- Test partition tolerance
- Verify recovery mechanisms

---

## Performance Tuning

### Throughput Optimization

```rust
Config {
    worker_threads: num_cpus::get(),
    wal_batch_size: 1000,
    wal_flush_interval_ms: 500,
    ..Default::default()
}
```

### Latency Optimization

```rust
Config {
    wal_batch_size: 1,
    wal_flush_interval_ms: 1,
    ..Default::default()
}
```

### Snapshot Tuning

```rust
Config {
    snapshot_lag_threshold: 1000, // Lower for faster catch-up
    ..Default::default()
}
```

---

## Best Practices

1. **Always use `is_initial_leader: true` on exactly ONE node** when bootstrapping a cluster
2. **Set unique `node_id` for each node**
3. **Use separate `wal_dir` for each node**
4. **Implement deterministic state machines**
5. **Handle errors gracefully** with retries where appropriate
6. **Monitor leader status** before writes
7. **Use learner promotion** for safe membership changes
8. **Tune WAL settings** based on workload (throughput vs latency)
9. **Keep snapshots small** for faster transfers
10. **Test partition tolerance** using `openraft-filters`

---

## Troubleshooting

### Common Issues

**Problem:** Node fails to start
**Solution:** Check that `wal_dir` is writable and `bind_addr` is not in use

**Problem:** Elections never complete
**Solution:** Ensure all peers can communicate and at least one node has `is_initial_leader: true`

**Problem:** Proposal returns error
**Solution:** Check if node is leader with `is_leader()`, wait for leader election

**Problem:** High latency on writes
**Solution:** Reduce `wal_batch_size` and `wal_flush_interval_ms`

**Problem:** High CPU usage
**Solution:** Reduce `worker_threads` or optimize state machine `apply()` method

---

## Additional Resources

- **Raft Paper:** [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- **QUIC Protocol:** [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html)
- **OpenRaft Documentation:** [openraft.rs](https://docs.rs/openraft/)

---

**Version:** 0.1.0
**Last Updated:** 2025-11-13
