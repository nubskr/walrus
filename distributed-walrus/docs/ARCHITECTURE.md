# Distributed Walrus Architecture

## Overview

Distributed Walrus is a distributed log system that provides Kafka-compatible APIs backed by replicated storage. It combines three key components:

- **Walrus**: High-performance Write-Ahead Log for local storage
- **Octopii**: Raft consensus framework for cluster coordination
- **Kafka Protocol**: Wire-compatible API for client integration

The system implements leader-based replication with automatic failover, partition management, and garbage collection.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Distributed Walrus Cluster                       │
│                                                                           │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
│  │   Node 1         │  │   Node 2         │  │   Node 3         │      │
│  │  (Leader)        │  │  (Follower)      │  │  (Follower)      │      │
│  │                  │  │                  │  │                  │      │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │      │
│  │  │   Kafka    │  │  │  │   Kafka    │  │  │  │   Kafka    │  │      │
│  │  │  Facade    │  │  │  │  Facade    │  │  │  │  Facade    │  │      │
│  │  │ :9092      │  │  │  │ :9092      │  │  │  │ :9092      │  │      │
│  │  └─────┬──────┘  │  │  └─────┬──────┘  │  │  └─────┬──────┘  │      │
│  │        │         │  │        │         │  │        │         │      │
│  │        ▼         │  │        ▼         │  │        ▼         │      │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │      │
│  │  │   Node     │◄─┼──┼─►│   Node     │◄─┼──┼─►│   Node     │  │      │
│  │  │ Controller │  │  │  │ Controller │  │  │  │ Controller │  │      │
│  │  └──┬──┬──┬───┘  │  │  └──┬──┬──┬───┘  │  │  └──┬──┬──┬───┘  │      │
│  │     │  │  │      │  │     │  │  │      │  │     │  │  │      │      │
│  │     │  │  └──────┼──┼─────┼──┼──┼──────┼──┼─────┼──┼──┼────► Monitor │
│  │     │  │         │  │     │  │  │      │  │     │  │  │      │      │
│  │     │  │         │  │     │  │  │      │  │     │  │  │      │      │
│  │  ┌──▼──▼──────┐  │  │  ┌──▼──▼──────┐  │  │  ┌──▼──▼──────┐  │      │
│  │  │  Metadata  │  │  │  │  Metadata  │  │  │  │  Metadata  │  │      │
│  │  │   State    │◄─┼──┼─►│   State    │◄─┼──┼─►│   State    │  │      │
│  │  │  Machine   │  │  │  │  Machine   │  │  │  │  Machine   │  │      │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │      │
│  │        ▲         │  │        ▲         │  │        ▲         │      │
│  │        │         │  │        │         │  │        │         │      │
│  │        │         │  │        │         │  │        │         │      │
│  │  ┌─────▼──────┐  │  │  ┌─────▼──────┐  │  │  ┌─────▼──────┐  │      │
│  │  │  Octopii   │  │  │  │  Octopii   │  │  │  │  Octopii   │  │      │
│  │  │   (Raft)   │◄─┼──┼─►│   (Raft)   │◄─┼──┼─►│   (Raft)   │  │      │
│  │  │  :6000     │  │  │  │  :6000     │  │  │  │  :6000     │  │      │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │      │
│  │                  │  │                  │  │                  │      │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │      │
│  │  │   Bucket   │  │  │  │   Bucket   │  │  │  │   Bucket   │  │      │
│  │  │  Service   │  │  │  │  Service   │  │  │  │  Service   │  │      │
│  │  └─────┬──────┘  │  │  └─────┬──────┘  │  │  └─────┬──────┘  │      │
│  │        │         │  │        │         │  │        │         │      │
│  │        ▼         │  │        ▼         │  │        ▼         │      │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │      │
│  │  │   Walrus   │  │  │  │   Walrus   │  │  │  │   Walrus   │  │      │
│  │  │    WAL     │  │  │  │    WAL     │  │  │  │    WAL     │  │      │
│  │  │  Storage   │  │  │  │  Storage   │  │  │  │  Storage   │  │      │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. NodeController

The `NodeController` is the central coordinator that orchestrates all operations within a node.

**Responsibilities:**
- Routes read/write requests to appropriate partition leaders
- Manages RPC forwarding between nodes
- Synchronizes leases with metadata state
- Handles cluster membership changes (join/leave)
- Tracks high watermarks for each partition

**Key Fields:**
```rust
pub struct NodeController {
    pub node_id: NodeId,                          // This node's identity
    pub bucket: Arc<BucketService>,               // Local storage interface
    pub metadata: Arc<MetadataStateMachine>,      // Replicated cluster state
    pub raft: Arc<OctopiiNode>,                   // Raft consensus engine
    pub offsets: Arc<RwLock<HashMap<String, u64>>>, // High watermark tracking
}
```

**Write Path Flow:**
```
Client Write Request
       │
       ▼
  route_and_append()
       │
       ├──► Is this node the leader? ──► YES ──► append_with_retry()
       │                                              │
       │                                              ▼
       │                                         BucketService
       │                                              │
       │                                              ▼
       │                                         Walrus WAL
       │
       └──► NO ──► Forward to leader via RPC
                         │
                         ▼
                   Leader node's
                   handle_rpc()
```

**Read Path Flow:**
```
Client Read Request
       │
       ▼
  route_and_read()
       │
       ├──► Is this node the leader? ──► YES ──► BucketService.read_by_key()
       │                                              │
       │                                              ▼
       │                                         Walrus WAL
       │                                              │
       │                                              ▼
       │                                      Return entries + watermark
       │
       └──► NO ──► Forward to leader via RPC
                         │
                         ▼
                   Leader node's
                   ForwardRead handler
                         │
                         ▼
                   Return entries + watermark
```

---

### 2. BucketService

The `BucketService` wraps the Walrus WAL with lease-based fencing to prevent split-brain writes.

**Lease Mechanism:**
```
┌─────────────────────────────────────────────────────────────────┐
│                      Lease Lifecycle                             │
│                                                                   │
│  Metadata State Machine              BucketService               │
│  ┌────────────────┐                  ┌─────────────┐            │
│  │ Partition 0    │                  │   Active    │            │
│  │ Leader: Node 1 │ ──sync_leases──► │   Leases    │            │
│  │ Generation: 5  │                  │   Set       │            │
│  └────────────────┘                  └──────┬──────┘            │
│                                              │                    │
│                                              ▼                    │
│                                       Check lease                 │
│                                       before write                │
│                                              │                    │
│                           ┌──────────────────┴────────────────┐  │
│                           │                                    │  │
│                           ▼                                    ▼  │
│                    Has Lease?                            No Lease │
│                           │                                    │  │
│                           │                                    ▼  │
│                           ▼                              Reject   │
│                      Allow Write                         Write!   │
│                           │                                       │
│                           ▼                                       │
│                      Walrus WAL                                   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Key Operations:**
- `grant_lease()`: Allows writes for a partition/generation
- `revoke_lease()`: Prevents writes for a partition/generation
- `sync_leases()`: Updates active leases from metadata state
- `append_by_key()`: Write with lease validation
- `read_by_key()`: Read without lease check

**WAL Key Format:**
```
t_{topic}_p_{partition}_g_{generation}

Example: t_logs_p_0_g_5
         └─topic─┘ └part┘ └gen┘
```

---

### 3. MetadataStateMachine

Replicated state machine managing cluster-wide metadata via Raft consensus.

**State Structure:**
```
ClusterState
│
├── topics: HashMap<TopicName, TopicInfo>
│   │
│   └── TopicInfo
│       ├── partitions: u32
│       └── partition_states: HashMap<u32, PartitionState>
│           │
│           └── PartitionState
│               ├── current_generation: u64
│               └── leader_node: NodeId
```

**Commands:**
```
┌──────────────────────────────────────────────────────────────────┐
│                     Metadata Commands                             │
│                                                                    │
│  1. CreateTopic                                                    │
│     ┌─────────────────────────────────────────────────┐          │
│     │ name: "logs"                                     │          │
│     │ partitions: 2                                    │          │
│     │ initial_leader: 1                                │          │
│     └─────────────────┬───────────────────────────────┘          │
│                       │                                            │
│                       ▼                                            │
│              Create TopicInfo                                      │
│              Initialize all partitions                             │
│              with generation=1                                     │
│                                                                    │
│  2. RolloverPartition                                              │
│     ┌─────────────────────────────────────────────────┐          │
│     │ name: "logs"                                     │          │
│     │ partition: 0                                     │          │
│     │ new_leader: 2                                    │          │
│     └─────────────────┬───────────────────────────────┘          │
│                       │                                            │
│                       ▼                                            │
│              Increment generation                                  │
│              Update leader assignment                              │
│              (triggers lease sync)                                 │
│                                                                    │
└──────────────────────────────────────────────────────────────────┘
```

**Query Operations:**
- `get_partition_leader(topic, partition)`: Returns (leader_id, generation)
- `assignments_for_node(node_id)`: Returns list of (topic, partition, generation) this node leads
- `reassign_leader(removed_node, new_leader)`: Moves leadership away from failed node

**Snapshot & Restore:**
The state machine implements Octopii's `StateMachineTrait` for persistence:
- `snapshot()`: Serializes entire ClusterState via bincode
- `restore()`: Deserializes and replaces state
- Applied to Raft log for crash recovery

---

### 4. Monitor

Background task monitoring disk usage and performing maintenance.

**Monitoring Loop:**
```
┌────────────────────────────────────────────────────────────────┐
│                     Monitor Loop (every 10s)                    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  check_rollovers()                                        │  │
│  │  ┌─────────────────────────────────────────────────┐     │  │
│  │  │ 1. Measure data namespace directory size        │     │  │
│  │  │                                                  │     │  │
│  │  │ 2. If size > MAX_SEGMENT_SIZE (1GB default)     │     │  │
│  │  │    ├─► Get all partitions I lead                │     │  │
│  │  │    └─► Propose RolloverPartition for each       │     │  │
│  │  │                                                  │     │  │
│  │  │ 3. Rollover increments generation                │     │  │
│  │  │    └─► New writes go to new generation          │     │  │
│  │  │    └─► Old generation becomes read-only         │     │  │
│  │  └─────────────────────────────────────────────────┘     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  run_gc()                                                 │  │
│  │  ┌─────────────────────────────────────────────────┐     │  │
│  │  │ 1. Get current cluster state                    │     │  │
│  │  │                                                  │     │  │
│  │  │ 2. For each partition:                          │     │  │
│  │  │    if current_generation > RETENTION (10)       │     │  │
│  │  │    └─► Delete generations < (current - 10)      │     │  │
│  │  │                                                  │     │  │
│  │  │ 3. Remove old WAL segment directories           │     │  │
│  │  │    └─► Free disk space                          │     │  │
│  │  └─────────────────────────────────────────────────┘     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└────────────────────────────────────────────────────────────────┘
```

**Configuration (Environment Variables):**
- `WALRUS_MONITOR_CHECK_MS`: Check interval (default: 10000ms)
- `WALRUS_MAX_SEGMENT_BYTES`: Rollover threshold (default: 1GB)
- `WALRUS_RETENTION_GENERATIONS`: Keep last N generations (default: 10)

**Generation Lifecycle:**
```
Generation 1 (active)
      │
      ├─► Disk usage exceeds threshold
      │
      ▼
Propose RolloverPartition
      │
      ▼
Generation 2 (active) | Generation 1 (read-only)
      │
      ├─► More rollovers...
      │
      ▼
Generation 12 (active) | Gen 11, 10, 9... Gen 2 (read-only)
      │
      ├─► GC detects Gen 2 is old (current - 10 = 2)
      │
      ▼
Delete Generation 2 directory
      │
      ▼
Generation 12 (active) | Gen 11, 10, 9... Gen 3 (read-only)
```

---

### 5. Kafka Facade

Provides wire-compatible Kafka protocol for client integration.

**Supported API Keys:**
```
┌────────────────────────────────────────────────────────────────┐
│                     Kafka Protocol APIs                         │
│                                                                  │
│  Standard APIs:                                                  │
│  ┌────────────────────────────────────────────────────────────┐│
│  │  0  - Produce       │ Write records to partition          │││
│  │  1  - Fetch         │ Read records from partition         │││
│  │  3  - Metadata      │ Get cluster topology                │││
│  │  18 - ApiVersions   │ List supported APIs                 │││
│  │  19 - CreateTopics  │ Create new topics                   │││
│  └────────────────────────────────────────────────────────────┘│
│                                                                  │
│  Internal/Test APIs:                                             │
│  ┌────────────────────────────────────────────────────────────┐│
│  │  50 - InternalState │ Query partition generations         │││
│  │  51 - Membership    │ Remove nodes                        │││
│  │  60 - TestControl   │ Inject test failures                │││
│  │  61-63              │ Force monitor/GC errors             │││
│  └────────────────────────────────────────────────────────────┘│
│                                                                  │
└────────────────────────────────────────────────────────────────┘
```

**Request Processing:**
```
TCP Connection (port 9092)
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Frame Decoder                                                 │
│ ┌────────────┐                                                │
│ │ [4B len]   │  ──► Read frame length                        │
│ └────────────┘                                                │
│ ┌────────────┐                                                │
│ │ [N bytes]  │  ──► Read frame payload                       │
│ └────────────┘                                                │
└────────────┬─────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────┐
│ RequestHeader Decode                                          │
│ ┌───────────────────────────────────────────────────────────┐│
│ │ api_key: i16         │ Which API?                        │││
│ │ api_version: i16     │ Protocol version                  │││
│ │ correlation_id: i32  │ Match request/response            │││
│ │ client_id: String    │ Client identifier                 │││
│ └───────────────────────────────────────────────────────────┘│
└────────────┬─────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────┐
│ Route to handler based on api_key                             │
│                                                                │
│  api_key == 0 (Produce)                                        │
│       │                                                        │
│       ├─► Decode: topic, partition, record_set                │
│       │                                                        │
│       └─► controller.route_and_append(...)                    │
│                                                                │
│  api_key == 1 (Fetch)                                          │
│       │                                                        │
│       ├─► Decode: topic, partition, offset, max_bytes         │
│       │                                                        │
│       └─► controller.route_and_read(...)                      │
│                                                                │
└────────────┬─────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────┐
│ Encode Response                                                │
│ ┌────────────┐                                                │
│ │ [4B len]   │  ──► Response length                          │
│ └────────────┘                                                │
│ ┌────────────────────────────────────────────────────────────┐│
│ │ [correlation_id][error_code][response_body]               ││
│ └────────────────────────────────────────────────────────────┘│
└────────────┬─────────────────────────────────────────────────┘
             │
             ▼
       Write to socket
```

**Metadata Response Format:**
```
encode_metadata_response():
  ┌──────────────────────────────────────────────────┐
  │ Broker Info:                                      │
  │   broker_id: node_id                              │
  │   host: 127.0.0.1                                 │
  │   port: kafka_port                                │
  └──────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────┐
  │ Topics: [                                         │
  │   {                                               │
  │     name: "logs",                                 │
  │     partitions: [                                 │
  │       {                                           │
  │         partition_id: 0,                          │
  │         leader: 1,                                │
  │         replicas: [1],                            │
  │         isr: [1]                                  │
  │       }                                           │
  │     ]                                             │
  │   }                                               │
  │ ]                                                 │
  └──────────────────────────────────────────────────┘
```

---

## Data Flow Diagrams

### Write Operation (Client → Replicated Storage)

```
┌─────────┐
│ Client  │
│(Kafka)  │
└────┬────┘
     │ Produce Request
     │ topic="logs", partition=0, data=[...]
     ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 2 (Follower)                                             │
│                                                                │
│  Kafka Facade (:9092)                                          │
│    │                                                           │
│    ├─► Decode Produce Request                                 │
│    │                                                           │
│    ▼                                                           │
│  NodeController.route_and_append()                             │
│    │                                                           │
│    ├─► Query metadata: Who leads partition 0?                 │
│    │   Answer: Node 1, generation 5                           │
│    │                                                           │
│    ├─► Get Node 1's RPC address from Raft peers               │
│    │                                                           │
│    └─► Forward via RPC: InternalOp::ForwardAppend             │
│          wal_key="t_logs_p_0_g_5"                             │
│          data=[...]                                            │
└────────────────────────────┬─────────────────────────────────┘
                             │ RPC over QUIC (:6000)
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 1 (Leader)                                               │
│                                                                │
│  Octopii RPC Handler                                           │
│    │                                                           │
│    ├─► Receive Custom RPC "Forward"                           │
│    │                                                           │
│    ▼                                                           │
│  NodeController.handle_rpc(ForwardAppend)                      │
│    │                                                           │
│    ├─► sync_leases_now()  (ensure lease is active)            │
│    │                                                           │
│    ├─► append_with_retry("t_logs_p_0_g_5", data)              │
│    │                                                           │
│    ▼                                                           │
│  BucketService.append_by_key()                                 │
│    │                                                           │
│    ├─► Check: active_leases contains "t_logs_p_0_g_5"?        │
│    │   ✓ YES → proceed                                        │
│    │   ✗ NO  → return NotLeaderForPartition error             │
│    │                                                           │
│    ▼                                                           │
│  Walrus WAL                                                    │
│    │                                                           │
│    ├─► batch_append_for_topic("t_logs_p_0_g_5", [data])       │
│    │   (io_uring on Linux for batched writes)                 │
│    │                                                           │
│    └─► fsync (based on FsyncSchedule)                         │
│                                                                │
│  NodeController                                                │
│    │                                                           │
│    └─► record_append() - increment high watermark             │
│                                                                │
│  Return InternalResp::Ok                                       │
└────────────────────────────┬─────────────────────────────────┘
                             │ RPC Response
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 2 (Follower)                                             │
│                                                                │
│  NodeController.route_and_append()                             │
│    │                                                           │
│    └─► Receive InternalResp::Ok                               │
│                                                                │
│  Kafka Facade                                                  │
│    │                                                           │
│    └─► Encode Produce Response (error_code=0)                 │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
                        ┌─────────┐
                        │ Client  │
                        │(Kafka)  │
                        └─────────┘
```

### Read Operation

```
┌─────────┐
│ Client  │
└────┬────┘
     │ Fetch Request
     │ topic="logs", partition=0, offset=0, max_bytes=1MB
     ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 3                                                        │
│                                                                │
│  Kafka Facade                                                  │
│    │                                                           │
│    ├─► Decode Fetch Request                                   │
│    │                                                           │
│    ▼                                                           │
│  NodeController.route_and_read()                               │
│    │                                                           │
│    ├─► Query metadata: Who leads partition 0?                 │
│    │   Answer: Node 1, generation 5                           │
│    │                                                           │
│    └─► Forward via RPC: InternalOp::ForwardRead               │
│          wal_key="t_logs_p_0_g_5"                             │
│          max_bytes=1048576                                     │
└────────────────────────────┬─────────────────────────────────┘
                             │ RPC
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 1 (Leader)                                               │
│                                                                │
│  NodeController.handle_rpc(ForwardRead)                        │
│    │                                                           │
│    ▼                                                           │
│  BucketService.read_by_key("t_logs_p_0_g_5", 1048576)         │
│    │                                                           │
│    │  (No lease check required for reads)                     │
│    │                                                           │
│    ▼                                                           │
│  Walrus WAL                                                    │
│    │                                                           │
│    ├─► batch_read_for_topic("t_logs_p_0_g_5", 1MB, false)     │
│    │   (io_uring on Linux for batched reads)                  │
│    │                                                           │
│    └─► Return Vec<Entry>                                      │
│                                                                │
│  NodeController                                                │
│    │                                                           │
│    ├─► current_high_watermark("t_logs_p_0_g_5")               │
│    │   → Returns tracked offset count                         │
│    │                                                           │
│    └─► Return InternalResp::ReadResult {                      │
│          data: [entry1, entry2, ...],                         │
│          high_watermark: 42                                    │
│        }                                                       │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Node 3                                                        │
│                                                                │
│  NodeController.route_and_read()                               │
│    │                                                           │
│    ├─► Extract data[offset] based on fetch_offset             │
│    │                                                           │
│    └─► Return (entries, high_watermark)                       │
│                                                                │
│  Kafka Facade                                                  │
│    │                                                           │
│    └─► Encode Fetch Response                                  │
│          error_code=0                                          │
│          high_watermark=42                                     │
│          record_set=[...]                                      │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
                        ┌─────────┐
                        │ Client  │
                        └─────────┘
```

---

## Cluster Coordination

### Lease Synchronization

```
┌────────────────────────────────────────────────────────────────┐
│               Lease Sync Loop (every 100ms)                     │
│                                                                  │
│  NodeController.run_lease_sync_loop()                            │
│    │                                                             │
│    └─► sync_leases_now()                                        │
│          │                                                       │
│          ├─► Query MetadataStateMachine                         │
│          │     assignments_for_node(this_node_id)               │
│          │     Returns: [(topic, partition, gen), ...]          │
│          │                                                       │
│          ├─► Build expected lease set:                          │
│          │     ["t_logs_p_0_g_5", "t_logs_p_1_g_3", ...]       │
│          │                                                       │
│          └─► BucketService.sync_leases(expected)                │
│                │                                                 │
│                ├─► Revoke leases not in expected set            │
│                │   (prevents writes to old generations)         │
│                │                                                 │
│                └─► Grant leases in expected set                 │
│                    (allows writes to current generations)       │
│                                                                  │
└────────────────────────────────────────────────────────────────┘
```

### Node Join Process

```
New Node (Node 3) wants to join cluster:

1. Start with join_addr pointing to existing node
   └─► --join 127.0.0.1:6000

2. New node starts up
   │
   ├─► Initialize Octopii with join target as peer
   │
   └─► Send InternalOp::JoinCluster via RPC
         node_id: 3
         addr: "127.0.0.1:6003"

3. Existing node (Leader) receives JoinCluster
   │
   ├─► NodeController.handle_join_cluster()
   │     │
   │     ├─► Resolve address
   │     │
   │     └─► raft.add_learner(3, addr)
   │           │
   │           └─► Learner receives Raft log replication
   │
   ├─► Background task monitors learner progress
   │     │
   │     └─► Every 500ms: is_learner_caught_up(3)?
   │           │
   │           └─► When caught up: raft.promote_learner(3)
   │                 │
   │                 └─► Node 3 becomes full voting member
   │
   └─► Return InternalResp::Ok

4. New node is now part of cluster
   │
   ├─► Can participate in leader election
   │
   ├─► Receives metadata updates via Raft
   │
   └─► Can be assigned partition leadership
```

### Partition Rollover Flow

```
Monitor detects high disk usage:

1. Monitor.check_rollovers()
   │
   ├─► dir_size(data_namespace) > 1GB
   │
   └─► For each partition this node leads:

2. Propose RolloverPartition command
   │
   └─► MetadataCmd::RolloverPartition {
         name: "logs",
         partition: 0,
         new_leader: 1  (self)
       }

3. Raft replicates command to all nodes

4. MetadataStateMachine.apply() on all nodes
   │
   ├─► partition_states[0].current_generation += 1  (5 → 6)
   │
   └─► partition_states[0].leader_node = 1

5. Lease sync detects change
   │
   └─► sync_leases_now()
         │
         ├─► Expected leases: ["t_logs_p_0_g_6"]
         │
         ├─► Revoke: "t_logs_p_0_g_5"
         │     └─► No more writes to generation 5
         │
         └─► Grant: "t_logs_p_0_g_6"
               └─► New writes go to generation 6

6. Old generation becomes read-only
   │
   └─► Clients can still read from gen 5
       └─► But all new writes go to gen 6

7. GC will eventually delete gen 5
   └─► When current_generation > 15 (5 + 10 retention)
```

---

## Storage Layout

### Directory Structure

```
./data/
├── node_1/
│   ├── raft_meta/              ← Octopii WAL (Raft log)
│   │   ├── manifest_*
│   │   ├── wal_*
│   │   └── snapshot_*
│   │
│   └── user_data/              ← Application data (BucketService)
│       └── data_plane/         ← Namespace for Walrus
│           ├── t_logs_p_0_g_5/ ← Partition 0, generation 5
│           │   ├── index.bin
│           │   ├── offsets.bin
│           │   ├── wal_0000000000
│           │   └── wal_0000000001
│           │
│           ├── t_logs_p_0_g_6/ ← Partition 0, generation 6 (active)
│           │   ├── index.bin
│           │   ├── offsets.bin
│           │   └── wal_0000000000
│           │
│           └── t_logs_p_1_g_3/ ← Partition 1, generation 3
│               ├── index.bin
│               ├── offsets.bin
│               └── wal_0000000000
│
├── node_2/
│   └── ... (same structure)
│
└── node_3/
    └── ... (same structure)
```

### WAL Key to Filesystem Mapping

```rust
walrus_path_for_key("/data/node_1/user_data/data_plane", "t_logs_p_0_g_5")
  │
  ├─► Sanitize key: replace non-alphanumeric with '_'
  │     "t_logs_p_0_g_5" → "t_logs_p_0_g_5" (already clean)
  │
  └─► Result: "/data/node_1/user_data/data_plane/t_logs_p_0_g_5"

walrus_path_for_key("/data/node_1/user_data/data_plane", "foo@bar#baz")
  │
  ├─► Sanitize: "foo_bar_baz"
  │
  └─► Result: "/data/node_1/user_data/data_plane/foo_bar_baz"

walrus_path_for_key("/data/node_1/user_data/data_plane", "!!!")
  │
  ├─► All invalid → use FNV checksum
  │     checksum64(b"!!!") → 0xabcdef123456
  │
  └─► Result: "/data/node_1/user_data/data_plane/ns_abcdef123456"
```

---

## High Watermark Tracking

The `NodeController` maintains high watermarks for partition visibility.

```
┌────────────────────────────────────────────────────────────────┐
│                    High Watermark Lifecycle                     │
│                                                                  │
│  offsets: RwLock<HashMap<String, u64>>                          │
│                                                                  │
│  Initial state: {}                                               │
│                                                                  │
│  After write to "t_logs_p_0_g_5":                               │
│    record_append("t_logs_p_0_g_5")                              │
│      └─► offsets["t_logs_p_0_g_5"] = 1                         │
│                                                                  │
│  After another write:                                            │
│    record_append("t_logs_p_0_g_5")                              │
│      └─► offsets["t_logs_p_0_g_5"] = 2                         │
│                                                                  │
│  Read operation queries:                                         │
│    current_high_watermark("t_logs_p_0_g_5")                     │
│      └─► Returns 2                                              │
│                                                                  │
│  After rollover to gen 6:                                        │
│    offsets = {                                                   │
│      "t_logs_p_0_g_5": 100,  ← old generation                   │
│      "t_logs_p_0_g_6": 5     ← new generation                   │
│    }                                                             │
│                                                                  │
│  Read from new generation:                                       │
│    current_high_watermark("t_logs_p_0_g_6")                     │
│      ├─► Check direct: offsets["t_logs_p_0_g_6"] → 5           │
│      │                                                           │
│      └─► Return 5                                               │
│                                                                  │
│  Read from old generation (no longer tracked):                   │
│    current_high_watermark("t_logs_p_0_g_999")                   │
│      ├─► Not in map, search prefix "t_logs_p_0_*"              │
│      │                                                           │
│      ├─► Find max of gen 5 (100) and gen 6 (5)                 │
│      │                                                           │
│      └─► Return 100                                             │
│                                                                  │
└────────────────────────────────────────────────────────────────┘
```

---

## Error Handling & Fault Tolerance

### Write Retry Logic

```
NodeController.append_with_retry(wal_key, data):
  │
  ├─► Attempt 1:
  │     │
  │     └─► BucketService.append_by_key(wal_key, data)
  │           │
  │           ├─► Success → return Ok
  │           │
  │           └─► Error (e.g., lease missing)
  │
  ├─► Attempt 2:
  │     │
  │     ├─► sync_leases_now()  (refresh leases)
  │     │
  │     └─► BucketService.append_by_key(wal_key, data)
  │           │
  │           ├─► Success → return Ok
  │           │
  │           └─► Error → return Err
  │
  └─► Max 2 attempts
```

### Read Timeout & Fallback

```
NodeController.route_and_read():
  │
  ├─► Is leader? → Read locally
  │
  └─► Not leader → Forward to leader
        │
        ├─► timeout(WALRUS_RPC_TIMEOUT_MS, forward_rpc())
        │     │
        │     ├─► Success → return (data, watermark)
        │     │
        │     └─► Timeout/Error:
        │           │
        │           ├─► Log warning
        │           │
        │           └─► return ([], local_high_watermark)
        │                 └─► Clients see empty data but valid watermark
```

### Leader Election Impact

```
Scenario: Node 1 (leader) crashes

1. Raft detects leader failure
   │
   └─► Followers timeout waiting for heartbeats

2. Follower campaigns for leadership
   │
   └─► Node 2 wins election

3. Node 2 becomes new Raft leader
   │
   ├─► Can now accept metadata changes
   │
   └─► But partition leadership still points to Node 1

4. Clients writing to partitions led by Node 1:
   │
   ├─► Forward to Node 1 (unreachable)
   │
   └─► RPC fails → return error to client

5. Manual intervention or future automation:
   │
   └─► Propose RolloverPartition for affected partitions
         new_leader: Node 2
         │
         └─► Writes resume to Node 2
```

**Note:** Distributed Walrus does not currently implement automatic partition leadership failover. This requires operator intervention or future enhancement.

---

## Configuration Reference

### Command-Line Flags (NodeConfig)

```
--node-id <ID>                    Unique node identifier (default: 1)
--data-dir <PATH>                 Root storage directory (default: ./data)
--join <ADDR>                     Join existing cluster at address
--initial-peer <ADDR>             Bootstrap peer addresses (repeatable)
--port <PORT>                     Kafka listener port (default: 9092)
--raft-port <PORT>                Raft/RPC listener port (default: 6000)
--raft-host <HOST>                Raft bind address (default: 127.0.0.1)
--raft-advertise-host <HOST>      Raft advertised address (overrides bind)
--log-file <PATH>                 Log output file (default: stdout)
```

### Environment Variables

**Walrus Configuration:**
- `WALRUS_DATA_DIR`: Override Walrus storage location
- `WALRUS_INSTANCE_KEY`: Default namespace
- `WALRUS_QUIET=1`: Suppress Walrus debug output
- `WALRUS_DISABLE_IO_URING`: Force mmap backend

**Monitor Configuration:**
- `WALRUS_MONITOR_CHECK_MS`: Check interval (default: 10000)
- `WALRUS_MAX_SEGMENT_BYTES`: Rollover threshold (default: 1000000000)
- `WALRUS_RETENTION_GENERATIONS`: GC retention (default: 10)

**RPC Configuration:**
- `WALRUS_RPC_TIMEOUT_MS`: Forward timeout (default: 2000)

---

## Performance Considerations

### Write Path Optimization

1. **Lease Caching**: Leases synced every 100ms, avoiding metadata queries per write
2. **Batched I/O**: Walrus uses io_uring on Linux for parallel writes
3. **Retry Logic**: Single retry with lease refresh minimizes RPC overhead
4. **Local Writes**: Leader writes directly to WAL without self-RPC

### Read Path Optimization

1. **No Lease Check**: Reads don't require lease validation
2. **Batched Reads**: Walrus batch_read uses io_uring for parallel reads
3. **Watermark Tracking**: In-memory HashMap for fast offset queries
4. **Timeout Fallback**: Non-blocking reads return empty on timeout

### Raft Overhead

- **Metadata Only**: Only control plane uses Raft (CreateTopic, RolloverPartition)
- **Data Plane Direct**: User writes go directly to leader's local WAL
- **Lease Decoupling**: Metadata changes propagate via Raft, data writes are local

### Garbage Collection

- **Async Deletion**: GC runs in background, doesn't block operations
- **Retention Window**: 10 generations = ~10GB × retention before cleanup
- **Per-Partition**: GC scans all partitions, deletes expired generations

---

## Security & Isolation

### Lease-Based Fencing

**Purpose:** Prevent split-brain writes during network partitions.

```
Scenario: Network partition isolates old leader

┌──────────────────┐         ┌──────────────────┐
│  Old Leader      │   ╳     │  New Leader      │
│  Node 1          │   ╳     │  Node 2          │
│  (partitioned)   │   ╳     │  (elected)       │
└──────────────────┘         └──────────────────┘
         │                            │
         │ Lease expired              │ Lease active
         │ (no metadata sync)         │ (synced via Raft)
         │                            │
         ▼                            ▼
    Writes fail                  Writes succeed
    (lease check)                (lease granted)
```

### Namespace Isolation

- Each WAL key maps to separate Walrus topic
- Topics stored in sanitized filesystem directories
- Prevents cross-partition interference

---

## Testing & Observability

### Test Control APIs

Distributed Walrus exposes test APIs for fault injection:

```
API 60 - TestControl
  op=0: ForceForwardReadError(bool)
  op=1: RevokeLeases{topic, partition}
  op=2: SyncLeases
  op=3: TriggerJoin{node_id, addr}

API 61: ForceMonitorError
API 62: ForceDirSizeError
API 63: ForceGcError
```

**Example Usage:**
```python
# Python client forcing read errors
client.send_test_control(op=0, flag=1)  # Enable errors
client.fetch("logs", 0)  # Will fail
client.send_test_control(op=0, flag=0)  # Disable errors
```

### Logging

Distributed Walrus uses `tracing` for structured logging:

```rust
tracing::info!("Node {} booting", node_config.node_id);
tracing::warn!("Write rejected for {} (missing lease)", key);
tracing::error!("Produce failure for {}-{}: {}", topic, partition, e);
```

**Log Levels:**
- `info`: Normal operations (boot, leadership, rollovers)
- `warn`: Recoverable issues (lease mismatches, retries)
- `error`: Failures (RPC timeouts, append failures)

---

## Future Enhancements

### Automatic Failover

Currently, partition leadership is not automatically reassigned on node failure. Future work:

```
┌──────────────────────────────────────────────────────────────┐
│ Failure Detector                                              │
│  │                                                             │
│  ├─► Monitor Raft membership changes                          │
│  │                                                             │
│  ├─► Detect node removal                                      │
│  │                                                             │
│  └─► Propose RolloverPartition for orphaned partitions        │
│        new_leader: next_available_node                        │
└──────────────────────────────────────────────────────────────┘
```

### Replication

Currently, each partition has a single leader with no replicas. Future work:

- Multi-replica partitions with ISR (in-sync replicas)
- Leader forwards writes to followers
- Quorum-based acknowledgment

### Dynamic Partition Assignment

Currently, rollover assigns leadership back to self. Future work:

- Load-aware partition assignment
- Balance partitions across nodes
- Automated rebalancing on node join/leave

---

## Glossary

- **BucketService**: Lease-aware wrapper around Walrus WAL
- **Generation**: Version of a partition's WAL; incremented on rollover
- **High Watermark**: Number of entries written to a partition
- **Lease**: Permission to write to a partition/generation
- **MetadataStateMachine**: Raft-replicated cluster state
- **NodeController**: Central coordinator for routing and operations
- **Octopii**: Raft consensus framework providing replication
- **Partition**: Subset of a topic's data stream
- **Rollover**: Increment generation, start new WAL segment
- **WAL Key**: Identifier for partition: `t_{topic}_p_{partition}_g_{generation}`
- **Walrus**: High-performance Write-Ahead Log

---

## Summary

Distributed Walrus provides a distributed log system with:

1. **Leader-based replication**: Partition leaders handle writes, forwards to Walrus WAL
2. **Raft coordination**: Metadata changes replicated via Octopii
3. **Lease-based fencing**: Prevents split-brain writes
4. **Kafka compatibility**: Wire-compatible protocol for existing clients
5. **Automatic maintenance**: Monitor handles rollovers and garbage collection
6. **High performance**: io_uring-accelerated batched I/O on Linux

The architecture separates concerns cleanly:
- **Control plane**: Raft-replicated metadata (topics, partitions, leaders)
- **Data plane**: Direct leader writes to local Walrus WAL
- **RPC layer**: Node-to-node forwarding for routing
- **Protocol layer**: Kafka facade for client compatibility

This design prioritizes simplicity, performance, and correctness while providing a familiar interface for stream processing workloads.
