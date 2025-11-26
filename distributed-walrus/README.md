# Distributed Walrus

A distributed streaming log system built on [Walrus](../README.md) with Raft consensus for metadata coordination. Provides fault-tolerant, write-ahead logging with automatic leadership rotation and segment-based partitioning.

## Overview

Distributed Walrus turns the single-node Walrus storage engine into a distributed system with:
- **Automatic load balancing** via segment-based leadership rotation
- **Fault tolerance** through Raft consensus (3+ nodes)
- **Simple client protocol** (connect to any node, auto-forwarding)
- **Sealed segments** for historical reads from any replica

## Architecture

### System Overview (30,000 ft)

![Distributed Walrus Architecture](docs/Distributed%20walrus.png)

Producers and consumers connect to any node (or via load balancer). The cluster automatically routes requests to the appropriate leader and manages segment rollovers for load distribution.

### Cluster Topology & Node Internals

![Distributed Walrus Nodes](docs/distributed%20walrus%20nodes.png)

Each node contains four key components that work together:

**Node Controller**
- Routes client requests to appropriate segment leaders
- Manages write leases (synced from cluster metadata every 100ms)
- Tracks logical offsets for rollover detection
- Forwards operations to remote leaders when needed

**Raft Engine** (Octopii)
- Maintains Raft consensus for metadata changes only (not data!)
- Handles leader election and log replication
- One node is Raft leader, others are followers
- Syncs metadata across all nodes via AppendEntries RPCs

**Cluster Metadata** (Raft State Machine)
- Stores topic → segment → leader mappings
- Tracks sealed segments and their entry counts
- Maintains node addresses for routing
- Replicated identically across all nodes

**Bucket Storage**
- Wraps Walrus engine with lease-based write fencing
- Only accepts writes if node holds lease for that segment
- Stores actual data in Walrus WAL files on disk
- Serves reads from any segment (sealed or active)

**Ports:**
- `:9091-9093` - Client TCP connections (REGISTER/PUT/GET)
- `:6001-6003` - Raft RPC (metadata sync between nodes)

### How Nodes Interact

```
    Client Request (PUT/GET)
            │
            ▼
    ┌───────────────┐
    │   Any Node    │  ◄─── Client connects to any node
    │   (e.g., N2)  │       (or via load balancer)
    └───────┬───────┘
            │
            ├─ Lookup topic leader in local metadata
            │  (e.g., "logs" → segment 1 → leader N1)
            │
            ▼
      Am I the leader?
            │
    ┌───────┴────────┐
    │                │
   YES              NO
    │                │
    │                └──► Forward to leader
    │                     via RPC
    │                          │
    ▼                          ▼
┌────────────┐         ┌────────────┐
│  Node 1    │         │  Node 1    │
│  (Leader)  │◄────────│  (Leader)  │
│            │  RPC    │            │
│ ┌────────┐ │         │ ┌────────┐ │
│ │ Lease  │ │         │ │ Lease  │ │
│ │ Check  │ │         │ │ Check  │ │
│ └───┬────┘ │         │ └───┬────┘ │
│     │      │         │     │      │
│     ▼      │         │     ▼      │
│ ┌────────┐ │         │ ┌────────┐ │
│ │ Walrus │ │         │ │ Walrus │ │
│ │ Append │ │         │ │ Append │ │
│ └────────┘ │         │ └────────┘ │
└────────────┘         └─────┬──────┘
       │                     │
       │  ◄──────────────────┘
       │     Response
       ▼
    Return OK

─────────────────────────────────────────────────────────

    Raft Consensus Network (Metadata Only)

┌─────────┐         ┌─────────┐         ┌─────────┐
│ Node 1  │◄───────►│ Node 2  │◄───────►│ Node 3  │
│ (Leader)│         │(Follower)│        │(Follower)│
└────┬────┘         └────┬────┘         └────┬────┘
     │                   │                   │
     │  AppendEntries    │                   │
     ├──────────────────►│                   │
     │                   │                   │
     │                   │  AppendEntries    │
     ├───────────────────┴──────────────────►│
     │                                       │
     │  ◄────────── ACK (Quorum) ───────────┤
     │                                       │
     └──► Commit metadata change
          (CreateTopic, RolloverTopic, etc.)

All nodes apply committed entries to Metadata state machine
→ Consistent view of topics, segments, leaders

─────────────────────────────────────────────────────────

    Segment Rollover & Leadership Rotation

Node 1 (owns "logs:1"):
  │
  ├─ Monitor detects: segment has 1M+ entries
  │
  └─► Propose RolloverTopic via Raft
        │
        ├─ Seal segment 1 (1,000,050 entries)
        ├─ Create segment 2
        └─ Transfer leadership to Node 2
              │
              ▼
┌──────────────────────────────────────────────┐
│ Lease Sync (within 100ms):                  │
│                                              │
│ Node 1: Loses lease for "logs:1"            │
│         → Writes fail with NotLeaderError   │
│         → But sealed data still readable!   │
│                                              │
│ Node 2: Gains lease for "logs:2"            │
│         → Starts accepting writes           │
│                                              │
│ Clients: Automatically routed to Node 2     │
│          for new writes                     │
└──────────────────────────────────────────────┘
```

**Key Interactions:**
1. **Client → Any Node**: Clients connect to any node; non-leaders forward to leader
2. **Node ↔ Node (Data)**: RPC for forwarding writes/reads to segment leaders
3. **Node ↔ Node (Raft)**: Consensus for metadata changes (topics, segments, nodes)
4. **Automatic Rollover**: Monitor detects full segments → Raft commit → Lease transfer

## Quick Start

### Running a 3-Node Cluster

```bash
# Start the cluster
make cluster-up

# Wait for initialization
make cluster-bootstrap

# Interact via CLI
cargo run --bin walrus-cli -- --addr 127.0.0.1:9091

# In the CLI:
> REGISTER logs
> PUT logs "hello world"
> GET logs
> STATE logs
> METRICS
```

### Manual Setup (without Docker)

```bash
# Node 1 (bootstrap leader)
cargo run -- \
  --node-id 1 \
  --raft-port 6001 \
  --client-port 9091

# Node 2 (join cluster)
cargo run -- \
  --node-id 2 \
  --raft-port 6002 \
  --client-port 9092 \
  --join 127.0.0.1:6001

# Node 3 (join cluster)
cargo run -- \
  --node-id 3 \
  --raft-port 6003 \
  --client-port 9093 \
  --join 127.0.0.1:6001
```

## Client Protocol

Simple length-prefixed text protocol over TCP:

```
Wire format:
  [4 bytes: length (little-endian)] [UTF-8 command]

Commands:
  REGISTER <topic>       → Create topic if missing
  PUT <topic> <payload>  → Append to topic
  GET <topic>            → Read next entry (shared cursor)
  STATE <topic>          → Get topic metadata (JSON)
  METRICS                → Get Raft metrics (JSON)

Responses:
  OK [payload]           → Success
  EMPTY                  → No data available (GET only)
  ERR <message>          → Error
```

See [docs/cli.md](docs/cli.md) for detailed CLI usage.

## Key Features

### Segment-Based Sharding
- Topics split into segments (~1M entries each by default)
- Each segment has a leader node that handles writes
- Leadership rotates round-robin on segment rollover
- Automatic load distribution across cluster

### Lease-Based Write Fencing
- Only the leader for a segment can write to it
- Leases derived from Raft-replicated metadata
- 100ms sync loop ensures lease consistency
- Prevents split-brain writes during leadership changes

### Sealed Segment Reads
- Old segments "sealed" when rolled over
- Original leader retains sealed data for reads
- Reads can be served from any replica with the data
- No data movement required during rollover

### Automatic Rollover
- Monitor loop (10s) checks segment sizes
- Triggers rollover when threshold exceeded
- Proposes metadata change via Raft
- Leader transfer happens automatically

## Configuration

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | (required) | Unique node identifier |
| `--data-dir` | `./data` | Root directory for storage |
| `--raft-port` | `6000` | Raft/Internal RPC port |
| `--raft-host` | `127.0.0.1` | Raft bind address |
| `--raft-advertise-host` | (raft-host) | Advertised Raft address |
| `--client-port` | `8080` | Client TCP port |
| `--client-host` | `127.0.0.1` | Client bind address |
| `--join` | - | Address of existing node to join |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WALRUS_MAX_SEGMENT_ENTRIES` | `1000000` | Entries before rollover |
| `WALRUS_MONITOR_CHECK_MS` | `10000` | Monitor loop interval |
| `WALRUS_DISABLE_IO_URING` | - | Use mmap instead of io_uring |
| `RUST_LOG` | `info` | Log level (debug, info, warn) |

## Testing

Comprehensive test suite included:

```bash
# Run all tests
make test

# Individual tests
make cluster-test-logs         # Basic smoke test
make cluster-test-rollover     # Segment rollover
make cluster-test-resilience   # Node failure recovery
make cluster-test-recovery     # Cluster restart persistence
make cluster-test-stress       # Concurrent writes
make cluster-test-multi-topic  # Multiple topics
```

## Architecture Deep Dive

See [docs/architecture.md](docs/architecture.md) for:
- Detailed component interactions
- Data flow diagrams
- Startup sequence
- Lease synchronization
- Rollover mechanics
- Failure scenarios

## Performance

- **Write throughput**: Single writer per segment (lease-based)
- **Read throughput**: Scales with replicas (sealed segments)
- **Latency**: ~1-2 RTT for forwarded ops + storage latency
- **Consensus overhead**: Metadata only (not data path)
- **Segment rollover**: ~1M entries default (~100MB depending on payload size)

## Limitations

- Single shared cursor per topic (no consumer groups yet)
- Entry-based rollover (not byte-based)
- No retention policies (currently disabled)
- No compaction for sealed segments
- Hardcoded "logs" topic on bootstrap (Node 1 only)

## License

See parent [Walrus](../README.md) project for license information.
