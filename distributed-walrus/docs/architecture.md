# Distributed Walrus Architecture

## Overview

Distributed Walrus is a distributed streaming log system built on top of the Walrus storage engine. It provides a fault-tolerant, distributed write-ahead log with automatic leadership rotation and segment-based partitioning. The system uses Raft consensus (via Octopii) for metadata coordination and Walrus for durable data storage.

## High-Level Architecture

```
                           ┌─────────────────────────────────┐
                           │      Client Applications         │
                           │  (walrus-cli, Python scripts)    │
                           └──┬────────────────────────────┬──┘
                              │                            │
                        TCP   │                            │   TCP
                    :8080-9093│                            │:8080-9093
                              │                            │
        ┌─────────────────────▼─────────┐   ┌─────────────▼──────────────┐
        │        Node 1 (Leader)        │   │         Node 2              │
        │                               │   │                             │
        │  ┌─────────────────────────┐  │   │  ┌─────────────────────┐   │
        │  │   Client Listener       │  │   │  │  Client Listener    │   │
        │  │   • Length-prefixed     │  │   │  │  • REGISTER/PUT/GET │   │
        │  │   • REGISTER/PUT/GET    │  │   │  │  • STATE/METRICS    │   │
        │  └──────────┬──────────────┘  │   │  └──────────┬──────────┘   │
        │             │                  │   │             │              │
        │             ▼                  │   │             ▼              │
        │  ┌─────────────────────────┐  │   │  ┌─────────────────────┐   │
        │  │   NodeController        │  │   │  │   NodeController    │   │
        │  │  ┌──────────────────┐   │  │   │  │  ┌──────────────┐   │   │
        │  │  │ Routing Logic    │   │  │   │  │  │ Routing Logic│   │   │
        │  │  │ • ensure_topic() │   │  │   │  │  │ • Forward to │   │   │
        │  │  │ • append/read    │   │  │   │  │  │   leader     │   │   │
        │  │  └──────────────────┘   │  │   │  │  └──────────────┘   │   │
        │  │  ┌──────────────────┐   │  │   │  │  ┌──────────────┐   │   │
        │  │  │ Lease Sync       │   │  │   │  │  │ Lease Sync   │   │   │
        │  │  │ • 100ms loop     │   │  │   │  │  │ • 100ms loop │   │   │
        │  │  │ • update_leases()│   │  │   │  │  │ update_leases│   │   │
        │  │  └──────────────────┘   │  │   │  │  └──────────────┘   │   │
        │  └─┬───────┬────────┬──────┘  │   │  └─┬───────┬─────┬────┘   │
        │    │       │        │         │   │    │       │     │        │
        │    │       │        │         │   │    │       │     │        │
        │ ┌──▼───┐ ┌▼──────┐ │         │   │ ┌──▼───┐ ┌▼────┐│        │
        │ │Meta- │ │Bucket │ │         │   │ │Meta- │ │Buckｅ││        │
        │ │data  │ │(Stor- │ │         │   │ │data  │ │t(Sto││        │
        │ │State │ │age)   │ │         │   │ │State │ │rage)││        │
        │ │      │ │       │ │         │   │ │      │ │     ││        │
        │ │Topics│ │Leases:│ │         │   │ │Topics│ │Lease││        │
        │ │Nodes │ │logs:1 │ │         │   │ │Nodes │ │logs:││        │
        │ │Segs  │ │logs:2 │ │         │   │ │Segs  │ │     ││        │
        │ └──┬───┘ └───┬───┘ │         │   │ └──┬───┘ └──┬──┘│        │
        │    │         │     │         │   │    │        │   │        │
        │    │    ┌────▼─────▼──────┐  │   │    │   ┌────▼───▼─────┐  │
        │    │    │  Walrus Engine  │  │   │    │   │ Walrus Engine│  │
        │    │    │  • batch_append │  │   │    │   │ • read_next  │  │
        │    │    │  • read_next    │  │   │    │   │ • Disk I/O   │  │
        │    │    └─────────────────┘  │   │    │   └──────────────┘  │
        │    │                         │   │    │                     │
        │    │    ┌──Monitor Loop────┐ │   │    │    ┌─Monitor Loop┐ │
        │    │    │ • Check segments │ │   │    │    │ • Rollover  │ │
        │    │    │ • Trigger        │ │   │    │    │   checks    │ │
        │    │    │   rollover       │ │   │    │    └─────────────┘ │
        │    │    └──────────────────┘ │   │    │                     │
        │    │                         │   │    │                     │
        │ ┌──▼─────────────────────────▼┐ │   │ ┌──▼─────────────────▼┐
        │ │   Octopii (Raft Engine)    │ │   │ │  Octopii (Raft)     │
        │ │   • Metadata replication   │◄┼───┼─┤  • Follower         │
        │ │   • Leader election        │ │   │ │  • Vote/heartbeat   │
        │ │   • Log commit             │ │   │ │  • Apply commands   │
        │ └────────────────────────────┘ │   │ └─────────────────────┘
        │                                │   │                         │
        │       Port :6001 (Raft RPC)    │   │   Port :6002 (Raft)     │
        └────────────────────────────────┘   └─────────────────────────┘
                         │                                  │
                         │      Raft Consensus Network      │
                         │  (AppendEntries, RequestVote)    │
                         └──────────────┬───────────────────┘
                                        │
                         ┌──────────────▼───────────────┐
                         │      Node 3                  │
                         │  • Similar structure         │
                         │  • Port :6003 (Raft)         │
                         │  • Port :9093 (Client)       │
                         └──────────────────────────────┘

Legend:
━━━ Raft consensus (metadata only, not data)
─── Component communication within node
↑↓  External connections (client TCP, node RPC)
```

## Component Interactions

Understanding how components collaborate is key to understanding the system. Here's how they work together:

### Startup & Initialization Flow

```
┌───────────────────────────────────────────────────────────────────────┐
│ Node Startup (main.rs:start_node)                                    │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  1. Parse CLI Config                                                 │
│     ├─ node_id, ports, join_addr, data directories                  │
│     └─ Create data_wal_dir and meta_wal_dir                         │
│                                                                       │
│  2. Initialize Storage (Bucket)                                      │
│     ├─ Create Walrus instance for "data_plane" namespace            │
│     ├─ Initialize empty lease set                                   │
│     └─ Ready to accept append/read (but lease checks will fail)     │
│                                                                       │
│  3. Initialize Metadata State Machine                               │
│     ├─ Create empty ClusterState (topics, nodes maps)               │
│     └─ Will be populated via Raft replication                       │
│                                                                       │
│  4. Start Octopii (Raft)                                            │
│     ├─ Load/create WAL in meta_wal_dir                              │
│     ├─ Bind to raft_port for peer communication                     │
│     ├─ If node_id==1 && !join: Bootstrap as initial leader          │
│     └─ If join specified: Contact existing node                     │
│                                                                       │
│  5. Create NodeController                                           │
│     ├─ Holds references: bucket, metadata, raft                     │
│     ├─ Initialize empty: offsets, read_cursors                      │
│     └─ Ready to coordinate between components                       │
│                                                                       │
│  6. Register Custom RPC Handler                                     │
│     ├─ Octopii handles Raft RPCs (AppendEntries, RequestVote)      │
│     └─ Custom handler for InternalOp (Forward*, JoinCluster)        │
│                                                                       │
│  7. Start Background Tasks                                          │
│     ├─ Client Listener (TCP server on client_port)                  │
│     ├─ Lease Update Loop (every 100ms: sync leases from metadata)   │
│     └─ Monitor Loop (every 10s: check for rollover triggers)        │
│                                                                       │
│  8. Bootstrap First Topic (node_id==1 only)                         │
│     ├─ Propose CreateTopic("logs", leader=1)                        │
│     ├─ Propose RolloverTopic (seal nothing, start segment 1)        │
│     └─ Call update_leases() to grant self write lease              │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### The Lease Synchronization Dance

Leases are the critical mechanism that prevents split-brain writes. Here's the continuous synchronization:

```
┌──────────────────────────────────────────────────────────────────┐
│ Lease Sync Flow (runs every 100ms per node)                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Controller::run_lease_update_loop()                            │
│     │                                                            │
│     ├─► Controller::update_leases()                             │
│           │                                                      │
│           ├─► Metadata::owned_topics(self.node_id)              │
│           │     └─ Query local replica of Raft state            │
│           │        Example result: [("logs", 1), ("metrics", 3)]│
│           │                                                      │
│           ├─► Convert to wal_keys: ["logs:1", "metrics:3"]      │
│           │                                                      │
│           ├─► Storage::update_leases(expected_set)              │
│           │     │                                                │
│           │     ├─ Compare current leases vs expected           │
│           │     ├─ Add missing: leases.insert("logs:1")         │
│           │     └─ Remove stale: leases.remove("old:5")         │
│           │                                                      │
│           └─► Sync peer addresses from metadata                 │
│                 └─ Update Raft peer table if addresses changed  │
│                                                                  │
│  Result: Storage now accepts appends for owned segments only    │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│ When Metadata Changes (via Raft):                               │
│                                                                  │
│  RolloverTopic committed                                         │
│     ├─ Metadata state updated (new leader, sealed segment)      │
│     │                                                            │
│     └─ Within 100ms:                                             │
│          ├─ Old leader: next update_leases() removes lease      │
│          │               writes to old segment start failing    │
│          │                                                       │
│          └─ New leader: next update_leases() grants lease       │
│                         starts accepting writes to new segment  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Write Path: Complete Interaction Chain

```
┌──────────────────────────────────────────────────────────────────────┐
│ Client → Cluster → Storage: Write Flow                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Client: PUT logs "hello world"                                     │
│     │                                                                │
│     ├─► [TCP to any node] Client Listener receives                 │
│           │                                                          │
│           ├─► Parse command: topic="logs", data="hello world"       │
│           │                                                          │
│           └─► Controller::ensure_topic("logs")                      │
│                 │                                                    │
│                 ├─► Metadata::get_topic_state("logs")               │
│                 │     └─ Check if topic exists                      │
│                 │                                                    │
│                 ├─ If missing:                                      │
│                 │   ├─ Hash("logs") % num_nodes → initial_leader   │
│                 │   ├─ Propose CreateTopic via Raft                │
│                 │   └─ Wait for Raft commit                        │
│                 │                                                    │
│                 └─► Controller::append_for_topic("logs", data)      │
│                       │                                              │
│                       ├─► Metadata::get_topic_state("logs")         │
│                       │     └─ Returns: {current_segment: 1,        │
│                       │                  leader_node: 2}            │
│                       │                                              │
│                       ├─ Am I leader? (self.node_id == 2?)          │
│                       │                                              │
│        ┌──────────────┴──────────────┐                              │
│       NO                             YES                             │
│        │                              │                              │
│        │                              │                              │
│   ┌────▼─────────────────┐      ┌────▼─────────────────┐           │
│   │ forward_append_remote│      │ forward_append (local)│           │
│   └────┬─────────────────┘      └────┬─────────────────┘           │
│        │                              │                              │
│        │ RPC to leader node           │                              │
│        │ InternalOp::ForwardAppend    │                              │
│        │                              │                              │
│        └────────────┬─────────────────┘                              │
│                     │                                                │
│                     ├─► append_with_retry(wal_key, data)            │
│                           │                                          │
│                           ├─► Storage::append_by_key("logs:1", data)│
│                           │     │                                    │
│                           │     ├─► ensure_lease("logs:1")           │
│                           │     │     └─ Check: "logs:1" in leases? │
│                           │     │        └─ YES → proceed            │
│                           │     │           NO  → NotLeaderError     │
│                           │     │                                    │
│                           │     ├─► Acquire per-key mutex            │
│                           │     │     └─ Prevents concurrent writes  │
│                           │     │                                    │
│                           │     └─► Walrus::batch_append_for_topic   │
│                           │           └─ Durable write to WAL file  │
│                           │                                          │
│                           └─► record_append(wal_key, 1)              │
│                                 └─ offsets["logs:1"] += 1            │
│                                    (used for rollover detection)    │
│                                                                      │
│  ◄── Return OK to client                                            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Read Path: Cursor Advancement Across Segments

```
┌──────────────────────────────────────────────────────────────────────┐
│ Client → Cluster → Storage: Read Flow with Cursor                   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Client: GET logs                                                   │
│     │                                                                │
│     └─► Controller::read_one_for_topic_shared("logs")               │
│           │                                                          │
│           ├─► Fetch shared cursor from read_cursors map             │
│           │    Initial: { segment: 1, delivered_in_segment: 0 }     │
│           │                                                          │
│           └─► read_one_for_topic("logs", &mut cursor) [LOOP]        │
│                 │                                                    │
│                 ├─► Metadata::get_topic_state("logs")               │
│                 │    Returns: {                                     │
│                 │      current_segment: 3,                          │
│                 │      sealed_segments: {1 → 1000000, 2 → 950000},  │
│                 │      segment_leaders: {1 → node2, 2 → node3}      │
│                 │    }                                              │
│                 │                                                    │
│                 ├─ Is cursor.segment (1) < current_segment (3)?     │
│                 │  YES → segment 1 is sealed                        │
│                 │    │                                              │
│                 │    ├─ sealed_count = 1_000_000                    │
│                 │    ├─ cursor.delivered_in_segment = 5             │
│                 │    └─ 5 < 1_000_000? YES, more data available    │
│                 │                                                    │
│                 ├─► Determine leader for segment 1                  │
│                 │    └─ segment_leaders[1] = node2                  │
│                 │                                                    │
│                 ├─ Is leader local? (self.node_id == node2?)        │
│                 │                                                    │
│       ┌─────────┴─────────┐                                         │
│      NO                   YES                                       │
│       │                    │                                         │
│  ┌────▼───────────┐  ┌────▼──────────┐                             │
│  │forward_read_   │  │forward_read   │                             │
│  │  remote        │  │  (local)      │                             │
│  │  (RPC)         │  │               │                             │
│  └────┬───────────┘  └────┬──────────┘                             │
│       │                   │                                         │
│       └────────┬──────────┘                                         │
│                │                                                    │
│                ├─► Storage::read_one("logs:1")                      │
│                │     └─ Walrus::read_next("logs:1", advance=true)  │
│                │        └─ Returns Some(b"entry_5")                │
│                │                                                    │
│                ├─ Got data?                                         │
│                │  YES:                                              │
│                │    ├─ cursor.delivered_in_segment += 1 (now 6)    │
│                │    └─ Return Some(entry)                          │
│                │                                                    │
│                │  NO (empty):                                       │
│                │    ├─ Is segment sealed?                           │
│                │    │  YES:                                         │
│                │    │    ├─ delivered >= sealed_count?              │
│                │    │    │  YES:                                    │
│                │    │    │    ├─ cursor.segment += 1 (now 2)        │
│                │    │    │    ├─ cursor.delivered = 0               │
│                │    │    │    └─ CONTINUE LOOP (try segment 2)      │
│                │    │    │                                          │
│                │    │    └─ NO: Return None (waiting for data)      │
│                │    │                                                │
│                │    └─ NO (active segment):                         │
│                │         └─ Return None (no data yet)               │
│                │                                                    │
│                └─► Update shared cursor in read_cursors map         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Metadata Replication via Raft

All cluster state changes flow through Raft consensus:

```
┌──────────────────────────────────────────────────────────────────────┐
│ Metadata Change Flow (CreateTopic example)                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Controller::ensure_topic("metrics")                                │
│     │                                                                │
│     ├─► propose_metadata(CreateTopic{name:"metrics", leader: 3})    │
│           │                                                          │
│           ├─ Am I Raft leader?                                      │
│           │                                                          │
│    ┌──────┴──────┐                                                  │
│   YES            NO                                                 │
│    │              │                                                  │
│    │              ├─► Wait for Raft leader election                 │
│    │              │    (check raft.raft_metrics())                  │
│    │              │                                                  │
│    │              └─► Forward to Raft leader via RPC                │
│    │                   InternalOp::ForwardMetadata                  │
│    │                                                                 │
│    └─► Serialize command: bincode(CreateTopic{...})                 │
│           │                                                          │
│           └─► Raft::propose(bytes)                                  │
│                 │                                                    │
│                 ├─► Append to local Raft log                        │
│                 │                                                    │
│                 ├─► Send AppendEntries RPC to followers             │
│                 │     │                                              │
│                 │     ├─ Node 2: append to log, ACK                 │
│                 │     └─ Node 3: append to log, ACK                 │
│                 │                                                    │
│                 ├─► Wait for quorum (2 of 3 nodes)                  │
│                 │                                                    │
│                 ├─► Commit entry (advance commit_index)             │
│                 │                                                    │
│                 └─► Apply to state machine on ALL nodes:            │
│                       │                                              │
│                       └─► Metadata::apply(command_bytes)            │
│                             │                                        │
│                             ├─ Deserialize CreateTopic              │
│                             │                                        │
│                             ├─ state.topics.insert("metrics", ...)  │
│                             │                                        │
│                             └─ Return b"CREATED"                    │
│                                                                      │
│  All nodes now have consistent view:                                │
│    topics["metrics"] = {                                            │
│      current_segment: 1,                                            │
│      leader_node: 3,                                                │
│      sealed_segments: {},                                           │
│      segment_leaders: {1 → 3}                                       │
│    }                                                                │
│                                                                      │
│  Next lease sync (within 100ms):                                    │
│    Node 3: grants self lease for "metrics:1"                        │
│    Other nodes: no lease for "metrics:1"                            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Monitor-Triggered Rollover

The monitor loop orchestrates leadership rotation:

```
┌──────────────────────────────────────────────────────────────────────┐
│ Monitor → Rollover → Lease Transfer                                 │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Node 2 (owns "logs:1"):                                            │
│     │                                                                │
│     └─► Monitor::tick() [every 10s]                                 │
│           │                                                          │
│           ├─► check_rollovers()                                     │
│                 │                                                    │
│                 ├─► Metadata::owned_topics(2)                       │
│                 │    └─ Returns: [("logs", 1)]                      │
│                 │                                                    │
│                 ├─► For each owned segment:                         │
│                 │     │                                              │
│                 │     ├─ wal_key = "logs:1"                         │
│                 │     │                                              │
│                 │     ├─► Controller::tracked_entry_count("logs:1") │
│                 │     │    └─ Returns: 1_000_050                    │
│                 │     │                                              │
│                 │     ├─ Check: 1_000_050 >= 1_000_000?             │
│                 │     │  YES → TRIGGER ROLLOVER                     │
│                 │     │                                              │
│                 │     ├─ Get Raft voters: [1, 2, 3]                │
│                 │     │                                              │
│                 │     ├─ Find my position: index 1 (node 2)         │
│                 │     │                                              │
│                 │     ├─ Calculate next: (1 + 1) % 3 = 2            │
│                 │     │   next_leader = voters[2] = node 3          │
│                 │     │                                              │
│                 │     └─► propose_metadata(RolloverTopic {          │
│                 │           name: "logs",                           │
│                 │           new_leader: 3,                          │
│                 │           sealed_segment_entry_count: 1_000_050   │
│                 │         })                                        │
│                 │                                                    │
│                 └─► Raft consensus (as shown in previous diagram)   │
│                       │                                              │
│                       └─► Metadata::apply(RolloverTopic)            │
│                             │                                        │
│                             ├─ sealed_segments[1] = 1_000_050       │
│                             ├─ segment_leaders[1] = 2 (preserve)    │
│                             ├─ current_segment = 2                  │
│                             ├─ leader_node = 3                      │
│                             └─ segment_leaders[2] = 3               │
│                                                                      │
│  Within 100ms (lease sync):                                         │
│                                                                      │
│    Node 2:                                                          │
│      └─ owned_topics(2) → []  (no longer owns anything)             │
│          └─ Leases: {} (removed "logs:1")                           │
│             └─ Future writes to "logs:1" → NotLeaderError           │
│                But sealed data still readable!                      │
│                                                                      │
│    Node 3:                                                          │
│      └─ owned_topics(3) → [("logs", 2)]                             │
│          └─ Leases: {"logs:2"} (granted new segment)                │
│             └─ Starts accepting writes to "logs:2"                  │
│                                                                      │
│  Client writes:                                                     │
│    PUT logs "new data" → Routes to node 3, appends to "logs:2"      │
│                                                                      │
│  Client reads:                                                      │
│    GET logs → Cursor on segment 1? → Routes to node 2               │
│              Cursor on segment 2? → Routes to node 3               │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. NodeController (`controller/mod.rs`)

The central coordination hub that glues together all components. It is responsible for:

- **Topic Routing**: Determines which node is the leader for a given topic/segment
- **Request Forwarding**: Routes read/write requests to the appropriate leader node
- **Lease Management**: Maintains write leases that enforce single-writer semantics
- **Offset Tracking**: Tracks logical offsets and maps them to Walrus physical offsets
- **Cursor Management**: Maintains read cursors for consumers across sealed segments

```
┌─────────────────────────────────────────────────────────┐
│                    NodeController                        │
├─────────────────────────────────────────────────────────┤
│  Key Responsibilities:                                  │
│  • ensure_topic(topic) -> Create if doesn't exist       │
│  • append_for_topic(topic, data) -> Write routing       │
│  • read_one_for_topic(topic, cursor) -> Read routing    │
│  • update_leases() -> Sync with metadata state          │
│  • propose_metadata(cmd) -> Forward to Raft leader      │
├─────────────────────────────────────────────────────────┤
│  State:                                                 │
│  • offsets: HashMap<wal_key, entry_count>               │
│  • read_cursors: HashMap<topic, ReadCursor>             │
│  • bucket: Arc<Storage>                                 │
│  • metadata: Arc<Metadata>                              │
│  • raft: Arc<OctopiiNode>                               │
└─────────────────────────────────────────────────────────┘
```

### 2. Metadata (`metadata.rs`)

Raft-replicated state machine that stores cluster topology and topic ownership:

```
┌──────────────────────────────────────────────────────────┐
│                  Metadata State Machine                   │
├──────────────────────────────────────────────────────────┤
│  ClusterState {                                          │
│    topics: HashMap<TopicName, TopicState>                │
│    nodes: HashMap<NodeId, RaftAddress>                   │
│  }                                                       │
│                                                          │
│  TopicState {                                            │
│    current_segment: u64                                  │
│    leader_node: NodeId                                   │
│    sealed_segments: HashMap<segment_id, entry_count>     │
│    segment_leaders: HashMap<segment_id, NodeId>          │
│  }                                                       │
├──────────────────────────────────────────────────────────┤
│  Commands (Raft-replicated):                            │
│  • CreateTopic { name, initial_leader }                 │
│  • RolloverTopic { name, new_leader, sealed_count }     │
│  • UpsertNode { node_id, addr }                         │
└──────────────────────────────────────────────────────────┘
```

### 3. Storage (Bucket) (`bucket.rs`)

Thin wrapper around Walrus that enforces lease-based write fencing:

```
┌──────────────────────────────────────────────────────────┐
│                    Storage (Bucket)                       │
├──────────────────────────────────────────────────────────┤
│  • Wraps Walrus write-ahead log engine                  │
│  • Enforces write leases (single writer per segment)    │
│  • Per-key write locks to prevent concurrent writes     │
│  • Namespace isolation (DATA_NAMESPACE = "data_plane")  │
├──────────────────────────────────────────────────────────┤
│  Operations:                                             │
│  • append_by_key(wal_key, data)                         │
│    - Check lease: MUST hold lease for wal_key           │
│    - Acquire per-key mutex                              │
│    - Call Walrus batch_append_for_topic                 │
│                                                          │
│  • read_one(wal_key)                                    │
│    - No lease required (reads allowed from any node)    │
│    - Call Walrus read_next                              │
│                                                          │
│  • update_leases(expected_set)                          │
│    - Sync active leases with metadata ownership         │
└──────────────────────────────────────────────────────────┘
```

### 4. Client Listener (`client.rs`)

TCP server that exposes a simple text protocol for clients:

```
┌──────────────────────────────────────────────────────────┐
│                   Client Listener                         │
├──────────────────────────────────────────────────────────┤
│  Protocol (length-prefixed text):                        │
│  • REGISTER <topic>        -> Create topic if missing    │
│  • PUT <topic> <payload>   -> Append to topic            │
│  • GET <topic>             -> Read next entry (shared)   │
│  • STATE <topic>           -> Get topic metadata (JSON)  │
│  • METRICS                 -> Get Raft metrics (JSON)    │
│                                                          │
│  Wire Format:                                            │
│  [4 bytes: length (LE)] [UTF-8 command text]            │
│                                                          │
│  Response:                                               │
│  [4 bytes: length (LE)] [UTF-8 response text]           │
│  • Success: "OK" or "OK <data>"                         │
│  • Empty read: "EMPTY"                                  │
│  • Error: "ERR <message>"                               │
└──────────────────────────────────────────────────────────┘
```

### 5. Monitor (`monitor.rs`)

Background loop that watches segment sizes and triggers rollovers:

```
┌──────────────────────────────────────────────────────────┐
│                      Monitor Loop                         │
├──────────────────────────────────────────────────────────┤
│  Runs every WALRUS_MONITOR_CHECK_MS (default: 10s)      │
│                                                          │
│  For each owned topic/segment:                          │
│    1. Check tracked_entry_count >= max_segment_entries  │
│    2. If threshold exceeded:                            │
│       - Select next leader (round-robin from voters)    │
│       - Propose RolloverTopic command to Raft           │
│                                                          │
│  Rollover Effect:                                        │
│    - Current segment sealed with final entry count      │
│    - New segment created with incremented ID            │
│    - Leadership transferred to next node                │
│    - Old leader retains sealed segment for reads        │
│                                                          │
│  Config:                                                 │
│  • WALRUS_MAX_SEGMENT_ENTRIES (default: 1,000,000)      │
│  • WALRUS_MONITOR_CHECK_MS (default: 10000)             │
└──────────────────────────────────────────────────────────┘
```

### 6. RPC Layer (`rpc.rs`)

Internal node-to-node communication protocol:

```
┌──────────────────────────────────────────────────────────┐
│                   Internal RPC Protocol                   │
├──────────────────────────────────────────────────────────┤
│  InternalOp (request operations):                        │
│  • ForwardAppend { wal_key, data }                       │
│    - Client wrote to non-leader, forward to leader      │
│                                                          │
│  • ForwardRead { wal_key, max_entries }                  │
│    - Read from sealed segment on different node         │
│                                                          │
│  • ForwardMetadata { cmd }                               │
│    - Non-Raft-leader forwards metadata change           │
│                                                          │
│  • JoinCluster { node_id, addr }                         │
│    - New node joining the cluster                       │
│                                                          │
│  InternalResp (responses):                               │
│  • Ok                                                    │
│  • ReadResult { data, high_watermark }                  │
│  • Error(message)                                        │
└──────────────────────────────────────────────────────────┘
```

## Data Flow Diagrams

### Write Path

```
┌────────┐
│ Client │
└───┬────┘
    │ PUT logs "hello"
    ▼
┌────────────────┐
│ Client Listener│
│  (any node)    │
└───┬────────────┘
    │ ensure_topic("logs")
    ▼
┌────────────────┐     ┌─────────────────────────────────────┐
│ Controller     │     │ If topic doesn't exist:             │
│                │────►│ 1. Hash topic name → initial_leader │
│                │     │ 2. Propose CreateTopic via Raft     │
└───┬────────────┘     └─────────────────────────────────────┘
    │ append_for_topic("logs", "hello")
    │
    │ Lookup metadata: logs → segment 1 → leader node 2
    │
    ▼
    ┌─────── Local node is leader? ─────┐
    │                                    │
   YES                                  NO
    │                                    │
    ▼                                    ▼
┌────────────────┐              ┌──────────────────┐
│ forward_append │              │ forward_append_  │
│   (local)      │              │   remote         │
└───┬────────────┘              └───┬──────────────┘
    │                               │
    │                               │ RPC to node 2
    │                               │ InternalOp::ForwardAppend
    │                               ▼
    │                          ┌────────────────┐
    │                          │ Node 2         │
    │                          │ handle_rpc     │
    │                          └───┬────────────┘
    │                              │
    └──────────────┬───────────────┘
                   │
                   ▼
          ┌─────────────────┐
          │ Check lease      │
          │ for wal_key      │
          └───┬──────────────┘
              │
              ▼
          ┌─────────────────┐
          │ Acquire per-key │
          │ write mutex     │
          └───┬──────────────┘
              │
              ▼
          ┌─────────────────┐
          │ Walrus::        │
          │ batch_append_   │
          │ for_topic       │
          └───┬──────────────┘
              │
              ▼
          ┌─────────────────┐
          │ Update offset   │
          │ counter         │
          └───┬──────────────┘
              │
              ▼
          ┌─────────────────┐
          │ Return OK       │
          └─────────────────┘
```

### Read Path

```
┌────────┐
│ Client │
└───┬────┘
    │ GET logs
    ▼
┌────────────────┐
│ Client Listener│
└───┬────────────┘
    │ read_one_for_topic_shared("logs")
    ▼
┌────────────────┐
│ Controller     │
│ (fetch shared  │
│  cursor)       │
└───┬────────────┘
    │ cursor = { segment: 1, delivered_in_segment: 0 }
    │
    ▼
┌───────────────────────────────────────────────────────┐
│ Loop: Read from current segment or advance            │
├───────────────────────────────────────────────────────┤
│ 1. Lookup segment leader from metadata:              │
│    - Active segment → current leader                 │
│    - Sealed segment → historical leader (or current) │
│                                                       │
│ 2. Is leader local?                                  │
│    YES → forward_read (local)                        │
│    NO  → forward_read_remote (RPC)                   │
│                                                       │
│ 3. Got data?                                         │
│    YES → increment cursor.delivered_in_segment       │
│          return data                                 │
│                                                       │
│    NO (empty) → segment sealed?                      │
│       YES → cursor.segment++                         │
│             cursor.delivered_in_segment = 0          │
│             continue loop                            │
│       NO  → return None (no more data available)     │
└───────────────────────────────────────────────────────┘
```

### Segment Rollover Flow

```
┌───────────────┐
│ Monitor Loop  │
│ (every 10s)   │
└───┬───────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ For each owned topic/segment:                        │
│   Check tracked_entry_count >= max_segment_entries   │
└───┬──────────────────────────────────────────────────┘
    │
    │ Threshold exceeded
    ▼
┌──────────────────────────────────────────────────────┐
│ Calculate next_leader:                               │
│   voters = [1, 2, 3]  (from Raft membership)         │
│   current_idx = position of self in voters           │
│   next_leader = voters[(current_idx + 1) % len]      │
└───┬──────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ Propose MetadataCmd::RolloverTopic {                 │
│   name: "logs",                                      │
│   new_leader: next_leader,                           │
│   sealed_segment_entry_count: tracked_count          │
│ }                                                    │
└───┬──────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ Raft commit → apply to Metadata state machine        │
│                                                      │
│ TopicState update:                                   │
│   sealed_segments[1] = 1_000_000                     │
│   segment_leaders[1] = current_leader                │
│   current_segment = 2                                │
│   leader_node = next_leader                          │
│   segment_leaders[2] = next_leader                   │
└───┬──────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ All nodes receive Raft apply:                        │
│   - Old leader: loses lease for segment 1            │
│                 keeps sealed segment readable        │
│   - New leader: gains lease for segment 2            │
│                 starts accepting writes              │
└──────────────────────────────────────────────────────┘
```

### Cluster Join Flow

```
┌──────────────┐
│ New Node 4   │
│ (starting)   │
└───┬──────────┘
    │ --join node1:6001
    ▼
┌──────────────────────────────────────────────────────┐
│ attempt_join():                                      │
│   Create InternalOp::JoinCluster {                   │
│     node_id: 4,                                      │
│     addr: "node4:6004"                               │
│   }                                                  │
└───┬──────────────────────────────────────────────────┘
    │
    │ RPC to node1:6001
    ▼
┌──────────────────────────────────────────────────────┐
│ Node 1 (leader):                                     │
│   handle_join_cluster(4, "node4:6004")               │
└───┬──────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ 1. raft.add_learner(4, socket_addr)                  │
│    → Add node 4 as non-voting learner                │
│                                                      │
│ 2. upsert_node(4, "node4:6004")                      │
│    → Store node address in metadata                 │
│                                                      │
│ 3. Background task: wait for catchup                 │
│    Loop (max 60s):                                   │
│      if raft.is_learner_caught_up(4):                │
│        raft.promote_learner(4)                       │
│        → Node 4 becomes voting member                │
│        break                                         │
│      sleep 500ms                                     │
└──────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│ Node 4 is now a full voting member:                 │
│   - Participates in Raft consensus                  │
│   - Can be assigned topic/segment leadership        │
│   - Will be included in rollover round-robin        │
└──────────────────────────────────────────────────────┘
```

## Key Design Patterns

### 1. Lease-Based Write Fencing

Only the node holding a lease for a wal_key can write to it. Leases are derived from metadata:

```
Metadata state:  logs → segment 1 → leader node 2

Node 2 lease set:  { "logs:1" }
Node 1 lease set:  { }
Node 3 lease set:  { }

Write to "logs:1" on node 2 → SUCCESS (has lease)
Write to "logs:1" on node 1 → ERROR: NotLeaderForPartition
```

### 2. Offset Mapping

Walrus uses physical offsets (segment files on disk). Distributed Walrus tracks logical entry counts:

```
Controller state:
  offsets["logs:1"] = 42

After append:
  offsets["logs:1"] = 43

Monitor checks:
  if offsets["logs:1"] >= max_segment_entries:
    trigger rollover
```

### 3. Read Cursor Advancement

Cursors track position across sealed segments using metadata:

```
Metadata:
  sealed_segments[1] = 1_000_000
  sealed_segments[2] = 800_000
  current_segment = 3

Cursor: { segment: 1, delivered_in_segment: 1_000_000 }
        → All entries consumed from segment 1
        → Advance to segment 2

Cursor: { segment: 2, delivered_in_segment: 800_000 }
        → All entries consumed from segment 2
        → Advance to segment 3 (active)
```

### 4. Leader Forwarding

Non-leader nodes forward operations to the current leader:

```
Client → Node 1: PUT logs "data"
Node 1 metadata lookup: logs → segment 1 → leader node 2
Node 1 → Node 2: InternalOp::ForwardAppend
Node 2: Perform local append
Node 2 → Node 1: InternalResp::Ok
Node 1 → Client: OK
```

## Configuration

### Node Configuration (CLI flags)

- `--node-id`: Unique node identifier (required)
- `--data-dir`: Root directory for storage (default: `./data`)
- `--raft-port`: Raft/Internal RPC port (default: 6000)
- `--raft-host`: Raft bind address (default: 127.0.0.1)
- `--raft-advertise-host`: Advertised Raft address for peers
- `--client-port`: Client TCP listener port (default: 8080)
- `--client-host`: Client bind address (default: 127.0.0.1)
- `--join`: Address of existing node to join (e.g., "127.0.0.1:6001")
- `--initial-peer`: Bootstrap peer addresses (leader only)

### Environment Variables

- `WALRUS_MAX_SEGMENT_ENTRIES`: Max entries before rollover (default: 1,000,000)
- `WALRUS_MONITOR_CHECK_MS`: Monitor loop interval (default: 10000)
- `WALRUS_DISABLE_IO_URING`: Disable io_uring, use mmap (for containers)
- `RUST_LOG`: Log level (e.g., `debug`, `info`)

## File Layout

```
data/
  node_{id}/
    user_data/           # Walrus data files (Storage bucket)
      data_plane/
        wal_{topic}_{segment}_*.wal
        wal_{topic}_{segment}_*.idx
    raft_meta/           # Raft write-ahead log and snapshots
      *.wal
      *.snapshot
```

## Testing Architecture

The system includes comprehensive integration tests:

```
┌──────────────────────────────────────────────────────────┐
│                     Test Suite                            │
├──────────────────────────────────────────────────────────┤
│ • logging_smoke_test.py                                  │
│   - Basic REGISTER/PUT/GET operations                    │
│                                                          │
│ • rollover_read_test.py                                  │
│   - Write beyond max_segment_entries                     │
│   - Verify segment rollover and leadership rotation      │
│   - Reads span sealed and active segments                │
│                                                          │
│ • resilience_test.py                                     │
│   - Stop/restart nodes during writes                     │
│   - Verify no data loss                                  │
│                                                          │
│ • recovery_test.py                                       │
│   - Stop entire cluster                                  │
│   - Restart and verify persistence                       │
│                                                          │
│ • stress_test.py                                         │
│   - Concurrent writes from multiple clients              │
│   - High throughput validation                           │
│                                                          │
│ • multi_topic_stress_test.py                            │
│   - Multiple topics with distributed leadership          │
│   - Load balancing verification                          │
└──────────────────────────────────────────────────────────┘
```

## Deployment Example

A typical 3-node cluster setup:

```
Node 1 (bootstrap leader):
  distributed-walrus \
    --node-id 1 \
    --raft-port 6001 \
    --client-port 9091

Node 2 (joins cluster):
  distributed-walrus \
    --node-id 2 \
    --raft-port 6002 \
    --client-port 9092 \
    --join node1:6001

Node 3 (joins cluster):
  distributed-walrus \
    --node-id 3 \
    --raft-port 6003 \
    --client-port 9093 \
    --join node1:6001
```

After bootstrap, the cluster will:
1. Node 1 becomes Raft leader
2. Nodes 2 and 3 join as learners, then promoted to voters
3. Topic leadership automatically distributed via hash-based assignment
4. Monitor loops trigger rollovers every ~1M entries
5. Leadership rotates round-robin across nodes

## Failure Scenarios

### Node Failure

```
Initial state:
  logs → segment 1 → leader node 2 ✓
  logs → segment 2 → leader node 3 ✓

Node 2 fails:
  - Raft detects heartbeat loss
  - Writes to segment 1 fail (no leader)
  - Reads from sealed portions of segment 1 still work (if stored locally)
  - New segments assigned to remaining nodes (1, 3)

Node 2 recovers:
  - Rejoins cluster as follower
  - Catches up Raft log from leader
  - May receive new segment leadership on next rollover
```

### Split Brain Prevention

Raft consensus prevents split brain:
- Writes require quorum (majority of voters)
- Leases only granted to Raft-acknowledged leader
- Failed leader cannot serve writes (loses lease)

### Network Partition

```
Partition: [Node 1, Node 2] | [Node 3]

Majority partition [1, 2]:
  - Continues operating normally
  - Can commit Raft writes
  - Serves reads and writes

Minority partition [3]:
  - Cannot achieve quorum
  - Stops accepting writes
  - Reads may serve stale data
  - Rejoins when partition heals
```

## Performance Characteristics

- **Write throughput**: Limited by Walrus storage engine and single-writer-per-segment
- **Read throughput**: Scales with number of nodes (sealed segments readable from any replica)
- **Latency**: ~1-2 RTT for forwarded operations, plus Walrus append latency
- **Leadership rotation**: Automatic load balancing every ~1M entries per segment
- **Consensus overhead**: Raft used only for metadata (topics, leases), not data path

## Future Enhancements

Potential improvements mentioned in the codebase:

- Retention policies (currently disabled via `WALRUS_RETENTION_GENERATIONS=0`)
- Configurable segment size limits (currently entry-based)
- Consumer group management (currently single shared cursor)
- Compaction for sealed segments
- Multi-topic read optimization
- Dynamic rebalancing based on load metrics
