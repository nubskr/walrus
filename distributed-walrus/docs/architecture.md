# Distributed Walrus Architecture

> A distributed write-ahead log built on Raft consensus and Walrus local storage

---

## What Is It?

Distributed Walrus is a **distributed WAL** that spreads writes across nodes using **segments**. When a segment gets full (1M entries by default), it automatically rolls over to the next node in round-robin fashion.

**Core idea:**
- One **topic** (like "logs") split into **segments** (s1, s2, s3...)
- Each segment has one **leader node** that can write to it
- Segments are immutable once sealed (rollover happens → new segment, new leader)
- Raft metadata tracks who owns what

---

## The System in 30 Seconds

```
Topic "logs" — spread across 3 nodes:

 ┌─────────────┬─────────────┬─────────────┬─────────────┐
 │  Segment 1  │  Segment 2  │  Segment 3  │  Segment 4  │
 │   (sealed)  │   (sealed)  │  (ACTIVE)   │  (pending)  │
 ├─────────────┼─────────────┼─────────────┼─────────────┤
 │   Node 1    │   Node 2    │   Node 3    │   Node 1    │
 │  [0→1M]     │  [1M→2M]    │  [2M→3M]    │ [not yet]   │
 └─────────────┴─────────────┴─────────────┴─────────────┘
                                    ▲
                                    │
                            writes go here

Round-robin: Node 1 → Node 2 → Node 3 → Node 1 → ...
```

**How it works:**
1. Client sends append to any node
2. Node checks Raft metadata: "Who owns the current segment?"
3. If this node owns it → write to local Walrus
4. If another node owns it → return error (client retries to correct node)
5. When segment hits 1M entries → monitor proposes rollover via Raft
6. Raft increments segment number, assigns next node round-robin

```
                         3-Node Cluster View
                         ===================

  ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
  │      Node 1         │   │      Node 2         │   │      Node 3         │
  │                     │   │                     │   │                     │
  │  ┌──────────────┐   │   │  ┌──────────────┐   │   │  ┌──────────────┐   │
  │  │ Controller   │   │   │  │ Controller   │   │   │  │ Controller   │   │
  │  └──────┬───────┘   │   │  └──────┬───────┘   │   │  └──────┬───────┘   │
  │         │           │   │         │           │   │         │           │
  │  ┌──────▼───────┐   │   │  ┌──────▼───────┐   │   │  ┌──────▼───────┐   │
  │  │   Storage    │   │   │  │   Storage    │   │   │  │   Storage    │   │
  │  │              │   │   │  │              │   │   │  │              │   │
  │  │ owns: s1, s4 │   │   │  │   owns: s2   │   │   │  │   owns: s3   │   │
  │  └──────┬───────┘   │   │  └──────┬───────┘   │   │  └──────┬───────┘   │
  │         │           │   │         │           │   │         │           │
  │  ┌──────▼───────┐   │   │  ┌──────▼───────┐   │   │  ┌──────▼───────┐   │
  │  │   Walrus     │   │   │  │   Walrus     │   │   │  │   Walrus     │   │
  │  │              │   │   │  │              │   │   │  │              │   │
  │  │  t_logs_s_1  │   │   │  │  t_logs_s_2  │   │   │  │  t_logs_s_3  │   │
  │  │  t_logs_s_4  │   │   │  │  (sealed)    │   │   │  │  (ACTIVE)    │   │
  │  │  (sealed)    │   │   │  │              │   │   │  │              │   │
  │  └──────────────┘   │   │  └──────────────┘   │   │  └──────────────┘   │
  │                     │   │                     │   │                     │
  └──────────┬──────────┘   └──────────┬──────────┘   └──────────┬──────────┘
             │                         │                         │
             └─────────────────────────┼─────────────────────────┘
                                       │
                            ╔══════════▼══════════╗
                            ║   Raft Metadata     ║
                            ║                     ║
                            ║  topic: "logs"      ║
                            ║  segment: 3         ║
                            ║  leader: node3      ║
                            ║  sealed_offset: 2M  ║
                            ╚═════════════════════╝

Write to segment 3 → must go to Node 3 (current leader)
Segments 1, 2, 4 are sealed (no more writes, read-only)
```

---

## Components

```
                Component Architecture (Single Node)
                ====================================

         ┌───────────────────────────────────────────────────┐
         │                                                   │
    RPC  │              NodeController                       │
   ─────►│  • Routes requests                                │
         │  • Updates leases every 100ms                     │
         │  • Tracks entry counts (in-memory HashMap)        │
         │                                                   │
         └───┬─────────────────┬─────────────────┬───────────┘
             │                 │                 │
             │ owns_topics()   │ propose()       │ update_leases()
             │                 │                 │
         ┌───▼──────┐     ┌────▼────────┐   ┌───▼──────────┐
         │          │     │             │   │              │
         │ Metadata │     │    Raft     │   │   Storage    │
         │ (state)  │◄────┤  Consensus  │   │  (leases)    │
         │          │     │             │   │              │
         └──────────┘     └─────────────┘   └───┬──────────┘
                                                 │ append()
                                                 │
                                             ┌───▼──────────┐
                                             │              │
                                             │   Walrus     │
                                             │ (local WAL)  │
                                             │              │
                                             └──────────────┘
                                                    │
                                                    ▼
                                             [disk: io_uring]

         ┌───────────────────────────────────────────────────┐
         │              Monitor                              │
         │  • Checks segment sizes every 10s                 │
         │  • Proposes rollover when over limit              │
         │                                                   │
         └───────────────────────────────────────────────────┘
```

### 1. Storage (bucket.rs)
**What it is:** Thin wrapper around Walrus (the local WAL engine)

**What it does:**
- Enforces **leases**: only write if you own the segment
- Per-key locks: prevent concurrent writes to same segment
- Simple API: `append_by_key(wal_key, data)` and `update_leases(keys)`

**Why leases?**
Prevents split-brain: if Raft reassigns segment 3 from node A to node B, node A must stop writing immediately.

```
Storage
  ├── Walrus engine (io_uring or mmap backend)
  ├── active_leases: HashSet<String>     (which segments can I write?)
  └── write_locks: per-key mutexes       (serialize writes to each segment)
```

---

### 2. Metadata (metadata.rs)
**What it is:** Raft state machine storing topic/segment ownership

**Data model:**
```rust
ClusterState {
    topics: HashMap<TopicName, TopicState>
}

TopicState {
    current_segment: u64,              // e.g., 3
    leader_node: NodeId,               // e.g., node 2
    last_sealed_entry_offset: u64,    // total entries in sealed segments
}
```

**Commands:**
- `CreateTopic { name, initial_leader }` - Bootstrap a new topic
- `RolloverTopic { name, new_leader, sealed_segment_entry_count }` - Seal current segment, start new one

**Key method:**
- `owned_topics(node_id)` - Returns `Vec<(topic, segment)>` this node currently leads

---

### 3. NodeController (controller/mod.rs)
**What it is:** Request router and lease coordinator

**Jobs:**
1. Handle RPC: `ForwardAppend`, `JoinCluster`, etc.
2. Update leases: fetch owned segments from Raft metadata → sync to Storage
3. Track entry counts: in-memory HashMap for rollover decisions
4. Retry logic: if append fails, update leases and retry once

**Key loop:**
```rust
run_lease_update_loop() {
    every 100ms:
        owned = metadata.owned_topics(this_node)
        storage.update_leases(owned)
}
```

**Why the loop?** Reactive polling. When Raft applies a rollover, this catches it within 100ms.

---

### 4. Monitor (monitor.rs)
**What it is:** Background loop that checks segment sizes and proposes rollovers

**How it works:**
```
Every 10 seconds:
    for each (topic, segment) I own:
        sealed = metadata.last_sealed_entry_offset
        current = controller.tracked_entry_count(segment)
        entries_in_segment = current - sealed

        if entries_in_segment > 1M:
            propose RolloverTopic via Raft
```

**Round-robin assignment:**
```rust
nodes = [1, 2, 3]
current_idx = nodes.position(my_id)
next_leader = nodes[(current_idx + 1) % nodes.len()]
```

**Environment variables:**
- `WALRUS_MONITOR_CHECK_MS` - how often to check (default 10s)
- `WALRUS_MAX_SEGMENT_ENTRIES` - rollover threshold (default 1M)

---

## Data Flow: Append

```
                     Write Request Flow
                     ==================

  ┌──────────┐
  │  Client  │  "Append to 'logs'"
  └─────┬────┘
        │
        │ 1. RPC: ForwardAppend { wal_key: "t_logs_s_3", data: [...] }
        │
        ▼
  ╔═══════════════════╗
  ║ NodeController    ║  ◄── (can land on any node)
  ╚═══════════════════╝
        │
        │ 2. update_leases() — "Do I own segment 3?"
        │
        ▼
  ┌───────────────────┐
  │  Metadata (Raft)  │
  │                   │
  │  logs:            │
  │   segment: 3      │
  │   leader: node2   │
  └─────────┬─────────┘
            │
            │ 3. Check ownership
            │
      ┌─────▼──────┐
      │  Own it?   │───no───► ❌ Error: "NotLeaderForPartition"
      └─────┬──────┘
            │ yes
            │
            ▼
  ┌───────────────────┐
  │  Storage          │
  │                   │
  │  ✓ Check lease    │
  │  ✓ Lock key       │
  │  ✓ Append         │
  └─────────┬─────────┘
            │
            ▼
  ┌───────────────────┐
  │  Walrus Engine    │  ◄── (local disk, io_uring)
  │                   │
  │  batch_append()   │
  └─────────┬─────────┘
            │
            │ ✅ Success
            │
            ▼
  ┌───────────────────┐
  │  NodeController   │
  │                   │
  │  record_append()  │  (entry count += 1)
  └───────────────────┘
```

---

## Data Flow: Rollover

```
                    Segment Rollover Flow
                    =====================

  ┌─────────────────────────────────────────────────────┐
  │  Monitor Loop (every 10s)                           │
  │                                                     │
  │  "Check my segments..."                             │
  │   Segment 3: 1.2M entries (OVER 1M limit!) 🚨       │
  └───────────────────┬─────────────────────────────────┘
                      │
                      │ Propose rollover
                      ▼
         ╔════════════════════════════╗
         ║    Raft Cluster            ║
         ║                            ║
         ║  RolloverTopic {           ║
         ║    name: "logs"            ║
         ║    new_leader: node3       ║
         ║    sealed_count: 200k      ║
         ║  }                         ║
         ╚════════════╦═══════════════╝
                      │
                      │ Consensus!
                      ▼
  ┌─────────────────────────────────────────────────────┐
  │  Metadata.apply()                                   │
  │                                                     │
  │  last_sealed_entry_offset += 200k  (2M → 2.2M)     │
  │  current_segment += 1              (3 → 4)         │
  │  leader_node = node3                               │
  └─────────────────┬───────────────────────────────────┘
                    │
                    │ All nodes see this update
                    ▼
  ┌─────────────────────────────────────────────────────┐
  │  Lease Update Loop (next 100ms tick)                │
  │                                                     │
  │  Node 2: ❌ loses lease for segment 3               │
  │  Node 3: ✅ gains lease for segment 4               │
  └─────────────────────────────────────────────────────┘

               Before                After
            ┌──────────┐         ┌──────────┐
  Segment 3 │  Node 2  │   →     │  SEALED  │
            │ (ACTIVE) │         └──────────┘
            └──────────┘
  Segment 4 │ (pending)│   →     │  Node 3  │
            └──────────┘         │ (ACTIVE) │
                                 └──────────┘

Two-phase commit:
  Phase 1: Monitor detects → Raft consensus (async)
  Phase 2: Lease loop reacts → Storage updated (polling)
```

---

## WAL Key Format

```
t_{topic}_s_{segment}
```

Examples:
- `t_logs_s_1` - topic "logs", segment 1
- `t_logs_s_42` - topic "logs", segment 42

This is the **physical key** passed to Walrus. It's also the lease identifier.

---

## Entry Count Tracking

**Problem:** Walrus is byte-oriented, but we rollover based on entry count.

**Current approach:**
- `NodeController.offsets: HashMap<String, u64>` - in-memory entry counter
- After each append: `record_append(wal_key, 1)`
- Monitor reads this to decide when to rollover

**Limitation:**
- Assumes 1 append = 1 entry (no batching)
- State is lost on restart (non-durable)
- Diverges if append succeeds but increment fails

**Better approach (not yet implemented):**
- Query Walrus directly for byte size
- Parse entries OR store entry count in segment metadata

---

## Cluster Membership

**Bootstrap (node 1):**
1. Start as single-node Raft cluster
2. Campaign for leadership
3. Create topic "logs" via Raft
4. Rollover immediately to create segment 1

**Join (nodes 2, 3, ...):**
1. Start with `--join-addr <node1-address>`
2. Send `JoinCluster` RPC to node 1
3. Node 1 adds as Raft **learner** (read-only)
4. Wait for learner to catch up (polls every 500ms, max 60s)
5. Promote learner to **voter** (can participate in consensus)

**Why learner → voter?**
If you add a voter immediately and it's behind, Raft can't commit (needs majority). Learners don't vote, so cluster stays available during catch-up.

---

## Lease Enforcement

**Goal:** Prevent stale writes when ownership changes

**How it works:**

1. **Raft decides ownership:**
   ```rust
   TopicState { segment: 3, leader: node2 }
   ```

2. **Lease update loop (every 100ms):**
   ```rust
   owned = metadata.owned_topics(my_node_id)  // e.g., [(logs, 3)]
   storage.update_leases(owned)               // grants t_logs_s_3
   ```

3. **Append checks lease:**
   ```rust
   if !active_leases.contains(wal_key) {
       bail!("NotLeaderForPartition")
   }
   ```

**Edge case:**
- Raft rolls over segment 3 from node 2 → node 3
- Node 2's lease loop hasn't run yet (could be 99ms away)
- Node 2 still has the lease for ~100ms after losing ownership

**Mitigation:**
- Append calls `update_leases()` before writing (eager sync)
- Retry logic also calls `update_leases()` on failure
- So in practice, stale lease window is ~1ms, not 100ms

---

## Testing & Development

**Test controls (via RPC):**
```rust
TestControl::ForceMonitorError       // Make monitor fail
TestControl::ForceDirSizeError       // Make size check fail
TestControl::RevokeLeases { topic }  // Manually drop all leases
TestControl::SyncLeases              // Force immediate lease update
TestControl::TriggerJoin { ... }     // Manual cluster join
```

**Environment overrides:**
```bash
# Use mmap backend instead of io_uring (for containers)
WALRUS_DISABLE_IO_URING=1

# Check segment sizes every 1 second (instead of 10s)
WALRUS_MONITOR_CHECK_MS=1000

# Rollover at 100k entries (instead of 1M)
WALRUS_MAX_SEGMENT_ENTRIES=100000
```

---

## Limitations & Future Work

### 1. No Read Path
Currently only writes are implemented. To add reads:
- Need to forward reads to segment owner (or allow reads from any replica)
- Need to track high watermark (last committed entry)
- Need to handle reads across sealed segments (s1 on node1, s2 on node2, etc.)

### 2. Entry Count Tracking
In-memory HashMap is fragile:
- Not durable (lost on restart)
- Assumes 1 append = 1 entry (no batching support)
- Can diverge from reality

**Fix:** Store entry count in Raft metadata, or parse Walrus on startup.

### 3. Reactive Lease Updates
100ms polling loop is wasteful and adds latency.

**Fix:** Make Raft state machine emit events when ownership changes → immediate lease update.

### 4. No Compaction/Retention
Old sealed segments stay on nodes forever.

**Fix:** Add retention policies (time-based or size-based), garbage collect old segments.

### 5. Single Topic
Hardcoded to "logs" topic in bootstrap.

**Fix:** Add topic management API (create/delete topics dynamically).

---

## FAQ

**Q: What happens if a node crashes?**

A: Depends on what it owned:
- **Sealed segments:** Data is durable on that node's disk. When it restarts, Raft knows it still owns those segments. Reads would fail until it's back (no read path yet).
- **Active segment:** Raft will eventually timeout waiting for heartbeats, elect new leader, and that leader can propose a rollover to move the segment to a healthy node. Writes block until then.

**Q: Can multiple nodes write to the same segment?**

A: No. Leases prevent this:
1. Raft metadata has one `leader_node` per segment
2. Storage checks lease before every write
3. If you don't have the lease, append fails

**Q: How does rollover avoid data loss?**

A: The monitor proposes rollover with the exact entry count in the segment:
```rust
RolloverTopic {
    sealed_segment_entry_count: 200_000  // exactly what's in s3
}
```
Raft adds this to `last_sealed_entry_offset`, so the next segment starts at the right offset.

**Q: What if two monitors propose rollover at the same time?**

A: Raft serializes them. Only one RolloverTopic command applies at a time. The second one will see the segment already rolled over (entry count is now low) and won't propose again.

**Q: Why round-robin assignment?**

A: Simple load balancing. Each node gets 1/N of the segments. More sophisticated schemes could consider disk space, CPU, network, etc.

**Q: What's the difference between Walrus and Distributed Walrus?**

A:
- **Walrus**: Local WAL engine (like RocksDB), single-node, byte-oriented, io_uring/mmap backend
- **Distributed Walrus**: Multi-node coordinator, uses Raft for metadata, routes writes to Walrus instances, handles segments and rollovers

Think of Distributed Walrus as "Kafka" and Walrus as "the local log storage."

**Q: Why not just use Kafka?**

A: This is simpler (no partitions, no consumer groups, no Zookeeper), and tightly integrated with Walrus's high-performance local WAL. It's purpose-built for Walrus.

---

## File Guide

```
src/
├── main.rs           # Binary wiring: start Raft, storage, controller, monitor
├── config.rs         # CLI args (--node-id, --raft-host, --join-addr, etc.)
├── metadata.rs       # Raft state machine (TopicState, CreateTopic, RolloverTopic)
├── bucket.rs         # Storage wrapper (leases, locks, Walrus API)
├── controller/
│   ├── mod.rs        # NodeController (RPC handler, lease updates)
│   ├── internal.rs   # forward_append() implementation
│   ├── types.rs      # wal_key() helper
│   └── topics.rs     # (empty, placeholder)
├── monitor.rs        # Background loop (check sizes → propose rollover)
└── rpc.rs            # InternalOp enum (ForwardAppend, JoinCluster, TestControl)
```

---

## Architecture Principles

1. **Raft is the source of truth** - All ownership decisions go through Raft consensus
2. **Leases prevent split-brain** - Storage only writes if it owns the segment
3. **Segments are immutable** - Once sealed, they never change (simplifies reasoning)
4. **Round-robin is enough** - Simple load balancing beats complex heuristics
5. **Monitor is stateless** - Just reads metadata and proposes, doesn't mutate directly
6. **Polling is acceptable** - 100ms lease update loop is simple and correct enough

---

## Summary

**Distributed Walrus** = Raft metadata + Walrus storage + lease enforcement

- **Segments** spread writes across nodes
- **Raft** decides who owns what
- **Leases** prevent stale writes
- **Monitor** triggers rollovers when segments get full
- **Simple** round-robin, no partitions, no consumer groups

It's a distributed WAL focused on simplicity and correctness.
