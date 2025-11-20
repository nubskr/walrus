## Distributed Walrus v0.1 – Design Document

### 1. Purpose & Scope

- Deliver a minimal distributed WAL built from existing Walrus (per-node storage) and Octopii (distributed runtime) without modifying core Walrus internals.
- Scope covers metadata service, routing, write/read flows, part rollovers, and operational guidance for a “dumb bucket, smart metadata” architecture.

### 2. Goals / Non-Goals

**Goals**

- Append-only topics across multiple nodes.
- Central metadata authority deciding routing and rollovers.
- Manual part rollovers redirecting future writes to colder buckets.
- Buckets remain simple storage engines.

**Non-Goals (v0.1)**

- Automatic rebalancing or key-aware placement.
- KV semantics, per-tenant fairness, or hot-key mitigation beyond part moves.
- Historical replay guarantees beyond optional part scans.
- Moving sealed data between buckets.

### 3. Architecture Overview

```
+-------------------------------+
|         Kafka Client          |
|  (Produce/Fetch/Metadata)     |
+-------------------------------+
                |
                v
+-------------------------------+
|     App Node (per machine)    |
| +---------------------------+ |
| |  Kafka Protocol Server    | |
| +------------+--------------+ |
|              |                |
|   +----------v-----------+    |
|   |   Metadata Service   |    |
|   |  (Octopii/OpenRaft)  |    |
|   +----------+-----------+    |
|              |                |
|   +----------v-----------+    |
|   |  Metadata State      |    |
|   |  Machine (TopicMeta) |    |
|   +----------+-----------+    |
|              |                |
|   +----------v-----------+    |
|   |  Walrus Bucket       |    |
|   |  (local storage)     |    |
|   +----------+-----------+    |
|              |                |
|   +----------v-----------+    |
|   | Shipping Lane (bulk) |    |
|   +----------------------+    |
+-------------------------------+
                |
                v
       (other App Nodes via Raft + data RPCs)

Cluster view:

     +---------------------+          +---------------------+          +---------------------+
     |      App Node 1     |<-------->|      App Node 2     |<-------->|      App Node 3     |
     | +-----------------+ |  Raft +  | +-----------------+ |  Raft +  | +-----------------+ |
     | | Metadata svc   |<+--quorum--+>| Metadata svc   |<+--quorum--+>| Metadata svc   | |
     | +--------+--------+ |          | +--------+--------+ |          | +--------+--------+ |
     |          |          | data RPC |          |          | data RPC |          |          |
     | +--------v--------+ |<-------->| +--------v--------+ |<-------->| +--------v--------+ |
     | | Walrus Bucket   | |          | | Walrus Bucket   | |          | | Walrus Bucket   | |
     | +-----------------+ |          | +-----------------+ |          | +-----------------+ |
     +---------------------+          +---------------------+          +---------------------+
```

- Each node runs *both* the metadata service process (participating in the Raft cluster) and a co-located bucket process (Walrus + RPC endpoint) sharing the machine’s storage.
- Buckets still expose network endpoints so other nodes’ clients can write/read remotely, but placement favors the bucket on the same physical node unless overridden.
- Clients talk to metadata for routing/part info, and to buckets for data plane operations.
- In practice we run a single **app** binary per node that hosts *all* of this glue: Octopii/OpenRaft runtime (for metadata consensus), the custom replicated state machine (topic metadata), the co-located Walrus bucket, and a network server that exposes the cluster through a Kafka-compatible wire protocol plus auxiliary RPCs. When needed for bulk data movement (future replication, repair, or snapshots), the same app can invoke Octopii’s Shipping Lane to transfer Walrus files or block ranges between nodes.

### 4. Components

#### 4.1 Buckets (Storage Nodes)

- Each Raft node hosts a co-located bucket: Walrus (`src/wal`) scoped to a node-specific directory.
- Responsibilities: append/read log files per `(topic_id, part_id)`, maintain local durability, expose instrumentation.
- No placement logic; reject writes for unknown/sealed parts. Co-location simply means the “local” bucket is the preferred placement target for that node’s share of topics, but remote nodes can still access it via RPC.
- RPC surface:
  - `Append {topic_id, part_id, records[]}` and `BatchAppend`.
  - `Read {topic_id, part_id, from_offset, max_bytes}` (tail or from beginning within part).
  - `DescribeBucket` (list known parts, sizes) for ops.

#### 4.2 Metadata Service

- Runs as an Octopii/OpenRaft application using Walrus-backed log storage (`octopii/src/openraft`, `octopii/src/wal`). Every metadata node shares hardware with exactly one bucket node.
- State machine stores topic metadata and bucket descriptors.
- Exposes `route`, `create_topic`, `roll_part`, `describe_topic`, `update_bucket_load`.
- OpenRaft replication enforces single active part per topic and serializes rollovers.

#### 4.3 Client Library

- Wraps metadata RPC + bucket RPC with caching.
- Write path: `route` → `append` to bucket → on `StalePart`, refresh route and retry.
- Read path: minimal variant uses current part; extended variant uses `part_locations` for full replay.

#### 4.4 Control Loop (optional)

- Background task reads bucket load metrics (`update_bucket_load`) and issues `RollPart` when thresholds reached.

#### 4.5 Protocol & Data Transfer Layer

- Each node’s app process embeds a server that speaks the Kafka protocol (Metadata/Produce/Fetch) so existing Kafka clients can interact with Walrus without custom SDKs. Internally those requests are routed to the metadata service or local bucket, and responses are shaped like Kafka broker replies.
- For large data transfers—future replication followers, snapshotting, or backfills—the app leverages Octopii’s Shipping Lane to stream Walrus data across nodes with built-in chunking and checksums.

### 5. Data Model

#### 5.1 Topic Metadata

```
TopicMeta {
  topic_id: u64
  topic_name: String
  current_part: u32
  current_bucket: BucketId
  next_entry_id: u64
  part_locations: Vec<PartRange>
  created_at_ms: u64
  last_rollover_ms: u64
}

PartRange {
  bucket: BucketId
  start_part: u32
  end_part: u32
  approx_size_bytes: u64
  sealed_at_ms: u64
}
```

#### 5.2 Bucket Registry

```
BucketMeta {
  bucket_id: BucketId
  rpc_address: SocketAddr
  wal_dir: PathBuf
  status: {Online, Draining, Offline}
  capacity_bytes: u64
  used_bytes: u64
  rolling_window_bytes: u64
  last_heartbeat_ms: u64
}
```

#### 5.3 Metadata State Machine Commands

```
enum Command {
  CreateTopic { topic_name, topic_id, bucket_hint },
  RollPart { topic_id, new_bucket_id },
  RegisterBucket { bucket_meta },
  UpdateBucketLoad { bucket_id, used_bytes, rolling_window_bytes },
  SealPart { topic_id, part_id },
}
```

- `RouteRequest`/`DescribeTopic` served via Raft read path (no log entry) or by querying leader.

### 6. APIs

#### 6.1 Metadata RPCs

- `RouteTopic(topic_id|topic_name) -> {topic_id, bucket_id, part_id}`
- `DescribeTopic(topic_id) -> TopicMeta`
- `CreateTopic(name, optional_bucket)`
- `ForceRoll(topic_id, optional_bucket)`
- `RegisterBucket(bucket_meta)`
- `BucketHeartbeat(bucket_id, metrics)`
- `ListBuckets/Topics`

#### 6.2 Bucket RPCs

- `Append` / `BatchAppend`
- `Read`
- `ListParts(topic_id)`
- `ReportMetrics`

### 7. Flows

#### 7.1 Topic Creation

1. Client → Metadata: `CreateTopic(name)`
2. Metadata assigns `topic_id`, chooses initial bucket (round-robin or capacity-aware).
3. Initializes `TopicMeta` (`current_part=0`, `current_bucket=bucket`, `part_locations=[{bucket,0,0}]`).
4. Reply with route tuple.

#### 7.2 Write Path

```
Client         Metadata           Bucket
  | Route(t)      |                  |
  |-------------->|                  |
  |<--(B1,P5)-----|                  |
  | Append records to B1,P5          |
  |--------------------------------->|
  |<--------------- ack -------------|
```

- Client caches `(topic -> (bucket, part))`.
- On `StalePart`/`UnknownPart`, invalidate cache and rerun `Route`.

#### 7.3 Part Rollover

- Triggered by control loop when bucket metrics exceed `MAX_PART_BYTES` or `MAX_PART_AGE`.
- Metadata applies `RollPart`:
  - `old_part = current_part`, `new_part = old_part + 1`.
  - `new_bucket = pick_cold_bucket()`.
  - Update `TopicMeta`, append `PartRange` for old bucket.
  - Clients see new tuple on next `route`.
- No data movement in v0.1; only future writes move.

#### 7.4 Read Path

- Tail reads: `route` → `Read(topic_id, part_id=current_part)` on bucket.
- From-beginning (optional): metadata returns ordered `part_locations`; client scans buckets sequentially.

#### 7.5 Failure & Recovery

- Metadata cluster tolerates f failures with `2f+1` nodes.
- Buckets are stateless beyond Walrus files. On missed heartbeats metadata marks them Offline.
- Writes against Offline bucket fail; operator issues `ForceRoll` to move future writes.
- No automatic data copy; sealed data unavailable until bucket returns or is manually copied.

### 8. ASCII Dataflow

```
Kafka client view:

      Produce/Fetch      Metadata Request
   +-----------------+        |
   | Kafka Clients   |        v
   +-----------------+   +---------------------+
          |            / | App Node (leader)   |
          |-----------/-->| - Kafka server     |
          |          /    | - Metadata svc     |
          |         /     | - Walrus bucket    |
          |        /      +---------------------+
          |       /
          |      / Fetch remote data
          |     v
   +---------------------+    Raft replication     +---------------------+
   | App Node (follower) |<----------------------->| App Node (follower) |
   | - Kafka server      |                        | - Kafka server      |
   | - Metadata svc      |                        | - Metadata svc      |
   | - Walrus bucket     |                        | - Walrus bucket     |
   +---------------------+                        +---------------------+

Shipping Lane usage (future):

   [Bucket Leader] --(Shipping Lane stream)--> [Bucket Follower]
        |  read Walrus blocks                    | append blocks into local Walrus
        +----------------------------------------+
```

### 9. Deployment

#### 9.1 Metadata/Bucket Nodes

- Minimum 3 combined nodes; each node hosts:
  - Octopii/OpenRaft process (metadata service) with its Walrus-backed log files.
  - A local bucket process (Walrus data directory dedicated to that node).
  - Shared QUIC endpoint for metadata RPCs and bucket RPCs (or separate ports if preferred).
- Co-location simplifies ops (single machine per node) while still allowing remote writes for re-placement.

#### 9.2 Bucket Details

- Each node’s bucket uses its own Walrus root (e.g., `/data/bucket-<node-id>`).
- RPC server runs alongside metadata process; local clients can short-circuit to UNIX sockets, remote clients use QUIC/HTTP.
- Metrics loop still sends `BucketHeartbeat` to the co-located metadata process (which then replicates updates).

#### 9.3 Clients

- Rust SDK using Octopii transport.
- CLI/tooling: create topic, list buckets, trigger rollovers.

#### 9.4 Configuration

- `MAX_PART_BYTES`, `MAX_PART_AGE`
- `ROUTE_CACHE_TTL`
- `BUCKET_HEARTBEAT_INTERVAL`
- `ROLL_TRIGGER_THRESHOLD`
- `ROUTE_RETRY_LIMIT`

### 10. Observability

- Buckets: per-topic part size/age, append latency, fsync backlog.
- Metadata: topic counts per bucket, pending rollovers, heartbeats, Raft metrics.
- Logging: metadata logs command application and placement decisions; buckets log append/read/fync errors.

### 11. Testing

- Unit tests for metadata state machine (command application, invariants).
- Integration tests with Octopii cluster:
  - Topic spanning multiple rollovers.
  - Bucket failure + manual roll.
  - Concurrent writers verifying single active part.
- Soak tests generating synthetic load to verify placement changes over time.

### 12. Migration & Compatibility

- Existing single-node Walrus unaffected.
- Run metadata service + single bucket for incremental adoption; clients still route through metadata but only one bucket is available.

### 13. Future Extensions

1. Automatic rebalancing using bucket load metrics and sealed-part copy.
2. Replicated buckets: introduce logical “bucket groups” where each group has a leader bucket (serves writes) and `replication_factor-1` follower buckets. Followers tail the leader’s Walrus reader (or Shipping Lane streaming) to copy new entries asynchronously, similar to Kafka ISR. Metadata service tracks group membership so `route()` returns the leader plus follower list for future synchronous/async ack upgrades.
3. Key-aware routing (`route(topic, key)`).
4. Metadata sharding/caching for large deployments.
5. Dedicated historical index service to compress `part_locations`.
6. Kafka-compatible wire protocol: implement Kafka’s Metadata/Produce/Fetch APIs on metadata/bucket nodes so standard Kafka clients can talk to the cluster without custom SDKs. Map Walrus topics/parts to Kafka topics/partitions, translate Walrus offsets to Kafka log offsets, and reuse the existing protocol ecosystem.

### 14. Open Questions

- Should `part_locations` live elsewhere long term to avoid unbounded growth?
- How to coordinate in-flight appends during rollovers? (e.g., `SealNotice` timestamps + bucket rejects late writes).
- Do we expose per-record logical offsets or stick to `(part, byte offset)`?
- Authentication (mTLS) for bucket↔metadata RPCs?

This document is the blueprint for implementing the distributed Walrus v0.1 cluster using existing Walrus buckets and Octopii’s distributed runtime.
