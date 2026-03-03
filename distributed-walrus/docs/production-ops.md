# Production Operations Guide

## Deployment Overview

Walrus is a distributed message streaming platform built on a WAL storage engine with Raft consensus for metadata coordination. A production cluster requires a minimum of **3 nodes** (tolerates 1 failure) spread across availability zones.

### Key Architectural Properties

- Raft coordinates **metadata only** (topic/segment/leader mappings), not the data path
- Each topic segment has a **single writer** (lease-fenced), leadership rotates round-robin on rollover
- Segment rollover occurs at 1M entries by default (configurable via `WALRUS_MAX_SEGMENT_ENTRIES`)
- Walrus is **I/O-bound** -- disk performance is the primary bottleneck

---

## AWS Infrastructure

### EC2 Instance Sizing

| Tier | Instance | vCPU | RAM | Use Case |
|------|----------|------|-----|----------|
| Small | `m6i.large` | 2 | 8 GB | Dev/staging, < 100K writes/sec |
| Medium | `m6i.xlarge` | 4 | 16 GB | Production, moderate throughput |
| High | `i4i.xlarge` | 4 | 32 GB | High throughput, local NVMe |

**Avoid**: `t3`/burstable (burst credits deplete under sustained writes), `c`-series (overpays for CPU), `r`-series (overpays for RAM).

### EBS Configuration (gp3)

| Setting | Default (free tier) | Recommended Production |
|---------|--------------------|-----------------------|
| IOPS | 3,000 | 10,000 -- 16,000 |
| Throughput | 125 MB/s | 500 -- 1,000 MB/s |

Extra IOPS cost ~$0.005/IOPS/mo. Going from 3K to 10K adds ~$35/mo per volume.

**Do NOT use EFS/NFS** -- WAL requires local filesystem semantics (pread/pwrite, fsync).

If using `i4i` instances with local NVMe: storage is ephemeral (lost on stop/terminate). Acceptable because Raft provides replication, but never let more than 1 node lose storage simultaneously in a 3-node cluster.

### Network Layout

```
VPC (10.0.0.0/16)
├── Private Subnet AZ-a (10.0.1.0/24)  -->  Node 1
├── Private Subnet AZ-b (10.0.2.0/24)  -->  Node 2
└── Private Subnet AZ-c (10.0.3.0/24)  -->  Node 3

Security Groups:
  sg-walrus-raft:   TCP 6001-6003, source: sg-walrus-raft (nodes only)
  sg-walrus-client: TCP 8080, source: sg-app (application tier)
```

| Port | Purpose | Exposure |
|------|---------|----------|
| `--raft-port` (6000+) | Raft consensus + internal RPC | Private -- cluster nodes only |
| `--client-port` (8080) | Client TCP connections | Internal -- app tier, behind NLB |

Place an **internal NLB** (TCP passthrough) in front of client ports for load distribution.

---

## Cluster Startup

### Node 1 (Bootstrap)

```bash
app-node \
  --node-id 1 \
  --data-dir /data/walrus/node1 \
  --raft-host 0.0.0.0 --raft-port 6001 \
  --raft-advertise-host 10.0.1.10 \
  --client-host 0.0.0.0 --client-port 8080 \
  --initial-peer 10.0.2.10:6002 \
  --initial-peer 10.0.3.10:6003
```

### Node 2 & 3 (Join)

```bash
app-node \
  --node-id 2 \
  --data-dir /data/walrus/node2 \
  --raft-host 0.0.0.0 --raft-port 6002 \
  --raft-advertise-host 10.0.2.10 \
  --client-host 0.0.0.0 --client-port 8080 \
  --join 10.0.1.10:6001
```

### Environment Variables

| Variable | Recommended | Notes |
|----------|-------------|-------|
| `RUST_LOG` | `info` | `debug` is too noisy for production |
| `WALRUS_MAX_SEGMENT_ENTRIES` | `1000000` | Higher = less rollover overhead |
| `WALRUS_MONITOR_CHECK_MS` | `10000` | Rollover check interval |
| `WALRUS_RPC_TIMEOUT_MS` | `4000` | Increase if cross-AZ latency is high |
| `WALRUS_DISABLE_IO_URING` | unset | Only set if io_uring causes issues |

---

## Monitoring

### Built-in Endpoints

**`METRICS` command** -- returns Raft state (JSON):
- Current term, vote, log state
- Membership configuration
- Last log index, commit index, applied index
- Leader/follower status

**`STATE <topic>` command** -- returns topic metadata (JSON):
- Current segment number and leader node
- Sealed segment entry counts
- Segment-to-leader mappings

### What to Monitor

| Metric | Source | Why |
|--------|--------|-----|
| Raft term | `METRICS` | Rapid increases = frequent elections = network instability |
| Commit index vs applied index | `METRICS` | Gap = state machine falling behind |
| Log index vs commit index | `METRICS` | Gap = uncommitted entries piling up |
| Leader ID stability | `METRICS` | Flapping = problem |
| Segment rollover cadence | `STATE` | Stuck = Raft can't commit or monitor loop stalled |
| Topic entry count | `STATE` / API | Track throughput |

### Infrastructure Metrics (External Tooling)

| Metric | Alert Threshold |
|--------|----------------|
| Disk usage | > 80% volume capacity |
| Disk write latency (p99) | > 10ms |
| Network packet loss between nodes | > 0.1% |
| TCP connect to client port | Fails = node down |
| File descriptor count | > 80% of ulimit |
| Process memory RSS | > 80% of allocated |

---

## Operational Runbooks

### 1. Node Crash Recovery

**Symptoms**: Process exits, TCP connections drop.

1. Check logs for panic, OOM, or disk errors
2. Verify disk health (`smartctl`, `dmesg`)
3. Restart with **same `--node-id` and `--data-dir`**
4. WAL data persists on disk; Raft log replays automatically
5. Verify via `METRICS` -- applied index should catch up to leader's commit index

**Never** change `--node-id` for a recovering node. **Never** wipe `--data-dir` unless deliberately decommissioning.

### 2. Raft Election Storm

**Symptoms**: Rapidly incrementing term numbers, frequent leader changes, write timeouts.

1. Check inter-node network (all Raft ports reachable between nodes)
2. Check disk latency -- slow fsync causes Raft heartbeat timeouts
3. Check for CPU throttling (container limits, cgroup constraints)
4. If one node is partitioned, check security group rules and NACLs
5. Increase `WALRUS_RPC_TIMEOUT_MS` if cross-AZ latency is high
6. If persistent, isolate the problematic node and investigate

### 3. Segment Rollover Stuck

**Symptoms**: Topic entry count exceeds threshold but segment number doesn't advance.

1. Verify Raft has a leader (`METRICS` -- `vote` field shows elected leader)
2. Check for "proposing rollover" in logs (monitor loop activity)
3. Verify `WALRUS_MONITOR_CHECK_MS` is reasonable
4. If Raft can't commit (quorum loss), restore quorum first
5. Check logs for `RolloverTopic` proposal failures

### 4. Quorum Loss

**Symptoms**: All writes fail, no Raft leader elected.

1. **Priority**: restore at least `ceil((N+1)/2)` nodes (2 of 3, 3 of 5)
2. Reads on sealed segments may still work (local WAL data)
3. New writes are completely blocked until quorum restores
4. Once quorum restores, Raft auto-elects leader, leases resync within 100ms
5. Do NOT attempt manual Raft state manipulation unless all other options are exhausted

### 5. Adding a Node (Scale Out)

1. Provision instance in same VPC, same security groups
2. Start with `--join`:
   ```bash
   app-node --node-id 4 --data-dir /data/walrus/node4 \
     --raft-host 0.0.0.0 --raft-port 6004 \
     --raft-advertise-host 10.0.4.10 \
     --client-host 0.0.0.0 --client-port 8080 \
     --join 10.0.1.10:6001
   ```
3. Verify via `METRICS` -- new node appears in membership
4. New node is picked up for round-robin segment leadership on future rollovers
5. Update NLB target group to include new client port

### 6. Rolling Vertical Upgrade

1. Identify which node to upgrade first (prefer a follower, not the active segment leader)
2. If the node is a segment leader, wait for natural rollover or force one to move leadership away
3. Stop the node
4. Resize instance / swap disk
5. Start the node with same `--node-id` and `--data-dir`
6. Verify recovery via `METRICS`
7. Repeat for remaining nodes, **one at a time** (3-node cluster can only lose 1)

### 7. Disk Full

**Symptoms**: Write failures, potential process crash.

1. Check which node hit capacity
2. WAL files are in `<data-dir>/node_X/user_data/`
3. No built-in retention/GC currently -- manual cleanup required
4. Identify fully-consumed sealed segments, remove old WAL files from `user_data/data_plane/`
5. Long-term: expand EBS volume (online resize supported for gp3), implement external retention, or archive sealed segments to S3

### 8. Client "NotLeaderForPartition" Errors

**Symptoms**: PUT commands return `NotLeaderForPartition`.

1. This is **normal** -- client hit a non-leader node for the active segment
2. The node controller auto-forwards to the correct leader via RPC
3. If forwarding fails, check inter-node RPC connectivity (Raft ports)
4. Run `STATE <topic>` to see which node is the current leader
5. If no leader exists, check Raft health

### 9. Performance Degradation

1. Check disk IOPS and write latency -- this is almost always the bottleneck
2. Verify io_uring is active (Linux only; disabled by `WALRUS_DISABLE_IO_URING`)
3. Check fsync interval -- `SyncEach` mode caps at ~5K writes/sec vs 1.2M without
4. Check segment rollover frequency -- too frequent adds Raft overhead
5. Check batch sizes -- single-entry writes are much slower than batched (up to 2000)
6. Check network latency between nodes -- affects forwarded writes
7. Temporarily enable `RUST_LOG=debug` to identify specific bottleneck

---

## Filesystem Layout

```
/data/walrus/node_X/
  raft_meta/         # Raft log (Octopii WAL)
  user_data/
    data_plane/      # Topic segment WAL files
    wal_files/       # WAL storage files (~1GB each)
```

---

## Performance Reference

### Storage Engine Benchmarks (Single Node)

Benchmarked with single-entry `append_for_topic()` using `pwrite()` syscalls, 500B-1KB random payloads, 10 concurrent writer threads (each writing to its own topic).

**Without fsync** (async 200ms default -- typical production config):

| System | Avg Throughput (writes/sec) | Avg Bandwidth (MB/s) | Max Throughput (writes/sec) | Max Bandwidth (MB/s) |
|--------|----------------------------|----------------------|----------------------------|----------------------|
| Walrus | 1,205,762 | 876.22 | 1,593,984 | 1,158.62 |
| Kafka | 1,112,120 | 808.33 | 1,424,073 | 1,035.74 |
| RocksDB | 432,821 | 314.53 | 1,000,000 | 726.53 |

**With fsync on every write** (`SyncEach` -- maximum durability):

| System | Avg Throughput (writes/sec) | Avg Bandwidth (MB/s) | Max Throughput (writes/sec) | Max Bandwidth (MB/s) |
|--------|----------------------------|----------------------|----------------------------|----------------------|
| RocksDB | 5,222 | 3.79 | 10,486 | 7.63 |
| Walrus | 4,980 | 3.60 | 11,389 | 8.19 |
| Kafka | 4,921 | 3.57 | 11,224 | 8.34 |

*Kafka benchmarked as single broker (no replication, no networking overhead). RocksDB benchmarked against its WAL. These are apples-to-apples storage engine comparisons.*

### Fsync Mode Impact

The fsync schedule is the single most impactful configuration knob:

| Fsync Mode | Throughput | Durability | Use Case |
|------------|-----------|------------|----------|
| `NoFsync` | ~1.2M writes/sec | Data at risk on power loss | Ephemeral/replayable data |
| `Milliseconds(200)` (default) | ~1.2M writes/sec | Up to 200ms of data at risk | Production default -- good balance |
| `Milliseconds(1000)` | ~1.2M writes/sec | Up to 1s of data at risk | High-throughput, tolerant workloads |
| `SyncEach` | ~5K writes/sec | No data loss | Financial, audit logs |

The gap between `NoFsync`/`Milliseconds(*)` and `SyncEach` is ~240x. This is a hardware limitation (disk fsync latency), not a Walrus limitation -- Kafka and RocksDB show the same cliff.

### Batch Writes (io_uring)

Batch writes via `batch_append_for_topic()` use io_uring on Linux for submission batching. Up to 2,000 entries per batch.

| Configuration | Throughput | Notes |
|---------------|-----------|-------|
| 16 threads, 2000 entries/batch, no fsync | Highest throughput | io_uring submission batching |
| 16 threads, 256 entries/batch, no fsync | Good throughput | Default batch benchmark config |
| Single entry writes, 10 threads | ~1.2M writes/sec | pwrite() per entry |

Batch writes amortize syscall overhead. The larger the batch, the fewer kernel transitions. On Linux with io_uring, this is where Walrus shines brightest.

### Read Performance

Read benchmarks use 5 threads, batch reads up to 5GB per call, 100KB entries:

- **Write phase**: Populates data at full speed
- **Read phase**: Sequential consumption from topic cursors
- Read throughput is typically disk-bandwidth-bound (sequential reads)
- Batch reads (`batch_read_for_topic`) are significantly faster than single-entry reads

### Distributed Cluster Overhead

| Factor | Impact |
|--------|--------|
| Raft metadata replication | Negligible -- metadata only, not data path |
| Lease sync loop (100ms) | Negligible -- lightweight in-memory check |
| Cross-node write forwarding | +1 network RTT per forwarded write |
| Segment rollover | Brief pause (~100ms) during leadership transfer |
| Monitor loop (10s) | Negligible -- periodic entry count check |

In a well-configured cluster where clients connect to the segment leader directly (or via NLB that happens to route there), distributed overhead is near-zero for writes. The data path is a direct WAL append, identical to single-node.

### Running Benchmarks

```bash
# Single-entry write throughput (10 threads, 2 min)
make bench-writes

# With fsync on every write
make bench-writes-sync

# With 100ms fsync interval
make bench-writes-fast

# Read throughput (1 min write + 1 min read)
make bench-reads

# Thread scaling curve (1-10 threads, 30s each)
make bench-scaling

# Batch write scaling (1-10 threads, 256 entries/batch)
make bench-batch-scaling

# Custom thread count
THREADS=32 make bench-scaling

# Custom batch size
BATCH=2000 THREADS=16 make bench-batch-scaling

# Force mmap backend (instead of io_uring)
BACKEND=mmap make bench-writes

# Walrus vs RocksDB comparison
make bench-walrus-vs-rocksdb

# Visualize results
make show-writes
make show-reads
make show-scaling
make show-batch-scaling
```

All benchmarks output CSV files for plotting and emit live progress to stdout.

---

## Scaling Guidance

**Scale vertically first** -- Walrus is I/O-bound, so faster disks (higher IOPS, NVMe) have the most impact. A single node with better storage will outperform adding more nodes, because:

- More nodes = more Raft coordination overhead
- Segment leadership is single-writer -- more nodes doesn't speed up writes to one topic
- Cross-node forwarding adds latency

**Scale horizontally when**:

- You need more **fault tolerance** (5 nodes tolerates 2 failures vs 1)
- You have **many topics** and want to distribute segment leaders across more disks
- **Read throughput** is the bottleneck (sealed segments readable from any replica)

## we collect these metrics:

1,users_total
2,users_daily
3,users_last_24_hours
4,users_last_30_days
5,users_month_to_date
6,messages_total
7,messages_daily
8,messages_last_24_hours
9,messages_last_30_days
10,messages_month_to_date
11,concurrent_users
12,concurrent_connections
13,translations_daily
14,image_moderations_daily
17,channels_total
15,users_engaged_last_30_days
16,users_engaged_month_to_date
