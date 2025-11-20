# Custom State Machines in Octopii

## Table of Contents

1. [Overview](#overview)
2. [The StateMachineTrait API](#the-statemachinetrait-api)
3. [Walrus: The Write-Ahead Log Engine](#walrus-the-write-ahead-log-engine)
4. [Types of Custom State Machines](#types-of-custom-state-machines)
5. [Implementation Patterns](#implementation-patterns)
6. [Advanced Topics](#advanced-topics)
7. [Performance Tuning](#performance-tuning)
8. [Gotchas and Best Practices](#gotchas-and-best-practices)
9. [Complete Examples](#complete-examples)

---

## Overview

Octopii allows you to implement **custom state machines** to replicate arbitrary application logic across a distributed cluster. The state machine abstraction provides:

- **Deterministic execution**: Same input → same output on all replicas
- **Durable persistence**: Backed by Walrus (Write-Ahead Log)
- **Snapshot/restore**: Fast catch-up for new/lagging nodes
- **Custom logic**: Implement any replicated data structure or application

### Key Concepts

```
┌────────────────────────────────────────────────────────────┐
│                    Your Application                        │
│                                                            │
│  ┌──────────────────────────────────────────────────┐    │
│  │         Custom State Machine                      │    │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │    │
│  │  │   apply()  │  │ snapshot() │  │ restore()  │ │    │
│  │  └────────────┘  └────────────┘  └────────────┘ │    │
│  └────────────┬─────────────────────────────────────┘    │
└───────────────┼──────────────────────────────────────────┘
                │
                ▼
┌────────────────────────────────────────────────────────────┐
│                    Octopii Raft Layer                      │
│  ┌──────────────────────────────────────────────────┐     │
│  │  Consensus + Replication + Membership            │     │
│  └────────────┬─────────────────────────────────────┘     │
└───────────────┼──────────────────────────────────────────┘
                │
                ▼
┌────────────────────────────────────────────────────────────┐
│                    Walrus (WAL)                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │  Durable Storage + Topics + Checkpointing        │     │
│  └──────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────┘
```

---

## The StateMachineTrait API

### Core Interface

**Location:** `src/state_machine.rs`

```rust
pub trait StateMachineTrait: Send + Sync {
    /// Apply a committed command to the state machine
    fn apply(&self, command: &[u8]) -> Result<Bytes, String>;

    /// Create a snapshot of the current state
    fn snapshot(&self) -> Vec<u8>;

    /// Restore state from a snapshot
    fn restore(&self, data: &[u8]) -> Result<(), String>;

    /// Optional: compact or clean up state
    fn compact(&self) -> Result<(), String> { Ok(()) }
}

pub type StateMachine = Arc<dyn StateMachineTrait>;
```

### Method Details

#### `apply(&self, command: &[u8]) -> Result<Bytes, String>`

**Purpose:** Execute a committed command and return a response.

**Contract:**
- MUST be deterministic (same input → same output)
- MUST be thread-safe (called concurrently)
- MUST be fast (runs in consensus critical path)
- SHOULD validate input
- SHOULD return meaningful errors

**Flow:**
```
Command committed by Raft
         │
         ▼
┌────────────────────┐
│ OpenRaft calls     │
│ apply_to_state_    │
│ machine()          │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Your apply()       │
│ implementation     │
└────────┬───────────┘
         │
         ▼
Response returned to client
```

**Example:**
```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Parse command
    let cmd_str = std::str::from_utf8(command)
        .map_err(|e| format!("Invalid UTF-8: {}", e))?;

    // Execute deterministically
    match cmd_str.split_whitespace().next() {
        Some("SET") => { /* ... */ Ok(Bytes::from("OK")) }
        Some("GET") => { /* ... */ Ok(Bytes::from("value")) }
        _ => Err("Unknown command".into())
    }
}
```

---

#### `snapshot(&self) -> Vec<u8>`

**Purpose:** Serialize the entire state for:
- Log compaction (reduce disk usage)
- Fast catch-up (transfer to lagging nodes)
- Crash recovery

**Contract:**
- MUST capture complete state
- MUST be consistent (atomic view)
- SHOULD be compact
- SHOULD be fast

**When Called:**
- Periodic log compaction
- On-demand via `force_snapshot_to_peer()`
- When peer lags by > `snapshot_lag_threshold`

**Example:**
```rust
fn snapshot(&self) -> Vec<u8> {
    let state = self.data.read().unwrap();
    bincode::serialize(&*state).unwrap_or_default()
}
```

---

#### `restore(&self, data: &[u8]) -> Result<(), String>`

**Purpose:** Restore state from a snapshot.

**Contract:**
- MUST completely replace current state
- MUST validate snapshot data
- MUST be atomic
- SHOULD handle version mismatches

**When Called:**
- Node joins cluster
- Node falls far behind (receives snapshot)
- Manual snapshot restore

**Example:**
```rust
fn restore(&self, data: &[u8]) -> Result<(), String> {
    let state: MyState = bincode::deserialize(data)
        .map_err(|e| format!("Invalid snapshot: {}", e))?;

    // Atomic replacement
    *self.data.write().unwrap() = state;
    Ok(())
}
```

---

#### `compact(&self) -> Result<(), String>` (Optional)

**Purpose:** Perform maintenance or cleanup.

**Use Cases:**
- Remove tombstones
- Merge compacted data structures
- Clear temporary data
- Optimize indices

**Example:**
```rust
fn compact(&self) -> Result<(), String> {
    let mut state = self.data.write().unwrap();

    // Remove expired entries
    state.retain(|_, v| !v.is_expired());

    // Optimize internal structures
    state.shrink_to_fit();

    Ok(())
}
```

---

## Walrus: The Write-Ahead Log Engine

Walrus is Octopii's high-performance Write-Ahead Log (WAL) implementation, providing durable persistence for state machines.

### Architecture

```
┌────────────────────────────────────────────────────────────┐
│                      Walrus                                │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │              Topic-Based Organization            │     │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │     │
│  │  │  logs    │  │ snapshots│  │  custom  │      │     │
│  │  │  topic   │  │  topic   │  │  topics  │      │     │
│  │  └──────────┘  └──────────┘  └──────────┘      │     │
│  └────────────┬─────────────────────────────────────┘     │
│               │                                           │
│  ┌────────────▼──────────────────────────────────────┐    │
│  │         Block-Based Storage                       │    │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐           │    │
│  │  │Block │ │Block │ │Block │ │Block │  ...       │    │
│  │  │  1   │ │  2   │ │  3   │ │  4   │           │    │
│  │  └──────┘ └──────┘ └──────┘ └──────┘           │    │
│  │                                                   │    │
│  │  Each block: 64KB, mmap'd, checksummed          │    │
│  └───────────────────────────────────────────────────┘    │
│                                                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │          Consistency Modes                       │     │
│  │  • StrictlyAtOnce: Every read checkpointed      │     │
│  │  • AtLeastOnce: Batch checkpointing             │     │
│  └──────────────────────────────────────────────────┘     │
│                                                            │
│  ┌──────────────────────────────────────────────────┐     │
│  │          Fsync Scheduling                        │     │
│  │  • SyncEach: Fsync every write                   │     │
│  │  • Milliseconds(N): Fsync every N ms            │     │
│  └──────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────┘
```

### API Overview

**Location:** `src/wal/wal/runtime/walrus.rs`

```rust
pub struct Walrus {
    // Internal fields
}

impl Walrus {
    // === Creation ===
    pub fn new() -> io::Result<Self>

    pub fn with_consistency(
        mode: ReadConsistency
    ) -> io::Result<Self>

    pub fn with_consistency_and_schedule(
        mode: ReadConsistency,
        fsync_schedule: FsyncSchedule,
    ) -> io::Result<Self>

    // === Writing ===
    pub fn append_for_topic(
        &self,
        topic: &str,
        data: &[u8]
    ) -> io::Result<()>

    pub fn batch_append_for_topic(
        &self,
        topic: &str,
        batch: &[&[u8]]
    ) -> io::Result<()>

    // === Reading ===
    pub fn read_next(
        &self,
        topic: &str,
        checkpoint: bool
    ) -> io::Result<Option<Entry>>

    pub fn batch_read_for_topic(
        &self,
        topic: &str,
        max_bytes: usize,
        checkpoint: bool
    ) -> io::Result<Vec<Entry>>
}
```

### Key Features

#### 1. Topic-Based Organization

Walrus organizes data into **topics** (like Kafka topics), allowing different subsystems to maintain independent logs.

```
┌─────────────────────────────────────────────┐
│              Walrus Instance                │
│                                             │
│  Topic: "wal_data"                          │
│    ├─ Entry 1: [command bytes]             │
│    ├─ Entry 2: [command bytes]             │
│    └─ Entry 3: [command bytes]             │
│                                             │
│  Topic: "snapshots"                         │
│    ├─ Entry 1: [snapshot at index 1000]    │
│    └─ Entry 2: [snapshot at index 2000]    │
│                                             │
│  Topic: "custom_audit_log"                  │
│    ├─ Entry 1: [audit event]               │
│    └─ Entry 2: [audit event]               │
└─────────────────────────────────────────────┘
```

**Usage:**
```rust
// Write to different topics
walrus.append_for_topic("wal_data", b"SET key val")?;
walrus.append_for_topic("audit_log", b"user_action")?;

// Read from specific topic
while let Some(entry) = walrus.read_next("wal_data", true)? {
    process_entry(entry);
}
```

---

#### 2. Read Consistency Modes

```rust
pub enum ReadConsistency {
    /// Checkpoint after every read (exactly-once semantics)
    StrictlyAtOnce,

    /// Checkpoint every N reads (at-least-once semantics, higher performance)
    AtLeastOnce { persist_every: u32 },
}
```

**StrictlyAtOnce:**
```
Read entry → Checkpoint immediately → Return entry

Pros: Exactly-once, no duplicate reads after crash
Cons: Higher latency (more disk I/O)
```

**AtLeastOnce:**
```
Read N entries → Checkpoint once → Return all entries

Pros: Lower latency, higher throughput
Cons: May re-read up to N entries after crash
```

**Example:**
```rust
// Strict consistency (default)
let walrus = Walrus::with_consistency(
    ReadConsistency::StrictlyAtOnce
)?;

// Batch consistency for performance
let walrus = Walrus::with_consistency(
    ReadConsistency::AtLeastOnce { persist_every: 100 }
)?;
```

---

#### 3. Fsync Scheduling

```rust
pub enum FsyncSchedule {
    /// Fsync after every write
    SyncEach,

    /// Fsync every N milliseconds
    Milliseconds(u64),
}
```

**Trade-offs:**

| Schedule | Latency | Throughput | Durability | Use Case |
|----------|---------|------------|------------|----------|
| `SyncEach` | High | Low | Immediate | Critical data, low write rate |
| `Milliseconds(N)` | Low | High | Delayed (up to N ms) | High throughput, acceptable lag |

**Example:**
```rust
// Low latency configuration
let walrus = Walrus::with_consistency_and_schedule(
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::SyncEach,
)?;

// High throughput configuration
let walrus = Walrus::with_consistency_and_schedule(
    ReadConsistency::AtLeastOnce { persist_every: 100 },
    FsyncSchedule::Milliseconds(200),
)?;
```

---

#### 4. Batch Operations

**Batch Writes:**
```rust
let batch = vec![
    b"SET key1 val1" as &[u8],
    b"SET key2 val2",
    b"SET key3 val3",
];
walrus.batch_append_for_topic("wal_data", &batch)?;
```

**Batch Reads:**
```rust
// Read up to 1MB of entries
let entries = walrus.batch_read_for_topic(
    "wal_data",
    1024 * 1024,  // max_bytes
    true          // checkpoint
)?;

for entry in entries {
    process(entry.data);
}
```

**Benefits:**
- Reduced system call overhead
- Better disk I/O batching
- Lower average latency
- Higher throughput

---

### Internal Data Flow

#### Write Path
```
User calls append_for_topic()
         │
         ▼
┌────────────────────┐
│ Get/create Writer  │
│ for topic          │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Allocate Block     │
│ from BlockAllocator│
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Write to Block     │
│ (mmap'd memory)    │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Compute checksum   │
│ (embedded in entry)│
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Trigger fsync      │
│ (if schedule met)  │
└────────────────────┘
```

#### Read Path
```
User calls read_next()
         │
         ▼
┌────────────────────┐
│ Check sealed       │
│ blocks first       │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ If exhausted,      │
│ check tail block   │
│ (active writer)    │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Read entry from    │
│ mmap'd block       │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Verify checksum    │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Checkpoint progress│
│ (if enabled)       │
└────────┬───────────┘
         │
         ▼
Return entry to caller
```

---

## Types of Custom State Machines

### 1. Key-Value Store

**Use Case:** Simple replicated storage

```rust
pub struct KvStateMachine {
    map: StdMutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl StateMachineTrait for KvStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;
        let mut tokens = s.split_whitespace();

        match tokens.next() {
            Some("SET") => {
                let key = tokens.next().ok_or("missing key")?;
                let val = tokens.next().ok_or("missing value")?;
                self.map.lock().unwrap().insert(
                    key.as_bytes().to_vec(),
                    val.as_bytes().to_vec()
                );
                Ok(Bytes::from("OK"))
            }
            Some("GET") => {
                let key = tokens.next().ok_or("missing key")?;
                match self.map.lock().unwrap().get(key.as_bytes()) {
                    Some(v) => Ok(Bytes::from(v.clone())),
                    None => Ok(Bytes::from("NOT_FOUND"))
                }
            }
            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&*self.map.lock().unwrap())
            .unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let map = bincode::deserialize(data)?;
        *self.map.lock().unwrap() = map;
        Ok(())
    }
}
```

---

### 2. Counter/Accumulator

**Use Case:** Distributed counters, metrics

```rust
pub struct CounterStateMachine {
    counters: RwLock<HashMap<String, i64>>,
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;
        let mut tokens = s.split_whitespace();

        match tokens.next() {
            Some("INC") => {
                let name = tokens.next().ok_or("missing name")?;
                let delta: i64 = tokens.next()
                    .unwrap_or("1")
                    .parse()
                    .map_err(|_| "invalid delta")?;

                let mut counters = self.counters.write().unwrap();
                let value = counters.entry(name.to_string())
                    .or_insert(0);
                *value += delta;
                Ok(Bytes::from(value.to_string()))
            }
            Some("GET") => {
                let name = tokens.next().ok_or("missing name")?;
                let counters = self.counters.read().unwrap();
                let value = counters.get(name).copied().unwrap_or(0);
                Ok(Bytes::from(value.to_string()))
            }
            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let counters = self.counters.read().unwrap();
        bincode::serialize(&*counters).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let counters = bincode::deserialize(data)?;
        *self.counters.write().unwrap() = counters;
        Ok(())
    }
}
```

---

### 3. Time-Series Database

**Use Case:** Replicated metrics, monitoring data

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct DataPoint {
    timestamp: u64,
    value: f64,
}

pub struct TimeSeriesStateMachine {
    series: RwLock<HashMap<String, Vec<DataPoint>>>,
}

impl StateMachineTrait for TimeSeriesStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;
        let mut tokens = s.split_whitespace();

        match tokens.next() {
            Some("PUT") => {
                let name = tokens.next().ok_or("missing name")?;
                let timestamp: u64 = tokens.next()
                    .ok_or("missing timestamp")?
                    .parse()
                    .map_err(|_| "invalid timestamp")?;
                let value: f64 = tokens.next()
                    .ok_or("missing value")?
                    .parse()
                    .map_err(|_| "invalid value")?;

                let mut series = self.series.write().unwrap();
                series.entry(name.to_string())
                    .or_insert_with(Vec::new)
                    .push(DataPoint { timestamp, value });

                Ok(Bytes::from("OK"))
            }
            Some("QUERY") => {
                let name = tokens.next().ok_or("missing name")?;
                let start: u64 = tokens.next()
                    .ok_or("missing start")?
                    .parse()
                    .map_err(|_| "invalid start")?;
                let end: u64 = tokens.next()
                    .ok_or("missing end")?
                    .parse()
                    .map_err(|_| "invalid end")?;

                let series = self.series.read().unwrap();
                let points = series.get(name)
                    .map(|pts| pts.iter()
                        .filter(|p| p.timestamp >= start && p.timestamp <= end)
                        .cloned()
                        .collect::<Vec<_>>())
                    .unwrap_or_default();

                let json = serde_json::to_string(&points)
                    .map_err(|e| e.to_string())?;
                Ok(Bytes::from(json))
            }
            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let series = self.series.read().unwrap();
        bincode::serialize(&*series).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let series = bincode::deserialize(data)?;
        *self.series.write().unwrap() = series;
        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        let mut series = self.series.write().unwrap();
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (30 * 24 * 60 * 60); // 30 days

        for points in series.values_mut() {
            points.retain(|p| p.timestamp >= cutoff);
        }

        Ok(())
    }
}
```

---

### 4. Event Log / Audit Trail

**Use Case:** Immutable event log, audit trail

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Event {
    id: u64,
    timestamp: u64,
    user: String,
    action: String,
    details: String,
}

pub struct EventLogStateMachine {
    events: RwLock<Vec<Event>>,
    next_id: AtomicU64,
}

impl StateMachineTrait for EventLogStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;
        let mut tokens = s.splitn(4, '\t');

        match tokens.next() {
            Some("LOG") => {
                let user = tokens.next().ok_or("missing user")?;
                let action = tokens.next().ok_or("missing action")?;
                let details = tokens.next().ok_or("missing details")?;

                let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let event = Event {
                    id,
                    timestamp,
                    user: user.to_string(),
                    action: action.to_string(),
                    details: details.to_string(),
                };

                self.events.write().unwrap().push(event);
                Ok(Bytes::from(id.to_string()))
            }
            Some("QUERY") => {
                let start_id: u64 = tokens.next()
                    .ok_or("missing start_id")?
                    .parse()
                    .map_err(|_| "invalid start_id")?;
                let limit: usize = tokens.next()
                    .unwrap_or("100")
                    .parse()
                    .map_err(|_| "invalid limit")?;

                let events = self.events.read().unwrap();
                let filtered: Vec<_> = events.iter()
                    .skip_while(|e| e.id < start_id)
                    .take(limit)
                    .cloned()
                    .collect();

                let json = serde_json::to_string(&filtered)
                    .map_err(|e| e.to_string())?;
                Ok(Bytes::from(json))
            }
            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let events = self.events.read().unwrap();
        let next_id = self.next_id.load(Ordering::SeqCst);
        let snapshot = (events.clone(), next_id);
        bincode::serialize(&snapshot).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let (events, next_id): (Vec<Event>, u64) =
            bincode::deserialize(data)?;
        *self.events.write().unwrap() = events;
        self.next_id.store(next_id, Ordering::SeqCst);
        Ok(())
    }
}
```

---

### 5. Service Registry / Configuration Store

**Use Case:** Distributed service discovery, config management

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceInfo {
    name: String,
    address: String,
    port: u16,
    metadata: HashMap<String, String>,
    last_heartbeat: u64,
}

pub struct RegistryStateMachine {
    services: RwLock<HashMap<String, ServiceInfo>>,
}

impl StateMachineTrait for RegistryStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;
        let mut tokens = s.split_whitespace();

        match tokens.next() {
            Some("REGISTER") => {
                let name = tokens.next().ok_or("missing name")?;
                let address = tokens.next().ok_or("missing address")?;
                let port: u16 = tokens.next()
                    .ok_or("missing port")?
                    .parse()
                    .map_err(|_| "invalid port")?;

                let info = ServiceInfo {
                    name: name.to_string(),
                    address: address.to_string(),
                    port,
                    metadata: HashMap::new(),
                    last_heartbeat: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                self.services.write().unwrap()
                    .insert(name.to_string(), info);

                Ok(Bytes::from("REGISTERED"))
            }
            Some("LOOKUP") => {
                let name = tokens.next().ok_or("missing name")?;
                let services = self.services.read().unwrap();

                match services.get(name) {
                    Some(info) => {
                        let result = format!("{}:{}", info.address, info.port);
                        Ok(Bytes::from(result))
                    }
                    None => Ok(Bytes::from("NOT_FOUND"))
                }
            }
            Some("HEARTBEAT") => {
                let name = tokens.next().ok_or("missing name")?;
                let mut services = self.services.write().unwrap();

                if let Some(info) = services.get_mut(name) {
                    info.last_heartbeat = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    Ok(Bytes::from("OK"))
                } else {
                    Ok(Bytes::from("NOT_REGISTERED"))
                }
            }
            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let services = self.services.read().unwrap();
        bincode::serialize(&*services).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let services = bincode::deserialize(data)?;
        *self.services.write().unwrap() = services;
        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        let mut services = self.services.write().unwrap();
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - 3600; // 1 hour

        services.retain(|_, info| info.last_heartbeat >= cutoff);
        Ok(())
    }
}
```

---

## Implementation Patterns

### Pattern 1: Immutable State Machine

**Pros:** Thread-safe by design, no locks needed
**Cons:** Higher memory usage, copying overhead

```rust
pub struct ImmutableStateMachine {
    state: Arc<RwLock<Arc<State>>>,
}

impl StateMachineTrait for ImmutableStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let current = self.state.read().unwrap().clone();
        let new_state = current.apply_command(command)?;
        *self.state.write().unwrap() = Arc::new(new_state);
        Ok(Bytes::from("OK"))
    }
}
```

---

### Pattern 2: Mutable State with Fine-Grained Locking

**Pros:** Better concurrency, lower overhead
**Cons:** More complex, potential deadlocks

```rust
pub struct ShardedStateMachine {
    shards: Vec<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl ShardedStateMachine {
    fn get_shard(&self, key: &[u8]) -> usize {
        let hash = hash_key(key);
        hash as usize % self.shards.len()
    }
}

impl StateMachineTrait for ShardedStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let (op, key, value) = parse_command(command)?;
        let shard_idx = self.get_shard(key);

        match op {
            Op::Set => {
                self.shards[shard_idx]
                    .write()
                    .unwrap()
                    .insert(key.to_vec(), value.to_vec());
                Ok(Bytes::from("OK"))
            }
            Op::Get => {
                let shard = self.shards[shard_idx].read().unwrap();
                match shard.get(key) {
                    Some(v) => Ok(Bytes::from(v.clone())),
                    None => Ok(Bytes::from("NOT_FOUND"))
                }
            }
        }
    }
}
```

---

### Pattern 3: Versioned State Machine

**Pros:** Time-travel queries, audit trail
**Cons:** Higher storage, complexity

```rust
pub struct VersionedStateMachine {
    versions: RwLock<Vec<(u64, State)>>,
    current_version: AtomicU64,
}

impl StateMachineTrait for VersionedStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;

        let mut versions = self.versions.write().unwrap();
        let current_state = versions.last()
            .map(|(_, s)| s.clone())
            .unwrap_or_default();

        let new_state = current_state.apply(command)?;
        versions.push((version, new_state));

        Ok(Bytes::from(version.to_string()))
    }

    // Support time-travel queries
    fn query_at_version(&self, version: u64) -> Option<State> {
        let versions = self.versions.read().unwrap();
        versions.iter()
            .rev()
            .find(|(v, _)| *v <= version)
            .map(|(_, s)| s.clone())
    }
}
```

---

## Advanced Topics

### 1. Custom Serialization

For better performance or compatibility:

```rust
use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize)]
pub struct MyState {
    data: HashMap<String, String>,
}

impl StateMachineTrait for MyStateMachine {
    fn snapshot(&self) -> Vec<u8> {
        // Zero-copy serialization with rkyv
        rkyv::to_bytes::<_, 256>(&*self.state.read().unwrap())
            .unwrap()
            .to_vec()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        // Zero-copy deserialization
        let archived = unsafe { rkyv::archived_root::<MyState>(data) };
        let state: MyState = archived.deserialize(&mut rkyv::Infallible)
            .map_err(|e| e.to_string())?;
        *self.state.write().unwrap() = state;
        Ok(())
    }
}
```

---

### 2. Incremental Snapshots

For large state machines:

```rust
pub struct IncrementalStateMachine {
    base_snapshot: RwLock<Vec<u8>>,
    delta_log: RwLock<Vec<Command>>,
    snapshot_threshold: usize,
}

impl StateMachineTrait for IncrementalStateMachine {
    fn snapshot(&self) -> Vec<u8> {
        let delta_log = self.delta_log.read().unwrap();

        if delta_log.len() > self.snapshot_threshold {
            // Full snapshot
            self.create_full_snapshot()
        } else {
            // Incremental: base + deltas
            let base = self.base_snapshot.read().unwrap().clone();
            let deltas = bincode::serialize(&*delta_log).unwrap();

            IncrementalSnapshot { base, deltas }
                .serialize()
        }
    }
}
```

---

### 3. Async I/O in State Machines

**WARNING:** `apply()` is synchronous, but you can use `tokio::task::block_in_place`:

```rust
impl StateMachineTrait for AsyncStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Use block_in_place for async operations
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.async_apply(command).await
            })
        })
    }
}

impl AsyncStateMachine {
    async fn async_apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Can use async I/O here
        let result = self.db.query(command).await?;
        Ok(Bytes::from(result))
    }
}
```

---

### 4. State Machine Composition

Combine multiple state machines:

```rust
pub struct CompositeStateMachine {
    kv_store: Arc<KvStateMachine>,
    counter: Arc<CounterStateMachine>,
    registry: Arc<RegistryStateMachine>,
}

impl StateMachineTrait for CompositeStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command)?;

        // Route to appropriate sub-state-machine
        match s.split(':').next() {
            Some("kv") => {
                let subcmd = &command[3..]; // Skip "kv:"
                self.kv_store.apply(subcmd)
            }
            Some("counter") => {
                let subcmd = &command[8..]; // Skip "counter:"
                self.counter.apply(subcmd)
            }
            Some("registry") => {
                let subcmd = &command[9..]; // Skip "registry:"
                self.registry.apply(subcmd)
            }
            _ => Err("unknown subsystem".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let kv_snap = self.kv_store.snapshot();
        let counter_snap = self.counter.snapshot();
        let registry_snap = self.registry.snapshot();

        CompositeSnapshot {
            kv: kv_snap,
            counter: counter_snap,
            registry: registry_snap,
        }.serialize()
    }
}
```

---

## Performance Tuning

### Measurement Points

```
┌─────────────────────────────────────────────┐
│          Performance Breakdown              │
├─────────────────────────────────────────────┤
│                                             │
│  1. Command Parsing           ~0.1-1 ms    │
│  2. Lock Acquisition          ~0.01 ms     │
│  3. State Update              ~0.1-10 ms   │
│  4. Lock Release              ~0.01 ms     │
│  5. Response Serialization    ~0.1-1 ms    │
│                                             │
│  Total apply() time: 0.32-12 ms            │
└─────────────────────────────────────────────┘
```

### Optimization Strategies

#### 1. Reduce Lock Contention

```rust
// Bad: Single global lock
struct SlowStateMachine {
    data: RwLock<HashMap<String, String>>,
}

// Good: Sharded locks
struct FastStateMachine {
    shards: Vec<RwLock<HashMap<String, String>>>,
}
```

#### 2. Use Efficient Data Structures

```rust
// Bad: Vec for lookups
data: Vec<(String, String)>

// Good: HashMap for lookups
data: HashMap<String, String>

// Better: FxHashMap for integer keys
use rustc_hash::FxHashMap;
data: FxHashMap<u64, String>
```

#### 3. Minimize Allocations

```rust
// Bad: Allocate on every call
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let s = String::from_utf8(command.to_vec())?; // Allocation!
    // ...
}

// Good: Work with borrowed data
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let s = std::str::from_utf8(command)?; // No allocation
    // ...
}
```

#### 4. Optimize Snapshots

```rust
// Bad: Expensive serialization
fn snapshot(&self) -> Vec<u8> {
    serde_json::to_vec(&*self.data.read().unwrap()).unwrap()
}

// Good: Binary serialization
fn snapshot(&self) -> Vec<u8> {
    bincode::serialize(&*self.data.read().unwrap()).unwrap()
}

// Better: Zero-copy with rkyv
fn snapshot(&self) -> Vec<u8> {
    rkyv::to_bytes(&*self.data.read().unwrap()).unwrap().to_vec()
}
```

---

## Gotchas and Best Practices

### Gotcha #1: Non-Determinism

**Problem:**
```rust
// BAD: Non-deterministic!
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let timestamp = SystemTime::now(); // ❌ Different on each replica!
    // ...
}
```

**Solution:**
```rust
// GOOD: Deterministic
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Extract timestamp from command
    let (timestamp, rest) = parse_command_with_timestamp(command)?;
    // Or use logical clock
    let logical_time = self.logical_clock.fetch_add(1);
    // ...
}
```

---

### Gotcha #2: Blocking Operations

**Problem:**
```rust
// BAD: Blocks consensus thread!
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    std::thread::sleep(Duration::from_secs(1)); // ❌ Blocks Raft!
    // ...
}
```

**Solution:**
```rust
// GOOD: Fast execution
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Keep apply() fast, defer heavy work
    self.work_queue.push(command);
    Ok(Bytes::from("QUEUED"))
}
```

---

### Gotcha #3: Snapshot Inconsistency

**Problem:**
```rust
// BAD: Non-atomic snapshot!
fn snapshot(&self) -> Vec<u8> {
    let part1 = self.data1.read().unwrap().clone();
    // Another thread modifies data2 here!
    let part2 = self.data2.read().unwrap().clone();
    serialize((part1, part2))
}
```

**Solution:**
```rust
// GOOD: Atomic snapshot
fn snapshot(&self) -> Vec<u8> {
    let guard1 = self.data1.read().unwrap();
    let guard2 = self.data2.read().unwrap();
    // Hold both locks until serialization complete
    serialize((&*guard1, &*guard2))
}

// BETTER: Single lock for related data
struct AtomicState {
    data1: T1,
    data2: T2,
}
state: RwLock<AtomicState>
```

---

### Best Practice #1: Validate Input

```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Always validate input
    if command.len() > MAX_COMMAND_SIZE {
        return Err("command too large".into());
    }

    let parsed = parse_command(command)
        .ok_or("invalid command format")?;

    // Validate semantics
    if !parsed.is_valid() {
        return Err("invalid command".into());
    }

    // Execute
    self.execute(parsed)
}
```

---

### Best Practice #2: Version Snapshots

```rust
#[derive(Serialize, Deserialize)]
struct VersionedSnapshot {
    version: u32,
    data: Vec<u8>,
}

fn snapshot(&self) -> Vec<u8> {
    bincode::serialize(&VersionedSnapshot {
        version: SNAPSHOT_VERSION,
        data: bincode::serialize(&*self.state.read().unwrap()).unwrap(),
    }).unwrap()
}

fn restore(&self, data: &[u8]) -> Result<(), String> {
    let snapshot: VersionedSnapshot = bincode::deserialize(data)?;

    match snapshot.version {
        1 => self.restore_v1(&snapshot.data),
        2 => self.restore_v2(&snapshot.data),
        _ => Err(format!("unsupported version {}", snapshot.version))
    }
}
```

---

### Best Practice #3: Instrument Performance

```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let start = Instant::now();

    let result = self.do_apply(command);

    let elapsed = start.elapsed();
    self.metrics.record_apply_latency(elapsed);

    if elapsed > Duration::from_millis(10) {
        warn!("Slow apply: {:?} for command {:?}", elapsed, command);
    }

    result
}
```

---

## Complete Examples

### Example: Distributed Task Queue

```rust
use octopii::{StateMachineTrait, OctopiiNode, Config, OctopiiRuntime};
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use std::collections::{VecDeque, HashMap};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Task {
    id: u64,
    payload: String,
    status: TaskStatus,
    assigned_to: Option<String>,
    created_at: u64,
    completed_at: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed,
}

pub struct TaskQueueStateMachine {
    tasks: RwLock<HashMap<u64, Task>>,
    queue: RwLock<VecDeque<u64>>,
    next_id: AtomicU64,
}

impl TaskQueueStateMachine {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            queue: RwLock::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
        }
    }
}

impl StateMachineTrait for TaskQueueStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        let mut tokens = s.splitn(3, '\t');

        match tokens.next() {
            Some("SUBMIT") => {
                let payload = tokens.next()
                    .ok_or("missing payload")?
                    .to_string();

                let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                let task = Task {
                    id,
                    payload,
                    status: TaskStatus::Pending,
                    assigned_to: None,
                    created_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    completed_at: None,
                };

                self.tasks.write().unwrap().insert(id, task);
                self.queue.write().unwrap().push_back(id);

                Ok(Bytes::from(id.to_string()))
            }

            Some("CLAIM") => {
                let worker = tokens.next()
                    .ok_or("missing worker id")?;

                let mut queue = self.queue.write().unwrap();
                let task_id = queue.pop_front()
                    .ok_or("no tasks available")?;

                let mut tasks = self.tasks.write().unwrap();
                let task = tasks.get_mut(&task_id)
                    .ok_or("task not found")?;

                task.status = TaskStatus::Assigned;
                task.assigned_to = Some(worker.to_string());

                let response = serde_json::to_string(&task)
                    .map_err(|e| e.to_string())?;
                Ok(Bytes::from(response))
            }

            Some("COMPLETE") => {
                let task_id: u64 = tokens.next()
                    .ok_or("missing task_id")?
                    .parse()
                    .map_err(|_| "invalid task_id")?;

                let mut tasks = self.tasks.write().unwrap();
                let task = tasks.get_mut(&task_id)
                    .ok_or("task not found")?;

                task.status = TaskStatus::Completed;
                task.completed_at = Some(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs());

                Ok(Bytes::from("OK"))
            }

            Some("STATUS") => {
                let task_id: u64 = tokens.next()
                    .ok_or("missing task_id")?
                    .parse()
                    .map_err(|_| "invalid task_id")?;

                let tasks = self.tasks.read().unwrap();
                let task = tasks.get(&task_id)
                    .ok_or("task not found")?;

                let response = serde_json::to_string(&task)
                    .map_err(|e| e.to_string())?;
                Ok(Bytes::from(response))
            }

            _ => Err("unknown command".into())
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let tasks = self.tasks.read().unwrap();
        let queue = self.queue.read().unwrap();
        let next_id = self.next_id.load(Ordering::SeqCst);

        let snapshot = (tasks.clone(), queue.clone(), next_id);
        bincode::serialize(&snapshot).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let (tasks, queue, next_id) = bincode::deserialize(data)
            .map_err(|e| e.to_string())?;

        *self.tasks.write().unwrap() = tasks;
        *self.queue.write().unwrap() = queue;
        self.next_id.store(next_id, Ordering::SeqCst);

        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        let mut tasks = self.tasks.write().unwrap();

        // Remove completed tasks older than 24 hours
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (24 * 60 * 60);

        tasks.retain(|_, task| {
            task.status != TaskStatus::Completed ||
            task.completed_at.unwrap_or(0) >= cutoff
        });

        Ok(())
    }
}

// Usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);
    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5001".parse()?,
        is_initial_leader: true,
        ..Default::default()
    };

    let sm = Arc::new(TaskQueueStateMachine::new());
    let node = OctopiiNode::new_with_state_machine(config, runtime, sm).await?;
    node.start().await?;

    // Submit a task
    let task_id = node.propose(b"SUBMIT\tprocess_video.mp4".to_vec()).await?;
    println!("Submitted task: {}", String::from_utf8_lossy(&task_id));

    // Worker claims a task
    let task = node.propose(b"CLAIM\tworker-1".to_vec()).await?;
    println!("Claimed task: {}", String::from_utf8_lossy(&task));

    // Complete the task
    let result = node.propose(format!("COMPLETE\t{}", String::from_utf8_lossy(&task_id)).as_bytes().to_vec()).await?;
    println!("Completed: {}", String::from_utf8_lossy(&result));

    Ok(())
}
```

---

**Version:** 0.1.0
**Last Updated:** 2025-11-13
