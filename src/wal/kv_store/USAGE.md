# KV Store Usage Guide

## Quick Start

```rust
use walrus_rust::wal::kv_store::store::KvStore;

// Create a new KV store with a namespace
let store = KvStore::new("my_app")?;

// Write a key-value pair
store.put(b"user:123", b"alice")?;

// Read the value back
if let Some(value) = store.get(b"user:123")? {
    println!("User: {}", String::from_utf8_lossy(&value));
}

// List all keys
let all_keys = store.keys();
println!("Total keys: {}", all_keys.len());

// Compact to reclaim space
store.compact()?;
```

## Installation & Setup

### Directory Structure

By default, the KV store creates its data directory at:
```
wal_kv/<namespace>/
```

You can customize the root directory using the `WALRUS_KV_DIR` environment variable:

```bash
export WALRUS_KV_DIR=/var/lib/myapp/kv
```

Then:
```rust
let store = KvStore::new("production")?;
// Creates: /var/lib/myapp/kv/production/
```

### Namespace Isolation

Each namespace gets its own isolated directory:

```rust
let user_store = KvStore::new("users")?;      // wal_kv/users/
let session_store = KvStore::new("sessions")?; // wal_kv/sessions/
let cache_store = KvStore::new("cache")?;      // wal_kv/cache/

// Completely isolated, no cross-talk
user_store.put(b"alice", b"user_data")?;
session_store.put(b"alice", b"session_data")?;  // Different "alice"
```

## Core Operations

### PUT (Write/Update)

```rust
// Simple put
store.put(b"key", b"value")?;

// Overwrite existing key
store.put(b"key", b"new_value")?;  // Old value becomes garbage

// Binary data
let data: Vec<u8> = vec![0xFF, 0xAA, 0xBB];
store.put(b"binary_key", &data)?;

// Large values (up to u32::MAX)
let large_value = vec![0u8; 10_000_000];  // 10 MB
store.put(b"large", &large_value)?;
```

**Performance Tips:**
- Keys up to 65,535 bytes (u16::MAX)
- Values up to 4,294,967,295 bytes (u32::MAX)
- Small keys = less memory overhead in KeyDir
- Writes are O(1), append-only

### GET (Read)

```rust
// Basic read
match store.get(b"key")? {
    Some(value) => println!("Found: {:?}", value),
    None => println!("Key not found"),
}

// Handling deleted keys
store.put(b"temp", b"data")?;
// ... later, if you implement delete ...
// Key returns None (not found)

// Check if key exists
let exists = store.get(b"key")?.is_some();
```

**Performance:**
- O(1) hash lookup in KeyDir
- 1 disk read (or page cache hit)
- Typical latency: 10-100 microseconds (SSD)

### KEYS (List All Keys)

```rust
// Get all keys
let all_keys = store.keys();

// Filter keys
let user_keys: Vec<_> = all_keys
    .iter()
    .filter(|k| k.starts_with(b"user:"))
    .collect();

// Count keys
println!("Total keys: {}", all_keys.len());

// Iterate and process
for key in all_keys {
    if let Some(value) = store.get(&key)? {
        // Process key-value pair
    }
}
```

**Note:** Returns a snapshot of all keys at the time of call. New writes won't appear in the returned vector.

### COMPACT (Garbage Collection)

```rust
// Manual compaction
store.compact()?;

// Compact periodically in background
use std::time::Duration;
use std::thread;

thread::spawn(move || {
    loop {
        thread::sleep(Duration::from_secs(3600));  // Every hour
        if let Err(e) = store.compact() {
            eprintln!("Compaction failed: {}", e);
        }
    }
});
```

**When to Compact:**
- After many updates/deletes (high garbage ratio)
- Periodically (e.g., hourly, daily)
- When disk space is running low
- Before shutdown (optional, for cleanliness)

**Compaction Behavior:**
- Rewrites only live data to active file
- Deletes old immutable files
- Updates KeyDir with new locations
- Blocks writes during compaction (single writer model)

## Advanced Patterns

### Batch Operations

While there's no native batch API, you can optimize multiple writes:

```rust
// Write multiple keys efficiently
let entries = vec![
    (b"key1", b"value1"),
    (b"key2", b"value2"),
    (b"key3", b"value3"),
];

for (k, v) in entries {
    store.put(k, v)?;  // Each write is atomic
}

// All writes go to same active block (fast)
```

### Structured Data (Serialization)

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct User {
    name: String,
    email: String,
    age: u32,
}

// Write structured data
let user = User {
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
    age: 30,
};

let serialized = bincode::serialize(&user)?;
store.put(b"user:alice", &serialized)?;

// Read structured data
if let Some(data) = store.get(b"user:alice")? {
    let user: User = bincode::deserialize(&data)?;
    println!("{} ({})", user.name, user.email);
}
```

### Key Prefixing (Namespaces within Store)

```rust
// Use prefixes to organize data
fn user_key(id: &str) -> Vec<u8> {
    format!("user:{}", id).into_bytes()
}

fn session_key(id: &str) -> Vec<u8> {
    format!("session:{}", id).into_bytes()
}

fn config_key(name: &str) -> Vec<u8> {
    format!("config:{}", name).into_bytes()
}

// Usage
store.put(&user_key("alice"), b"user_data")?;
store.put(&session_key("xyz123"), b"session_data")?;
store.put(&config_key("max_connections"), b"100")?;

// List all users
let all_keys = store.keys();
let user_keys: Vec<_> = all_keys
    .iter()
    .filter(|k| k.starts_with(b"user:"))
    .collect();
```

### Sharding for High Throughput

Since KV store has a single writer, shard across multiple instances:

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

struct ShardedKvStore {
    shards: Vec<KvStore>,
}

impl ShardedKvStore {
    fn new(namespace: &str, num_shards: usize) -> io::Result<Self> {
        let mut shards = Vec::new();
        for i in 0..num_shards {
            let shard_name = format!("{}_shard_{}", namespace, i);
            shards.push(KvStore::new(&shard_name)?);
        }
        Ok(Self { shards })
    }

    fn shard_for_key(&self, key: &[u8]) -> &KvStore {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.shards.len();
        &self.shards[idx]
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.shard_for_key(key).put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.shard_for_key(key).get(key)
    }
}

// Usage
let store = ShardedKvStore::new("my_app", 16)?;  // 16 shards
store.put(b"key", b"value")?;  // Automatically routed to correct shard
```

### Time-To-Live (TTL) Pattern

Implement TTL using key naming:

```rust
use std::time::{SystemTime, UNIX_EPOCH};

fn write_with_ttl(
    store: &KvStore,
    key: &[u8],
    value: &[u8],
    ttl_seconds: u64,
) -> io::Result<()> {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() + ttl_seconds;

    // Store expiry as part of value
    let mut data = Vec::new();
    data.extend_from_slice(&expiry.to_le_bytes());
    data.extend_from_slice(value);

    store.put(key, &data)
}

fn read_with_ttl(store: &KvStore, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    if let Some(data) = store.get(key)? {
        if data.len() < 8 {
            return Ok(None);  // Invalid format
        }

        let expiry = u64::from_le_bytes(data[..8].try_into().unwrap());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if now < expiry {
            Ok(Some(data[8..].to_vec()))  // Not expired
        } else {
            Ok(None)  // Expired
        }
    } else {
        Ok(None)
    }
}

// Usage
write_with_ttl(&store, b"session:xyz", b"user_data", 3600)?;  // 1 hour TTL
```

### Background Compaction with Metrics

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CompactionStats {
    last_run: Arc<AtomicU64>,
    total_runs: Arc<AtomicU64>,
}

impl CompactionStats {
    fn new() -> Self {
        Self {
            last_run: Arc::new(AtomicU64::new(0)),
            total_runs: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_compaction(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_run.store(now, Ordering::Relaxed);
        self.total_runs.fetch_add(1, Ordering::Relaxed);
    }
}

fn start_background_compaction(
    store: Arc<KvStore>,
    interval: Duration,
) -> CompactionStats {
    let stats = CompactionStats::new();
    let stats_clone = CompactionStats {
        last_run: Arc::clone(&stats.last_run),
        total_runs: Arc::clone(&stats.total_runs),
    };

    std::thread::spawn(move || {
        loop {
            std::thread::sleep(interval);

            let start = Instant::now();
            match store.compact() {
                Ok(_) => {
                    stats_clone.record_compaction();
                    let elapsed = start.elapsed();
                    println!("Compaction completed in {:?}", elapsed);
                }
                Err(e) => {
                    eprintln!("Compaction error: {}", e);
                }
            }
        }
    });

    stats
}

// Usage
let store = Arc::new(KvStore::new("my_app")?);
let stats = start_background_compaction(
    Arc::clone(&store),
    Duration::from_secs(3600),  // Compact every hour
);
```

## Error Handling

```rust
use std::io::{Error, ErrorKind};

// Handle different error cases
match store.put(b"key", b"value") {
    Ok(_) => println!("Success"),
    Err(e) => {
        match e.kind() {
            ErrorKind::InvalidInput => {
                // Key or value too large
                eprintln!("Entry too large: {}", e);
            }
            ErrorKind::OutOfMemory => {
                // Disk full
                eprintln!("Disk full: {}", e);
            }
            _ => {
                // Other I/O errors
                eprintln!("I/O error: {}", e);
            }
        }
    }
}

// Handle corrupted data
match store.get(b"key") {
    Ok(Some(value)) => {
        // Success
    }
    Ok(None) => {
        // Key not found or deleted
    }
    Err(e) => {
        match e.kind() {
            ErrorKind::InvalidData => {
                // Checksum mismatch or corrupt entry
                eprintln!("Data corruption detected: {}", e);
                // Maybe trigger repair or alert
            }
            ErrorKind::NotFound => {
                // File disappeared (shouldn't happen)
                eprintln!("Data file missing: {}", e);
            }
            _ => {
                eprintln!("Read error: {}", e);
            }
        }
    }
}
```

## Recovery & Crash Safety

### Automatic Recovery

The KV store automatically recovers on startup:

```rust
// After a crash, simply create a new instance
let store = KvStore::new("my_app")?;

// Recovery happens automatically:
// 1. Scans all data files
// 2. Loads hint files (fast path)
// 3. Rebuilds KeyDir
// 4. Validates checksums
// 5. Ready to use
```

### Manual Recovery

```rust
// Check if recovery is needed (hint files missing)
use std::path::Path;

let data_dir = Path::new("wal_kv/my_app");
if data_dir.exists() {
    println!("Existing data found, will recover");
    let start = Instant::now();
    let store = KvStore::new("my_app")?;
    println!("Recovered in {:?}", start.elapsed());
    println!("Keys loaded: {}", store.keys().len());
}
```

### Data Validation

```rust
// Verify data integrity
let all_keys = store.keys();
let mut valid_count = 0;
let mut invalid_count = 0;

for key in all_keys {
    match store.get(&key) {
        Ok(Some(_)) => valid_count += 1,
        Ok(None) => {
            println!("Tombstone or deleted: {:?}", key);
        }
        Err(e) => {
            invalid_count += 1;
            eprintln!("Corrupt entry for key {:?}: {}", key, e);
        }
    }
}

println!("Valid: {}, Invalid: {}", valid_count, invalid_count);
```

## Performance Tuning

### Memory Management

```rust
// Monitor KeyDir memory usage
let num_keys = store.keys().len();
let estimated_memory = num_keys * 80;  // ~80 bytes per key
println!("Estimated KeyDir memory: {} MB", estimated_memory / 1_000_000);

// If memory is tight, consider:
// 1. Using shorter keys (hash long keys)
// 2. Sharding across multiple stores
// 3. Implementing key eviction (LRU)
```

### Disk Usage Monitoring

```rust
use std::fs;

fn get_disk_usage(namespace: &str) -> io::Result<u64> {
    let dir = Path::new("wal_kv").join(namespace);
    let mut total = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        total += entry.metadata()?.len();
    }

    Ok(total)
}

// Monitor and trigger compaction
let usage = get_disk_usage("my_app")?;
let num_keys = store.keys().len();
let avg_size = usage / num_keys.max(1) as u64;

println!("Disk usage: {} MB", usage / 1_000_000);
println!("Average entry size: {} bytes", avg_size);

// If usage is high relative to live data, compact
if usage > num_keys as u64 * avg_size * 2 {
    println!("High garbage ratio, compacting...");
    store.compact()?;
}
```

## Best Practices

### 1. Choose Appropriate Namespaces

```rust
// Good: Separate concerns
let user_db = KvStore::new("users")?;
let session_db = KvStore::new("sessions")?;
let cache_db = KvStore::new("cache")?;

// Bad: Everything in one namespace
let everything = KvStore::new("data")?;  // Hard to manage
```

### 2. Use Small Keys

```rust
// Good: Short, efficient keys
store.put(b"u:123", value)?;  // 5 bytes

// Bad: Long, wasteful keys
store.put(b"user:00000000-0000-0000-0000-000000000123", value)?;  // 42 bytes

// Better: Hash long keys if needed
use std::collections::hash_map::DefaultHasher;
let long_key = b"very_long_key_that_wastes_memory";
let mut hasher = DefaultHasher::new();
long_key.hash(&mut hasher);
let short_key = hasher.finish().to_le_bytes();
store.put(&short_key, value)?;
```

### 3. Compact Regularly

```rust
// Set up periodic compaction
let store = Arc::new(KvStore::new("my_app")?);
let store_clone = Arc::clone(&store);

std::thread::spawn(move || {
    loop {
        std::thread::sleep(Duration::from_secs(3600));
        store_clone.compact().ok();
    }
});
```

### 4. Handle Errors Gracefully

```rust
// Don't panic on errors
if let Err(e) = store.put(b"key", b"value") {
    eprintln!("Failed to write: {}", e);
    // Log, retry, or degrade gracefully
}

// Always check get() results
match store.get(b"key")? {
    Some(v) => process(v),
    None => use_default(),
}
```

### 5. Monitor Performance

```rust
use std::time::Instant;

// Track operation latency
let start = Instant::now();
store.put(b"key", b"value")?;
let duration = start.elapsed();
if duration.as_micros() > 1000 {
    println!("Slow write: {:?}", duration);
}
```

## Migration & Backup

### Export Data

```rust
use std::fs::File;
use std::io::Write;

fn export_to_json(store: &KvStore, path: &str) -> io::Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "{{")?;

    let keys = store.keys();
    for (i, key) in keys.iter().enumerate() {
        if let Some(value) = store.get(key)? {
            let key_str = String::from_utf8_lossy(key);
            let value_str = String::from_utf8_lossy(&value);
            write!(file, "  {:?}: {:?}", key_str, value_str)?;
            if i < keys.len() - 1 {
                writeln!(file, ",")?;
            }
        }
    }

    writeln!(file, "\n}}")?;
    Ok(())
}
```

### Import Data

```rust
fn import_from_backup(store: &KvStore, entries: Vec<(Vec<u8>, Vec<u8>)>) -> io::Result<()> {
    for (key, value) in entries {
        store.put(&key, &value)?;
    }
    Ok(())
}
```

## Troubleshooting

### Problem: High Memory Usage

**Cause:** Too many keys in KeyDir
**Solution:**
- Shard across multiple stores
- Use shorter keys
- Implement key eviction

### Problem: Slow Writes

**Cause:** Disk bottleneck or lock contention
**Solution:**
- Use faster storage (SSD)
- Shard across multiple stores
- Check disk I/O stats

### Problem: Slow Recovery

**Cause:** Missing hint files
**Solution:**
- Ensure hint files are generated (automatic)
- Don't delete .hint files
- Recovery generates them if missing

### Problem: Disk Space Growing

**Cause:** Garbage accumulation
**Solution:**
- Run `compact()` regularly
- Monitor garbage ratio
- Set up automatic compaction

## Example: Complete Application

```rust
use walrus_rust::wal::kv_store::store::KvStore;
use std::sync::Arc;
use std::time::Duration;

fn main() -> io::Result<()> {
    // 1. Initialize store
    let store = Arc::new(KvStore::new("my_app")?);
    println!("Store initialized with {} keys", store.keys().len());

    // 2. Start background compaction
    let store_clone = Arc::clone(&store);
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(3600));
            println!("Running compaction...");
            store_clone.compact().ok();
        }
    });

    // 3. Application logic
    store.put(b"config:version", b"1.0.0")?;
    store.put(b"user:alice", b"alice@example.com")?;
    store.put(b"user:bob", b"bob@example.com")?;

    // 4. Read data
    if let Some(email) = store.get(b"user:alice")? {
        println!("Alice's email: {}", String::from_utf8_lossy(&email));
    }

    // 5. List users
    let user_count = store.keys()
        .iter()
        .filter(|k| k.starts_with(b"user:"))
        .count();
    println!("Total users: {}", user_count);

    Ok(())
}
```

This guide covers the essential usage patterns for the KV store. For architecture details, see `ARCHITECTURE.md`.
