# Key-based Walrus Instances

Walrus supports namespaced storage so that different workloads can use distinct
write-ahead logs with their own durability guarantees. Each instance is backed
by its own subdirectory under `wal_files/`, and the directory name is a
sanitized version of the key you supply.

## Why It Helps

- **Tailored durability**: Critical topics can fsync aggressively without
  penalising lighter workloads.
- **Operational isolation**: Recovery sweeps, compaction and cleanup run per
  namespace, reducing the blast radius of corruption.
- **Simple ergonomics**: No need to juggle environment variables or manual
  directory management; just pick a key and go.

## Creating a Keyed Instance

```rust,no_run
use walrus_rust::{Walrus, ReadConsistency, FsyncSchedule};

# fn main() -> std::io::Result<()> {
let wal = Walrus::with_consistency_and_schedule_for_key(
    "transactions",
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::SyncEach,
)?;

wal.append_for_topic("payments", b"txn-42 completed")?;
# Ok(())
# }
```

Every file that instance creates lives inside `wal_files/transactions/`. You can
spin up additional instances with different keys to get isolated indexes and log
segments without touching the global configuration.

If you prefer to keep using the default constructors, set the environment
variable `WALRUS_INSTANCE_KEY=<your-key>` before creating the `Walrus` instance.
The namespace-aware path resolution will automatically place all files under
`wal_files/<sanitized-key>/`, so even legacy code can opt into isolation without
source changes.
