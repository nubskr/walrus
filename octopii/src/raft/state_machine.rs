use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const TOPIC_STATE_MACHINE: &str = "state_machine";
const TOPIC_STATE_MACHINE_SNAPSHOT: &str = "state_machine_snapshot";

// State machine compaction threshold: checkpoint after this many operations
const STATE_MACHINE_COMPACTION_THRESHOLD: usize = 5000;

/// Trait for implementing custom replicated state machines
///
/// Implement this trait to create your own state machine that will be
/// replicated across the Raft cluster. The trait provides methods for:
/// - Applying commands to the state machine
/// - Creating and restoring snapshots
/// - Compaction for long-term storage efficiency
///
/// # Example
///
/// ```rust
/// use octopii::raft::StateMachineTrait;
/// use bytes::Bytes;
///
/// struct CounterStateMachine {
///     counter: std::sync::RwLock<u64>,
/// }
///
/// impl StateMachineTrait for CounterStateMachine {
///     fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
///         // Parse command and update counter
///         // ...
///         Ok(Bytes::from("OK"))
///     }
///
///     fn snapshot(&self) -> Vec<u8> {
///         // Serialize current state
///         // ...
///         vec![]
///     }
///
///     fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
///         // Deserialize and restore state
///         Ok(())
///     }
///
///     fn compact(&self) -> Result<(), String> {
///         // Optional: perform compaction
///         Ok(())
///     }
/// }
/// ```
pub trait StateMachineTrait: Send + Sync {
    /// Apply a command to the state machine
    ///
    /// This method is called when a command is committed by Raft. The implementation
    /// should be deterministic - given the same command sequence, all replicas must
    /// produce the same state.
    ///
    /// # Arguments
    /// * `command` - The command bytes to apply (format is implementation-defined)
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The result of applying the command
    /// * `Err(String)` - An error message if the command is invalid
    fn apply(&self, command: &[u8]) -> Result<Bytes, String>;

    /// Create a snapshot of the current state
    ///
    /// Snapshots are used for:
    /// - Fast recovery after crashes
    /// - Catching up slow followers
    /// - Log compaction
    ///
    /// # Returns
    /// A byte vector containing the serialized state
    fn snapshot(&self) -> Vec<u8>;

    /// Restore state from a snapshot
    ///
    /// This method is called when loading a snapshot, either during recovery
    /// or when catching up as a follower.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot bytes to restore from
    ///
    /// # Returns
    /// * `Ok(())` - Snapshot successfully restored
    /// * `Err(String)` - An error message if the snapshot is invalid
    fn restore(&self, snapshot: &[u8]) -> Result<(), String>;

    /// Compact the state machine (optional)
    ///
    /// Called periodically to allow the state machine to compact its internal
    /// state and reclaim space. The default implementation does nothing.
    fn compact(&self) -> Result<(), String> {
        Ok(())
    }
}

// Serializable types for KV state machine data
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

pub struct StateMachineEntry {
    pub key: String,
    pub value: Vec<u8>,
}

// Serializable snapshot type for KV state machine
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

pub struct StateMachineSnapshot {
    pub entries: Vec<(String, Vec<u8>)>,
}

/// Simple key-value state machine implementation (NOW DURABLE!)
///
/// This is the default state machine that implements a basic key-value store
/// with SET, GET, and DELETE operations. It serves as both a functional
/// implementation and an example of how to implement StateMachineTrait.
pub struct KvStateMachine {
    data: RwLock<HashMap<String, Bytes>>,
    wal: Option<Arc<crate::wal::WriteAheadLog>>,
    // Track operations since last compaction
    ops_since_compaction: RwLock<usize>,
}

impl KvStateMachine {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            wal: None,
            ops_since_compaction: RwLock::new(0),
        }
    }

    /// Create state machine with Walrus backing and recovery
    pub fn with_wal(wal: Arc<crate::wal::WriteAheadLog>) -> Self {
        let sm = Self {
            data: RwLock::new(HashMap::new()),
            wal: Some(wal),
            ops_since_compaction: RwLock::new(0),
        };

        // Recover state from Walrus
        sm.recover_from_walrus();
        sm
    }

    fn recover_from_walrus(&self) {
        if let Some(wal) = &self.wal {
            tracing::info!("Starting state machine recovery from Walrus...");
            let mut recovered = HashMap::new();

            // Step 1: Check for latest snapshot first
            let mut snapshot_found = false;
            loop {
                match wal.walrus.read_next(TOPIC_STATE_MACHINE_SNAPSHOT, true) {
                    Ok(Some(entry)) => {
                        // Deserialize snapshot with rkyv
                        let archived =
                            unsafe { rkyv::archived_root::<StateMachineSnapshot>(&entry.data) };
                        let snapshot: Result<StateMachineSnapshot, _> =
                            archived.deserialize(&mut rkyv::Infallible);
                        let snapshot = snapshot.expect("Failed to deserialize snapshot");
                        // Load snapshot into recovered map
                        recovered.clear();
                        for (key, value) in snapshot.entries {
                            recovered.insert(key, Bytes::from(value));
                        }
                        snapshot_found = true;
                        tracing::info!(
                            "✓ Loaded state machine snapshot with {} entries",
                            recovered.len()
                        );
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Step 2: Replay operations since snapshot (or all if no snapshot)
            let mut op_count = 0;
            loop {
                match wal.walrus.read_next(TOPIC_STATE_MACHINE, true) {
                    Ok(Some(entry)) => {
                        op_count += 1;
                        // Zero-copy deserialize with rkyv
                        let archived =
                            unsafe { rkyv::archived_root::<StateMachineEntry>(&entry.data) };
                        let sm_entry: StateMachineEntry =
                            match archived.deserialize(&mut rkyv::Infallible) {
                                Ok(d) => d,
                                Err(_) => {
                                    tracing::warn!("Failed to deserialize state machine entry");
                                    break;
                                }
                            };

                        if sm_entry.value.is_empty() {
                            // Tombstone - delete the key
                            recovered.remove(&sm_entry.key);
                        } else {
                            // Normal entry - insert/update
                            recovered.insert(sm_entry.key, Bytes::from(sm_entry.value));
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            if snapshot_found {
                tracing::info!(
                    "✓ Recovery complete: snapshot + {} operations = {} entries",
                    op_count,
                    recovered.len()
                );
            } else if !recovered.is_empty() {
                tracing::info!(
                    "✓ Recovered {} entries from {} operations",
                    recovered.len(),
                    op_count
                );
            } else {
                tracing::info!("No state machine entries to recover");
            }

            if !recovered.is_empty() {
                *self.data.write().unwrap() = recovered;
            }

            // Reset compaction counter
            *self.ops_since_compaction.write().unwrap() = 0;
        }
    }

    /// Apply a command to the KV state machine (NOW DURABLE if WAL is set!)
    pub fn apply_kv(&self, command: &[u8]) -> Result<Bytes, String> {
        // Simple command format: "SET key value" or "GET key"
        let cmd_str =
            String::from_utf8(command.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.as_slice() {
            ["SET", key, value] => {
                let key_str = key.to_string();
                let value_bytes = Bytes::from(value.to_string());

                // Persist to Walrus BEFORE updating in-memory state
                if let Some(wal) = &self.wal {
                    let sm_entry = StateMachineEntry {
                        key: key_str.clone(),
                        value: value_bytes.to_vec(),
                    };

                    let bytes = rkyv::to_bytes::<_, 256>(&sm_entry)
                        .map_err(|e| format!("Serialization failed: {:?}", e))?;

                    tokio::task::block_in_place(|| {
                        wal.walrus.append_for_topic(TOPIC_STATE_MACHINE, &bytes)
                    })
                    .map_err(|e| format!("WAL append failed: {}", e))?;

                    tracing::trace!("✓ Persisted SET {} = {}", key_str, value);
                }

                // Now safe to update in-memory
                let mut data = self.data.write().unwrap();
                data.insert(key_str, value_bytes);

                // Increment ops counter for compaction tracking
                *self.ops_since_compaction.write().unwrap() += 1;

                Ok(Bytes::from("OK"))
            }
            ["GET", key] => {
                let data = self.data.read().unwrap();
                match data.get(*key) {
                    Some(value) => Ok(value.clone()),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            ["DELETE", key] => {
                // Persist tombstone to Walrus
                if let Some(wal) = &self.wal {
                    let sm_entry = StateMachineEntry {
                        key: key.to_string(),
                        value: vec![], // Empty = tombstone
                    };

                    let bytes = rkyv::to_bytes::<_, 256>(&sm_entry)
                        .map_err(|e| format!("Serialization failed: {:?}", e))?;

                    tokio::task::block_in_place(|| {
                        wal.walrus.append_for_topic(TOPIC_STATE_MACHINE, &bytes)
                    })
                    .map_err(|e| format!("WAL append failed: {}", e))?;

                    tracing::trace!("✓ Persisted DELETE {}", key);
                }

                let mut data = self.data.write().unwrap();
                data.remove(*key);

                // Increment ops counter for compaction tracking
                *self.ops_since_compaction.write().unwrap() += 1;

                Ok(Bytes::from("OK"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    /// Compact state machine by writing snapshot and checkpointing old operations
    /// This prevents unbounded WAL growth and enables Walrus space reclamation
    pub fn compact_state_machine(&self) -> Result<(), String> {
        let ops_count = *self.ops_since_compaction.read().unwrap();

        if ops_count < STATE_MACHINE_COMPACTION_THRESHOLD {
            // Not enough operations accumulated, skip compaction
            return Ok(());
        }

        if let Some(wal) = &self.wal {
            tracing::info!("State machine compaction triggered: {} operations since last snapshot (threshold: {})",
                ops_count, STATE_MACHINE_COMPACTION_THRESHOLD);

            // Get current state
            let data = self.data.read().unwrap();
            let snapshot = StateMachineSnapshot {
                entries: data.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect(),
            };

            // Serialize and persist snapshot
            let bytes = rkyv::to_bytes::<_, 4096>(&snapshot)
                .map_err(|e| format!("Snapshot serialization failed: {:?}", e))?;

            tokio::task::block_in_place(|| {
                wal.walrus
                    .append_for_topic(TOPIC_STATE_MACHINE_SNAPSHOT, &bytes)
            })
            .map_err(|e| format!("Failed to persist state machine snapshot: {}", e))?;

            tracing::info!(
                "✓ State machine compaction complete: snapshot with {} entries persisted",
                snapshot.entries.len()
            );

            // Reset counter
            *self.ops_since_compaction.write().unwrap() = 0;

            // Note: Old operations are automatically reclaimable by Walrus
            // because we use read_next(TOPIC_STATE_MACHINE, true) during recovery,
            // which checkpoints the cursor. Next recovery loads snapshot first,
            // then only replays operations since snapshot.

            Ok(())
        } else {
            // No WAL, skip compaction
            Ok(())
        }
    }

    /// Get a snapshot of the current state (internal method)
    pub fn snapshot_hashmap(&self) -> HashMap<String, Bytes> {
        let data = self.data.read().unwrap();
        data.clone()
    }

    /// Restore from a snapshot (internal method)
    pub fn restore_hashmap(&self, snapshot: HashMap<String, Bytes>) {
        let mut data = self.data.write().unwrap();
        *data = snapshot;
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement the StateMachineTrait for KvStateMachine
impl StateMachineTrait for KvStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        self.apply_kv(command)
    }

    fn snapshot(&self) -> Vec<u8> {
        let data = self.data.read().unwrap();
        let snapshot = StateMachineSnapshot {
            entries: data.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect(),
        };

        // Serialize snapshot with rkyv
        rkyv::to_bytes::<_, 4096>(&snapshot)
            .map(|bytes| bytes.to_vec())
            .unwrap_or_else(|e| {
                tracing::error!("Failed to serialize snapshot: {:?}", e);
                vec![]
            })
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        if snapshot.is_empty() {
            return Ok(());
        }

        // Deserialize snapshot with rkyv
        let archived = unsafe { rkyv::archived_root::<StateMachineSnapshot>(snapshot) };
        let snapshot: StateMachineSnapshot = archived
            .deserialize(&mut rkyv::Infallible)
            .map_err(|e| format!("Failed to deserialize snapshot: {:?}", e))?;

        // Restore data
        let mut data = self.data.write().unwrap();
        data.clear();
        for (key, value) in snapshot.entries {
            data.insert(key, Bytes::from(value));
        }

        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        self.compact_state_machine()
    }
}

/// Type alias for backward compatibility and ease of use
/// Represents a trait object that can be any state machine implementation
pub type StateMachine = Arc<dyn StateMachineTrait>;
