use crate::error::Result;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use raft::{prelude::*, Storage};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, RwLock as StdRwLock};

// Walrus topic names for different Raft state components
const TOPIC_LOG: &str = "raft_log";
const TOPIC_LOG_RECOVERY: &str = "raft_log_recovery"; // Recovery-only topic (fresh cursor on restart)
const TOPIC_HARD_STATE: &str = "raft_hard_state";
const TOPIC_HARD_STATE_RECOVERY: &str = "raft_hard_state_recovery"; // Recovery-only topic
const TOPIC_CONF_STATE: &str = "raft_conf_state";
const TOPIC_CONF_STATE_RECOVERY: &str = "raft_conf_state_recovery"; // Recovery-only topic
const TOPIC_SNAPSHOT: &str = "raft_snapshot";

// Log compaction threshold: create snapshot after this many entries
const LOG_COMPACTION_THRESHOLD: u64 = 10000; // High threshold to avoid blocking fsync during normal operation

// Serializable wrapper types for rkyv

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct HardStateData {
    term: u64,
    vote: u64,
    commit: u64,
}

impl From<&HardState> for HardStateData {
    fn from(hs: &HardState) -> Self {
        Self {
            term: hs.term,
            vote: hs.vote,
            commit: hs.commit,
        }
    }
}

impl From<&HardStateData> for HardState {
    fn from(data: &HardStateData) -> Self {
        let mut hs = HardState::default();
        hs.term = data.term;
        hs.vote = data.vote;
        hs.commit = data.commit;
        hs
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct ConfStateData {
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}

impl From<&ConfState> for ConfStateData {
    fn from(cs: &ConfState) -> Self {
        Self {
            voters: cs.voters.clone(),
            learners: cs.learners.clone(),
            voters_outgoing: cs.voters_outgoing.clone(),
            learners_next: cs.learners_next.clone(),
            auto_leave: cs.auto_leave,
        }
    }
}

impl From<&ConfStateData> for ConfState {
    fn from(data: &ConfStateData) -> Self {
        let mut cs = ConfState::default();
        cs.voters = data.voters.clone();
        cs.learners = data.learners.clone();
        cs.voters_outgoing = data.voters_outgoing.clone();
        cs.learners_next = data.learners_next.clone();
        cs.auto_leave = data.auto_leave;
        cs
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct SnapshotMetadataData {
    conf_state: ConfStateData,
    index: u64,
    term: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct SnapshotData {
    metadata: SnapshotMetadataData,
    data: Vec<u8>,
}

/// Raft storage implementation backed by WAL
pub struct WalStorage {
    wal: Arc<WriteAheadLog>,
    // In-memory cache for recent entries (wrapped in Arc for cheap cloning)
    entries: Arc<StdRwLock<Vec<Entry>>>,
    hard_state: Arc<StdRwLock<HardState>>,
    conf_state: Arc<StdRwLock<ConfState>>,
    snapshot: Arc<StdRwLock<Snapshot>>,
}

impl Clone for WalStorage {
    fn clone(&self) -> Self {
        Self {
            wal: Arc::clone(&self.wal),
            entries: Arc::clone(&self.entries),
            hard_state: Arc::clone(&self.hard_state),
            conf_state: Arc::clone(&self.conf_state),
            snapshot: Arc::clone(&self.snapshot),
        }
    }
}

impl WalStorage {
    /// Create a new WAL-backed storage with crash recovery
    pub fn new(wal: Arc<WriteAheadLog>) -> Self {
        let storage = Self {
            wal,
            entries: Arc::new(StdRwLock::new(Vec::new())),
            hard_state: Arc::new(StdRwLock::new(HardState::default())),
            conf_state: Arc::new(StdRwLock::new(ConfState::default())),
            snapshot: Arc::new(StdRwLock::new(Snapshot::default())),
        };

        // Recover state from Walrus topics
        storage.recover_from_walrus();
        storage
    }

    /// Recover all Raft state from Walrus topics
    fn recover_from_walrus(&self) {
        tracing::info!("Starting Raft state recovery from Walrus...");

        // 1. Recover hard state (latest entry wins)
        if let Ok(hs) = self.recover_hard_state() {
            if hs.term > 0 || hs.vote > 0 || hs.commit > 0 {
                *self.hard_state.write().unwrap() = hs.clone();
                tracing::info!(
                    "✓ Recovered hard state: term={}, vote={}, commit={}",
                    hs.term,
                    hs.vote,
                    hs.commit
                );
            }
        }

        // 2. Recover conf state (latest entry wins)
        if let Ok(cs) = self.recover_conf_state() {
            if !cs.voters.is_empty() {
                *self.conf_state.write().unwrap() = cs.clone();
                tracing::info!(
                    "✓ Recovered conf state: voters={:?}, learners={:?}",
                    cs.voters,
                    cs.learners
                );
            }
        }

        // 3. Recover snapshot (latest entry wins)
        if let Ok(snap) = self.recover_snapshot() {
            if snap.get_metadata().index > 0 {
                *self.snapshot.write().unwrap() = snap.clone();
                tracing::info!(
                    "✓ Recovered snapshot at index {} (term {})",
                    snap.get_metadata().index,
                    snap.get_metadata().term
                );
            }
        }

        // 4. Recover log entries (rebuild entire log)
        if let Ok(entries) = self.recover_log_entries() {
            if !entries.is_empty() {
                *self.entries.write().unwrap() = entries.clone();
                tracing::info!("✓ Recovered {} log entries", entries.len());
            }
        }

        tracing::info!("Raft state recovery complete");
    }

    fn recover_hard_state(&self) -> crate::error::Result<HardState> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<HardState> = None;
        let mut entry_count = 0;

        // Read ALL hard_state entries in a single batch read with checkpoint=false
        // This reads from the beginning without persisting cursor advancement
        // Since hard_state entries are small, they should all fit in one batch
        const MAX_BATCH_BYTES: usize = 10_000_000; // 10MB - plenty for all hard states

        match walrus.batch_read_for_topic(TOPIC_HARD_STATE_RECOVERY, MAX_BATCH_BYTES, false) {
            Ok(batch) => {
                for entry in batch {
                    entry_count += 1;
                    let archived = unsafe { rkyv::archived_root::<HardStateData>(&entry.data) };
                    let data: HardStateData = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => {
                            tracing::warn!("Failed to deserialize hard state entry");
                            continue;
                        }
                    };
                    latest = Some((&data).into());
                }
            }
            Err(e) => {
                tracing::warn!("Error reading hard_state batch: {}", e);
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered hard_state from {} entries (latest used)", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_conf_state(&self) -> crate::error::Result<ConfState> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<ConfState> = None;
        let mut entry_count = 0;

        // Read ALL conf_state entries in a single batch read with checkpoint=false
        const MAX_BATCH_BYTES: usize = 10_000_000; // 10MB - plenty for all conf states

        match walrus.batch_read_for_topic(TOPIC_CONF_STATE_RECOVERY, MAX_BATCH_BYTES, false) {
            Ok(batch) => {
                for entry in batch {
                    entry_count += 1;
                    let archived = unsafe { rkyv::archived_root::<ConfStateData>(&entry.data) };
                    let data: ConfStateData = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => {
                            tracing::warn!("Failed to deserialize conf state entry");
                            continue;
                        }
                    };
                    latest = Some((&data).into());
                }
            }
            Err(e) => {
                tracing::warn!("Error reading conf_state batch: {}", e);
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered conf_state from {} entries (latest used)", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_snapshot(&self) -> crate::error::Result<Snapshot> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<Snapshot> = None;
        let mut entry_count = 0;

        // Use checkpoint=true to advance cursor and enable Walrus space reclamation
        // For snapshot, we want the LATEST entry, so cursor persistence is fine
        loop {
            match walrus.read_next(TOPIC_SNAPSHOT, true) {
                Ok(Some(entry)) => {
                    entry_count += 1;
                    let archived = unsafe { rkyv::archived_root::<SnapshotData>(&entry.data) };
                    let snap_data: SnapshotData = match archived.deserialize(&mut rkyv::Infallible)
                    {
                        Ok(d) => d,
                        Err(_) => {
                            tracing::warn!("Failed to deserialize snapshot entry");
                            break;
                        }
                    };

                    let mut snapshot = Snapshot::default();
                    snapshot.data = Bytes::from(snap_data.data);

                    let metadata = snapshot.mut_metadata();
                    metadata.index = snap_data.metadata.index;
                    metadata.term = snap_data.metadata.term;
                    *metadata.mut_conf_state() = (&snap_data.metadata.conf_state).into();

                    latest = Some(snapshot);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered snapshot from {} entries", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_log_entries(&self) -> crate::error::Result<Vec<Entry>> {
        let walrus = &self.wal.walrus;
        let mut entries = Vec::new();

        // Get snapshot index to determine which entries to keep
        let snapshot = self.snapshot.read().unwrap();
        let snapshot_index = snapshot.get_metadata().index;

        tracing::info!(
            "Recovering log entries (snapshot at index {}) using batch reads",
            snapshot_index
        );

        // Read log entries in batches WITH checkpointing
        // CRITICAL: We checkpoint ALL entries (even old ones) to enable Walrus space reclamation,
        // but only KEEP entries after the snapshot index in memory
        let mut total_entries = 0;
        let mut compacted_entries = 0;

        // Use batch reads for 10-50x faster recovery
        const MAX_BATCH_BYTES: usize = 10_000_000; // 10MB per batch

        // Read from TOPIC_LOG_RECOVERY instead of TOPIC_LOG for crash recovery
        // The recovery topic has a fresh cursor on each restart (new Walrus instance),
        // so we always read all entries from the beginning, regardless of checkpoint persistence
        // This ensures complete log recovery while still allowing cursor persistence for normal operations
        loop {
            match walrus.batch_read_for_topic(TOPIC_LOG_RECOVERY, MAX_BATCH_BYTES, true) {
                Ok(batch) if batch.is_empty() => break, // No more entries
                Ok(batch) => {
                    for entry_data in batch {
                        total_entries += 1;

                        // Deserialize protobuf Entry (from raft-rs)
                        let parse_result: protobuf::ProtobufResult<Entry> =
                            protobuf::Message::parse_from_bytes(&entry_data.data);
                        match parse_result {
                            Ok(raft_entry) => {
                                if raft_entry.index > snapshot_index {
                                    // KEEP entries after snapshot - needed for recovery
                                    entries.push(raft_entry);
                                } else {
                                    // DISCARD entries before snapshot - already included in snapshot
                                    // We still checkpoint them (checkpoint=true above) to enable Walrus space reclamation
                                    compacted_entries += 1;
                                    tracing::trace!(
                                        "Skipping compacted entry at index {}",
                                        raft_entry.index
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to deserialize log entry: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Batch read error during recovery: {}", e);
                    break;
                }
            }
        }

        if total_entries > 0 {
            tracing::info!(
                "✓ Recovered {} log entries: {} kept (after snapshot), {} compacted (reclaimable by Walrus)",
                total_entries,
                entries.len(),
                compacted_entries
            );
        }

        Ok(entries)
    }

    /// Apply a snapshot to storage (NOW DURABLE!)
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> crate::error::Result<()> {
        // Serialize snapshot with rkyv
        let snap_data = SnapshotData {
            metadata: SnapshotMetadataData {
                conf_state: ConfStateData::from(snapshot.get_metadata().get_conf_state()),
                index: snapshot.get_metadata().index,
                term: snapshot.get_metadata().term,
            },
            data: snapshot.data.to_vec(),
        };

        let bytes = rkyv::to_bytes::<_, 4096>(&snap_data).map_err(|e| {
            crate::error::OctopiiError::Wal(format!("Failed to serialize snapshot: {:?}", e))
        })?;

        // Persist to Walrus
        tokio::task::block_in_place(|| self.wal.walrus.append_for_topic(TOPIC_SNAPSHOT, &bytes))?;

        tracing::debug!(
            "Persisted snapshot at index {} (term {})",
            snapshot.get_metadata().index,
            snapshot.get_metadata().term
        );

        // Update conf state with proper persistence
        self.set_conf_state(snapshot.get_metadata().get_conf_state().clone());

        // Update hard state with proper persistence
        // Preserve the vote field, update term and commit from snapshot
        let current_vote = {
            let hs = self.hard_state.read().unwrap();
            hs.vote
        };

        let mut new_hs = HardState::default();
        new_hs.term = snapshot.get_metadata().term;
        new_hs.vote = current_vote;
        new_hs.commit = snapshot.get_metadata().index;
        self.set_hard_state(new_hs);

        // Update in-memory snapshot state (do this last to avoid holding locks)
        let mut snap = self.snapshot.write().unwrap();
        *snap = snapshot.clone();

        Ok(())
    }

    /// Append entries to storage (async version for WAL persistence)
    /// Uses Walrus batch_append_for_topic for improved performance (up to 2000 entries atomically)
    pub async fn append_entries(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Serialize all entries to protobuf
        let serialized: Vec<Vec<u8>> = entries
            .iter()
            .map(|entry| {
                protobuf::Message::write_to_bytes(entry)
                    .map_err(|e| crate::error::OctopiiError::Wal(format!("Protobuf error: {}", e)))
            })
            .collect::<Result<Vec<_>>>()?;

        // DUAL-WRITE: Write to both main and recovery topics
        // The recovery topic is read with a fresh cursor on each restart, ensuring complete log recovery
        // This avoids Walrus's cursor persistence interfering with Raft recovery requirements
        //
        // Using individual append_for_topic calls instead of batch_append_for_topic
        // to avoid io_uring initialization in resource-constrained environments
        let walrus = &self.wal.walrus;
        tokio::task::block_in_place(|| {
            for data in &serialized {
                walrus.append_for_topic(TOPIC_LOG, data)?;
                walrus.append_for_topic(TOPIC_LOG_RECOVERY, data)?;
            }
            Ok::<(), crate::error::OctopiiError>(())
        })?;

        tracing::info!(
            "✓ Appended {} entries to WAL (both main and recovery topics)",
            entries.len()
        );

        // Update in-memory cache
        let mut cache = self.entries.write().unwrap();
        cache.extend_from_slice(entries);

        Ok(())
    }

    /// Append entries synchronously (updates in-memory cache only)
    /// WAL persistence happens separately
    pub fn append_entries_sync(&self, entries: &[Entry]) {
        let mut cache = self.entries.write().unwrap();
        for entry in entries {
            cache.push(entry.clone());
        }
        tracing::trace!("Appended {} entries to in-memory cache", entries.len());
    }

    /// Set hard state (NOW DURABLE!)
    pub fn set_hard_state(&self, hs: HardState) {
        // Serialize with rkyv
        let data = HardStateData::from(&hs);
        let bytes = rkyv::to_bytes::<_, 256>(&data).expect("Failed to serialize hard state");

        // Persist to BOTH regular and recovery topics
        // Regular topic can be used for optimization, recovery topic ensures correct recovery
        tokio::task::block_in_place(|| {
            self.wal.walrus.append_for_topic(TOPIC_HARD_STATE, &bytes)?;
            self.wal.walrus.append_for_topic(TOPIC_HARD_STATE_RECOVERY, &bytes)?;
            Ok::<(), crate::error::OctopiiError>(())
        })
        .expect("Failed to persist hard state");

        tracing::debug!(
            "✓ Persisted hard state: term={}, vote={}, commit={}",
            data.term,
            data.vote,
            data.commit
        );

        // Update in-memory cache
        let mut state = self.hard_state.write().unwrap();
        *state = hs;
    }

    /// Set conf state (NOW DURABLE!)
    pub fn set_conf_state(&self, cs: ConfState) {
        // Serialize with rkyv
        let data = ConfStateData::from(&cs);
        let bytes = rkyv::to_bytes::<_, 256>(&data).expect("Failed to serialize conf state");

        // Persist to BOTH regular and recovery topics
        // Regular topic can be used for optimization, recovery topic ensures correct recovery
        tokio::task::block_in_place(|| {
            self.wal.walrus.append_for_topic(TOPIC_CONF_STATE, &bytes)?;
            self.wal.walrus.append_for_topic(TOPIC_CONF_STATE_RECOVERY, &bytes)?;
            Ok::<(), crate::error::OctopiiError>(())
        })
        .expect("Failed to persist conf state");

        tracing::debug!(
            "✓ Persisted conf state: voters={:?}, learners={:?}",
            data.voters,
            data.learners
        );

        // Update in-memory cache
        let mut state = self.conf_state.write().unwrap();
        *state = cs;
    }

    /// Compact logs by creating a snapshot and trimming old entries
    /// This prevents unbounded log growth and enables Walrus space reclamation
    pub fn compact_logs(
        &self,
        applied_index: u64,
        state_machine_data: Vec<u8>,
    ) -> crate::error::Result<()> {
        let entries = self.entries.read().unwrap();
        let snapshot_metadata = self.snapshot.read().unwrap().get_metadata().clone();

        // Check if we've accumulated enough entries since last snapshot
        let entries_since_snapshot = applied_index.saturating_sub(snapshot_metadata.index);

        if entries_since_snapshot < LOG_COMPACTION_THRESHOLD {
            // Not enough entries accumulated, skip compaction
            return Ok(());
        }

        drop(entries); // Release read lock before acquiring write locks

        tracing::info!(
            "Log compaction triggered: {} entries since last snapshot (threshold: {})",
            entries_since_snapshot,
            LOG_COMPACTION_THRESHOLD
        );

        // Create new snapshot at applied_index
        let mut snapshot = Snapshot::default();
        snapshot.data = Bytes::from(state_machine_data);

        let metadata = snapshot.mut_metadata();
        metadata.index = applied_index;

        // Get current term from hard state
        let hard_state = self.hard_state.read().unwrap();
        metadata.term = hard_state.term;

        // Get current conf state
        let conf_state = self.conf_state.read().unwrap();
        *metadata.mut_conf_state() = conf_state.clone();

        // Persist snapshot to Walrus
        self.apply_snapshot(snapshot.clone())?;

        // Trim old entries from in-memory cache (keep entries after snapshot)
        let mut entries_mut = self.entries.write().unwrap();
        entries_mut.retain(|e| e.index > applied_index);

        tracing::info!(
            "✓ Log compaction complete: snapshot at index {}, {} entries retained",
            applied_index,
            entries_mut.len()
        );

        // Note: Old log entries before snapshot are automatically reclaimable by Walrus:
        // - TOPIC_LOG: Normal operations checkpoint cursor, allowing space reclamation
        // - TOPIC_LOG_RECOVERY: Recovery reads checkpoint cursor, allowing space reclamation
        // Both topics will eventually reclaim space for entries before the snapshot index

        Ok(())
    }

    // NOTE: Removed create_snapshot_from_walrus() and checkpoint_old_entries() methods.
    //
    // For Raft snapshots, use the state machine's snapshot() method instead,
    // which returns the current in-memory state without re-reading from Walrus.
    // Space reclamation happens automatically via Walrus's checkpoint mechanism.
}

impl Storage for WalStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.hard_state.read().unwrap().clone();
        let conf_state = self.conf_state.read().unwrap().clone();

        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let entries = self.entries.read().unwrap();

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let first_index = entries[0].index;
        let last_index = entries[entries.len() - 1].index;

        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let start = (low - first_index) as usize;
        let end = (high - first_index) as usize;

        let mut result = entries[start..end].to_vec();

        if let Some(max) = max_size.into() {
            let mut size = 0u64;
            let mut count = 0;
            for entry in &result {
                size += entry.data.len() as u64;
                if size > max {
                    break;
                }
                count += 1;
            }
            result.truncate(count);
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();

        if entries.is_empty() {
            let snapshot = self.snapshot.read().unwrap();
            if idx == snapshot.get_metadata().index {
                return Ok(snapshot.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let first_index = entries[0].index;
        let last_index = entries[entries.len() - 1].index;

        if idx < first_index {
            let snapshot = self.snapshot.read().unwrap();
            if idx == snapshot.get_metadata().index {
                return Ok(snapshot.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx > last_index {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let offset = (idx - first_index) as usize;
        Ok(entries[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.first() {
            Ok(entry.index)
        } else {
            let snapshot = self.snapshot.read().unwrap();
            Ok(snapshot.get_metadata().index + 1)
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.last() {
            Ok(entry.index)
        } else {
            let snapshot = self.snapshot.read().unwrap();
            Ok(snapshot.get_metadata().index)
        }
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let snapshot = self.snapshot.read().unwrap();
        Ok(snapshot.clone())
    }
}
