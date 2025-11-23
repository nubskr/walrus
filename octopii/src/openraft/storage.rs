#![cfg(feature = "openraft")]

use crate::error::OctopiiError;
use crate::openraft::types::{AppEntry, AppNodeId, AppResponse, AppTypeConfig};
use crate::state_machine::StateMachine;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use openraft::{
    alias::SnapshotDataOf,
    storage::{
        EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
        Snapshot, SnapshotMeta,
    },
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftTypeConfig, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// --- Log Store ---
#[derive(Clone, Debug, Default)]
pub struct MemLogStore {
    inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
}

#[derive(Debug)]
struct MemLogStoreInner {
    last_purged_log_id: Option<LogId<AppTypeConfig>>,
    log: BTreeMap<u64, Entry<AppTypeConfig>>,
    committed: Option<LogId<AppTypeConfig>>,
    vote: Option<openraft::Vote<AppTypeConfig>>,
}

impl Default for MemLogStoreInner {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl MemLogStoreInner {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id);

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>>,
    {
        for entry in entries {
            self.log.insert(entry.log_id.index, entry);
        }
        callback.io_completed(Ok(())).await;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

impl MemLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogReader<AppTypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}

impl RaftLogStorage<AppTypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// --- State Machine Store ---
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<AppTypeConfig>,
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<AppTypeConfig>>,
    pub last_membership: StoredMembership<AppTypeConfig>,
    pub data: BTreeMap<String, String>,
}

pub struct MemStateMachine {
    sm: StateMachine,
    state_machine: tokio::sync::RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: tokio::sync::RwLock<Option<StoredSnapshot>>,
}

impl MemStateMachine {
    pub fn new(sm: StateMachine) -> Arc<Self> {
        Arc::new(Self {
            sm,
            state_machine: tokio::sync::RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: tokio::sync::RwLock::new(None),
        })
    }
}

impl RaftSnapshotBuilder<AppTypeConfig> for Arc<MemStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<AppTypeConfig>, io::Error> {
        let state_machine = self.state_machine.read().await;
        let data = bincode::serialize(&state_machine.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id.node_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<AppTypeConfig> for Arc<MemStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<AppTypeConfig>>,
            StoredMembership<AppTypeConfig>,
        ),
        io::Error,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm:
            Stream<Item = Result<EntryResponder<AppTypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut sm = self.state_machine.write().await;

        while let Some((entry, responder)) = entries.try_next().await? {
            sm.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => AppResponse(Vec::new()),
                EntryPayload::Normal(ref data) => {
                    let result = self
                        .sm
                        .apply(&data.0)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    AppResponse(result.to_vec())
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    AppResponse(Vec::new())
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<AppTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<AppTypeConfig>,
        snapshot: SnapshotDataOf<AppTypeConfig>,
    ) -> Result<(), io::Error> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let updated_state_machine_data: BTreeMap<String, String> =
            bincode::deserialize(&new_snapshot.data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data.clone(),
        };

        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated_state_machine;

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Also restore into the state machine
        let snapshot_bytes = bincode::serialize(&updated_state_machine_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.sm
            .restore(&snapshot_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<AppTypeConfig>>, io::Error> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Cursor::new(data),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

/// Helper to create a new memory log store
pub fn new_mem_log_store() -> MemLogStore {
    MemLogStore::new()
}

#[derive(Clone)]
pub struct WalLogStore {
    inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
    wal: Arc<WriteAheadLog>,
}

#[derive(Serialize, Deserialize)]
enum WalLogRecord {
    LogEntry(Entry<AppTypeConfig>),
    Vote(openraft::Vote<AppTypeConfig>),
    Committed(Option<LogId<AppTypeConfig>>),
    Purged(LogId<AppTypeConfig>),
    Truncated(LogId<AppTypeConfig>),
}

impl WalLogStore {
    pub async fn new(wal: Arc<WriteAheadLog>) -> Result<Self, OctopiiError> {
        let store = Self {
            inner: Arc::new(tokio::sync::Mutex::new(MemLogStoreInner::default())),
            wal,
        };
        store.recover_from_wal().await?;
        Ok(store)
    }

    async fn recover_from_wal(&self) -> Result<(), OctopiiError> {
        let entries = self.wal.read_all().await.unwrap_or_else(|_| Vec::new());
        let mut inner = self.inner.lock().await;
        for raw in entries {
            let record: WalLogRecord = bincode::deserialize(&raw)
                .map_err(|e| OctopiiError::Wal(format!("Failed to deserialize WAL record: {e}")))?;
            match record {
                WalLogRecord::LogEntry(entry) => {
                    inner.log.insert(entry.log_id.index, entry);
                }
                WalLogRecord::Vote(vote) => {
                    inner.vote = Some(vote);
                }
                WalLogRecord::Committed(committed) => {
                    inner.committed = committed;
                }
                WalLogRecord::Purged(log_id) => {
                    let keys = inner
                        .log
                        .range(..=log_id.index)
                        .map(|(idx, _)| *idx)
                        .collect::<Vec<_>>();
                    for key in keys {
                        inner.log.remove(&key);
                    }
                    inner.last_purged_log_id = Some(log_id);
                }
                WalLogRecord::Truncated(log_id) => {
                    let keys = inner
                        .log
                        .range(log_id.index..)
                        .map(|(idx, _)| *idx)
                        .collect::<Vec<_>>();
                    for key in keys {
                        inner.log.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    async fn persist_record(&self, record: &WalLogRecord) -> Result<(), io::Error> {
        let data =
            bincode::serialize(record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.wal
            .append(Bytes::from(data))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}

impl RaftLogReader<AppTypeConfig> for WalLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}

impl RaftLogStorage<AppTypeConfig> for WalLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.committed = committed;
        }
        self.persist_record(&WalLogRecord::Committed(committed))
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.vote = Some(vote.clone());
        }
        self.persist_record(&WalLogRecord::Vote(vote.clone())).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut to_persist = Vec::new();
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                to_persist.push(entry.clone());
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        for entry in to_persist {
            self.persist_record(&WalLogRecord::LogEntry(entry)).await?;
        }
        callback.io_completed(Ok(())).await;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await?;
        }
        self.persist_record(&WalLogRecord::Truncated(log_id)).await
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await?;
        }
        self.persist_record(&WalLogRecord::Purged(log_id)).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub async fn new_wal_log_store(wal: Arc<WriteAheadLog>) -> Result<WalLogStore, OctopiiError> {
    WalLogStore::new(wal).await
}

/// Helper to create a new memory state machine
pub fn new_mem_state_machine(sm: StateMachine) -> Arc<MemStateMachine> {
    MemStateMachine::new(sm)
}
