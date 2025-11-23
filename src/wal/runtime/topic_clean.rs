use crate::wal::config::debug_print;
use crate::wal::paths::WalPathManager;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct CleanMarkerRecord {
    pub generation: u64,
    pub is_clean: bool,
}

impl Default for CleanMarkerRecord {
    fn default() -> Self {
        Self {
            generation: 0,
            is_clean: true,
        }
    }
}

pub struct CleanMarkerStore {
    path: String,
    store: RwLock<HashMap<String, CleanMarkerRecord>>,
}

impl CleanMarkerStore {
    pub fn new_in(paths: &WalPathManager, file_name: &str) -> std::io::Result<Self> {
        paths.ensure_root()?;
        let path = paths.index_path(file_name);
        let map = if path.exists() {
            let bytes = fs::read(&path)?;
            if bytes.is_empty() {
                HashMap::new()
            } else {
                // SAFETY: file contents come from our previous rkyv serialization
                let archived =
                    unsafe { rkyv::archived_root::<HashMap<String, CleanMarkerRecord>>(&bytes) };
                archived
                    .deserialize(&mut rkyv::Infallible)
                    .unwrap_or_default()
            }
        } else {
            HashMap::new()
        };
        Ok(Self {
            path: path.to_string_lossy().into_owned(),
            store: RwLock::new(map),
        })
    }

    pub fn snapshot(&self) -> HashMap<String, CleanMarkerRecord> {
        self.store
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub fn persist_updates(&self, updates: &[(String, CleanMarkerRecord)]) -> std::io::Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut guard = self
            .store
            .write()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "store lock poisoned"))?;
        for (topic, record) in updates {
            guard.insert(topic.clone(), record.clone());
        }
        Self::persist_map(&self.path, &guard)
    }

    fn persist_map(path: &str, map: &HashMap<String, CleanMarkerRecord>) -> std::io::Result<()> {
        let tmp_path = format!("{}.tmp", path);
        let bytes = rkyv::to_bytes::<_, 256>(map).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("clean marker serialize failed: {:?}", e),
            )
        })?;
        fs::write(&tmp_path, &bytes)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

pub struct TopicCleanState {
    generation: AtomicU64,
    is_clean: AtomicBool,
}

impl TopicCleanState {
    fn new(record: CleanMarkerRecord) -> Self {
        Self {
            generation: AtomicU64::new(record.generation),
            is_clean: AtomicBool::new(record.is_clean),
        }
    }

    fn update(&self, desired_clean: bool) -> Option<CleanMarkerRecord> {
        if self.is_clean.load(Ordering::Acquire) == desired_clean {
            return None;
        }
        let next_gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.is_clean.store(desired_clean, Ordering::Release);
        Some(CleanMarkerRecord {
            generation: next_gen,
            is_clean: desired_clean,
        })
    }

    fn snapshot(&self) -> CleanMarkerRecord {
        CleanMarkerRecord {
            generation: self.generation.load(Ordering::Acquire),
            is_clean: self.is_clean.load(Ordering::Acquire),
        }
    }
}

impl Default for TopicCleanState {
    fn default() -> Self {
        Self::new(CleanMarkerRecord::default())
    }
}

pub struct TopicCleanTracker {
    states: RwLock<HashMap<String, Arc<TopicCleanState>>>,
    store: Arc<CleanMarkerStore>,
    persist_tx: mpsc::Sender<String>,
}

impl TopicCleanTracker {
    pub fn new(store: Arc<CleanMarkerStore>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<String>();
        let tracker = Arc::new(Self {
            states: RwLock::new(HashMap::new()),
            store,
            persist_tx: tx,
        });
        Self::spawn_persister(&tracker, rx);
        tracker
    }

    pub fn hydrate(&self, snapshot: HashMap<String, CleanMarkerRecord>) {
        if snapshot.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.states.write() {
            for (topic, record) in snapshot {
                guard.insert(topic, Arc::new(TopicCleanState::new(record)));
            }
        }
    }

    pub fn mark_dirty(&self, topic: &str) {
        self.update_state(topic, false);
    }

    pub fn mark_clean(&self, topic: &str) {
        self.update_state(topic, true);
    }

    pub fn topic_is_clean(&self, topic: &str) -> bool {
        self.states
            .read()
            .ok()
            .and_then(|guard| guard.get(topic).cloned())
            .map(|state| state.is_clean.load(Ordering::Acquire))
            .unwrap_or(true)
    }

    fn update_state(&self, topic: &str, desired_clean: bool) {
        let state = self.get_or_insert_state(topic);
        if state.update(desired_clean).is_some() {
            let _ = self.persist_tx.send(topic.to_string());
        }
    }

    fn get_or_insert_state(&self, topic: &str) -> Arc<TopicCleanState> {
        if let Ok(guard) = self.states.read() {
            if let Some(existing) = guard.get(topic) {
                return existing.clone();
            }
        }
        let mut guard = self
            .states
            .write()
            .expect("topic tracker map write lock poisoned");
        guard
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(TopicCleanState::default()))
            .clone()
    }

    fn spawn_persister(tracker: &Arc<Self>, rx: mpsc::Receiver<String>) {
        let weak = Arc::downgrade(tracker);
        thread::spawn(move || {
            let mut pending = HashSet::new();
            loop {
                match rx.recv_timeout(Duration::from_millis(5)) {
                    Ok(topic) => {
                        pending.insert(topic);
                        while let Ok(t) = rx.try_recv() {
                            pending.insert(t);
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                if pending.is_empty() {
                    continue;
                }
                if let Some(strong) = weak.upgrade() {
                    if let Err(err) = strong.persist_topics(&pending) {
                        debug_print!("[clean] persist error: {}", err);
                    }
                } else {
                    break;
                }
                pending.clear();
            }
        });
    }

    fn persist_topics(&self, topics: &HashSet<String>) -> std::io::Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let mut updates = Vec::with_capacity(topics.len());
        if let Ok(guard) = self.states.read() {
            for topic in topics {
                if let Some(state) = guard.get(topic) {
                    updates.push((topic.clone(), state.snapshot()));
                }
            }
        }
        self.store.persist_updates(&updates)
    }

    #[cfg(test)]
    pub fn force_flush_for_test(&self) -> std::io::Result<()> {
        let snapshot = {
            let guard = self.states.read().unwrap();
            guard
                .iter()
                .map(|(topic, state)| (topic.clone(), state.snapshot()))
                .collect::<Vec<_>>()
        };
        self.store.persist_updates(&snapshot)
    }
}
