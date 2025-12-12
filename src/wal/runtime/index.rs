use crate::wal::paths::WalPathManager;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct BlockPos {
    pub cur_block_idx: u64,
    pub cur_block_offset: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
struct IndexFile {
    positions: HashMap<String, BlockPos>,
    counts: HashMap<String, u64>,
}

pub struct WalIndex {
    positions: HashMap<String, BlockPos>,
    counts: HashMap<String, u64>,
    path: String,
}

impl WalIndex {
    pub fn new(file_name: &str) -> std::io::Result<Self> {
        let paths = WalPathManager::default();
        Self::new_in(&paths, file_name)
    }

    pub(super) fn new_in(paths: &WalPathManager, file_name: &str) -> std::io::Result<Self> {
        paths.ensure_root()?;
        let path = paths.index_path(file_name);
        let (positions, counts) = path
            .exists()
            .then(|| fs::read(&path).ok())
            .flatten()
            .and_then(|bytes| {
                if bytes.is_empty() {
                    return None;
                }
                // First try the new IndexFile format; fall back to legacy positions-only map.
                // SAFETY: bytes come from our own persisted index file.
                let archived: &rkyv::Archived<IndexFile> =
                    unsafe { rkyv::archived_root::<IndexFile>(&bytes) };
                match rkyv::Deserialize::<IndexFile, _>::deserialize(
                    archived,
                    &mut rkyv::Infallible,
                ) {
                    Ok(file) => Some((file.positions, file.counts)),
                    Err(_) => {
                        // Fallback to legacy map-only format.
                        let legacy: &rkyv::Archived<HashMap<String, BlockPos>> =
                            unsafe { rkyv::archived_root::<HashMap<String, BlockPos>>(&bytes) };
                        rkyv::Deserialize::<HashMap<String, BlockPos>, _>::deserialize(
                            legacy,
                            &mut rkyv::Infallible,
                        )
                        .ok()
                        .map(|m| (m, HashMap::new()))
                    }
                }
            })
            .unwrap_or_default();

        Ok(Self {
            positions,
            counts,
            path: path.to_string_lossy().into_owned(),
        })
    }

    pub fn set(&mut self, key: String, idx: u64, offset: u64) -> std::io::Result<()> {
        self.positions.insert(
            key,
            BlockPos {
                cur_block_idx: idx,
                cur_block_offset: offset,
            },
        );
        self.persist()
    }

    pub fn get(&self, key: &str) -> Option<&BlockPos> {
        self.positions.get(key)
    }

    pub fn remove(&mut self, key: &str) -> std::io::Result<Option<BlockPos>> {
        let result = self.positions.remove(key);
        self.counts.remove(key);
        if result.is_some() {
            self.persist()?;
        }
        Ok(result)
    }

    pub fn increment_count(&mut self, key: &str, delta: u64) -> std::io::Result<()> {
        let entry = self.counts.entry(key.to_string()).or_insert(0);
        *entry = entry.saturating_add(delta);
        self.persist()
    }

    pub fn decrement_count(&mut self, key: &str, delta: u64) -> std::io::Result<()> {
        let entry = self.counts.entry(key.to_string()).or_insert(0);
        *entry = entry.saturating_sub(delta);
        self.persist()
    }

    pub fn get_count(&self, key: &str) -> u64 {
        *self.counts.get(key).unwrap_or(&0)
    }

    fn persist(&self) -> std::io::Result<()> {
        let tmp_path = format!("{}.tmp", self.path);
        let file = IndexFile {
            positions: self.positions.clone(),
            counts: self.counts.clone(),
        };
        let bytes = rkyv::to_bytes::<_, 256>(&file).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("index serialize failed: {:?}", e),
            )
        })?;

        fs::write(&tmp_path, &bytes)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}
