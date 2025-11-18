use crate::wal::config::{checksum64, DEFAULT_BLOCK_SIZE, MAX_ALLOC};
use crate::wal::kv_store::block::KvBlock;
use crate::wal::kv_store::config::*;
use crate::wal::kv_store::hint::{self, HintEntry};
use crate::wal::paths::WalPathManager;
use std::io::Write;
use crate::wal::runtime::allocator::BlockAllocator;
use crate::wal::storage::StorageKeeper;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Debug)]
struct EntryLocation {
    file_id: u64,
    offset: u64,
    len: u32,
}

type KeyDir = Arc<RwLock<HashMap<Vec<u8>, EntryLocation>>>;

pub struct KvStore {
    allocator: Arc<BlockAllocator>,
    key_dir: KeyDir,
    active_block: Arc<RwLock<Option<KvBlock>>>,
    active_offset: Arc<RwLock<u64>>,
    kv_root: PathBuf,
}

impl KvStore {
    pub fn new(namespace: &str) -> Result<Self> {
        // 1. Setup Directory
        let kv_root_str = std::env::var("WALRUS_KV_DIR").unwrap_or_else(|_| DEFAULT_KV_DIR.to_string());
        let mut kv_root = PathBuf::from(kv_root_str);
        kv_root.push(namespace);

        // 2. Initialize PathManager and Allocator
        // This automatically creates the directory and the first active file.
        let paths = Arc::new(WalPathManager::new(kv_root.clone()));
        let allocator = Arc::new(BlockAllocator::new(paths)?);

        let store = Self {
            allocator,
            key_dir: Arc::new(RwLock::new(HashMap::new())),
            active_block: Arc::new(RwLock::new(None)),
            active_offset: Arc::new(RwLock::new(0)),
            kv_root,
        };

        // 3. Recover Index from disk
        store.recover()?;
        
        Ok(store)
    }

    fn recover(&self) -> Result<()> {
        if !self.kv_root.exists() {
            return Ok(());
        }

        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.kv_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    let stem_str = stem.to_string_lossy();
                    // Ignore .hint files in this initial scan, we care about data files
                    if path.extension().map_or(false, |ext| ext == "hint") {
                        continue;
                    }
                    if let Ok(id) = stem_str.parse::<u64>() {
                         files.push((id, path));
                    }
                }
            }
        }
        // Sort by ID (timestamp) to replay in order
        files.sort_by_key(|f| f.0);

        let mut index = self.key_dir.write().unwrap();

        for (file_id, path) in files {
            // Check for hint file
            let hint_path = hint::hint_file_path(&self.kv_root, file_id);
            if hint_path.exists() {
                // FAST PATH: Load from hint
                let f = std::fs::File::open(&hint_path)?;
                let mut reader = std::io::BufReader::new(f);
                loop {
                    match HintEntry::read(&mut reader) {
                        Ok(entry) => {
                             // Update index (blindly overwrite, since we process files in order)
                             // We don't know if it's a tombstone from hint alone unless we store flags in hint.
                             // Wait, design doc says: "KeyLen, ValLen, Offset, Key". No flags.
                             // If we don't store flags, we don't know if it's deleted!
                             // Critical Fix: We MUST store flags or value length 0?
                             // "Tombstone" is a flag in the Header.
                             // If we want to support deletions, the Hint MUST indicate it.
                             // Let's assume for now if ValLen is special? No.
                             // Let's assume we just update the index. If it's a tombstone, we might point to it.
                             // But GET will read the header and see the tombstone flag and return None.
                             // So it is SAFE to point to a tombstone in the index.
                             // Optimization: Don't index tombstones? But we need to know key is deleted to remove it from index.
                             // If we don't index it, we might leave an old version in index!
                             // So we MUST update index to point to this new location (which is a tombstone).
                             // When GET reads it, it sees tombstone and returns None.
                             // Correct.
                             
                             // Wait, if we have a previous valid entry in index, and we see a tombstone in hint.
                             // If we just "insert" the tombstone location into index...
                             // Then GET(key) -> looks up location -> reads header -> sees Tombstone -> returns None.
                             // This works perfectly.
                             
                             let total_len = KV_HEADER_SIZE + entry.key_len as usize + entry.val_len as usize + KV_CHECKSUM_SIZE;
                             index.insert(entry.key, EntryLocation {
                                 file_id,
                                 offset: entry.offset,
                                 len: total_len as u32,
                             });
                        }
                        Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            // Corrupt hint file? Fallback to data file?
                            // For now, fail or log. Let's log and fallback if we could, but simpler to fail.
                            return Err(e);
                        }
                    }
                }
                continue;
            }

            // SLOW PATH: Scan data file
            let path_str = path.to_string_lossy().to_string();
            let storage = StorageKeeper::get_storage_arc(&path_str)?;
            let len = storage.len();
            let mut offset = 0;
            
            // We will collect entries to write a hint file if this is NOT the active file.
            // How do we know if it's active? 
            // "files" list is sorted. The last one might be active.
            // But we haven't initialized allocator yet (or we have, but we don't know which one it picked).
            // Actually, allocator.new() creates a NEW file. 
            // So ALL files we found in read_dir (before allocator created new one) are technically "immutable" / "old" 
            // UNLESS we are restarting and picking up the last file?
            // Walrus allocator always creates a NEW file on startup (`paths.create_new_file()?`).
            // So ALL existing files are immutable history. We can generate hints for ALL of them.
            
            let mut new_hints = Vec::new();

            while offset + KV_OVERHEAD <= len {
                let mut header = [0u8; KV_HEADER_SIZE];
                storage.read(offset, &mut header);

                let flags = header[0];
                let k_len = u16::from_le_bytes(header[1..3].try_into().unwrap()) as usize;
                let v_len = u32::from_le_bytes(header[3..7].try_into().unwrap()) as usize;
                
                let total_len = KV_HEADER_SIZE + k_len + v_len + KV_CHECKSUM_SIZE;

                if offset + total_len > len {
                    break;
                }

                // Verify Checksum (Slow but safe)
                let mut entry_buf = vec![0u8; total_len];
                storage.read(offset, &mut entry_buf);
                
                // ... (Checksum verification omitted for brevity in reasoning, but strictly should be here) ...
                // Re-using existing verification logic
                let stored_csum_bytes = &entry_buf[total_len - KV_CHECKSUM_SIZE..];
                let stored_csum = u32::from_le_bytes(stored_csum_bytes.try_into().unwrap());
                let data_to_check = &entry_buf[..total_len - KV_CHECKSUM_SIZE];
                let calculated = checksum64(data_to_check) as u32;

                if calculated == stored_csum {
                    let key = entry_buf[KV_HEADER_SIZE..KV_HEADER_SIZE + k_len].to_vec();
                    
                    // Update Index
                    // Note: We insert even if it's a tombstone, so GET sees the tombstone.
                    // If we just removed it from index, a previous valid entry in an older file might resurface!
                    // (Unless we are compacting, but here we are just replaying history).
                    // Wait, if we remove it from index, then GET returns None (Key not found). 
                    // That IS the correct behavior for "Deleted".
                    // BUT, if there is an OLDER file with the key, and we remove the mapping...
                    // The index doesn't store "history". It stores "latest location".
                    // If we remove it, the key is gone from HashMap. The older version is NOT in the map anymore.
                    // So "remove" is correct.
                    
                    if flags & FLAG_TOMBSTONE != 0 {
                        index.remove(&key);
                    } else {
                        index.insert(key.clone(), EntryLocation {
                            file_id,
                            offset: offset as u64,
                            len: total_len as u32,
                        });
                    }
                    
                    // Collect for Hint
                    new_hints.push(HintEntry {
                        key_len: k_len as u16,
                        val_len: v_len as u32,
                        offset: offset as u64,
                        key,
                    });
                    
                    offset += total_len;
                } else {
                    break;
                }
            }
            
            // Generate Hint File
            // We only generate hint if we successfully scanned the file.
            if !new_hints.is_empty() {
                 let hint_path = hint::hint_file_path(&self.kv_root, file_id);
                 // Create and write
                 if let Ok(f) = std::fs::File::create(&hint_path) {
                     let mut writer = std::io::BufWriter::new(f);
                     for h in new_hints {
                         let _ = h.write(&mut writer);
                     }
                     let _ = writer.flush();
                 }
            }
        }
        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let needed_size = KV_OVERHEAD + key.len() + value.len();
        if needed_size > MAX_ALLOC as usize {
             return Err(Error::new(ErrorKind::InvalidInput, "Entry too large"));
        }

        loop {
            let mut block_guard = self.active_block.write().unwrap();
            let mut offset_guard = self.active_offset.write().unwrap();

            if block_guard.is_none() {
                let b = if needed_size as u64 > DEFAULT_BLOCK_SIZE {
                    unsafe { self.allocator.alloc_block(needed_size as u64)? }
                } else {
                    unsafe { self.allocator.get_next_available_block()? }
                };
                *block_guard = Some(KvBlock(b));
                *offset_guard = 0;
            }

            let block = block_guard.as_ref().unwrap();
            let current_offset = *offset_guard;

            if current_offset + needed_size as u64 > block.0.limit {
                *block_guard = None;
                continue;
            }

            let new_offset = block.write_kv(current_offset, key, value, false)?;
            
            let file_id = Self::parse_file_id(&block.0.file_path)?;
            let absolute_offset = block.0.offset + current_offset;
            
            let mut index = self.key_dir.write().unwrap();
            index.insert(key.to_vec(), EntryLocation {
                file_id,
                offset: absolute_offset,
                len: needed_size as u32,
            });

            *offset_guard = new_offset;
            return Ok(());
        }
    }

    pub fn compact(&self) -> Result<()> {
        let mut active_id = {
            let guard = self.active_block.read().unwrap();
            if let Some(b) = guard.as_ref() {
                 Some(Self::parse_file_id(&b.0.file_path)?)
            } else {
                None
            }
        };

        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.kv_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                 if path.extension().map_or(false, |e| e == "hint") { continue; }
                 if let Some(stem) = path.file_stem() {
                      if let Ok(id) = stem.to_string_lossy().parse::<u64>() {
                           files.push((id, path));
                      }
                 }
            }
        }

        if files.is_empty() { return Ok(()); }

        if active_id.is_none() {
             files.sort_by_key(|f| f.0);
             if let Some((max_id, _)) = files.last() {
                 active_id = Some(*max_id);
             }
        }
        
        let threshold_id = active_id.unwrap_or(0);

        // Process each file
        for (file_id, path) in files {
             if file_id >= threshold_id { continue; }
             
             let path_str = path.to_string_lossy().to_string();
             let path_str = path.to_string_lossy().to_string();
             let storage = StorageKeeper::get_storage_arc(&path_str)?;
             let len = storage.len();
             let mut offset = 0;
             
             while offset + KV_OVERHEAD <= len {
                  let mut header = [0u8; KV_HEADER_SIZE];
                  storage.read(offset, &mut header);

                  let flags = header[0];
                  let k_len = u16::from_le_bytes(header[1..3].try_into().unwrap()) as usize;
                  let v_len = u32::from_le_bytes(header[3..7].try_into().unwrap()) as usize;
                  
                  let total_len = KV_HEADER_SIZE + k_len + v_len + KV_CHECKSUM_SIZE;

                  if offset + total_len > len { break; }

                  // Verify Checksum
                  let mut entry_buf = vec![0u8; total_len];
                  storage.read(offset, &mut entry_buf);
                  
                  let stored_csum_bytes = &entry_buf[total_len - KV_CHECKSUM_SIZE..];
                  let stored_csum = u32::from_le_bytes(stored_csum_bytes.try_into().unwrap());
                  let data_to_check = &entry_buf[..total_len - KV_CHECKSUM_SIZE];
                  let calculated = checksum64(data_to_check) as u32;

                  if calculated == stored_csum {
                      let key = entry_buf[KV_HEADER_SIZE..KV_HEADER_SIZE + k_len].to_vec();
                      
                      // Check Liveness
                      // We need to know if index[key] points to (file_id, offset)
                      let is_live = {
                          let index = self.key_dir.read().unwrap();
                          if let Some(loc) = index.get(&key) {
                              loc.file_id == file_id && loc.offset == offset as u64
                          } else {
                              false
                          }
                      };

                      if is_live {
                           // Value is at KV_HEADER_SIZE + k_len
                           let val = entry_buf[KV_HEADER_SIZE + k_len .. KV_HEADER_SIZE + k_len + v_len].to_vec();
                           
                           // Rewrite to active file
                           // This will update the index to point to the NEW location
                           self.put(&key, &val)?;
                      }
                      // If not live, we just skip (it's garbage)
                      
                      offset += total_len;
                  } else {
                      break; // Corruption
                  }
             }
             
             // Delete old file and hint
             let _ = std::fs::remove_file(&path);
             let hint_path = hint::hint_file_path(&self.kv_root, file_id);
             if hint_path.exists() {
                 let _ = std::fs::remove_file(hint_path);
             }
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let loc = {
            let index = self.key_dir.read().unwrap();
            match index.get(key) {
                Some(l) => *l,
                None => return Ok(None),
            }
        };

        let path = self.kv_root.join(format!("{}", loc.file_id));
        let path_str = path.to_string_lossy().to_string();
        
        // If file doesn't exist, return None (shouldn't happen if Index is consistent)
        let storage = match StorageKeeper::get_storage_arc(&path_str) {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let len = loc.len as usize;
        if loc.offset as usize + len > storage.len() {
             return Err(Error::new(ErrorKind::InvalidData, "Index points past EOF"));
        }

        let mut buffer = vec![0u8; len];
        storage.read(loc.offset as usize, &mut buffer);

        // Verify Checksum
        let stored_csum_bytes = &buffer[len - KV_CHECKSUM_SIZE..];
        let stored_csum = u32::from_le_bytes(stored_csum_bytes.try_into().unwrap());
        let data_to_check = &buffer[..len - KV_CHECKSUM_SIZE];
        let calculated = checksum64(data_to_check) as u32;

        if calculated != stored_csum {
             return Err(Error::new(ErrorKind::InvalidData, "Checksum mismatch"));
        }

        let flags = buffer[0];
        if flags & FLAG_TOMBSTONE != 0 {
            return Ok(None);
        }
        
        let k_len = u16::from_le_bytes(buffer[1..3].try_into().unwrap()) as usize;
        let v_len = u32::from_le_bytes(buffer[3..7].try_into().unwrap()) as usize;
        
        let val = buffer[KV_HEADER_SIZE + k_len .. KV_HEADER_SIZE + k_len + v_len].to_vec();
        
        Ok(Some(val))
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        let index = self.key_dir.read().unwrap();
        index.keys().cloned().collect()
    }

    fn parse_file_id(path_str: &str) -> Result<u64> {
        let path = Path::new(path_str);
        let file_stem = path.file_stem().ok_or(Error::new(ErrorKind::InvalidInput, "No filename"))?;
        let s = file_stem.to_string_lossy();
        s.parse::<u64>().map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid file ID format"))
    }
}
