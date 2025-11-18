use crate::wal::config::{MAX_FILE_SIZE, now_millis_str, sanitize_namespace, wal_data_dir};
use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct WalPathManager {
    root: PathBuf,
    preallocated: Arc<Mutex<Option<String>>>,
}

impl WalPathManager {
    pub(crate) fn default() -> Self {
        let mut root = wal_data_dir();
        if let Some(key) = thread_namespace() {
            root.push(sanitize_namespace(&key));
        } else if let Ok(key) = std::env::var("WALRUS_INSTANCE_KEY") {
            root.push(sanitize_namespace(&key));
        }
        Self { 
            root,
            preallocated: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn for_key(key: &str) -> Self {
        let mut root = wal_data_dir();
        root.push(sanitize_namespace(key));
        Self { 
            root,
            preallocated: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn new(root: PathBuf) -> Self {
        Self { 
            root,
            preallocated: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn ensure_root(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.root)
    }

    pub(crate) fn index_path(&self, file_name: &str) -> PathBuf {
        self.root.join(format!("{}_index.db", file_name))
    }

    fn create_file_internal(&self) -> std::io::Result<String> {
        self.ensure_root()?;
        let file_name = now_millis_str();
        let path = self.root.join(&file_name);
        let f = std::fs::File::create(&path)?;
        f.set_len(MAX_FILE_SIZE)?;

        // Sync file metadata (size, etc.) to disk
        f.sync_all()?;

        // CRITICAL for Linux: Sync parent directory to ensure directory entry is durable
        // Without this, the file might exist but not be visible in directory listing after crash
        let dir = std::fs::File::open(&self.root)?;
        dir.sync_all()?;

        Ok(path.to_string_lossy().into_owned())
    }

    fn replenish(&self) -> std::io::Result<()> {
        if self.preallocated.lock().unwrap().is_some() {
            return Ok(());
        }
        let path = self.create_file_internal()?;
        let mut guard = self.preallocated.lock().unwrap();
        if guard.is_none() {
            *guard = Some(path);
        } else {
            // Race condition: someone else replenished?
            // Or we consumed and replenished quickly?
            // Just delete the extra file to avoid strays
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }

    pub(crate) fn create_new_file(&self) -> std::io::Result<String> {
        // 1. Try to take preallocated
        let pre = {
            let mut guard = self.preallocated.lock().unwrap();
            guard.take()
        };

        // 2. Spawn replenishment in background
        let myself = self.clone();
        std::thread::spawn(move || {
             let _ = myself.replenish();
        });

        if let Some(path) = pre {
             return Ok(path);
        }

        // 3. Fallback
        self.create_file_internal()
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
}

thread_local! {
    static THREAD_NAMESPACE: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn set_thread_namespace(key: &str) {
    THREAD_NAMESPACE.with(|tls| {
        *tls.borrow_mut() = Some(key.to_string());
    });
}

pub(crate) fn clear_thread_namespace() {
    THREAD_NAMESPACE.with(|tls| {
        tls.borrow_mut().take();
    });
}

pub(crate) fn thread_namespace() -> Option<String> {
    THREAD_NAMESPACE.with(|tls| tls.borrow().clone())
}
