use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// Macro for test output that respects WALRUS_QUIET
#[macro_export]
macro_rules! test_println {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            println!($($arg)*);
        }
    };
}

#[macro_export]
macro_rules! test_eprintln {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            eprintln!($($arg)*);
        }
    };
}

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
pub fn sanitize_key(key: &str) -> String {
    let mut sanitized: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.trim_matches('_').is_empty() {
        sanitized = format!("ns_{:x}", checksum64(key.as_bytes()));
    }
    sanitized
}

#[allow(dead_code)]
fn checksum64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub struct TestEnv {
    _guard: MutexGuard<'static, ()>,
    dir: PathBuf,
}

impl TestEnv {
    pub fn new() -> Self {
        let guard = TEST_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let unique = format!(
            "walrus-test-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            counter
        );
        let dir = std::env::temp_dir().join(unique);

        let namespace_key = format!(
            "test-key-{:x}-{:x}-{:x}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            counter
        );

        unsafe {
            std::env::set_var("WALRUS_QUIET", "1");
            std::env::set_var("WALRUS_DATA_DIR", dir.as_os_str());
            if std::env::var_os("WALRUS_INSTANCE_KEY").is_none() {
                std::env::set_var("WALRUS_INSTANCE_KEY", &namespace_key);
            }
        }

        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        Self { _guard: guard, dir }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
        unsafe {
            if matches!(
                std::env::var("WALRUS_QUIET"),
                Err(std::env::VarError::NotPresent)
            ) {
                std::env::remove_var("WALRUS_INSTANCE_KEY");
            }
        }
    }
}

pub fn current_wal_dir() -> PathBuf {
    let mut base = std::env::var_os("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("wal_files"));
    if let Some(key) = std::env::var_os("WALRUS_INSTANCE_KEY") {
        base.push(sanitize_key(&key.to_string_lossy()));
    }
    base
}

#[allow(dead_code)]
pub fn wal_root_dir() -> PathBuf {
    std::env::var_os("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("wal_files"))
}
