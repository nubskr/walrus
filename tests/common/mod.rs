use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};


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

static BASE_DIR: OnceLock<PathBuf> = OnceLock::new();
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ThreadKeyState {
    active: Option<String>,
    last: Option<String>,
}

thread_local! {
    static THREAD_KEYS: RefCell<ThreadKeyState> = RefCell::new(ThreadKeyState::default());
}

fn ensure_base_dir() -> PathBuf {
    BASE_DIR
        .get_or_init(|| {
            let unique = format!(
                "walrus-test-run-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            let dir = std::env::temp_dir().join(unique);
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("failed to create walrus test root");
            unsafe {
                std::env::set_var("WALRUS_QUIET", "1");
                std::env::set_var("WALRUS_DATA_DIR", &dir);
            }
            dir
        })
        .clone()
}

fn next_namespace_key(counter: u64) -> String {
    format!(
        "test-key-{:x}-{:x}-{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        counter
    )
}

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
    key: String,
    dir: PathBuf,
}

impl TestEnv {
    pub fn new() -> Self {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = ensure_base_dir();
        let key = next_namespace_key(counter);
        let dir = base.join(sanitize_key(&key));

        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("failed to create per-test walrus dir");

        THREAD_KEYS.with(|state| {
            let mut st = state.borrow_mut();
            st.active = Some(key.clone());
            st.last = Some(key.clone());
        });
        walrus_rust::wal::__set_thread_namespace_for_tests(&key);

        Self { key, dir }
    }

    pub fn namespace_key(&self) -> &str {
        &self.key
    }

    pub fn unique_key(&self, base: &str) -> String {
        format!("{}-{}", base, self.key)
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
        THREAD_KEYS.with(|state| {
            let mut st = state.borrow_mut();
            if st.active.as_deref() == Some(self.key.as_str()) {
                st.active = None;
            }
            st.last = Some(self.key.clone());
        });
        walrus_rust::wal::__clear_thread_namespace_for_tests();
    }
}

pub fn current_wal_dir() -> PathBuf {
    let mut base = ensure_base_dir();
    let key = THREAD_KEYS.with(|state| {
        let st = state.borrow();
        st.active
            .as_ref()
            .or(st.last.as_ref())
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    });
    base.push(sanitize_key(&key));
    base
}

#[allow(dead_code)]
pub fn wal_root_dir() -> PathBuf {
    ensure_base_dir()
}
