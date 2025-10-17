use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

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

        let unique = format!(
            "walrus-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);

        unsafe {
            std::env::set_var("WALRUS_QUIET", "1");
            std::env::set_var("WALRUS_DATA_DIR", dir.as_os_str());
        }

        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        Self { _guard: guard, dir }
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.dir.clone()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn current_wal_dir() -> PathBuf {
    std::env::var_os("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("wal_files"))
}
