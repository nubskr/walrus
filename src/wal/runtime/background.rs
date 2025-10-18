use crate::wal::config::{FsyncSchedule, debug_print};
use crate::wal::storage::{StorageImpl, open_storage_for_path};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::DELETION_TX;

#[cfg(target_os = "linux")]
use crate::wal::config::USE_FD_BACKEND;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
use io_uring;

pub(super) fn start_background_workers(fsync_schedule: FsyncSchedule) -> Arc<mpsc::Sender<String>> {
    let (tx, rx) = mpsc::channel::<String>();
    let tx_arc = Arc::new(tx);
    let (del_tx, del_rx) = mpsc::channel::<String>();
    let del_tx_arc = Arc::new(del_tx);
    let _ = DELETION_TX.set(del_tx_arc.clone());
    let pool: HashMap<String, StorageImpl> = HashMap::new();
    let tick = Arc::new(AtomicU64::new(0));
    let sleep_millis = match fsync_schedule {
        FsyncSchedule::Milliseconds(ms) => ms.max(1),
        FsyncSchedule::SyncEach => 5000, // Still run background thread for cleanup, but less frequently
        FsyncSchedule::NoFsync => 10000, // Even less frequent cleanup when no fsyncing
    };

    thread::spawn(move || {
        let mut pool = pool;
        let tick = tick;
        let del_rx = del_rx;
        let mut delete_pending = HashSet::new();

        #[cfg(target_os = "linux")]
        let mut ring = io_uring::IoUring::new(2048).expect("Failed to create io_uring");

        loop {
            thread::sleep(Duration::from_millis(sleep_millis));

            // Phase 1: Collect unique paths to flush
            let mut unique = HashSet::new();
            while let Ok(path) = rx.try_recv() {
                unique.insert(path);
            }

            if !unique.is_empty() {
                debug_print!("[flush] scheduling {} paths", unique.len());
            }

            // Phase 2: Open/map files if needed
            for path in unique.iter() {
                // Skip if file doesn't exist
                if !Path::new(&path).exists() {
                    debug_print!("[flush] file does not exist, skipping: {}", path);
                    continue;
                }

                if !pool.contains_key(path) {
                    match open_storage_for_path(path) {
                        Ok(storage) => {
                            pool.insert(path.clone(), storage);
                        }
                        Err(e) => {
                            debug_print!("[flush] failed to open storage for {}: {}", path, e);
                        }
                    }
                }
            }

            // Phase 3: Flush operations
            #[cfg(target_os = "linux")]
            {
                if USE_FD_BACKEND.load(Ordering::Relaxed) {
                    // FD backend: Use io_uring for batched fsync
                    let mut fsync_batch = Vec::new();

                    for path in unique.iter() {
                        if let Some(storage) = pool.get(path) {
                            if let Some(fd_backend) = storage.as_fd() {
                                let raw_fd = fd_backend.file().as_raw_fd();
                                fsync_batch.push((raw_fd, path.clone()));
                            }
                        }
                    }

                    if !fsync_batch.is_empty() {
                        debug_print!("[flush] batching {} fsync operations", fsync_batch.len());

                        // Push all fsync operations to submission queue
                        for (i, (raw_fd, _path)) in fsync_batch.iter().enumerate() {
                            let fd = io_uring::types::Fd(*raw_fd);

                            let fsync_op =
                                io_uring::opcode::Fsync::new(fd).build().user_data(i as u64);

                            unsafe {
                                if ring.submission().push(&fsync_op).is_err() {
                                    // Submission queue full, submit current batch
                                    ring.submit().expect("Failed to submit fsync batch");
                                    ring.submission()
                                        .push(&fsync_op)
                                        .expect("Failed to push fsync op");
                                }
                            }
                        }

                        // Single syscall to submit all fsync operations!
                        match ring.submit_and_wait(fsync_batch.len()) {
                            Ok(submitted) => {
                                debug_print!(
                                    "[flush] submitted {} fsync ops in one syscall",
                                    submitted
                                );
                            }
                            Err(e) => {
                                debug_print!("[flush] failed to submit fsync batch: {}", e);
                            }
                        }

                        // Process completions
                        for _ in 0..fsync_batch.len() {
                            if let Some(cqe) = ring.completion().next() {
                                let idx = cqe.user_data() as usize;
                                let result = cqe.result();

                                if result < 0 {
                                    let (_fd, path) = &fsync_batch[idx];
                                    debug_print!(
                                        "[flush] fsync error for {}: error code {}",
                                        path,
                                        result
                                    );
                                }
                            }
                        }
                    }
                } else {
                    for path in unique.iter() {
                        if let Some(storage) = pool.get_mut(path) {
                            if let Err(e) = storage.flush() {
                                debug_print!("[flush] flush error for {}: {}", path, e);
                            }
                        }
                    }
                }
            }

            #[cfg(not(target_os = "linux"))]
            {
                for path in unique.iter() {
                    if let Some(storage) = pool.get_mut(path) {
                        if let Err(e) = storage.flush() {
                            debug_print!("[flush] flush error for {}: {}", path, e);
                        }
                    }
                }
            }

            // Phase 4: Handle deletion requests
            while let Ok(path) = del_rx.try_recv() {
                debug_print!("[reclaim] deletion requested: {}", path);
                delete_pending.insert(path);
            }

            // Phase 5: Periodic cleanup
            let n = tick.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= 1000 {
                // WARN: we clean up once every 1000 times the fsync runs
                if tick
                    .compare_exchange(n, 0, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    let mut empty: HashMap<String, StorageImpl> = HashMap::new();
                    std::mem::swap(&mut pool, &mut empty); // reset map every hour to avoid unconstrained overflow

                    // Perform batched deletions now that mmaps/fds are dropped
                    for path in delete_pending.drain() {
                        match fs::remove_file(&path) {
                            Ok(_) => debug_print!("[reclaim] deleted file {}", path),
                            Err(e) => {
                                debug_print!("[reclaim] delete failed for {}: {}", path, e)
                            }
                        }
                    }
                }
            }
        }
    });

    tx_arc
}
