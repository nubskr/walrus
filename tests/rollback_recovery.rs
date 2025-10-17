use walrus_rust::{enable_fd_backend, FsyncSchedule, ReadConsistency, Walrus};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::io::Write;
use std::os::unix::fs::FileExt;

// Global lock to serialize tests (prevents parallel test interference)
static TEST_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn setup_test_env() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe { std::env::set_var("WALRUS_QUIET", "1"); }
    let _ = std::fs::remove_dir_all("wal_files");
    std::fs::create_dir_all("wal_files").unwrap();
    guard
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all("wal_files");
}

// ============================================================================
// ROLLBACK HEADER ZEROING TESTS
// ============================================================================

#[test]
fn test_zeroed_header_stops_block_scanning() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write some entries successfully
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Write 5 entries (but don't read them - we want to read after recovery)
        for i in 0..5 {
            let data = format!("entry_{}", i);
            wal.append_for_topic("zero_test", data.as_bytes()).unwrap();
        }

        drop(wal);
    }

    // Phase 2: Manually corrupt the 3rd entry by zeroing its header
    // This simulates what happens during rollback cleanup
    {
        let wal_files: Vec<_> = std::fs::read_dir("wal_files")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        assert!(!wal_files.is_empty(), "Should have at least one WAL file");

        // Zero the header of the 3rd entry (at offset 2 * (64 + data_len))
        // Entry 0: offset 0, Entry 1: offset ~80, Entry 2: offset ~160
        let entry_0_size = 64 + "entry_0".len();
        let entry_1_size = 64 + "entry_1".len();
        let entry_2_offset = entry_0_size + entry_1_size;

        let file_path = wal_files[0].path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .unwrap();

        // Zero 64 bytes (PREFIX_META_SIZE) at entry_2's position
        let zeros = vec![0u8; 64];
        file.write_at(&zeros, entry_2_offset as u64).unwrap();
        file.sync_all().unwrap();
    }

    // Phase 3: Recovery should stop scanning at the zeroed header
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should only recover entries 0 and 1 (before the zeroed header)
        let e0 = wal.read_next("zero_test").unwrap().unwrap();
        assert_eq!(e0.data, b"entry_0");

        let e1 = wal.read_next("zero_test").unwrap().unwrap();
        assert_eq!(e1.data, b"entry_1");

        // Should not see entries 2, 3, 4 (they're after the zeroed header)
        assert!(wal.read_next("zero_test").unwrap().is_none());
    }

    cleanup_test_env();
}

#[test]
fn test_concurrent_rollback_cleanup() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Try many concurrent batch writes - most will roll back
    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let data = vec![i as u8; 1024 * 1024]; // 1MB
            let entries: Vec<&[u8]> = vec![data.as_slice(); 3];

            barrier_clone.wait();
            wal_clone.batch_append_for_topic("rollback_cleanup", &entries)
        });

        handles.push(handle);
    }

    let mut successes = 0;
    let mut rollbacks = 0;

    for handle in handles {
        match handle.join().unwrap() {
            Ok(_) => successes += 1,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => rollbacks += 1,
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    // Exactly one should succeed, rest should roll back
    assert_eq!(successes, 1, "Expected exactly 1 success");
    assert_eq!(rollbacks, num_threads - 1, "Expected {} rollbacks", num_threads - 1);

    // Read what's actually committed
    let mut count = 0;
    while wal.read_next("rollback_cleanup").unwrap().is_some() {
        count += 1;
    }

    // Should only see the 3 entries from the successful batch
    assert_eq!(count, 3, "Should only see entries from successful batch");

    cleanup_test_env();
}

#[test]
fn test_recovery_after_partial_write_simulation() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write a batch successfully
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let entries: Vec<&[u8]> = vec![b"good_1", b"good_2", b"good_3"];
        wal.batch_append_for_topic("partial_test", &entries).unwrap();

        drop(wal);
    }

    // Phase 2: Simulate partial write by adding garbage after valid data
    {
        let wal_files: Vec<_> = std::fs::read_dir("wal_files")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        let file_path = wal_files[0].path();

        // Append some garbage data that looks like it might be an entry
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&file_path)
            .unwrap();

        // Write a fake header with non-zero length but garbage data
        let fake_header = vec![0xFF; 64];
        file.write_all(&fake_header).unwrap();
        file.write_all(b"garbage_data_that_should_not_be_read").unwrap();
        file.sync_all().unwrap();
    }

    // Phase 3: Recovery should stop at invalid data
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should only recover the 3 valid entries
        assert_eq!(wal.read_next("partial_test").unwrap().unwrap().data, b"good_1");
        assert_eq!(wal.read_next("partial_test").unwrap().unwrap().data, b"good_2");
        assert_eq!(wal.read_next("partial_test").unwrap().unwrap().data, b"good_3");

        // Should not read the garbage
        assert!(wal.read_next("partial_test").unwrap().is_none());
    }

    cleanup_test_env();
}

#[test]
fn test_rollback_with_block_spanning() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Write initial data to fill most of first block
    let large_data = vec![0xAA; 8 * 1024 * 1024]; // 8MB
    wal.append_for_topic("spanning_test", &large_data).unwrap();

    // Read it
    let entry = wal.read_next("spanning_test").unwrap().unwrap();
    assert_eq!(entry.data.len(), 8 * 1024 * 1024);

    // Now try concurrent batches that will span blocks
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];

    for i in 0..3 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            // Large enough to require multiple blocks
            let entry = vec![i as u8; 6 * 1024 * 1024]; // 6MB per entry
            let entries: Vec<&[u8]> = vec![entry.as_slice(); 3]; // 18MB total

            barrier_clone.wait();
            wal_clone.batch_append_for_topic("spanning_test", &entries)
        });

        handles.push(handle);
    }

    let mut successes = 0;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(_) => successes += 1,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    assert_eq!(successes, 1, "Exactly one batch should succeed");

    // Verify we can read exactly 3 entries from the successful batch
    let mut count = 0;
    while wal.read_next("spanning_test").unwrap().is_some() {
        count += 1;
    }

    assert_eq!(count, 3, "Should read 3 entries from successful batch");

    cleanup_test_env();
}

#[test]
fn test_multiple_rollbacks_same_block() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Do multiple rounds of concurrent writes to the same topic
    for round in 0..5 {
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];

        for i in 0..3 {
            let wal_clone = wal.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                let data = format!("round_{}_thread_{}", round, i);
                let entries: Vec<&[u8]> = vec![data.as_bytes(); 2];

                barrier_clone.wait();
                wal_clone.batch_append_for_topic("multi_rollback", &entries)
            });

            handles.push(handle);
        }

        let mut successes = 0;
        for handle in handles {
            if handle.join().unwrap().is_ok() {
                successes += 1;
            }
        }

        assert_eq!(successes, 1, "Exactly one batch should succeed in round {}", round);
    }

    // Should have 5 successful batches * 2 entries each = 10 entries
    let mut count = 0;
    while wal.read_next("multi_rollback").unwrap().is_some() {
        count += 1;
    }

    assert_eq!(count, 10, "Should have exactly 10 entries from 5 successful batches");

    cleanup_test_env();
}

#[test]
fn test_recovery_preserves_data_before_zeroed_headers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write mixed small and large entries (but don't read them yet)
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        wal.append_for_topic("preserve_test", b"small_1").unwrap();

        let large = vec![0xBB; 2 * 1024 * 1024]; // 2MB
        wal.append_for_topic("preserve_test", &large).unwrap();

        wal.append_for_topic("preserve_test", b"small_2").unwrap();

        drop(wal);
    }

    // Phase 2: Zero the header of the large entry
    {
        let wal_files: Vec<_> = std::fs::read_dir("wal_files")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        let file_path = wal_files[0].path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .unwrap();

        // Zero header at offset of second entry
        let offset = 64 + "small_1".len();
        let zeros = vec![0u8; 64];
        file.write_at(&zeros, offset as u64).unwrap();
        file.sync_all().unwrap();
    }

    // Phase 3: Recovery should see first entry only
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should recover first entry
        assert_eq!(wal.read_next("preserve_test").unwrap().unwrap().data, b"small_1");

        // Should not see the large entry or anything after
        assert!(wal.read_next("preserve_test").unwrap().is_none());

        // WAL should still be usable for new writes
        wal.append_for_topic("preserve_test", b"new_after_recovery").unwrap();
        assert_eq!(wal.read_next("preserve_test").unwrap().unwrap().data, b"new_after_recovery");
    }

    cleanup_test_env();
}
