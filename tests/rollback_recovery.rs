mod common;

use common::{TestEnv, current_wal_dir};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus, enable_fd_backend};

fn setup_test_env() -> TestEnv {
    TestEnv::new()
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all(current_wal_dir());
}

// Helper to calculate entry offsets (PREFIX_META_SIZE + data length)
fn entry_offset(data_len: usize) -> usize {
    64 + data_len // PREFIX_META_SIZE = 64
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

        // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
        // Even with fsync + directory sync, the kernel needs time to make files visible to fs::read_dir()
        thread::sleep(Duration::from_millis(50));
    }

    // Phase 2: Manually zero the header of entry_2 (simulates rollback cleanup)
    {
        let wal_files: Vec<_> = std::fs::read_dir(current_wal_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        assert_eq!(wal_files.len(), 1, "Should have exactly one WAL file");

        // Calculate exact offset of entry_2
        let offset_0 = 0;
        let offset_1 = entry_offset("entry_0".len());
        let offset_2 = offset_1 + entry_offset("entry_1".len());

        let file_path = wal_files[0].path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .unwrap();

        // Zero 64 bytes (PREFIX_META_SIZE) at entry_2's header
        let zeros = vec![0u8; 64];
        file.write_at(&zeros, offset_2 as u64)
            .expect("Failed to zero header");
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
        let e0 = wal
            .read_next("zero_test", true)
            .unwrap()
            .expect("Should read entry_0");
        assert_eq!(e0.data, b"entry_0", "First entry should be entry_0");

        let e1 = wal
            .read_next("zero_test", true)
            .unwrap()
            .expect("Should read entry_1");
        assert_eq!(e1.data, b"entry_1", "Second entry should be entry_1");

        // Should not see entries 2, 3, 4 (they're after the zeroed header)
        let e2 = wal.read_next("zero_test", true).unwrap();
        assert!(
            e2.is_none(),
            "Should not read entry_2 or beyond (zeroed header stops scan)"
        );

        // Verify WAL is still usable for new writes
        wal.append_for_topic("zero_test", b"new_entry").unwrap();
        let new = wal
            .read_next("zero_test", true)
            .unwrap()
            .expect("Should read new entry after recovery");
        assert_eq!(
            new.data, b"new_entry",
            "New writes should work after recovery"
        );
    }

    cleanup_test_env();
}

#[test]
fn test_concurrent_rollback_cleanup() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    // Concurrent batch writes - only one succeeds, rest roll back and zero headers
    let num_threads = 5;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            // Use distinct data patterns to identify which batch succeeded
            let data = vec![i as u8; 512 * 1024]; // 512KB
            let entries: Vec<&[u8]> = vec![data.as_slice(); 3];

            barrier_clone.wait();
            wal_clone.batch_append_for_topic("rollback_cleanup", &entries)
        });

        handles.push(handle);
    }

    let mut successes = 0;
    let mut rollbacks = 0;
    let mut winner_pattern = None;

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join().unwrap() {
            Ok(_) => {
                successes += 1;
                winner_pattern = Some(i as u8);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => rollbacks += 1,
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    assert_eq!(successes, 1, "Exactly one batch should succeed");
    assert_eq!(
        rollbacks,
        num_threads - 1,
        "All other batches should roll back"
    );

    // Verify only the winner's data is visible
    let winner = winner_pattern.expect("Should have one winner");
    let mut count = 0;
    while let Some(entry) = wal.read_next("rollback_cleanup", true).unwrap() {
        assert_eq!(entry.data.len(), 512 * 1024, "Entry size should be 512KB");
        assert_eq!(
            entry.data[0], winner,
            "All entries should be from winner thread"
        );
        count += 1;
    }

    assert_eq!(
        count, 3,
        "Should read exactly 3 entries from successful batch"
    );

    cleanup_test_env();
}

#[test]
fn test_rollback_with_block_spanning() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap(),
    );

    // Write initial data to fill most of first block (10MB blocks)
    let large_data = vec![0xAA; 8 * 1024 * 1024]; // 8MB
    wal.append_for_topic("spanning_test", &large_data).unwrap();

    // Read it to establish position
    let entry = wal
        .read_next("spanning_test", true)
        .unwrap()
        .expect("Should read initial 8MB entry");
    assert_eq!(
        entry.data.len(),
        8 * 1024 * 1024,
        "Initial entry should be 8MB"
    );
    assert_eq!(
        entry.data[0], 0xAA,
        "Initial entry should have 0xAA pattern"
    );

    // Concurrent batches that span multiple blocks (remaining ~2MB + need 18MB = spans 2 blocks)
    let num_threads = 3;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let entry = vec![(0x10 + i) as u8; 6 * 1024 * 1024]; // 6MB per entry, distinct patterns
            let entries: Vec<&[u8]> = vec![entry.as_slice(); 3]; // 18MB total

            barrier_clone.wait();
            wal_clone.batch_append_for_topic("spanning_test", &entries)
        });

        handles.push(handle);
    }

    let mut successes = 0;
    let mut winner_pattern = None;

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join().unwrap() {
            Ok(_) => {
                successes += 1;
                winner_pattern = Some((0x10 + i) as u8);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("Unexpected error during concurrent write: {}", e),
        }
    }

    assert_eq!(successes, 1, "Exactly one multi-block batch should succeed");

    // Verify only winner's entries are visible (rollback zeroed losers' headers across blocks)
    let winner = winner_pattern.expect("Should have one winner");
    let mut count = 0;
    while let Some(entry) = wal.read_next("spanning_test", true).unwrap() {
        assert_eq!(entry.data.len(), 6 * 1024 * 1024, "Entry should be 6MB");
        assert_eq!(entry.data[0], winner, "Entry should be from winner batch");
        count += 1;
    }

    assert_eq!(count, 3, "Should read exactly 3 entries from winning batch");

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

        // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
        // Even with fsync + directory sync, the kernel needs time to make files visible to fs::read_dir()
        thread::sleep(Duration::from_millis(50));
    }

    // Phase 2: Zero the header of the large entry (simulates mid-batch rollback)
    {
        let wal_files: Vec<_> = std::fs::read_dir(current_wal_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.path().to_str().unwrap().ends_with("_index.db"))
            .collect();

        assert_eq!(wal_files.len(), 1, "Should have exactly one WAL file");

        let file_path = wal_files[0].path();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open WAL file");

        // Calculate offset of large entry (second entry)
        let offset_large = entry_offset("small_1".len());

        let zeros = vec![0u8; 64]; // PREFIX_META_SIZE
        file.write_at(&zeros, offset_large as u64)
            .expect("Failed to zero header");
        file.sync_all().unwrap();
    }

    // Phase 3: Recovery should preserve data before zeroed header, stop at zero
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should recover first entry (before zeroed header)
        let e1 = wal
            .read_next("preserve_test", true)
            .unwrap()
            .expect("Should read small_1");
        assert_eq!(e1.data, b"small_1", "First entry should be small_1");

        // Should not see the large entry (zeroed header) or small_2 (after zeroed header)
        let e2 = wal.read_next("preserve_test", true).unwrap();
        assert!(
            e2.is_none(),
            "Should not read past zeroed header (preserves data before, blocks garbage after)"
        );

        // WAL should still be usable for new writes after encountering zeroed header
        wal.append_for_topic("preserve_test", b"new_after_recovery")
            .unwrap();
        let new_entry = wal
            .read_next("preserve_test", true)
            .unwrap()
            .expect("Should read new entry");
        assert_eq!(
            new_entry.data, b"new_after_recovery",
            "New writes should work after recovery"
        );
    }

    cleanup_test_env();
}
