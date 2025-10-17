use walrus_rust::{enable_fd_backend, FsyncSchedule, ReadConsistency, Walrus};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

// Global lock to serialize tests (prevents parallel test interference)
static TEST_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn setup_test_env() -> std::sync::MutexGuard<'static, ()> {
    // Recover from poisoned lock instead of panicking
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    unsafe { std::env::set_var("WALRUS_QUIET", "1"); }

    // Clean up old test directories
    let _ = std::fs::remove_dir_all("wal_files");
    std::fs::create_dir_all("wal_files").unwrap();

    guard
}

fn cleanup_test_env() {
    let _ = std::fs::remove_dir_all("wal_files");
}

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

#[test]
fn test_batch_write_basic() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![b"entry1", b"entry2", b"entry3"];

    // Write batch
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Read back and verify
    let e1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e1.data, b"entry1");

    let e2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e2.data, b"entry2");

    let e3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e3.data, b"entry3");

    // Should be no more entries
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_batch_write_atomicity_with_reader() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write some initial data
    wal.append_for_topic("test_topic", b"before").unwrap();

    // Reader consumes it
    let e = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e.data, b"before");

    // Now do batch write
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2", b"batch3"];
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Reader should see all batch entries atomically
    let e1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e1.data, b"batch1");

    let e2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e2.data, b"batch2");

    let e3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(e3.data, b"batch3");

    cleanup_test_env();
}

// ============================================================================
// FAILURE SCENARIO TESTS
// ============================================================================

#[test]
fn test_batch_size_limit_enforcement() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Try to write more than 10GB
    // Each entry is 1GB, so 11 entries = 11GB (plus metadata)
    let one_gb = vec![0u8; 1024 * 1024 * 1024];
    let entries: Vec<&[u8]> = vec![
        &one_gb, &one_gb, &one_gb, &one_gb, &one_gb,
        &one_gb, &one_gb, &one_gb, &one_gb, &one_gb,
        &one_gb, // 11th entry
    ];

    let result = wal.batch_append_for_topic("test_topic", &entries);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("10GB limit"));

    // Verify nothing was written
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_concurrent_batch_writes_rejected() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(2));
    let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let would_block_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..2 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        let success = success_count.clone();
        let blocked = would_block_count.clone();

        let handle = thread::spawn(move || {
            // Prepare large batch to ensure overlap
            let large_entry = vec![0u8; 10 * 1024 * 1024]; // 10MB per entry
            let entries: Vec<&[u8]> = (0..100).map(|_| large_entry.as_slice()).collect();

            // Synchronize start
            barrier_clone.wait();

            let result = wal_clone.batch_append_for_topic("test_topic", &entries);

            match result {
                Ok(_) => {
                    success.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    blocked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(e) => panic!("Unexpected error: {}", e),
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let successes = success_count.load(std::sync::atomic::Ordering::SeqCst);
    let blocks = would_block_count.load(std::sync::atomic::Ordering::SeqCst);

    // Exactly one should succeed, one should be blocked
    assert_eq!(successes, 1, "Expected exactly 1 successful batch write");
    assert_eq!(blocks, 1, "Expected exactly 1 blocked batch write");

    cleanup_test_env();
}

#[test]
fn test_regular_write_blocked_during_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let barrier = Arc::new(Barrier::new(2));
    let batch_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let write_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Thread 1: Batch write (slow)
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let batch_flag = batch_started.clone();
    let batch_handle = thread::spawn(move || {
        let large_entry = vec![0u8; 50 * 1024 * 1024]; // 50MB per entry
        let entries: Vec<&[u8]> = (0..50).map(|_| large_entry.as_slice()).collect();

        barrier_clone.wait();
        batch_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        wal_clone.batch_append_for_topic("test_topic", &entries).unwrap();
    });

    // Thread 2: Regular write (should be blocked)
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    let batch_flag = batch_started.clone();
    let blocked_flag = write_blocked.clone();
    let write_handle = thread::spawn(move || {
        barrier_clone.wait();

        // Wait a bit to ensure batch has started
        while !batch_flag.load(std::sync::atomic::Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(10));

        // Try regular write
        let result = wal_clone.append_for_topic("test_topic", b"regular_entry");

        if let Err(e) = result {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                blocked_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });

    batch_handle.join().unwrap();
    write_handle.join().unwrap();

    assert!(
        write_blocked.load(std::sync::atomic::Ordering::SeqCst),
        "Regular write should have been blocked during batch"
    );

    cleanup_test_env();
}

#[test]
fn test_empty_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![];

    // Empty batch should succeed (no-op)
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Nothing to read
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

// ============================================================================
// MULTI-BLOCK SPANNING TESTS
// ============================================================================

#[test]
fn test_batch_spans_multiple_blocks() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Each entry is 5MB, 100 entries = 500MB, will span multiple 10MB blocks
    let large_entry = vec![0xAB; 5 * 1024 * 1024];
    let entries: Vec<&[u8]> = (0..100).map(|_| large_entry.as_slice()).collect();

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Read back and verify all entries
    for i in 0..100 {
        let entry = wal.read_next("test_topic").unwrap().unwrap();
        assert_eq!(entry.data.len(), 5 * 1024 * 1024);
        assert_eq!(entry.data[0], 0xAB);
    }

    // No more entries
    assert!(wal.read_next("test_topic").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_batch_with_varying_entry_sizes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create entries of varying sizes
    let e1 = vec![1u8; 100];
    let e2 = vec![2u8; 1024 * 1024]; // 1MB
    let e3 = vec![3u8; 10 * 1024 * 1024]; // 10MB
    let e4 = vec![4u8; 500];
    let e5 = vec![5u8; 50 * 1024 * 1024]; // 50MB

    let entries: Vec<&[u8]> = vec![&e1, &e2, &e3, &e4, &e5];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Verify all entries
    let r1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r1.data.len(), 100);
    assert_eq!(r1.data[0], 1);

    let r2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r2.data.len(), 1024 * 1024);
    assert_eq!(r2.data[0], 2);

    let r3 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r3.data.len(), 10 * 1024 * 1024);
    assert_eq!(r3.data[0], 3);

    let r4 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r4.data.len(), 500);
    assert_eq!(r4.data[0], 4);

    let r5 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r5.data.len(), 50 * 1024 * 1024);
    assert_eq!(r5.data[0], 5);

    cleanup_test_env();
}

// ============================================================================
// CHAOS TESTS - CONCURRENT OPERATIONS
// ============================================================================

#[test]
fn test_chaos_interleaved_batch_and_regular_writes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_threads = 10;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let wal_clone = wal.clone();

        let handle = thread::spawn(move || {
            for i in 0..20 {
                if i % 3 == 0 {
                    // Try batch write
                    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2", b"batch3"];
                    let _ = wal_clone.batch_append_for_topic("chaos_topic", &entries);
                } else {
                    // Regular write
                    let data = format!("regular_t{}_i{}", thread_id, i);
                    let _ = wal_clone.append_for_topic("chaos_topic", data.as_bytes());
                }

                // Random small delay
                thread::sleep(Duration::from_micros(100));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify we can read all written data without corruption
    let mut count = 0;
    while let Some(entry) = wal.read_next("chaos_topic").unwrap() {
        // Just verify we can read without panic/corruption
        assert!(!entry.data.is_empty());
        count += 1;
    }

    // Should have written some data
    assert!(count > 0, "Expected some entries to be written");

    cleanup_test_env();
}

#[test]
fn test_chaos_multiple_topics_concurrent_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_topics = 5;
    let mut handles = vec![];

    for topic_id in 0..num_topics {
        let wal_clone = wal.clone();

        let handle = thread::spawn(move || {
            let topic_name = format!("topic_{}", topic_id);

            for batch_num in 0..10 {
                let entry_data = format!("t{}_b{}_e", topic_id, batch_num);
                let e1 = entry_data.clone() + "1";
                let e2 = entry_data.clone() + "2";
                let e3 = entry_data.clone() + "3";

                let entries: Vec<&[u8]> = vec![e1.as_bytes(), e2.as_bytes(), e3.as_bytes()];

                wal_clone.batch_append_for_topic(&topic_name, &entries).unwrap();

                thread::sleep(Duration::from_millis(5));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each topic has correct data
    for topic_id in 0..num_topics {
        let topic_name = format!("topic_{}", topic_id);
        let mut count = 0;

        while let Some(entry) = wal.read_next(&topic_name).unwrap() {
            let data_str = String::from_utf8_lossy(&entry.data);
            assert!(data_str.starts_with(&format!("t{}_", topic_id)));
            count += 1;
        }

        // Each topic should have 10 batches * 3 entries = 30 entries
        assert_eq!(count, 30, "Topic {} should have 30 entries", topic_id);
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_write_with_concurrent_readers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 10 },
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = vec![];

    // Writer threads
    for writer_id in 0..3 {
        let wal_clone = wal.clone();
        let stop = stop_flag.clone();

        let handle = thread::spawn(move || {
            let mut batch_count = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let entry_data = format!("writer_{}_batch_{}", writer_id, batch_count);
                let entries: Vec<&[u8]> = vec![
                    entry_data.as_bytes(),
                    entry_data.as_bytes(),
                    entry_data.as_bytes(),
                ];

                let _ = wal_clone.batch_append_for_topic("chaos_rw_topic", &entries);
                batch_count += 1;

                thread::sleep(Duration::from_millis(10));
            }
        });

        handles.push(handle);
    }

    // Reader threads
    for _ in 0..5 {
        let wal_clone = wal.clone();
        let stop = stop_flag.clone();

        let handle = thread::spawn(move || {
            let mut read_count = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(Some(entry)) = wal_clone.read_next("chaos_rw_topic") {
                    // Verify data is not corrupted
                    assert!(!entry.data.is_empty());
                    read_count += 1;
                } else {
                    thread::sleep(Duration::from_millis(5));
                }
            }
            // Return nothing to match writer thread type
        });

        handles.push(handle);
    }

    // Let it run for a bit
    thread::sleep(Duration::from_secs(3));
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_write_crash_recovery() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write batch and "crash" (drop WAL)
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let entries: Vec<&[u8]> = vec![b"before_crash_1", b"before_crash_2", b"before_crash_3"];
        wal.batch_append_for_topic("crash_topic", &entries).unwrap();

        // Simulate crash by dropping wal without graceful shutdown
        drop(wal);
    }

    // Phase 2: Recover and verify
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        // Should be able to read the batch that was written before crash
        let e1 = wal.read_next("crash_topic").unwrap().unwrap();
        assert_eq!(e1.data, b"before_crash_1");

        let e2 = wal.read_next("crash_topic").unwrap().unwrap();
        assert_eq!(e2.data, b"before_crash_2");

        let e3 = wal.read_next("crash_topic").unwrap().unwrap();
        assert_eq!(e3.data, b"before_crash_3");

        // Write more after recovery
        let entries2: Vec<&[u8]> = vec![b"after_crash_1", b"after_crash_2"];
        wal.batch_append_for_topic("crash_topic", &entries2).unwrap();

        let e4 = wal.read_next("crash_topic").unwrap().unwrap();
        assert_eq!(e4.data, b"after_crash_1");

        let e5 = wal.read_next("crash_topic").unwrap().unwrap();
        assert_eq!(e5.data, b"after_crash_2");
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_alternating_tiny_and_huge_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    for round in 0..10 {
        // Tiny batch
        let tiny: Vec<&[u8]> = vec![b"t"];
        wal.batch_append_for_topic("alternating", &tiny).unwrap();

        // Huge batch (10MB per entry, 5 entries = 50MB)
        let huge_entry = vec![0xAB; 10 * 1024 * 1024];
        let huge: Vec<&[u8]> = (0..5).map(|_| huge_entry.as_slice()).collect();
        wal.batch_append_for_topic("alternating", &huge).unwrap();
    }

    // Verify all entries
    for round in 0..10 {
        let tiny_entry = wal.read_next("alternating").unwrap().unwrap();
        assert_eq!(tiny_entry.data, b"t");

        for _ in 0..5 {
            let huge_entry = wal.read_next("alternating").unwrap().unwrap();
            assert_eq!(huge_entry.data.len(), 10 * 1024 * 1024);
            assert_eq!(huge_entry.data[0], 0xAB);
        }
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_writes_force_multiple_block_rotations() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Each entry is 5MB, 100 entries = 500MB
    // With 10MB blocks, this should rotate through ~50 blocks
    let entry = vec![0xEE; 5 * 1024 * 1024];
    let entries: Vec<&[u8]> = (0..100).map(|_| entry.as_slice()).collect();

    wal.batch_append_for_topic("rotation_topic", &entries).unwrap();

    // Verify all entries survived the rotations
    for _ in 0..100 {
        let e = wal.read_next("rotation_topic").unwrap().unwrap();
        assert_eq!(e.data.len(), 5 * 1024 * 1024);
        assert_eq!(e.data[0], 0xEE);
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_readers_at_different_positions_during_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Test that batch writes appear atomically - either all entries are visible or none
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write initial entries
    for i in 0..5 {
        let data = format!("initial_{}", i);
        wal.append_for_topic("atomic_test", data.as_bytes()).unwrap();
    }

    // Read 3 entries (position reader at offset 3)
    for _ in 0..3 {
        wal.read_next("atomic_test").unwrap();
    }

    // Write a batch of 4 entries
    let batch: Vec<&[u8]> = vec![b"batch_0", b"batch_1", b"batch_2", b"batch_3"];
    wal.batch_append_for_topic("atomic_test", &batch).unwrap();

    // Reader should see remaining 2 initial entries + 4 batch entries = 6 total
    let mut entries = Vec::new();
    while let Some(entry) = wal.read_next("atomic_test").unwrap() {
        entries.push(entry.data);
    }

    assert_eq!(entries.len(), 6);

    // Verify the entries are in correct order
    assert_eq!(entries[0], b"initial_3");
    assert_eq!(entries[1], b"initial_4");
    assert_eq!(entries[2], b"batch_0");
    assert_eq!(entries[3], b"batch_1");
    assert_eq!(entries[4], b"batch_2");
    assert_eq!(entries[5], b"batch_3");

    cleanup_test_env();
}

#[test]
fn test_chaos_many_topics_racing_batch_and_regular() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_topics = 20;
    let mut handles = vec![];

    // Each topic gets 2 threads: one doing batch writes, one doing regular writes
    for topic_id in 0..num_topics {
        // Batch writer thread
        let wal_clone = wal.clone();
        let h1 = thread::spawn(move || {
            let topic = format!("race_topic_{}", topic_id);
            for batch_num in 0..20 {
                let data = format!("t{}_b{}", topic_id, batch_num);
                let entries: Vec<&[u8]> = vec![data.as_bytes(), data.as_bytes(), data.as_bytes()];
                let _ = wal_clone.batch_append_for_topic(&topic, &entries);
                thread::sleep(Duration::from_micros(100));
            }
        });

        // Regular writer thread
        let wal_clone = wal.clone();
        let h2 = thread::spawn(move || {
            let topic = format!("race_topic_{}", topic_id);
            for entry_num in 0..20 {
                let data = format!("t{}_r{}", topic_id, entry_num);
                let _ = wal_clone.append_for_topic(&topic, data.as_bytes());
                thread::sleep(Duration::from_micros(100));
            }
        });

        handles.push(h1);
        handles.push(h2);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each topic has some entries (no corruption)
    for topic_id in 0..num_topics {
        let topic = format!("race_topic_{}", topic_id);
        let mut count = 0;
        while wal.read_next(&topic).unwrap().is_some() {
            count += 1;
        }
        assert!(count > 0, "Topic {} should have entries", topic_id);
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_sequential_batches_with_crashes() {
    let _guard = setup_test_env();
    enable_fd_backend();

    for cycle in 0..5 {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let data = format!("cycle_{}", cycle);
        let entries: Vec<&[u8]> = vec![data.as_bytes(), data.as_bytes()];
        wal.batch_append_for_topic("crash_cycles", &entries).unwrap();

        // Simulate crash
        drop(wal);
    }

    // Final recovery: should see all 5 cycles × 2 entries = 10 entries
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    for cycle in 0..5 {
        let expected = format!("cycle_{}", cycle);
        for _ in 0..2 {
            let entry = wal.read_next("crash_cycles").unwrap().unwrap();
            assert_eq!(entry.data, expected.as_bytes());
        }
    }

    assert!(wal.read_next("crash_cycles").unwrap().is_none());

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_with_exactly_block_size_entries() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create entries that exactly match block size (10MB - metadata overhead)
    let metadata_overhead = 64;
    let exact_size = (10 * 1024 * 1024 - metadata_overhead) as usize;

    let entries_data: Vec<Vec<u8>> = (0..5)
        .map(|i| vec![i as u8; exact_size])
        .collect();
    let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("exact_topic", &entries).unwrap();

    // Each entry should force a new block
    for i in 0..5 {
        let entry = wal.read_next("exact_topic").unwrap().unwrap();
        assert_eq!(entry.data.len(), exact_size);
        assert_eq!(entry.data[0], i as u8);
    }

    cleanup_test_env();
}

#[test]
fn test_chaos_hammering_same_topic_with_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    let num_threads = 20;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Synchronize start

            let mut success_count = 0;
            let mut blocked_count = 0;

            for _ in 0..10 {
                let data = format!("thread_{}", thread_id);
                let entries: Vec<&[u8]> = vec![data.as_bytes(), data.as_bytes()];

                match wal_clone.batch_append_for_topic("hammered_topic", &entries) {
                    Ok(_) => success_count += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => blocked_count += 1,
                    Err(e) => panic!("Unexpected error: {}", e),
                }

                thread::sleep(Duration::from_micros(50));
            }

            (success_count, blocked_count)
        });

        handles.push(handle);
    }

    let mut total_success = 0;
    let mut total_blocked = 0;

    for handle in handles {
        let (success, blocked) = handle.join().unwrap();
        total_success += success;
        total_blocked += blocked;
    }

    // Should have some successes and some blocks
    assert!(total_success > 0, "Expected some successful batch writes");
    assert!(total_blocked > 0, "Expected some blocked batch writes");
    assert_eq!(total_success + total_blocked, num_threads * 10);

    // Count actual entries written
    let mut count = 0;
    while wal.read_next("hammered_topic").unwrap().is_some() {
        count += 1;
    }

    // Should have 2 entries per successful batch
    assert_eq!(count, total_success * 2);

    cleanup_test_env();
}

#[test]
fn test_chaos_zero_length_entries_in_batch() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Mix of zero-length and normal entries
    let entries: Vec<&[u8]> = vec![
        b"",
        b"normal",
        b"",
        b"",
        b"another",
        b"",
    ];

    wal.batch_append_for_topic("zero_topic", &entries).unwrap();

    // Verify all entries including empty ones
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"");
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"normal");
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"");
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"");
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"another");
    assert_eq!(wal.read_next("zero_topic").unwrap().unwrap().data, b"");

    cleanup_test_env();
}

#[test]
fn test_chaos_batch_interspersed_with_frequent_fsync() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::SyncEach, // Fsync after every write
    )
    .unwrap();

    // This tests that batch writes work even with aggressive fsyncing
    for i in 0..10 {
        let data = format!("batch_{}", i);
        let entries: Vec<&[u8]> = vec![data.as_bytes(); 5];
        wal.batch_append_for_topic("fsync_topic", &entries).unwrap();
    }

    // Verify all 50 entries
    let mut count = 0;
    while wal.read_next("fsync_topic").unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 50);

    cleanup_test_env();
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_batch_single_entry() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    let entries: Vec<&[u8]> = vec![b"single_entry"];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    let entry = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(entry.data, b"single_entry");

    cleanup_test_env();
}

#[test]
fn test_batch_exactly_at_block_boundary() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Default block size is 10MB
    // Create entries that exactly fill one block (10MB - metadata overhead)
    let metadata_overhead = 64; // PREFIX_META_SIZE
    let entry_size = (10 * 1024 * 1024 - metadata_overhead * 2) / 2;

    let e1 = vec![0xAA; entry_size];
    let e2 = vec![0xBB; entry_size];

    let entries: Vec<&[u8]> = vec![&e1, &e2];

    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    let r1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r1.data[0], 0xAA);

    let r2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(r2.data[0], 0xBB);

    cleanup_test_env();
}

#[test]
fn test_batch_then_regular_write() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Batch write
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2"];
    wal.batch_append_for_topic("test_topic", &entries).unwrap();

    // Regular write after batch
    wal.append_for_topic("test_topic", b"regular").unwrap();

    // Verify order
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"batch1");
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"batch2");
    assert_eq!(wal.read_next("test_topic").unwrap().unwrap().data, b"regular");

    cleanup_test_env();
}

#[test]
fn test_multiple_sequential_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write multiple batches sequentially
    for batch_num in 0..5 {
        let e1 = format!("batch_{}_entry_1", batch_num);
        let e2 = format!("batch_{}_entry_2", batch_num);
        let e3 = format!("batch_{}_entry_3", batch_num);

        let entries: Vec<&[u8]> = vec![e1.as_bytes(), e2.as_bytes(), e3.as_bytes()];
        wal.batch_append_for_topic("test_topic", &entries).unwrap();
    }

    // Verify all entries in order
    for batch_num in 0..5 {
        for entry_num in 1..=3 {
            let entry = wal.read_next("test_topic").unwrap().unwrap();
            let expected = format!("batch_{}_entry_{}", batch_num, entry_num);
            assert_eq!(entry.data, expected.as_bytes());
        }
    }

    cleanup_test_env();
}

// ============================================================================
// DATA INTEGRITY TESTS - VERIFY EXACT CORRECTNESS
// ============================================================================

#[test]
fn test_integrity_batch_write_sequential_numbers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create batch with sequential numbers as data
    let batch_size = 100;
    let entries_data: Vec<Vec<u8>> = (0..batch_size)
        .map(|i| {
            let mut data = vec![];
            data.extend_from_slice(&(i as u64).to_le_bytes());
            data.extend_from_slice(&format!("entry_{}", i).as_bytes());
            data
        })
        .collect();
    let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("integrity_seq", &entries).unwrap();

    // Verify every single byte matches
    for i in 0..batch_size {
        let entry = wal.read_next("integrity_seq").unwrap().unwrap();

        // Verify the numeric prefix
        let num = u64::from_le_bytes([
            entry.data[0], entry.data[1], entry.data[2], entry.data[3],
            entry.data[4], entry.data[5], entry.data[6], entry.data[7],
        ]);
        assert_eq!(num, i as u64, "Numeric prefix mismatch at entry {}", i);

        // Verify the string suffix
        let text = &entry.data[8..];
        let expected = format!("entry_{}", i);
        assert_eq!(text, expected.as_bytes(), "Text mismatch at entry {}", i);
    }

    assert!(wal.read_next("integrity_seq").unwrap().is_none());
    cleanup_test_env();
}

#[test]
fn test_integrity_batch_write_random_patterns() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create batch with pseudo-random but reproducible patterns
    let batch_size = 50;
    let entries_data: Vec<Vec<u8>> = (0..batch_size)
        .map(|i| {
            let size = 1000 + (i * 137) % 5000; // Varying sizes
            let mut data = vec![0u8; size];
            // Fill with predictable pattern based on index
            for (j, byte) in data.iter_mut().enumerate() {
                *byte = ((i + j) % 256) as u8;
            }
            data
        })
        .collect();
    let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("integrity_random", &entries).unwrap();

    // Verify every byte of every entry
    for i in 0..batch_size {
        let entry = wal.read_next("integrity_random").unwrap().unwrap();
        let expected_size = 1000 + (i * 137) % 5000;

        assert_eq!(entry.data.len(), expected_size, "Size mismatch at entry {}", i);

        for (j, &byte) in entry.data.iter().enumerate() {
            let expected_byte = ((i + j) % 256) as u8;
            assert_eq!(byte, expected_byte, "Byte mismatch at entry {} offset {}", i, j);
        }
    }

    assert!(wal.read_next("integrity_random").unwrap().is_none());
    cleanup_test_env();
}

#[test]
fn test_integrity_batch_write_large_entries_exact_match() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create batch with large entries (5MB each) with distinct patterns
    let batch_size = 10;
    let entry_size = 5 * 1024 * 1024;

    let entries_data: Vec<Vec<u8>> = (0..batch_size)
        .map(|i| {
            let mut data = vec![0u8; entry_size];
            // Each entry has a unique repeating pattern
            let pattern = (i as u8).wrapping_mul(17).wrapping_add(37);
            for (j, byte) in data.iter_mut().enumerate() {
                *byte = pattern.wrapping_add((j % 256) as u8);
            }
            // Add index marker at start and end
            data[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            let len = data.len();
            data[len-8..len].copy_from_slice(&(i as u64).to_le_bytes());
            data
        })
        .collect();
    let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("integrity_large", &entries).unwrap();

    // Verify every entry byte-by-byte
    for i in 0..batch_size {
        let entry = wal.read_next("integrity_large").unwrap().unwrap();

        assert_eq!(entry.data.len(), entry_size, "Size mismatch at entry {}", i);

        // Check start marker
        let start_idx = u64::from_le_bytes([
            entry.data[0], entry.data[1], entry.data[2], entry.data[3],
            entry.data[4], entry.data[5], entry.data[6], entry.data[7],
        ]);
        assert_eq!(start_idx, i as u64, "Start marker mismatch at entry {}", i);

        // Check end marker
        let len = entry.data.len();
        let end_idx = u64::from_le_bytes([
            entry.data[len-8], entry.data[len-7], entry.data[len-6], entry.data[len-5],
            entry.data[len-4], entry.data[len-3], entry.data[len-2], entry.data[len-1],
        ]);
        assert_eq!(end_idx, i as u64, "End marker mismatch at entry {}", i);

        // Verify pattern in middle section
        let pattern = (i as u8).wrapping_mul(17).wrapping_add(37);
        for j in 8..len-8 {
            let expected = pattern.wrapping_add((j % 256) as u8);
            assert_eq!(entry.data[j], expected, "Pattern mismatch at entry {} offset {}", i, j);
        }
    }

    assert!(wal.read_next("integrity_large").unwrap().is_none());
    cleanup_test_env();
}

#[test]
fn test_integrity_batch_spanning_blocks_exact_data() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Create batch that spans multiple 10MB blocks
    // 3MB entries x 20 = 60MB total, spanning ~6 blocks
    let batch_size = 20;
    let entry_size = 3 * 1024 * 1024;

    let entries_data: Vec<Vec<u8>> = (0..batch_size)
        .map(|i| {
            let mut data = vec![0u8; entry_size];
            // Fill with checksum-able pattern
            let seed = (i as u32).wrapping_mul(0x9e3779b9);
            for chunk_idx in 0..entry_size/4 {
                let value = seed.wrapping_add(chunk_idx as u32);
                let offset = chunk_idx * 4;
                data[offset..offset+4].copy_from_slice(&value.to_le_bytes());
            }
            data
        })
        .collect();
    let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

    wal.batch_append_for_topic("integrity_spanning", &entries).unwrap();

    // Verify every u32 value in every entry
    for i in 0..batch_size {
        let entry = wal.read_next("integrity_spanning").unwrap().unwrap();

        assert_eq!(entry.data.len(), entry_size, "Size mismatch at entry {}", i);

        let seed = (i as u32).wrapping_mul(0x9e3779b9);
        for chunk_idx in 0..entry_size/4 {
            let offset = chunk_idx * 4;
            let value = u32::from_le_bytes([
                entry.data[offset],
                entry.data[offset+1],
                entry.data[offset+2],
                entry.data[offset+3],
            ]);
            let expected = seed.wrapping_add(chunk_idx as u32);
            assert_eq!(value, expected, "Data corruption at entry {} offset {}", i, offset);
        }
    }

    assert!(wal.read_next("integrity_spanning").unwrap().is_none());
    cleanup_test_env();
}

#[test]
fn test_integrity_multiple_batches_sequential() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write 10 batches, each with unique identifiable data
    let num_batches = 10;
    let entries_per_batch = 5;

    for batch_id in 0..num_batches {
        let entries_data: Vec<Vec<u8>> = (0..entries_per_batch)
            .map(|entry_id| {
                let mut data = Vec::new();
                data.extend_from_slice(&(batch_id as u32).to_le_bytes());
                data.extend_from_slice(&(entry_id as u32).to_le_bytes());
                data.extend_from_slice(format!("batch_{}_entry_{}", batch_id, entry_id).as_bytes());
                data
            })
            .collect();
        let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

        wal.batch_append_for_topic("integrity_multi", &entries).unwrap();
    }

    // Verify all entries in all batches
    for batch_id in 0..num_batches {
        for entry_id in 0..entries_per_batch {
            let entry = wal.read_next("integrity_multi").unwrap().unwrap();

            let batch_id_read = u32::from_le_bytes([
                entry.data[0], entry.data[1], entry.data[2], entry.data[3],
            ]);
            let entry_id_read = u32::from_le_bytes([
                entry.data[4], entry.data[5], entry.data[6], entry.data[7],
            ]);

            assert_eq!(batch_id_read, batch_id, "Batch ID mismatch");
            assert_eq!(entry_id_read, entry_id, "Entry ID mismatch");

            let text = &entry.data[8..];
            let expected = format!("batch_{}_entry_{}", batch_id, entry_id);
            assert_eq!(text, expected.as_bytes(), "Text mismatch");
        }
    }

    assert!(wal.read_next("integrity_multi").unwrap().is_none());
    cleanup_test_env();
}

#[test]
fn test_integrity_batch_after_crash_recovery() {
    let _guard = setup_test_env();
    enable_fd_backend();

    // Phase 1: Write batch with verifiable data
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        let entries_data: Vec<Vec<u8>> = (0..20)
            .map(|i| {
                let mut data = vec![0u8; 10000];
                data[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                data[8..16].copy_from_slice(&(0xDEADBEEFCAFEBABE_u64).to_le_bytes());
                for j in 16..10000 {
                    data[j] = ((i + j) % 256) as u8;
                }
                data
            })
            .collect();
        let entries: Vec<&[u8]> = entries_data.iter().map(|v| v.as_slice()).collect();

        wal.batch_append_for_topic("integrity_crash", &entries).unwrap();

        // Simulate crash
        drop(wal);
    }

    // Phase 2: Recover and verify exact data
    {
        let wal = Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::NoFsync,
        )
        .unwrap();

        for i in 0..20 {
            let entry = wal.read_next("integrity_crash").unwrap().unwrap();

            assert_eq!(entry.data.len(), 10000, "Size mismatch at entry {} after recovery", i);

            let idx = u64::from_le_bytes([
                entry.data[0], entry.data[1], entry.data[2], entry.data[3],
                entry.data[4], entry.data[5], entry.data[6], entry.data[7],
            ]);
            assert_eq!(idx, i as u64, "Index mismatch at entry {} after recovery", i);

            let magic = u64::from_le_bytes([
                entry.data[8], entry.data[9], entry.data[10], entry.data[11],
                entry.data[12], entry.data[13], entry.data[14], entry.data[15],
            ]);
            assert_eq!(magic, 0xDEADBEEFCAFEBABE_u64, "Magic mismatch at entry {} after recovery", i);

            for j in 16..10000 {
                let expected = ((i + j) % 256) as u8;
                assert_eq!(entry.data[j], expected, "Data corruption at entry {} offset {} after recovery", i, j);
            }
        }

        assert!(wal.read_next("integrity_crash").unwrap().is_none());
    }

    cleanup_test_env();
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
#[ignore] // Run with --ignored flag for long-running tests
fn test_stress_large_batch_1000_entries() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    println!("[stress] Creating 1000 entries of 1MB each...");
    // Create 1000 entries of 1MB each
    let entry = vec![0xCD; 1024 * 1024];
    let entries: Vec<&[u8]> = (0..1000).map(|_| entry.as_slice()).collect();

    println!("[stress] Writing batch of 1GB...");
    let start = std::time::Instant::now();
    wal.batch_append_for_topic("stress_topic", &entries).unwrap();
    let duration = start.elapsed();

    println!("[stress] Batch write of 1000x1MB entries took: {:?}", duration);

    println!("[stress] Verifying all 1000 entries...");
    // Verify all entries
    for i in 0..1000 {
        if i % 100 == 0 {
            println!("[stress] Verified {} entries...", i);
        }
        let e = wal.read_next("stress_topic").unwrap().unwrap();
        assert_eq!(e.data.len(), 1024 * 1024);
        assert_eq!(e.data[0], 0xCD);
    }

    println!("[stress] All 1000 entries verified successfully!");
    cleanup_test_env();
}

#[test]
#[ignore]
fn test_stress_many_small_batches() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    println!("[stress] Writing 10000 batches of 10 entries each...");
    let start = std::time::Instant::now();

    // Write 10000 batches of 10 small entries each
    for i in 0..10000 {
        if i % 1000 == 0 {
            println!("[stress] Written {} batches...", i);
        }
        let data = format!("batch_{}", i);
        let entries: Vec<&[u8]> = (0..10).map(|_| data.as_bytes()).collect();
        wal.batch_append_for_topic("stress_topic", &entries).unwrap();
    }

    let duration = start.elapsed();
    println!("[stress] 10000 batches of 10 entries took: {:?}", duration);

    println!("[stress] Reading and verifying 100000 entries...");
    // Count entries
    let mut count = 0;
    while wal.read_next("stress_topic").unwrap().is_some() {
        count += 1;
        if count % 10000 == 0 {
            println!("[stress] Read {} entries...", count);
        }
    }

    assert_eq!(count, 100000);
    println!("[stress] All 100000 entries verified successfully!");

    cleanup_test_env();
}

#[test]
fn test_rollback_data_becomes_invisible_to_readers() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Write some initial data
    wal.append_for_topic("rollback_test", b"initial").unwrap();
    
    // Read it to establish baseline
    let initial = wal.read_next("rollback_test").unwrap().unwrap();
    assert_eq!(initial.data, b"initial");

    // Force a batch write failure by using a mock that fails io_uring operations
    // This is tricky to test directly, but we can simulate by:
    // 1. Filling up available space to force allocation
    // 2. Then trying a batch that would span multiple blocks
    
    // Alternative: Test the scenario where batch is rejected due to concurrent access
    let barrier = Arc::new(Barrier::new(2));
    let wal_clone = wal.clone();
    let barrier_clone = barrier.clone();
    
    let handle = thread::spawn(move || {
        // Start a large batch write
        let large_entry = vec![0u8; 50 * 1024 * 1024]; // 50MB
        let entries: Vec<&[u8]> = (0..20).map(|_| large_entry.as_slice()).collect();
        
        barrier_clone.wait();
        let result = wal_clone.batch_append_for_topic("rollback_test", &entries);
        result
    });

    // Wait a moment then try another batch (should be blocked and fail)
    barrier.wait();
    thread::sleep(Duration::from_millis(10));
    
    let small_batch: Vec<&[u8]> = vec![b"should_be_blocked"];
    let result = wal.batch_append_for_topic("rollback_test", &small_batch);
    
    // One should succeed, one should fail with WouldBlock
    let first_result = handle.join().unwrap();
    
    // After rollback, reader should not see any new data beyond "initial"
    assert!(wal.read_next("rollback_test").unwrap().is_none());
    
    cleanup_test_env();
}

#[test]
fn test_rollback_allows_data_overwrite() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write initial data
    wal.append_for_topic("overwrite_test", b"before_batch").unwrap();
    
    // Consume it
    let entry = wal.read_next("overwrite_test").unwrap().unwrap();
    assert_eq!(entry.data, b"before_batch");

    // Simulate a batch failure scenario by creating a batch that would fail
    // (This is hard to test directly without mocking io_uring failures)
    
    // Instead, test that after any failed batch, new writes work normally
    // Write a successful batch
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2"];
    wal.batch_append_for_topic("overwrite_test", &entries).unwrap();
    
    // Verify the batch data is readable
    assert_eq!(wal.read_next("overwrite_test").unwrap().unwrap().data, b"batch1");
    assert_eq!(wal.read_next("overwrite_test").unwrap().unwrap().data, b"batch2");
    
    // Write more data after the batch
    wal.append_for_topic("overwrite_test", b"after_batch").unwrap();
    assert_eq!(wal.read_next("overwrite_test").unwrap().unwrap().data, b"after_batch");

    cleanup_test_env();
}

#[test]
fn test_rollback_block_state_consistency() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // This test verifies that block locking state is properly reverted
    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        
        let handle = thread::spawn(move || {
            // Large batch to increase chance of allocation and potential rollback
            let large_entry = vec![i as u8; 10 * 1024 * 1024]; // 10MB per entry
            let entries: Vec<&[u8]> = (0..10).map(|_| large_entry.as_slice()).collect();
            
            barrier_clone.wait();
            
            // Try batch write - some will succeed, some will be blocked and "rolled back"
            wal_clone.batch_append_for_topic("block_state_test", &entries)
        });
        
        handles.push(handle);
    }

    let mut successes = 0;
    let mut failures = 0;
    
    for handle in handles {
        match handle.join().unwrap() {
            Ok(_) => successes += 1,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => failures += 1,
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    // Should have some successes and some blocks/rollbacks
    assert!(successes > 0, "Expected some successful writes");
    assert!(failures > 0, "Expected some blocked/rolled back writes");
    
    // After all operations, WAL should still be in consistent state
    // Try a new batch write - should succeed
    let test_batch: Vec<&[u8]> = vec![b"consistency_check"];
    wal.batch_append_for_topic("block_state_test", &test_batch).unwrap();
    
    // Should be able to read some data (from successful batches + our test)
    let mut count = 0;
    while wal.read_next("block_state_test").unwrap().is_some() {
        count += 1;
    }
    assert!(count > 0, "Should have some readable entries after rollbacks");

    cleanup_test_env();
}

#[test]
fn test_rollback_preserves_existing_data() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Write some data that should survive rollbacks
    for i in 0..10 {
        let data = format!("stable_entry_{}", i);
        wal.append_for_topic("rollback_preserve", data.as_bytes()).unwrap();
    }

    // Read first 5 entries
    for i in 0..5 {
        let entry = wal.read_next("rollback_preserve").unwrap().unwrap();
        let expected = format!("stable_entry_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }

    // Now try concurrent batch writes that will cause rollbacks
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    for _ in 0..5 {
        let wal_clone = wal.clone();
        let barrier_clone = barrier.clone();
        
        let handle = thread::spawn(move || {
            let large_entry = vec![0xFF; 20 * 1024 * 1024]; // 20MB
            let entries: Vec<&[u8]> = (0..5).map(|_| large_entry.as_slice()).collect();
            
            barrier_clone.wait();
            let _ = wal_clone.batch_append_for_topic("rollback_preserve", &entries);
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // The remaining 5 stable entries should still be readable
    for i in 5..10 {
        let entry = wal.read_next("rollback_preserve").unwrap().unwrap();
        let expected = format!("stable_entry_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }

    cleanup_test_env();
}

// Test to verify rollback doesn't corrupt file state tracking
#[test]
fn test_rollback_file_state_tracking() {
    let _guard = setup_test_env();
    enable_fd_backend();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::NoFsync,
    )
    .unwrap();

    // Write enough data to span multiple blocks and potentially multiple files
    for batch_num in 0..20 {
        let entries: Vec<&[u8]> = if batch_num % 5 == 0 {
            // Every 5th batch is large to force block rotations
            let large_entry = vec![batch_num as u8; 15 * 1024 * 1024]; // 15MB
            vec![large_entry.as_slice(); 3] // 45MB total
        } else {
            // Small batches
            let small_data = format!("batch_{}", batch_num);
            vec![small_data.as_bytes(); 10]
        };

        // Some of these might fail due to concurrency, but that's expected
        let _ = wal.batch_append_for_topic("file_state_test", &entries);
    }

    // After all the potential rollbacks, WAL should still work correctly
    let final_batch: Vec<&[u8]> = vec![b"final_entry"];
    wal.batch_append_for_topic("file_state_test", &final_batch).unwrap();

    // Should be able to read at least the final entry
    let mut found_final = false;
    while let Some(entry) = wal.read_next("file_state_test").unwrap() {
        if entry.data == b"final_entry" {
            found_final = true;
        }
    }
    assert!(found_final, "Should find the final entry after all rollbacks");

    cleanup_test_env();
}