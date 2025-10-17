use walrus_rust::{enable_fd_backend, FsyncSchedule, ReadConsistency, Walrus};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

// Global lock to serialize tests (prevents parallel test interference)
static TEST_LOCK: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

fn setup_test_env() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap();

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

    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 1 },
        FsyncSchedule::NoFsync,
    )
    .unwrap());

    // Pre-populate with some entries
    for i in 0..10 {
        let data = format!("pre_{}", i);
        wal.append_for_topic("positioned_topic", data.as_bytes()).unwrap();
    }

    // Position readers at different offsets
    let reader1_wal = wal.clone();
    let reader2_wal = wal.clone();
    let reader3_wal = wal.clone();

    // Reader 1: reads 0 entries (at beginning)
    // Reader 2: reads 5 entries (in middle)
    for _ in 0..5 {
        reader2_wal.read_next("positioned_topic").unwrap();
    }
    // Reader 3: reads all 10 entries (at end)
    for _ in 0..10 {
        reader3_wal.read_next("positioned_topic").unwrap();
    }

    // Now write a batch
    let batch: Vec<&[u8]> = vec![b"batch_1", b"batch_2", b"batch_3"];
    wal.batch_append_for_topic("positioned_topic", &batch).unwrap();

    // Reader 1 should see all 10 pre + 3 batch = 13 entries
    let mut r1_count = 0;
    while reader1_wal.read_next("positioned_topic").unwrap().is_some() {
        r1_count += 1;
    }
    assert_eq!(r1_count, 13);

    // Reader 2 should see 5 pre + 3 batch = 8 entries
    let mut r2_count = 0;
    while reader2_wal.read_next("positioned_topic").unwrap().is_some() {
        r2_count += 1;
    }
    assert_eq!(r2_count, 8);

    // Reader 3 should see only 3 batch entries
    let mut r3_count = 0;
    while reader3_wal.read_next("positioned_topic").unwrap().is_some() {
        r3_count += 1;
    }
    assert_eq!(r3_count, 3);

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
