mod common;

use common::{TestEnv, current_wal_dir, sanitize_key, wal_root_dir};
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use walrus_rust::wal::{FsyncSchedule, ReadConsistency, Walrus};

fn setup_env() -> TestEnv {
    let env = TestEnv::new();
    thread::sleep(Duration::from_millis(50));
    env
}

#[test]
fn test_strictly_at_once_consistency() {
    let _env = setup_env();
    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("test", b"msg1").unwrap();
    wal.append_for_topic("test", b"msg2").unwrap();

    let entry1 = wal.read_next("test", true).unwrap().unwrap();
    assert_eq!(entry1.data, b"msg1");

    drop(wal);

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    let wal2 = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let entry2 = wal2.read_next("test", true).unwrap().unwrap();
    assert_eq!(entry2.data, b"msg2");
}

#[test]
fn test_at_least_once_consistency() {
    let _env = setup_env();

    // Test that AtLeastOnce mode can be created successfully
    let _wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();
}

#[test]
fn test_fsync_schedule() {
    let _env = setup_env();
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000),
    )
    .unwrap();
    wal.append_for_topic("test", b"data").unwrap();
    let entry = wal.read_next("test", true).unwrap().unwrap();
    assert_eq!(entry.data, b"data");
}

#[test]
fn test_fsync_schedule_sync_each() {
    let _env = setup_env();
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::SyncEach,
    )
    .unwrap();

    // Test that SyncEach mode works correctly
    wal.append_for_topic("sync_each_test", b"msg1").unwrap();
    wal.append_for_topic("sync_each_test", b"msg2").unwrap();
    wal.append_for_topic("sync_each_test", b"msg3").unwrap();

    let entry1 = wal.read_next("sync_each_test", true).unwrap().unwrap();
    assert_eq!(entry1.data, b"msg1");

    let entry2 = wal.read_next("sync_each_test", true).unwrap().unwrap();
    assert_eq!(entry2.data, b"msg2");

    let entry3 = wal.read_next("sync_each_test", true).unwrap().unwrap();
    assert_eq!(entry3.data, b"msg3");

    // Verify no more entries
    assert!(wal.read_next("sync_each_test", true).unwrap().is_none());
}

#[test]
fn test_constructors() {
    let _env = setup_env();
    let wal1 = Walrus::new().unwrap();
    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 5 }).unwrap();
    let wal3 = Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 2 },
        FsyncSchedule::Milliseconds(3000),
    )
    .unwrap();

    wal1.append_for_topic("test", b"data1").unwrap();
    wal2.append_for_topic("test", b"data2").unwrap();
    wal3.append_for_topic("test", b"data3").unwrap();
}

#[test]
fn test_crash_recovery_strictly_at_once() {
    let _env = setup_env();

    {
        let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();

        for i in 1..=5 {
            let msg = format!("recovery_msg_{}", i);
            wal.append_for_topic("recovery_test", msg.as_bytes())
                .unwrap();
        }

        let entry1 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry1.data, b"recovery_msg_1");

        let entry2 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry2.data, b"recovery_msg_2");
    }

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    {
        let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();

        let entry3 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry3.data, b"recovery_msg_3");

        let entry4 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry4.data, b"recovery_msg_4");

        let entry5 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry5.data, b"recovery_msg_5");

        assert!(wal.read_next("recovery_test", true).unwrap().is_none());
    }
}

#[test]
fn test_crash_recovery_at_least_once() {
    let _env = setup_env();

    {
        let wal =
            Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();

        for i in 1..=7 {
            let msg = format!("at_least_once_msg_{}", i);
            wal.append_for_topic("recovery_test", msg.as_bytes())
                .unwrap();
        }

        let entry1 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry1.data, b"at_least_once_msg_1");

        let entry2 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry2.data, b"at_least_once_msg_2");
    }

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    {
        let wal =
            Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();

        let entry1 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry1.data, b"at_least_once_msg_1");

        let entry2 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry2.data, b"at_least_once_msg_2");

        let entry3 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry3.data, b"at_least_once_msg_3");
    }

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    {
        let wal =
            Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();

        let entry4 = wal.read_next("recovery_test", true).unwrap().unwrap();
        assert_eq!(entry4.data, b"at_least_once_msg_4");
    }
}

#[test]
fn test_multiple_topics_different_consistency_behavior() {
    let _env = setup_env();

    let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 2 }).unwrap();

    wal.append_for_topic("topic_a", b"a1").unwrap();
    wal.append_for_topic("topic_b", b"b1").unwrap();
    wal.append_for_topic("topic_a", b"a2").unwrap();
    wal.append_for_topic("topic_b", b"b2").unwrap();

    assert_eq!(wal.read_next("topic_a", true).unwrap().unwrap().data, b"a1");
    assert_eq!(wal.read_next("topic_b", true).unwrap().unwrap().data, b"b1");

    drop(wal);

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 2 }).unwrap();

    assert_eq!(
        wal2.read_next("topic_a", true).unwrap().unwrap().data,
        b"a1"
    );
    assert_eq!(
        wal2.read_next("topic_b", true).unwrap().unwrap().data,
        b"b1"
    );
}

#[test]
fn test_configuration_with_concurrent_operations() {
    let _env = setup_env();

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::Milliseconds(2000),
        )
        .unwrap(),
    );

    let wal_writer = Arc::clone(&wal);
    let writer_handle = thread::spawn(move || {
        for i in 0..10 {
            let msg = format!("concurrent_msg_{}", i);
            wal_writer
                .append_for_topic("concurrent", msg.as_bytes())
                .unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });

    thread::sleep(Duration::from_millis(50));

    let mut read_count = 0;
    let start_time = Instant::now();

    while start_time.elapsed() < Duration::from_millis(200) && read_count < 10 {
        if let Some(entry) = wal.read_next("concurrent", true).unwrap() {
            let expected = format!("concurrent_msg_{}", read_count);
            assert_eq!(entry.data, expected.as_bytes());
            read_count += 1;
        }
        thread::sleep(Duration::from_millis(10));
    }

    writer_handle.join().unwrap();

    while let Some(entry) = wal.read_next("concurrent", true).unwrap() {
        let expected = format!("concurrent_msg_{}", read_count);
        assert_eq!(entry.data, expected.as_bytes());
        read_count += 1;
        if read_count >= 10 {
            break;
        }
    }

    assert_eq!(read_count, 10);
}

#[test]
fn test_persist_every_zero_clamping() {
    let _env = setup_env();

    let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 0 }).unwrap();

    wal.append_for_topic("test", b"msg1").unwrap();
    wal.append_for_topic("test", b"msg2").unwrap();

    let entry1 = wal.read_next("test", true).unwrap().unwrap();
    assert_eq!(entry1.data, b"msg1");

    drop(wal);

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 0 }).unwrap();

    let entry2 = wal2.read_next("test", true).unwrap().unwrap();
    assert_eq!(entry2.data, b"msg2");
}

#[test]
fn test_log_file_deletion_with_fast_fsync() {
    let _env = setup_env();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1), // Ultra-fast 1ms fsync
    )
    .unwrap();

    test_println!("Creating first 999MB entry...");
    let large_data_1 = vec![0xAA; 999 * 1024 * 1024]; // 999MB of 0xAA bytes
    wal.append_for_topic("deletion_test", &large_data_1)
        .unwrap();

    test_println!("Creating second 999MB entry...");
    let large_data_2 = vec![0xBB; 999 * 1024 * 1024]; // 999MB of 0xBB bytes
    wal.append_for_topic("deletion_test", &large_data_2)
        .unwrap();

    let wal_dir = current_wal_dir();
    let files_after_writes = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();

    test_println!(
        "Files after writing 2x999MB entries: {}",
        files_after_writes.len()
    );
    for file in &files_after_writes {
        test_println!("  File: {:?}", file.file_name());
    }

    assert!(
        files_after_writes.len() >= 2,
        "Should have at least 2 files after writing 2x999MB entries"
    );

    test_println!("Reading first entry (999MB)...");
    let entry1 = wal.read_next("deletion_test", true).unwrap().unwrap();
    assert_eq!(entry1.data.len(), 999 * 1024 * 1024);
    assert_eq!(entry1.data[0], 0xAA); // Verify it's the first entry

    test_println!("First entry read successfully. Waiting for file cleanup...");

    test_println!("Waiting 60 seconds for background deletion to process...");

    for i in 1..=12 {
        thread::sleep(Duration::from_secs(5));
        let wal_dir = current_wal_dir();
        let current_files = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let binding = entry.file_name();
                let name = binding.to_string_lossy();
                !name.ends_with("_index.db") && !name.ends_with(".tmp")
            })
            .count();
        test_println!("After {} seconds: {} files remaining", i * 5, current_files);

        if current_files < files_after_writes.len() {
            test_println!("FILE DELETION DETECTED at {} seconds!", i * 5);
            break;
        }
    }

    let wal_dir = current_wal_dir();
    let files_after_read = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();

    test_println!(
        "Files after reading first entry and waiting: {}",
        files_after_read.len()
    );
    for file in &files_after_read {
        test_println!("  File: {:?}", file.file_name());
    }

    if files_after_read.len() < files_after_writes.len() {
        test_println!(
            "SUCCESS: File deletion occurred! {} -> {} files",
            files_after_writes.len(),
            files_after_read.len()
        );
    } else {
        test_println!(
            "INFO: Files still present, deletion may require more time or different conditions"
        );
    }

    test_println!("Reading second entry to verify WAL integrity...");
    let entry2 = wal.read_next("deletion_test", true).unwrap().unwrap();
    assert_eq!(entry2.data.len(), 999 * 1024 * 1024);
    assert_eq!(entry2.data[0], 0xBB); // Verify it's the second entry

    test_println!("Test completed successfully!");
}

#[test]
fn test_log_file_deletion_with_large_data() {
    let _env = setup_env();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000),
    )
    .unwrap();

    let data_size = 1024; // 1KB per entry
    let num_entries = 1000; // 1MB total

    for i in 0..num_entries {
        let data = format!("large_test_entry_{:04}_", i).repeat(data_size / 20);
        wal.append_for_topic("large_deletion_test", data.as_bytes())
            .unwrap();
    }

    for i in 0..num_entries {
        let entry = wal.read_next("large_deletion_test", true).unwrap().unwrap();
        let expected_prefix = format!("large_test_entry_{:04}_", i);
        let entry_str = String::from_utf8_lossy(&entry.data);
        assert!(
            entry_str.starts_with(&expected_prefix),
            "Entry {} doesn't start with expected prefix",
            i
        );
    }

    assert!(
        wal.read_next("large_deletion_test", true)
            .unwrap()
            .is_none()
    );

    let wal_dir = current_wal_dir();
    let files_before = if wal_dir.exists() {
        std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let binding = entry.file_name();
                let name = binding.to_string_lossy();
                !name.ends_with("_index.db") && !name.ends_with(".tmp")
            })
            .count()
    } else {
        0
    };

    test_println!("Large data test - Files before: {}", files_before);

    drop(wal);
    thread::sleep(Duration::from_secs(3));

    let wal_dir = current_wal_dir();
    let files_after = if wal_dir.exists() {
        std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let binding = entry.file_name();
                let name = binding.to_string_lossy();
                !name.ends_with("_index.db") && !name.ends_with(".tmp")
            })
            .count()
    } else {
        0
    };

    test_println!("Large data test - Files after: {}", files_after);
}

#[test]
fn test_file_state_tracking() {
    let _env = setup_env();

    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000),
    )
    .unwrap();

    for i in 0..50 {
        let msg = format!("state_tracking_msg_{}", i);
        wal.append_for_topic("state_test", msg.as_bytes()).unwrap();
    }

    let wal_dir = current_wal_dir();
    assert!(wal_dir.exists());
    let files_exist = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        });
    assert!(files_exist, "Should have created log files");

    for i in 0..25 {
        let entry = wal.read_next("state_test", true).unwrap().unwrap();
        let expected = format!("state_tracking_msg_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }

    let files_still_exist = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        });
    assert!(
        files_still_exist,
        "Files should still exist with unread data"
    );

    for i in 25..50 {
        let entry = wal.read_next("state_test", true).unwrap().unwrap();
        let expected = format!("state_tracking_msg_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }

    assert!(wal.read_next("state_test", true).unwrap().is_none());
}

#[test]
fn key_based_instances_use_isolated_directories() {
    let env = setup_env();
    let tx_key = env.unique_key("transactions");
    let analytics_key = env.unique_key("analytics");

    {
        let wal = Walrus::with_consistency_for_key(&tx_key, ReadConsistency::StrictlyAtOnce)
            .unwrap();
        wal.append_for_topic("tx", b"txn-1").unwrap();
    }

    {
        let wal = Walrus::with_consistency_for_key(&analytics_key, ReadConsistency::StrictlyAtOnce)
            .unwrap();
        wal.append_for_topic("events", b"evt-1").unwrap();
    }

    let mut dir_names: Vec<_> = fs::read_dir(wal_root_dir())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    dir_names.sort();

    assert!(
        dir_names.contains(&sanitize_key(&analytics_key)),
        "expected analytics namespace directory to exist"
    );
    assert!(
        dir_names.contains(&sanitize_key(&tx_key)),
        "expected transactions namespace directory to exist"
    );
}

#[test]
fn key_based_instances_recover_independently() {
    let env = setup_env();
    let tx_key = env.unique_key("transactions");
    let analytics_key = env.unique_key("analytics");

    {
        let wal = Walrus::with_consistency_for_key(&tx_key, ReadConsistency::StrictlyAtOnce)
            .unwrap();
        wal.append_for_topic("tx", b"a").unwrap();
        wal.append_for_topic("tx", b"b").unwrap();
    }

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    {
        let wal = Walrus::with_consistency_for_key(&analytics_key, ReadConsistency::StrictlyAtOnce)
            .unwrap();
        wal.append_for_topic("events", b"x").unwrap();
    }

    // Small delay to allow Linux kernel dcache/io_uring cleanup to complete
    thread::sleep(Duration::from_millis(50));

    let wal_tx =
        Walrus::with_consistency_for_key(&tx_key, ReadConsistency::StrictlyAtOnce).unwrap();
    assert_eq!(wal_tx.read_next("tx", true).unwrap().unwrap().data, b"a");
    assert_eq!(wal_tx.read_next("tx", true).unwrap().unwrap().data, b"b");
    assert!(wal_tx.read_next("tx", true).unwrap().is_none());

    let wal_an =
        Walrus::with_consistency_for_key(&analytics_key, ReadConsistency::StrictlyAtOnce).unwrap();
    assert!(wal_an.read_next("tx", true).unwrap().is_none());
    assert_eq!(
        wal_an.read_next("events", true).unwrap().unwrap().data,
        b"x"
    );
    assert!(wal_an.read_next("events", true).unwrap().is_none());
}

#[test]
fn key_names_are_sanitized_for_directories() {
    let _env = setup_env();
    let key = "prod/payments::v1";

    {
        let wal = Walrus::with_consistency_for_key(key, ReadConsistency::StrictlyAtOnce).unwrap();
        wal.append_for_topic("topic", b"payload").unwrap();
    }

    let expected_dir = wal_root_dir().join(sanitize_key(key));
    assert!(
        expected_dir.is_dir(),
        "expected namespace directory {:?} to exist",
        expected_dir
    );
}
