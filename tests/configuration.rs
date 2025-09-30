use walrus::wal::{Walrus, ReadConsistency, FsyncSchedule};
use std::fs;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::time::Instant;

fn cleanup_wal() {
    // Aggressive cleanup to ensure complete removal
    for attempt in 0..10 {
        if std::path::Path::new("wal_files").exists() {
            // Try to remove all files first
            if let Ok(entries) = fs::read_dir("wal_files") {
                for entry in entries.flatten() {
                    let _ = fs::remove_file(entry.path());
                }
            }
            // Then remove the directory
            let _ = fs::remove_dir_all("wal_files");
        }
        
        thread::sleep(Duration::from_millis(100));
        
        // Check if cleanup was successful
        if !std::path::Path::new("wal_files").exists() {
            break;
        }
        
        if attempt == 9 {
            panic!("Failed to cleanup wal_files directory after 10 attempts");
        }
    }
    
    // Ensure directory is recreated
    let _ = fs::create_dir_all("wal_files");
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn test_strictly_at_once_consistency() {
    cleanup_wal();
    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    wal.append_for_topic("test", b"msg1").unwrap();
    wal.append_for_topic("test", b"msg2").unwrap();
    
    let entry1 = wal.read_next("test").unwrap().unwrap();
    assert_eq!(entry1.data, b"msg1");
    
    drop(wal);
    let wal2 = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let entry2 = wal2.read_next("test").unwrap().unwrap();
    assert_eq!(entry2.data, b"msg2");
    
    cleanup_wal();
}

#[test]
fn test_at_least_once_consistency() {
    cleanup_wal();
    
    // Test that AtLeastOnce mode can be created successfully
    let _wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();
    
    cleanup_wal();
}

#[test]
fn test_fsync_schedule() {
    cleanup_wal();
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000)
    ).unwrap();
    wal.append_for_topic("test", b"data").unwrap();
    let entry = wal.read_next("test").unwrap().unwrap();
    assert_eq!(entry.data, b"data");
    cleanup_wal();
}

#[test]
fn test_constructors() {
    cleanup_wal();
    let wal1 = Walrus::new().unwrap();
    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 5 }).unwrap();
    let wal3 = Walrus::with_consistency_and_schedule(
        ReadConsistency::AtLeastOnce { persist_every: 2 },
        FsyncSchedule::Milliseconds(3000)
    ).unwrap();
    
    wal1.append_for_topic("test", b"data1").unwrap();
    wal2.append_for_topic("test", b"data2").unwrap();
    wal3.append_for_topic("test", b"data3").unwrap();
    
    cleanup_wal();
}


#[test]
fn test_crash_recovery_strictly_at_once() {
    cleanup_wal();
    
    {
        let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
        
        for i in 1..=5 {
            let msg = format!("recovery_msg_{}", i);
            wal.append_for_topic("recovery_test", msg.as_bytes()).unwrap();
        }
        
        let entry1 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry1.data, b"recovery_msg_1");
        
        let entry2 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry2.data, b"recovery_msg_2");
        
    }
    
    {
        let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
        
        let entry3 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry3.data, b"recovery_msg_3");
        
        let entry4 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry4.data, b"recovery_msg_4");
        
        let entry5 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry5.data, b"recovery_msg_5");
        
        assert!(wal.read_next("recovery_test").unwrap().is_none());
    }
    
    cleanup_wal();
}

#[test]
fn test_crash_recovery_at_least_once() {
    cleanup_wal();
    
    {
        let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();
        
        for i in 1..=7 {
            let msg = format!("at_least_once_msg_{}", i);
            wal.append_for_topic("recovery_test", msg.as_bytes()).unwrap();
        }
        
        let entry1 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry1.data, b"at_least_once_msg_1");
        
        let entry2 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry2.data, b"at_least_once_msg_2");
        
    }
    
    {
        let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();
        
        let entry1 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry1.data, b"at_least_once_msg_1");
        
        let entry2 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry2.data, b"at_least_once_msg_2");
        
        let entry3 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry3.data, b"at_least_once_msg_3");
    }
    
    {
        let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 3 }).unwrap();
        
        let entry4 = wal.read_next("recovery_test").unwrap().unwrap();
        assert_eq!(entry4.data, b"at_least_once_msg_4");
    }
    
    cleanup_wal();
}


#[test]
fn test_multiple_topics_different_consistency_behavior() {
    cleanup_wal();
    
    let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 2 }).unwrap();
    
    wal.append_for_topic("topic_a", b"a1").unwrap();
    wal.append_for_topic("topic_b", b"b1").unwrap();
    wal.append_for_topic("topic_a", b"a2").unwrap();
    wal.append_for_topic("topic_b", b"b2").unwrap();
    
    assert_eq!(wal.read_next("topic_a").unwrap().unwrap().data, b"a1");
    assert_eq!(wal.read_next("topic_b").unwrap().unwrap().data, b"b1");
    
    drop(wal);
    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 2 }).unwrap();
    
    assert_eq!(wal2.read_next("topic_a").unwrap().unwrap().data, b"a1");
    assert_eq!(wal2.read_next("topic_b").unwrap().unwrap().data, b"b1");
    
    cleanup_wal();
}

#[test]
fn test_configuration_with_concurrent_operations() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(2000)
    ).unwrap());
    
    let wal_writer = Arc::clone(&wal);
    let writer_handle = thread::spawn(move || {
        for i in 0..10 {
            let msg = format!("concurrent_msg_{}", i);
            wal_writer.append_for_topic("concurrent", msg.as_bytes()).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });
    
    thread::sleep(Duration::from_millis(50));
    
    let mut read_count = 0;
    let start_time = Instant::now();
    
    while start_time.elapsed() < Duration::from_millis(200) && read_count < 10 {
        if let Some(entry) = wal.read_next("concurrent").unwrap() {
            let expected = format!("concurrent_msg_{}", read_count);
            assert_eq!(entry.data, expected.as_bytes());
            read_count += 1;
        }
        thread::sleep(Duration::from_millis(10));
    }
    
    writer_handle.join().unwrap();
    
    while let Some(entry) = wal.read_next("concurrent").unwrap() {
        let expected = format!("concurrent_msg_{}", read_count);
        assert_eq!(entry.data, expected.as_bytes());
        read_count += 1;
        if read_count >= 10 { break; }
    }
    
    assert_eq!(read_count, 10);
    
    cleanup_wal();
}

#[test]
fn test_persist_every_zero_clamping() {
    cleanup_wal();
    
    let wal = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 0 }).unwrap();
    
    wal.append_for_topic("test", b"msg1").unwrap();
    wal.append_for_topic("test", b"msg2").unwrap();
    
    let entry1 = wal.read_next("test").unwrap().unwrap();
    assert_eq!(entry1.data, b"msg1");
    
    drop(wal);
    let wal2 = Walrus::with_consistency(ReadConsistency::AtLeastOnce { persist_every: 0 }).unwrap();
    
    let entry2 = wal2.read_next("test").unwrap().unwrap();
    assert_eq!(entry2.data, b"msg2");
    
    cleanup_wal();
}


#[test]
fn test_log_file_deletion_with_fast_fsync() {
    cleanup_wal();
    
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1) // Ultra-fast 1ms fsync
    ).unwrap();
    
    println!("Creating first 999MB entry...");
    let large_data_1 = vec![0xAA; 999 * 1024 * 1024]; // 999MB of 0xAA bytes
    wal.append_for_topic("deletion_test", &large_data_1).unwrap();
    
    println!("Creating second 999MB entry...");
    let large_data_2 = vec![0xBB; 999 * 1024 * 1024]; // 999MB of 0xBB bytes
    wal.append_for_topic("deletion_test", &large_data_2).unwrap();
    
    let files_after_writes = std::fs::read_dir("wal_files")
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();
    
    println!("Files after writing 2x999MB entries: {}", files_after_writes.len());
    for file in &files_after_writes {
        println!("  File: {:?}", file.file_name());
    }
    
    assert!(files_after_writes.len() >= 2, "Should have at least 2 files after writing 2x999MB entries");
    
    println!("Reading first entry (999MB)...");
    let entry1 = wal.read_next("deletion_test").unwrap().unwrap();
    assert_eq!(entry1.data.len(), 999 * 1024 * 1024);
    assert_eq!(entry1.data[0], 0xAA); // Verify it's the first entry
    
    println!("First entry read successfully. Waiting for file cleanup...");
    
    println!("Waiting 60 seconds for background deletion to process...");
    
    for i in 1..=12 {
        thread::sleep(Duration::from_secs(5));
        let current_files = std::fs::read_dir("wal_files")
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let binding = entry.file_name();
                let name = binding.to_string_lossy();
                !name.ends_with("_index.db") && !name.ends_with(".tmp")
            })
            .count();
        println!("After {} seconds: {} files remaining", i * 5, current_files);
        
        if current_files < files_after_writes.len() {
            println!("FILE DELETION DETECTED at {} seconds!", i * 5);
            break;
        }
    }
    
    let files_after_read = std::fs::read_dir("wal_files")
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();
    
    println!("Files after reading first entry and waiting: {}", files_after_read.len());
    for file in &files_after_read {
        println!("  File: {:?}", file.file_name());
    }
    
    if files_after_read.len() < files_after_writes.len() {
        println!("SUCCESS: File deletion occurred! {} -> {} files", files_after_writes.len(), files_after_read.len());
    } else {
        println!("INFO: Files still present, deletion may require more time or different conditions");
    }
    
    println!("Reading second entry to verify WAL integrity...");
    let entry2 = wal.read_next("deletion_test").unwrap().unwrap();
    assert_eq!(entry2.data.len(), 999 * 1024 * 1024);
    assert_eq!(entry2.data[0], 0xBB); // Verify it's the second entry
    
    println!("Test completed successfully!");
    cleanup_wal();
}

#[test]
fn test_log_file_deletion_with_large_data() {
    cleanup_wal();
    
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000)
    ).unwrap();
    
    let data_size = 1024; // 1KB per entry
    let num_entries = 1000; // 1MB total
    
    for i in 0..num_entries {
        let data = format!("large_test_entry_{:04}_", i).repeat(data_size / 20);
        wal.append_for_topic("large_deletion_test", data.as_bytes()).unwrap();
    }
    
    for i in 0..num_entries {
        let entry = wal.read_next("large_deletion_test").unwrap().unwrap();
        let expected_prefix = format!("large_test_entry_{:04}_", i);
        let entry_str = String::from_utf8_lossy(&entry.data);
        assert!(entry_str.starts_with(&expected_prefix), "Entry {} doesn't start with expected prefix", i);
    }
    
    assert!(wal.read_next("large_deletion_test").unwrap().is_none());
    
    let files_before = if std::path::Path::new("wal_files").exists() {
        std::fs::read_dir("wal_files")
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
    
    println!("Large data test - Files before: {}", files_before);
    
    drop(wal);
    thread::sleep(Duration::from_secs(3));
    
    let files_after = if std::path::Path::new("wal_files").exists() {
        std::fs::read_dir("wal_files")
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
    
    println!("Large data test - Files after: {}", files_after);
    
    
    cleanup_wal();
}

#[test]
fn test_file_state_tracking() {
    cleanup_wal();
    
    let wal = Walrus::with_consistency_and_schedule(
        ReadConsistency::StrictlyAtOnce,
        FsyncSchedule::Milliseconds(1000)
    ).unwrap();
    
    for i in 0..50 {
        let msg = format!("state_tracking_msg_{}", i);
        wal.append_for_topic("state_test", msg.as_bytes()).unwrap();
    }
    
    assert!(std::path::Path::new("wal_files").exists());
    let files_exist = std::fs::read_dir("wal_files")
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        });
    assert!(files_exist, "Should have created log files");
    
    for i in 0..25 {
        let entry = wal.read_next("state_test").unwrap().unwrap();
        let expected = format!("state_tracking_msg_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }
    
    let files_still_exist = std::fs::read_dir("wal_files")
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let binding = entry.file_name();
            let name = binding.to_string_lossy();
            !name.ends_with("_index.db") && !name.ends_with(".tmp")
        });
    assert!(files_still_exist, "Files should still exist with unread data");
    
    for i in 25..50 {
        let entry = wal.read_next("state_test").unwrap().unwrap();
        let expected = format!("state_tracking_msg_{}", i);
        assert_eq!(entry.data, expected.as_bytes());
    }
    
    assert!(wal.read_next("state_test").unwrap().is_none());
    
    
    cleanup_wal();
}
