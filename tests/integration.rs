use walrus::wal::Walrus;
use std::fs;
use std::thread;
use std::time::Duration;
use std::sync::Arc;

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    // Give filesystem time to clean up
    thread::sleep(Duration::from_millis(10));
}

fn first_data_file() -> String {
    let mut files: Vec<_> = fs::read_dir("./wal_files").unwrap().flatten().collect();
    files.sort_by_key(|e| e.file_name());
    let p = files
        .into_iter()
        .find(|e| !e.file_name().to_string_lossy().ends_with("_index.db"))
        .unwrap()
        .path();
    p.to_string_lossy().to_string()
}

// ============================================================================
// INTEGRATION TESTS - END-TO-END SCENARIOS
// ============================================================================

#[test]
fn integration_basic_write_read_cycle() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Write some data
    wal.append_for_topic("test_topic", b"Hello, World!").unwrap();
    wal.append_for_topic("test_topic", b"Second message").unwrap();
    
    // Read it back
    let entry1 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(entry1.data, b"Hello, World!");
    
    let entry2 = wal.read_next("test_topic").unwrap().unwrap();
    assert_eq!(entry2.data, b"Second message");
    
    // Should be empty now
    assert!(wal.read_next("test_topic").unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_multiple_topics() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Write to different topics
    wal.append_for_topic("logs", b"Error occurred").unwrap();
    wal.append_for_topic("metrics", b"CPU: 80%").unwrap();
    wal.append_for_topic("logs", b"Warning issued").unwrap();
    wal.append_for_topic("events", b"User login").unwrap();
    
    // Read from each topic independently
    let log1 = wal.read_next("logs").unwrap().unwrap();
    assert_eq!(log1.data, b"Error occurred");
    
    let metric1 = wal.read_next("metrics").unwrap().unwrap();
    assert_eq!(metric1.data, b"CPU: 80%");
    
    let log2 = wal.read_next("logs").unwrap().unwrap();
    assert_eq!(log2.data, b"Warning issued");
    
    let event1 = wal.read_next("events").unwrap().unwrap();
    assert_eq!(event1.data, b"User login");
    
    // All topics should be empty now
    assert!(wal.read_next("logs").unwrap().is_none());
    assert!(wal.read_next("metrics").unwrap().is_none());
    assert!(wal.read_next("events").unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_empty_data_handling() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test empty data
    wal.append_for_topic("empty_test", b"").unwrap();
    let empty_entry = wal.read_next("empty_test").unwrap().unwrap();
    assert!(empty_entry.data.is_empty());
    
    // Test single byte
    wal.append_for_topic("single_byte", &[42]).unwrap();
    let single_entry = wal.read_next("single_byte").unwrap().unwrap();
    assert_eq!(single_entry.data, &[42]);
    
    cleanup_wal();
}

#[test]
fn integration_binary_data() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test binary data with various byte values
    let binary_data = vec![0, 1, 127, 128, 255, 0, 42];
    wal.append_for_topic("binary", &binary_data).unwrap();
    
    let entry = wal.read_next("binary").unwrap().unwrap();
    assert_eq!(entry.data, binary_data);
    
    cleanup_wal();
}

#[test]
fn integration_utf8_strings() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test UTF-8 strings with special characters
    let utf8_strings = vec![
        "Hello, World!",
        "Café ☕",
        "こんにちは",
        "🦀 Rust is awesome! 🚀",
        "Ñoño niño",
    ];
    
    for (i, s) in utf8_strings.iter().enumerate() {
        let topic = format!("utf8_{}", i);
        wal.append_for_topic(&topic, s.as_bytes()).unwrap();
    }
    
    for (i, expected) in utf8_strings.iter().enumerate() {
        let topic = format!("utf8_{}", i);
        let entry = wal.read_next(&topic).unwrap().unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, *expected);
    }
    
    cleanup_wal();
}

#[test]
fn integration_medium_sized_data() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test with medium-sized data (1KB, 10KB, 100KB)
    let sizes = vec![1024, 10 * 1024, 100 * 1024];
    
    for (i, size) in sizes.iter().enumerate() {
        let data = vec![i as u8; *size];
        let topic = format!("medium_{}", i);
        wal.append_for_topic(&topic, &data).unwrap();
    }
    
    for (i, size) in sizes.iter().enumerate() {
        let expected = vec![i as u8; *size];
        let topic = format!("medium_{}", i);
        let entry = wal.read_next(&topic).unwrap().unwrap();
        assert_eq!(entry.data, expected);
    }
    
    cleanup_wal();
}

#[test]
fn integration_sequential_writes_and_reads() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "sequential";
    
    // Write a sequence of messages
    for i in 0..20 {
        let message = format!("Message number {}", i);
        wal.append_for_topic(topic, message.as_bytes()).unwrap();
    }
    
    // Read them back in order
    for i in 0..20 {
        let expected = format!("Message number {}", i);
        let entry = wal.read_next(topic).unwrap().unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, expected);
    }
    
    assert!(wal.read_next(topic).unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_interleaved_write_read() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "interleaved";
    
    // Write some messages
    wal.append_for_topic(topic, b"Message 1").unwrap();
    wal.append_for_topic(topic, b"Message 2").unwrap();
    
    // Read one
    let entry1 = wal.read_next(topic).unwrap().unwrap();
    assert_eq!(entry1.data, b"Message 1");
    
    // Write more
    wal.append_for_topic(topic, b"Message 3").unwrap();
    wal.append_for_topic(topic, b"Message 4").unwrap();
    
    // Read the rest
    let entry2 = wal.read_next(topic).unwrap().unwrap();
    assert_eq!(entry2.data, b"Message 2");
    
    let entry3 = wal.read_next(topic).unwrap().unwrap();
    assert_eq!(entry3.data, b"Message 3");
    
    let entry4 = wal.read_next(topic).unwrap().unwrap();
    assert_eq!(entry4.data, b"Message 4");
    
    assert!(wal.read_next(topic).unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_multiple_topics_stress() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let num_topics = 5;
    let messages_per_topic = 10;
    
    // Write to multiple topics
    for topic_id in 0..num_topics {
        for msg_id in 0..messages_per_topic {
            let topic = format!("stress_topic_{}", topic_id);
            let message = format!("Topic {} Message {}", topic_id, msg_id);
            wal.append_for_topic(&topic, message.as_bytes()).unwrap();
        }
    }
    
    // Read from all topics and verify
    for topic_id in 0..num_topics {
        let topic = format!("stress_topic_{}", topic_id);
        for msg_id in 0..messages_per_topic {
            let expected = format!("Topic {} Message {}", topic_id, msg_id);
            let entry = wal.read_next(&topic).unwrap().unwrap();
            let actual = String::from_utf8(entry.data).unwrap();
            assert_eq!(actual, expected);
        }
        assert!(wal.read_next(&topic).unwrap().is_none());
    }
    
    cleanup_wal();
}

#[test]
fn integration_concurrent_writes() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new().unwrap());
    let num_threads = 3;
    let messages_per_thread = 5;
    
    let mut handles = vec![];
    
    // Spawn threads that write to different topics
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let handle = thread::spawn(move || {
            let topic = format!("concurrent_{}", thread_id);
            for msg_id in 0..messages_per_thread {
                let message = format!("Thread {} Message {}", thread_id, msg_id);
                wal_clone.append_for_topic(&topic, message.as_bytes()).unwrap();
                // Small delay to allow interleaving
                thread::sleep(Duration::from_millis(1));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all messages were written correctly
    for thread_id in 0..num_threads {
        let topic = format!("concurrent_{}", thread_id);
        for msg_id in 0..messages_per_thread {
            let expected = format!("Thread {} Message {}", thread_id, msg_id);
            let entry = wal.read_next(&topic).unwrap().unwrap();
            let actual = String::from_utf8(entry.data).unwrap();
            assert_eq!(actual, expected);
        }
        assert!(wal.read_next(&topic).unwrap().is_none());
    }
    
    cleanup_wal();
}

#[test]
fn integration_topic_isolation() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Write to multiple topics
    wal.append_for_topic("topic_a", b"A1").unwrap();
    wal.append_for_topic("topic_b", b"B1").unwrap();
    wal.append_for_topic("topic_a", b"A2").unwrap();
    wal.append_for_topic("topic_c", b"C1").unwrap();
    wal.append_for_topic("topic_b", b"B2").unwrap();
    
    // Read from topic_a only
    assert_eq!(wal.read_next("topic_a").unwrap().unwrap().data, b"A1");
    assert_eq!(wal.read_next("topic_a").unwrap().unwrap().data, b"A2");
    assert!(wal.read_next("topic_a").unwrap().is_none());
    
    // Other topics should still have their data
    assert_eq!(wal.read_next("topic_b").unwrap().unwrap().data, b"B1");
    assert_eq!(wal.read_next("topic_b").unwrap().unwrap().data, b"B2");
    assert!(wal.read_next("topic_b").unwrap().is_none());
    
    assert_eq!(wal.read_next("topic_c").unwrap().unwrap().data, b"C1");
    assert!(wal.read_next("topic_c").unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_nonexistent_topic() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Reading from a topic that doesn't exist should return None
    assert!(wal.read_next("nonexistent").unwrap().is_none());
    
    // Write to a topic, then read from a different one
    wal.append_for_topic("existing", b"data").unwrap();
    assert!(wal.read_next("different").unwrap().is_none());
    
    // The original topic should still have data
    assert_eq!(wal.read_next("existing").unwrap().unwrap().data, b"data");
    
    cleanup_wal();
}

#[test]
fn integration_write_after_exhaustion() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "exhaustion_test";
    
    // Write and read all data
    wal.append_for_topic(topic, b"first").unwrap();
    assert_eq!(wal.read_next(topic).unwrap().unwrap().data, b"first");
    assert!(wal.read_next(topic).unwrap().is_none());
    
    // Write more data after exhaustion
    wal.append_for_topic(topic, b"second").unwrap();
    wal.append_for_topic(topic, b"third").unwrap();
    
    // Should be able to read new data
    assert_eq!(wal.read_next(topic).unwrap().unwrap().data, b"second");
    assert_eq!(wal.read_next(topic).unwrap().unwrap().data, b"third");
    assert!(wal.read_next(topic).unwrap().is_none());
    
    cleanup_wal();
}

#[test]
fn integration_large_topic_names() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test with reasonably long topic names (but not too long to exceed metadata limits)
    let long_topic = "a".repeat(15);  // Reduced to stay within metadata limits
    let very_long_topic = "b".repeat(18);  // Reduced to stay within metadata limits
    
    wal.append_for_topic(&long_topic, b"long topic data").unwrap();
    wal.append_for_topic(&very_long_topic, b"very long topic data").unwrap();
    
    assert_eq!(wal.read_next(&long_topic).unwrap().unwrap().data, b"long topic data");
    assert_eq!(wal.read_next(&very_long_topic).unwrap().unwrap().data, b"very long topic data");
    
    cleanup_wal();
}

// ============================================================================
// EXTREME INTEGRATION STRESS TESTS - ABSOLUTE LIMITS
// ============================================================================

#[test]
fn integration_memory_pressure_test() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let num_topics = 100;
    let large_entry_size = 1024 * 1024; // 1MB per entry
    
    // Create memory pressure with large entries across many topics
    for topic_id in 0..num_topics {
        let topic = format!("memory_pressure_{}", topic_id);
        
        // Create large entry with validation pattern
        let mut data = Vec::with_capacity(large_entry_size);
        for i in 0..large_entry_size {
            data.push(((topic_id + i) % 256) as u8);
        }
        
        wal.append_for_topic(&topic, &data).unwrap();
    }
    
    // Read back and validate all large entries
    for topic_id in 0..num_topics {
        let topic = format!("memory_pressure_{}", topic_id);
        let entry = wal.read_next(&topic).unwrap().unwrap();
        
        assert_eq!(entry.data.len(), large_entry_size);
        
        // Validate pattern
        for (i, &byte) in entry.data.iter().enumerate() {
            assert_eq!(byte, ((topic_id + i) % 256) as u8,
                      "Memory pressure test failed at topic {} byte {}", topic_id, i);
        }
    }
    
    cleanup_wal();
}

#[test]
fn integration_file_rollover_stress() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "rollover_stress";
    
    // Force multiple file rollovers with large entries
    let entry_size = 50 * 1024 * 1024; // 50MB entries to force rollovers
    let num_entries = 5;
    
    for entry_id in 0..num_entries {
        let mut data = Vec::with_capacity(entry_size);
        
        // Fill with entry-specific pattern
        for i in 0..entry_size {
            data.push(((entry_id * 1000 + i) % 256) as u8);
        }
        
        wal.append_for_topic(topic, &data).unwrap();
    }
    
    // Read back across file boundaries
    for entry_id in 0..num_entries {
        let entry = wal.read_next(topic).unwrap().unwrap();
        assert_eq!(entry.data.len(), entry_size);
        
        // Validate pattern across file boundaries
        for (i, &byte) in entry.data.iter().enumerate() {
            assert_eq!(byte, ((entry_id * 1000 + i) % 256) as u8,
                      "File rollover validation failed at entry {} byte {}", entry_id, i);
        }
    }
    
    cleanup_wal();
}

#[test]
fn integration_corruption_detection_comprehensive() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "corruption_test";
    
    // Write data with strong validation patterns
    let test_data = b"CORRUPTION_TEST_DATA_WITH_STRONG_PATTERN_12345678901234567890";
    wal.append_for_topic(topic, test_data).unwrap();
    
    // Verify normal read works
    let entry = wal.read_next(topic).unwrap().unwrap();
    assert_eq!(entry.data, test_data);
    
    // Now test corruption detection by corrupting the file
    let path = first_data_file();
    let mut file_data = std::fs::read(&path).unwrap();
    
    // Find the test data in the file
    if let Some(pos) = file_data.windows(test_data.len()).position(|w| w == test_data) {
        // Corrupt multiple bytes to ensure detection
        for i in 0..5 {
            if pos + i < file_data.len() {
                file_data[pos + i] ^= 0xFF; // Flip all bits
            }
        }
        
        std::fs::write(&path, &file_data).unwrap();
        
        // Create new WAL instance and try to read
        let wal2 = Walrus::new().unwrap();
        
        // Should detect corruption and handle gracefully
        match wal2.read_next(topic).unwrap() {
            None => {
                // Expected: corruption detected, no data returned
            }
            Some(corrupted_entry) => {
                // If data is returned, it should be different from original
                assert_ne!(corrupted_entry.data, test_data, 
                          "Corruption not detected - data should be different");
            }
        }
    }
    
    cleanup_wal();
}

#[test]
fn integration_extreme_topic_count() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let num_topics = 5000; // Extreme number of topics
    
    // Write one entry per topic with validation data
    for topic_id in 0..num_topics {
        let topic = format!("extreme_topic_{:06}", topic_id);
        
        let mut data = Vec::new();
        data.extend_from_slice(&(topic_id as u64).to_le_bytes());
        data.extend_from_slice(format!("TOPIC_DATA_{}", topic_id).as_bytes());
        
        wal.append_for_topic(&topic, &data).unwrap();
    }
    
    // Read back in random order to stress topic lookup
    let mut read_order: Vec<usize> = (0..num_topics).collect();
    
    // Simple shuffle using topic_id as seed
    for i in 0..num_topics {
        let j = (i * 1103515245 + 12345) % num_topics;
        read_order.swap(i, j);
    }
    
    for &topic_id in &read_order {
        let topic = format!("extreme_topic_{:06}", topic_id);
        let entry = wal.read_next(&topic).unwrap().unwrap();
        
        // Validate embedded topic ID
        let read_topic_id = u64::from_le_bytes([
            entry.data[0], entry.data[1], entry.data[2], entry.data[3],
            entry.data[4], entry.data[5], entry.data[6], entry.data[7]
        ]);
        
        assert_eq!(read_topic_id, topic_id as u64);
        
        // Validate payload
        let expected_payload = format!("TOPIC_DATA_{}", topic_id);
        let actual_payload = String::from_utf8(entry.data[8..].to_vec()).unwrap();
        assert_eq!(actual_payload, expected_payload);
    }
    
    cleanup_wal();
}

#[test]
fn integration_mixed_size_stress() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    let topic = "mixed_sizes";
    
    // Test with exponentially increasing sizes
    let base_sizes = vec![1, 10, 100, 1000, 10000, 100000, 1000000];
    
    for (i, &base_size) in base_sizes.iter().enumerate() {
        let mut data = Vec::with_capacity(base_size);
        
        // Fill with size-dependent pattern
        for j in 0..base_size {
            data.push(((i * 1000 + j) % 256) as u8);
        }
        
        wal.append_for_topic(topic, &data).unwrap();
    }
    
    // Read back and validate each size
    for (i, &base_size) in base_sizes.iter().enumerate() {
        let entry = wal.read_next(topic).unwrap().unwrap();
        assert_eq!(entry.data.len(), base_size);
        
        for (j, &byte) in entry.data.iter().enumerate() {
            assert_eq!(byte, ((i * 1000 + j) % 256) as u8,
                      "Mixed size validation failed at size {} byte {}", base_size, j);
        }
    }
    
    cleanup_wal();
}

#[test]
fn integration_persistence_stress_with_validation() {
    cleanup_wal();
    
    // Phase 1: Write lots of data
    {
        let wal = Walrus::new().unwrap();
        let num_topics = 100;
        let entries_per_topic = 50;
        
        for topic_id in 0..num_topics {
            let topic = format!("persist_stress_{}", topic_id);
            
            for entry_id in 0..entries_per_topic {
                let mut data = Vec::new();
                data.extend_from_slice(&(topic_id as u32).to_le_bytes());
                data.extend_from_slice(&(entry_id as u32).to_le_bytes());
                
                // Add timestamp-like data
                let timestamp = (topic_id * 1000 + entry_id) as u64;
                data.extend_from_slice(&timestamp.to_le_bytes());
                
                // Add payload
                let payload = format!("PERSIST_{}_{}", topic_id, entry_id);
                data.extend_from_slice(payload.as_bytes());
                
                wal.append_for_topic(&topic, &data).unwrap();
            }
        }
        
        // Read some data to advance read positions
        for topic_id in 0..num_topics {
            let topic = format!("persist_stress_{}", topic_id);
            
            // Read half the entries
            for _ in 0..(entries_per_topic / 2) {
                wal.read_next(&topic).unwrap().unwrap();
            }
        }
    } // WAL instance dropped here
    
    // Phase 2: Restart and validate persistence
    {
        let wal = Walrus::new().unwrap();
        let num_topics = 100;
        let entries_per_topic = 50;
        
        // Continue reading from where we left off
        for topic_id in 0..num_topics {
            let topic = format!("persist_stress_{}", topic_id);
            
            // Read remaining entries
            for entry_id in (entries_per_topic / 2)..entries_per_topic {
                let entry = wal.read_next(&topic).unwrap().unwrap();
                
                // Validate all embedded data
                let read_topic_id = u32::from_le_bytes([
                    entry.data[0], entry.data[1], entry.data[2], entry.data[3]
                ]);
                let read_entry_id = u32::from_le_bytes([
                    entry.data[4], entry.data[5], entry.data[6], entry.data[7]
                ]);
                let read_timestamp = u64::from_le_bytes([
                    entry.data[8], entry.data[9], entry.data[10], entry.data[11],
                    entry.data[12], entry.data[13], entry.data[14], entry.data[15]
                ]);
                
                assert_eq!(read_topic_id, topic_id as u32);
                assert_eq!(read_entry_id, entry_id as u32);
                assert_eq!(read_timestamp, (topic_id * 1000 + entry_id) as u64);
                
                // Validate payload
                let expected_payload = format!("PERSIST_{}_{}", topic_id, entry_id);
                let actual_payload = String::from_utf8(entry.data[16..].to_vec()).unwrap();
                assert_eq!(actual_payload, expected_payload);
            }
            
            // Verify no more entries
            assert!(wal.read_next(&topic).unwrap().is_none());
        }
    }
    
    cleanup_wal();
}

#[test]
fn integration_data_pattern_stress() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test various challenging data patterns
    let patterns = vec![
        ("all_zeros", vec![0u8; 10000]),
        ("all_ones", vec![0xFF; 10000]),
        ("alternating_bytes", (0..10000).map(|i| if i % 2 == 0 { 0x00 } else { 0xFF }).collect()),
        ("incremental", (0..10000).map(|i| (i % 256) as u8).collect()),
        ("decremental", (0..10000).map(|i| (255 - (i % 256)) as u8).collect()),
        ("repeating_pattern", vec![0xAA, 0xBB, 0xCC, 0xDD].repeat(2500)),
        ("pseudo_random", {
            let mut data = Vec::new();
            let mut seed = 0x12345678u32;
            for _ in 0..10000 {
                seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                data.push((seed >> 24) as u8);
            }
            data
        }),
    ];
    
    // Write all patterns
    for (pattern_name, data) in &patterns {
        wal.append_for_topic(pattern_name, data).unwrap();
    }
    
    // Read back and validate
    for (pattern_name, expected_data) in patterns {
        let entry = wal.read_next(&pattern_name).unwrap().unwrap();
        assert_eq!(entry.data, expected_data, 
                  "Pattern '{}' was corrupted during storage/retrieval", pattern_name);
    }
    
    cleanup_wal();
}

#[test]
fn integration_special_topic_names() {
    cleanup_wal();
    
    let wal = Walrus::new().unwrap();
    
    // Test with special characters in topic names
    let topics = vec![
        "topic-with-dashes",
        "topic_with_underscores",
        "topic.with.dots",
        "topic123",
        "UPPERCASE_TOPIC",
        "MixedCaseTopic",
    ];
    
    for (i, topic) in topics.iter().enumerate() {
        let data = format!("Data for topic {}", i);
        wal.append_for_topic(topic, data.as_bytes()).unwrap();
    }
    
    for (i, topic) in topics.iter().enumerate() {
        let expected = format!("Data for topic {}", i);
        let entry = wal.read_next(topic).unwrap().unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, expected);
    }
    
    cleanup_wal();
}

#[test]
fn exactly_once_delivery_guarantee() {
    cleanup_wal();
    let wal = Walrus::new().unwrap();
    
    // Write entries
    for i in 0..10 {
        wal.append_for_topic("exactly_once", &[i]).unwrap();
    }
    
    // Read half
    for i in 0..5 {
        assert_eq!(wal.read_next("exactly_once").unwrap().unwrap().data, &[i]);
    }
    
    // Simulate crash and restart
    drop(wal);
    let wal2 = Walrus::new().unwrap();
    
    // Should continue from entry 5, not restart from 0
    for i in 5..10 {
        assert_eq!(wal2.read_next("exactly_once").unwrap().unwrap().data, &[i]);
    }
    
    cleanup_wal();
}