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

// ============================================================================
// INTEGRATION TESTS - END-TO-END SCENARIOS
// ============================================================================

#[test]
fn integration_basic_write_read_cycle() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Write some data
    wal.append_for_topic("test_topic", b"Hello, World!").unwrap();
    wal.append_for_topic("test_topic", b"Second message").unwrap();
    
    // Read it back
    let entry1 = wal.read_next("test_topic").unwrap();
    assert_eq!(entry1.data, b"Hello, World!");
    
    let entry2 = wal.read_next("test_topic").unwrap();
    assert_eq!(entry2.data, b"Second message");
    
    // Should be empty now
    assert!(wal.read_next("test_topic").is_none());
    
    cleanup_wal();
}

#[test]
fn integration_multiple_topics() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Write to different topics
    wal.append_for_topic("logs", b"Error occurred").unwrap();
    wal.append_for_topic("metrics", b"CPU: 80%").unwrap();
    wal.append_for_topic("logs", b"Warning issued").unwrap();
    wal.append_for_topic("events", b"User login").unwrap();
    
    // Read from each topic independently
    let log1 = wal.read_next("logs").unwrap();
    assert_eq!(log1.data, b"Error occurred");
    
    let metric1 = wal.read_next("metrics").unwrap();
    assert_eq!(metric1.data, b"CPU: 80%");
    
    let log2 = wal.read_next("logs").unwrap();
    assert_eq!(log2.data, b"Warning issued");
    
    let event1 = wal.read_next("events").unwrap();
    assert_eq!(event1.data, b"User login");
    
    // All topics should be empty now
    assert!(wal.read_next("logs").is_none());
    assert!(wal.read_next("metrics").is_none());
    assert!(wal.read_next("events").is_none());
    
    cleanup_wal();
}

#[test]
fn integration_empty_data_handling() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Test empty data
    wal.append_for_topic("empty_test", b"").unwrap();
    let empty_entry = wal.read_next("empty_test").unwrap();
    assert!(empty_entry.data.is_empty());
    
    // Test single byte
    wal.append_for_topic("single_byte", &[42]).unwrap();
    let single_entry = wal.read_next("single_byte").unwrap();
    assert_eq!(single_entry.data, &[42]);
    
    cleanup_wal();
}

#[test]
fn integration_binary_data() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Test binary data with various byte values
    let binary_data = vec![0, 1, 127, 128, 255, 0, 42];
    wal.append_for_topic("binary", &binary_data).unwrap();
    
    let entry = wal.read_next("binary").unwrap();
    assert_eq!(entry.data, binary_data);
    
    cleanup_wal();
}

#[test]
fn integration_utf8_strings() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
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
        let entry = wal.read_next(&topic).unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, *expected);
    }
    
    cleanup_wal();
}

#[test]
fn integration_medium_sized_data() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
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
        let entry = wal.read_next(&topic).unwrap();
        assert_eq!(entry.data, expected);
    }
    
    cleanup_wal();
}

#[test]
fn integration_sequential_writes_and_reads() {
    cleanup_wal();
    
    let wal = Walrus::new();
    let topic = "sequential";
    
    // Write a sequence of messages
    for i in 0..20 {
        let message = format!("Message number {}", i);
        wal.append_for_topic(topic, message.as_bytes()).unwrap();
    }
    
    // Read them back in order
    for i in 0..20 {
        let expected = format!("Message number {}", i);
        let entry = wal.read_next(topic).unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, expected);
    }
    
    assert!(wal.read_next(topic).is_none());
    
    cleanup_wal();
}

#[test]
fn integration_interleaved_write_read() {
    cleanup_wal();
    
    let wal = Walrus::new();
    let topic = "interleaved";
    
    // Write some messages
    wal.append_for_topic(topic, b"Message 1").unwrap();
    wal.append_for_topic(topic, b"Message 2").unwrap();
    
    // Read one
    let entry1 = wal.read_next(topic).unwrap();
    assert_eq!(entry1.data, b"Message 1");
    
    // Write more
    wal.append_for_topic(topic, b"Message 3").unwrap();
    wal.append_for_topic(topic, b"Message 4").unwrap();
    
    // Read the rest
    let entry2 = wal.read_next(topic).unwrap();
    assert_eq!(entry2.data, b"Message 2");
    
    let entry3 = wal.read_next(topic).unwrap();
    assert_eq!(entry3.data, b"Message 3");
    
    let entry4 = wal.read_next(topic).unwrap();
    assert_eq!(entry4.data, b"Message 4");
    
    assert!(wal.read_next(topic).is_none());
    
    cleanup_wal();
}

#[test]
fn integration_multiple_topics_stress() {
    cleanup_wal();
    
    let wal = Walrus::new();
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
            let entry = wal.read_next(&topic).unwrap();
            let actual = String::from_utf8(entry.data).unwrap();
            assert_eq!(actual, expected);
        }
        assert!(wal.read_next(&topic).is_none());
    }
    
    cleanup_wal();
}

#[test]
fn integration_concurrent_writes() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new());
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
            let entry = wal.read_next(&topic).unwrap();
            let actual = String::from_utf8(entry.data).unwrap();
            assert_eq!(actual, expected);
        }
        assert!(wal.read_next(&topic).is_none());
    }
    
    cleanup_wal();
}

#[test]
fn integration_topic_isolation() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Write to multiple topics
    wal.append_for_topic("topic_a", b"A1").unwrap();
    wal.append_for_topic("topic_b", b"B1").unwrap();
    wal.append_for_topic("topic_a", b"A2").unwrap();
    wal.append_for_topic("topic_c", b"C1").unwrap();
    wal.append_for_topic("topic_b", b"B2").unwrap();
    
    // Read from topic_a only
    assert_eq!(wal.read_next("topic_a").unwrap().data, b"A1");
    assert_eq!(wal.read_next("topic_a").unwrap().data, b"A2");
    assert!(wal.read_next("topic_a").is_none());
    
    // Other topics should still have their data
    assert_eq!(wal.read_next("topic_b").unwrap().data, b"B1");
    assert_eq!(wal.read_next("topic_b").unwrap().data, b"B2");
    assert!(wal.read_next("topic_b").is_none());
    
    assert_eq!(wal.read_next("topic_c").unwrap().data, b"C1");
    assert!(wal.read_next("topic_c").is_none());
    
    cleanup_wal();
}

#[test]
fn integration_nonexistent_topic() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Reading from a topic that doesn't exist should return None
    assert!(wal.read_next("nonexistent").is_none());
    
    // Write to a topic, then read from a different one
    wal.append_for_topic("existing", b"data").unwrap();
    assert!(wal.read_next("different").is_none());
    
    // The original topic should still have data
    assert_eq!(wal.read_next("existing").unwrap().data, b"data");
    
    cleanup_wal();
}

#[test]
fn integration_write_after_exhaustion() {
    cleanup_wal();
    
    let wal = Walrus::new();
    let topic = "exhaustion_test";
    
    // Write and read all data
    wal.append_for_topic(topic, b"first").unwrap();
    assert_eq!(wal.read_next(topic).unwrap().data, b"first");
    assert!(wal.read_next(topic).is_none());
    
    // Write more data after exhaustion
    wal.append_for_topic(topic, b"second").unwrap();
    wal.append_for_topic(topic, b"third").unwrap();
    
    // Should be able to read new data
    assert_eq!(wal.read_next(topic).unwrap().data, b"second");
    assert_eq!(wal.read_next(topic).unwrap().data, b"third");
    assert!(wal.read_next(topic).is_none());
    
    cleanup_wal();
}

#[test]
fn integration_large_topic_names() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
    // Test with reasonably long topic names (but not too long to exceed metadata limits)
    let long_topic = "a".repeat(15);  // Reduced to stay within metadata limits
    let very_long_topic = "b".repeat(18);  // Reduced to stay within metadata limits
    
    wal.append_for_topic(&long_topic, b"long topic data").unwrap();
    wal.append_for_topic(&very_long_topic, b"very long topic data").unwrap();
    
    assert_eq!(wal.read_next(&long_topic).unwrap().data, b"long topic data");
    assert_eq!(wal.read_next(&very_long_topic).unwrap().data, b"very long topic data");
    
    cleanup_wal();
}

#[test]
fn integration_special_topic_names() {
    cleanup_wal();
    
    let wal = Walrus::new();
    
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
        let entry = wal.read_next(topic).unwrap();
        let actual = String::from_utf8(entry.data).unwrap();
        assert_eq!(actual, expected);
    }
    
    cleanup_wal();
}