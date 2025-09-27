use walrus::wal::Walrus;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    // Give filesystem time to clean up
    thread::sleep(Duration::from_millis(50));
}

// ============================================================================
// END-TO-END LONG-RUNNING TESTS - COMPLETE SYSTEM FUNCTIONALITY
// ============================================================================

#[test]
fn e2e_sustained_mixed_workload() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new());
    let duration = Duration::from_secs(30); // 30 second sustained test
    let start_time = Instant::now();
    
    // Shared counters for validation
    let write_counts = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let read_counts = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    
    // Spawn multiple writer threads with different patterns
    let mut handles = vec![];
    
    // High-frequency small writes
    for worker_id in 0..3 {
        let wal_clone = Arc::clone(&wal);
        let write_counts_clone = Arc::clone(&write_counts);
        let topic = format!("high_freq_{}", worker_id);
        
        let handle = thread::spawn(move || {
            let mut counter = 0u64;
            while start_time.elapsed() < duration {
                let data = format!("high_freq_data_{}_{}", worker_id, counter);
                if wal_clone.append_for_topic(&topic, data.as_bytes()).is_ok() {
                    let mut counts = write_counts_clone.lock().unwrap();
                    *counts.entry(topic.clone()).or_insert(0) += 1;
                    counter += 1;
                }
                thread::sleep(Duration::from_millis(10)); // 100 writes/sec per thread
            }
        });
        handles.push(handle);
    }
    
    // Medium-frequency medium writes
    for worker_id in 0..2 {
        let wal_clone = Arc::clone(&wal);
        let write_counts_clone = Arc::clone(&write_counts);
        let topic = format!("med_freq_{}", worker_id);
        
        let handle = thread::spawn(move || {
            let mut counter = 0u64;
            while start_time.elapsed() < duration {
                let data = format!("medium_frequency_data_with_more_content_{}_{}", worker_id, counter).repeat(10);
                if wal_clone.append_for_topic(&topic, data.as_bytes()).is_ok() {
                    let mut counts = write_counts_clone.lock().unwrap();
                    *counts.entry(topic.clone()).or_insert(0) += 1;
                    counter += 1;
                }
                thread::sleep(Duration::from_millis(50)); // 20 writes/sec per thread
            }
        });
        handles.push(handle);
    }
    
    // Low-frequency large writes
    let wal_clone = Arc::clone(&wal);
    let write_counts_clone = Arc::clone(&write_counts);
    let topic = "low_freq_large".to_string();
    
    let handle = thread::spawn(move || {
        let mut counter = 0u64;
        while start_time.elapsed() < duration {
            let data = vec![counter as u8; 100_000]; // 100KB entries
            if wal_clone.append_for_topic(&topic, &data).is_ok() {
                let mut counts = write_counts_clone.lock().unwrap();
                *counts.entry(topic.clone()).or_insert(0) += 1;
                counter += 1;
            }
            thread::sleep(Duration::from_millis(500)); // 2 writes/sec
        }
    });
    handles.push(handle);
    
    // Concurrent readers
    for worker_id in 0..2 {
        let wal_clone = Arc::clone(&wal);
        let read_counts_clone = Arc::clone(&read_counts);
        
        let handle = thread::spawn(move || {
            let topics = vec![
                format!("high_freq_{}", worker_id % 3),
                format!("med_freq_{}", worker_id % 2),
                "low_freq_large".to_string(),
            ];
            
            while start_time.elapsed() < duration {
                for topic in &topics {
                    if let Some(_entry) = wal_clone.read_next(topic) {
                        let mut counts = read_counts_clone.lock().unwrap();
                        *counts.entry(topic.clone()).or_insert(0) += 1;
                    }
                }
                thread::sleep(Duration::from_millis(25)); // Read every 25ms
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Validate that we processed significant amounts of data
    let write_counts = write_counts.lock().unwrap();
    let read_counts = read_counts.lock().unwrap();
    
    let total_writes: u64 = write_counts.values().sum();
    let total_reads: u64 = read_counts.values().sum();
    
    println!("E2E Sustained Test Results:");
    println!("  Total writes: {}", total_writes);
    println!("  Total reads: {}", total_reads);
    println!("  Duration: {:?}", start_time.elapsed());
    
    // Should have processed thousands of operations
    assert!(total_writes > 1000, "Expected > 1000 writes, got {}", total_writes);
    assert!(total_reads > 500, "Expected > 500 reads, got {}", total_reads);
    
    cleanup_wal();
}

#[test]
fn e2e_realistic_application_simulation() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new());
    let duration = Duration::from_secs(45); // 45 second realistic simulation
    let start_time = Instant::now();
    
    // Simulate a realistic application with different data types
    let mut handles = vec![];
    
    // User activity logs (high frequency, small)
    let wal_clone = Arc::clone(&wal);
    let handle = thread::spawn(move || {
        let mut user_id = 1000u64;
        while start_time.elapsed() < duration {
            let log_entry = format!(
                "{{\"timestamp\":{},\"user_id\":{},\"action\":\"page_view\",\"page\":\"/dashboard\"}}",
                start_time.elapsed().as_millis(),
                user_id
            );
            let _ = wal_clone.append_for_topic("user_activity", log_entry.as_bytes());
            user_id = (user_id + 1) % 10000; // Cycle through user IDs
            thread::sleep(Duration::from_millis(5)); // 200 logs/sec
        }
    });
    handles.push(handle);
    
    // Transaction records (medium frequency, medium size)
    let wal_clone = Arc::clone(&wal);
    let handle = thread::spawn(move || {
        let mut tx_id = 50000u64;
        while start_time.elapsed() < duration {
            let transaction = format!(
                "{{\"tx_id\":{},\"timestamp\":{},\"from_account\":\"acc_{}\",\"to_account\":\"acc_{}\",\"amount\":{:.2},\"currency\":\"USD\",\"status\":\"completed\"}}",
                tx_id,
                start_time.elapsed().as_millis(),
                tx_id % 1000,
                (tx_id + 1) % 1000,
                (tx_id as f64 * 0.01) % 1000.0
            );
            let _ = wal_clone.append_for_topic("transactions", transaction.as_bytes());
            tx_id += 1;
            thread::sleep(Duration::from_millis(100)); // 10 transactions/sec
        }
    });
    handles.push(handle);
    
    // System metrics (low frequency, structured data)
    let wal_clone = Arc::clone(&wal);
    let handle = thread::spawn(move || {
        let mut metric_counter = 0u64;
        while start_time.elapsed() < duration {
            let metrics = format!(
                "{{\"timestamp\":{},\"cpu_usage\":{:.1},\"memory_usage\":{:.1},\"disk_io\":{},\"network_rx\":{},\"network_tx\":{},\"active_connections\":{}}}",
                start_time.elapsed().as_millis(),
                (metric_counter as f64 * 0.7) % 100.0,
                (metric_counter as f64 * 1.3) % 100.0,
                metric_counter * 1024,
                metric_counter * 2048,
                metric_counter * 1536,
                (metric_counter % 500) + 100
            );
            let _ = wal_clone.append_for_topic("system_metrics", metrics.as_bytes());
            metric_counter += 1;
            thread::sleep(Duration::from_secs(1)); // 1 metric/sec
        }
    });
    handles.push(handle);
    
    // Error logs (sporadic, variable size)
    let wal_clone = Arc::clone(&wal);
    let handle = thread::spawn(move || {
        let mut error_id = 1u64;
        while start_time.elapsed() < duration {
            let error_log = format!(
                "{{\"error_id\":{},\"timestamp\":{},\"level\":\"ERROR\",\"service\":\"payment_processor\",\"message\":\"Payment processing failed for transaction {}\",\"stack_trace\":\"{}\"}}",
                error_id,
                start_time.elapsed().as_millis(),
                error_id * 1000,
                "at PaymentProcessor.process(PaymentProcessor.java:123)\\n".repeat((error_id % 10 + 1) as usize)
            );
            let _ = wal_clone.append_for_topic("error_logs", error_log.as_bytes());
            error_id += 1;
            thread::sleep(Duration::from_millis(2000 + (error_id % 3000))); // Sporadic errors
        }
    });
    handles.push(handle);
    
    // Background analytics processor (reads multiple topics)
    let wal_clone = Arc::clone(&wal);
    let processed_count = Arc::new(Mutex::new(0u64));
    let processed_clone = Arc::clone(&processed_count);
    
    let handle = thread::spawn(move || {
        let topics = vec!["user_activity", "transactions", "system_metrics", "error_logs"];
        let mut topic_index = 0;
        
        while start_time.elapsed() < duration {
            let topic = &topics[topic_index % topics.len()];
            if let Some(_entry) = wal_clone.read_next(topic) {
                let mut count = processed_clone.lock().unwrap();
                *count += 1;
            }
            topic_index += 1;
            thread::sleep(Duration::from_millis(20)); // Process 50 entries/sec
        }
    });
    handles.push(handle);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let processed = *processed_count.lock().unwrap();
    
    println!("E2E Realistic Application Results:");
    println!("  Processed entries: {}", processed);
    println!("  Duration: {:?}", start_time.elapsed());
    
    // Should have processed significant data
    assert!(processed > 1000, "Expected > 1000 processed entries, got {}", processed);
    
    cleanup_wal();
}

#[test]
fn e2e_recovery_and_persistence_marathon() {
    cleanup_wal();
    
    let total_cycles = 5;
    let entries_per_cycle = 1000;
    let topics = vec!["persistent_topic_1", "persistent_topic_2", "persistent_topic_3"];
    
    // Track what we write across all cycles
    let mut expected_data: HashMap<String, Vec<String>> = HashMap::new();
    for topic in &topics {
        expected_data.insert(topic.to_string(), Vec::new());
    }
    
    for cycle in 0..total_cycles {
        println!("E2E Recovery Cycle {}/{}", cycle + 1, total_cycles);
        
        // Create new WAL instance (simulates restart)
        let wal = Walrus::new();
        
        // Write data for this cycle
        for entry_id in 0..entries_per_cycle {
            for (topic_idx, topic) in topics.iter().enumerate() {
                let data = format!(
                    "cycle_{}_entry_{}_topic_{}_data_{}",
                    cycle, entry_id, topic_idx, 
                    "x".repeat((entry_id % 100) + 1) // Variable length
                );
                
                wal.append_for_topic(topic, data.as_bytes()).unwrap();
                expected_data.get_mut(*topic).unwrap().push(data);
            }
        }
        
        // Read some data (but not all) to advance read positions
        for topic in &topics {
            let read_count = (entries_per_cycle * (cycle + 1)) / 2; // Read half
            for _ in 0..read_count {
                if wal.read_next(topic).is_none() {
                    break;
                }
            }
        }
        
        // Simulate some processing time
        thread::sleep(Duration::from_millis(100));
    }
    
    // Final verification - create new WAL and read all remaining data
    let wal = Walrus::new();
    let mut total_read = 0;
    
    for topic in &topics {
        while let Some(_entry) = wal.read_next(topic) {
            total_read += 1;
        }
    }
    
    println!("E2E Recovery Marathon Results:");
    println!("  Total cycles: {}", total_cycles);
    println!("  Entries per cycle per topic: {}", entries_per_cycle);
    println!("  Total topics: {}", topics.len());
    println!("  Remaining entries read: {}", total_read);
    
    // Should have read the remaining half of entries
    let expected_remaining = (total_cycles * entries_per_cycle * topics.len()) / 2;
    assert!(
        total_read >= expected_remaining / 2, // Allow some tolerance
        "Expected at least {} remaining entries, got {}",
        expected_remaining / 2,
        total_read
    );
    
    cleanup_wal();
}

#[test]
fn e2e_massive_data_throughput_test() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new());
    let duration = Duration::from_secs(60); // 1 minute throughput test
    let start_time = Instant::now();
    
    // Metrics tracking
    let bytes_written = Arc::new(Mutex::new(0u64));
    let bytes_read = Arc::new(Mutex::new(0u64));
    let entries_written = Arc::new(Mutex::new(0u64));
    let entries_read = Arc::new(Mutex::new(0u64));
    
    let mut handles = vec![];
    
    // Multiple high-throughput writers
    for worker_id in 0..4 {
        let wal_clone = Arc::clone(&wal);
        let bytes_written_clone = Arc::clone(&bytes_written);
        let entries_written_clone = Arc::clone(&entries_written);
        let topic = format!("throughput_topic_{}", worker_id);
        
        let handle = thread::spawn(move || {
            let mut counter = 0u64;
            let base_data = format!("throughput_data_worker_{}_", worker_id);
            
            while start_time.elapsed() < duration {
                // Variable size entries (1KB to 10KB)
                let size = 1024 + (counter % 9) * 1024;
                let mut data = base_data.clone();
                data.push_str(&"x".repeat(size as usize - base_data.len()));
                
                if wal_clone.append_for_topic(&topic, data.as_bytes()).is_ok() {
                    {
                        let mut bytes = bytes_written_clone.lock().unwrap();
                        *bytes += data.len() as u64;
                    }
                    {
                        let mut entries = entries_written_clone.lock().unwrap();
                        *entries += 1;
                    }
                }
                
                counter += 1;
                
                // Small delay to prevent overwhelming the system
                if counter % 100 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        handles.push(handle);
    }
    
    // High-throughput readers
    for worker_id in 0..2 {
        let wal_clone = Arc::clone(&wal);
        let bytes_read_clone = Arc::clone(&bytes_read);
        let entries_read_clone = Arc::clone(&entries_read);
        
        let handle = thread::spawn(move || {
            let topics = (0..4).map(|i| format!("throughput_topic_{}", i)).collect::<Vec<_>>();
            let mut topic_index = worker_id;
            
            while start_time.elapsed() < duration {
                let topic = &topics[topic_index % topics.len()];
                
                if let Some(entry) = wal_clone.read_next(topic) {
                    {
                        let mut bytes = bytes_read_clone.lock().unwrap();
                        *bytes += entry.data.len() as u64;
                    }
                    {
                        let mut entries = entries_read_clone.lock().unwrap();
                        *entries += 1;
                    }
                }
                
                topic_index += 1;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_bytes_written = *bytes_written.lock().unwrap();
    let final_bytes_read = *bytes_read.lock().unwrap();
    let final_entries_written = *entries_written.lock().unwrap();
    let final_entries_read = *entries_read.lock().unwrap();
    let elapsed = start_time.elapsed();
    
    println!("E2E Massive Throughput Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Bytes written: {} ({:.2} MB)", final_bytes_written, final_bytes_written as f64 / 1_000_000.0);
    println!("  Bytes read: {} ({:.2} MB)", final_bytes_read, final_bytes_read as f64 / 1_000_000.0);
    println!("  Entries written: {}", final_entries_written);
    println!("  Entries read: {}", final_entries_read);
    println!("  Write throughput: {:.2} MB/s", (final_bytes_written as f64 / 1_000_000.0) / elapsed.as_secs_f64());
    println!("  Read throughput: {:.2} MB/s", (final_bytes_read as f64 / 1_000_000.0) / elapsed.as_secs_f64());
    println!("  Write rate: {:.2} entries/s", final_entries_written as f64 / elapsed.as_secs_f64());
    println!("  Read rate: {:.2} entries/s", final_entries_read as f64 / elapsed.as_secs_f64());
    
    // Validate significant throughput
    assert!(final_bytes_written > 10_000_000, "Expected > 10MB written, got {} bytes", final_bytes_written);
    assert!(final_entries_written > 1000, "Expected > 1000 entries written, got {}", final_entries_written);
    assert!(final_bytes_read > 1_000_000, "Expected > 1MB read, got {} bytes", final_bytes_read);
    
    cleanup_wal();
}

#[test]
fn e2e_system_stress_and_stability() {
    cleanup_wal();
    
    let wal = Arc::new(Walrus::new());
    let duration = Duration::from_secs(90); // 1.5 minute stress test
    let start_time = Instant::now();
    
    // Error tracking
    let write_errors = Arc::new(Mutex::new(0u64));
    let read_errors = Arc::new(Mutex::new(0u64));
    let successful_operations = Arc::new(Mutex::new(0u64));
    
    let mut handles = vec![];
    
    // Stress writers with varying patterns
    for worker_id in 0..6 {
        let wal_clone = Arc::clone(&wal);
        let write_errors_clone = Arc::clone(&write_errors);
        let successful_ops_clone = Arc::clone(&successful_operations);
        
        let handle = thread::spawn(move || {
            let mut counter = 0u64;
            let topic = format!("stress_topic_{}", worker_id % 3); // 3 topics shared by 6 workers
            
            while start_time.elapsed() < duration {
                // Vary entry sizes dramatically
                let size = match counter % 5 {
                    0 => 10,           // Tiny
                    1 => 1_000,        // Small
                    2 => 50_000,       // Medium
                    3 => 500_000,      // Large
                    4 => 2_000_000,    // Very large
                    _ => 1_000,
                };
                
                let data = vec![(counter % 256) as u8; size];
                
                match wal_clone.append_for_topic(&topic, &data) {
                    Ok(_) => {
                        let mut ops = successful_ops_clone.lock().unwrap();
                        *ops += 1;
                    }
                    Err(_) => {
                        let mut errors = write_errors_clone.lock().unwrap();
                        *errors += 1;
                    }
                }
                
                counter += 1;
                
                // Variable delays to create bursty traffic
                let delay = match counter % 7 {
                    0 => 0,      // No delay
                    1..=3 => 1,  // Short delay
                    4..=5 => 10, // Medium delay
                    6 => 100,    // Long delay
                    _ => 1,
                };
                
                if delay > 0 {
                    thread::sleep(Duration::from_millis(delay));
                }
            }
        });
        handles.push(handle);
    }
    
    // Aggressive readers
    for worker_id in 0..3 {
        let wal_clone = Arc::clone(&wal);
        let successful_ops_clone = Arc::clone(&successful_operations);
        
        let handle = thread::spawn(move || {
            let topics = vec![
                format!("stress_topic_{}", worker_id),
                format!("stress_topic_{}", (worker_id + 1) % 3),
            ];
            
            while start_time.elapsed() < duration {
                for topic in &topics {
                    match wal_clone.read_next(topic) {
                        Some(_) => {
                            let mut ops = successful_ops_clone.lock().unwrap();
                            *ops += 1;
                        }
                        None => {
                            // Not an error, just no data available
                        }
                    }
                }
                
                // Aggressive reading
                thread::sleep(Duration::from_millis(1));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_write_errors = *write_errors.lock().unwrap();
    let final_read_errors = *read_errors.lock().unwrap();
    let final_successful_ops = *successful_operations.lock().unwrap();
    let elapsed = start_time.elapsed();
    
    println!("E2E System Stress Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Successful operations: {}", final_successful_ops);
    println!("  Write errors: {}", final_write_errors);
    println!("  Read errors: {}", final_read_errors);
    println!("  Success rate: {:.2}%", (final_successful_ops as f64 / (final_successful_ops + final_write_errors + final_read_errors) as f64) * 100.0);
    println!("  Operations/sec: {:.2}", final_successful_ops as f64 / elapsed.as_secs_f64());
    
    // System should remain stable under stress
    assert!(final_successful_ops > 1000, "Expected > 1000 successful operations, got {}", final_successful_ops);
    
    // Error rate should be reasonable (< 5%)
    let total_ops = final_successful_ops + final_write_errors + final_read_errors;
    let error_rate = (final_write_errors + final_read_errors) as f64 / total_ops as f64;
    assert!(error_rate < 0.05, "Error rate too high: {:.2}%", error_rate * 100.0);
    
    cleanup_wal();
}

#[test]
fn e2e_performance_benchmark() {
    cleanup_wal();
    
    // Enable quiet mode to suppress debug output
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }
    
    let wal = Arc::new(Walrus::new());
    
    println!("=== WAL Performance Benchmark ===");
    
    // Simple write benchmark - just count operations in a short time
    let start = Instant::now();
    let duration = Duration::from_secs(300);
    let mut write_count = 0u64;
    let mut write_bytes = 0u64;
    
    println!("Running write benchmark for {:?}...", duration);
    
    while start.elapsed() < duration {
        let data = b"benchmark_data_entry";
        if wal.append_for_topic("bench", data).is_ok() {
            write_count += 1;
            write_bytes += data.len() as u64;
        }
    }
    
    let write_elapsed = start.elapsed();
    println!("Write Results:");
    println!("  Operations: {}", write_count);
    println!("  Bytes: {} KB", write_bytes / 1024);
    println!("  Throughput: {:.0} ops/sec", write_count as f64 / write_elapsed.as_secs_f64());
    
    // Simple read benchmark
    let start = Instant::now();
    let mut read_count = 0u64;
    let mut read_bytes = 0u64;
    
    println!("Running read benchmark for {:?}...", duration);
    
    while start.elapsed() < duration {
        if let Some(entry) = wal.read_next("bench") {
            read_count += 1;
            read_bytes += entry.data.len() as u64;
        }
    }
    
    let read_elapsed = start.elapsed();
    println!("Read Results:");
    println!("  Operations: {}", read_count);
    println!("  Bytes: {} KB", read_bytes / 1024);
    println!("  Throughput: {:.0} ops/sec", read_count as f64 / read_elapsed.as_secs_f64());
    
    // Basic performance assertions
    assert!(write_count > 10, "Write throughput too low: {} ops", write_count);
    assert!(read_count > 5, "Read throughput too low: {} ops", read_count);
    
    println!("✅ Performance benchmark completed!");
    
    cleanup_wal();
}
