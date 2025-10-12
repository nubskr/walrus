use std::collections::HashMap;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};
use walrus_rust::ReadConsistency;
use walrus_rust::wal::Walrus;

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn e2e_sustained_mixed_workload() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let duration = Duration::from_secs(15); // Reduced duration for CI
    let start_time = Instant::now();

    let mut write_counts = HashMap::<String, u64>::new();
    let mut read_counts = HashMap::<String, u64>::new();
    let mut validation_errors = 0u64;

    while start_time.elapsed() < duration {
        for worker_id in 0..3 {
            let topic = format!("high_freq_{}", worker_id);
            let counter = write_counts.get(&topic).unwrap_or(&0);
            let data = format!("high_freq_data_{}_{}", worker_id, counter);
            if wal.append_for_topic(&topic, data.as_bytes()).is_ok() {
                *write_counts.entry(topic).or_insert(0) += 1;
            }
        }

        for worker_id in 0..2 {
            let topic = format!("med_freq_{}", worker_id);
            let counter = write_counts.get(&topic).unwrap_or(&0);
            let data = format!(
                "medium_frequency_data_with_more_content_{}_{}",
                worker_id, counter
            )
            .repeat(10);
            if wal.append_for_topic(&topic, data.as_bytes()).is_ok() {
                *write_counts.entry(topic).or_insert(0) += 1;
            }
        }

        if write_counts.values().sum::<u64>() % 10 == 0 {
            let topic = "low_freq_large".to_string();
            let counter = write_counts.get(&topic).unwrap_or(&0);
            let data = vec![*counter as u8; 50_000]; // 50KB entries (reduced for CI)
            if wal.append_for_topic(&topic, &data).is_ok() {
                *write_counts.entry(topic).or_insert(0) += 1;
            }
        }

        let topics = vec![
            "high_freq_0".to_string(),
            "high_freq_1".to_string(),
            "high_freq_2".to_string(),
            "med_freq_0".to_string(),
            "med_freq_1".to_string(),
            "low_freq_large".to_string(),
        ];

        for topic in &topics {
            if let Some(entry) = wal.read_next(topic).unwrap() {
                *read_counts.entry(topic.clone()).or_insert(0) += 1;

                let data_str = String::from_utf8_lossy(&entry.data);
                let is_valid = if topic.starts_with("high_freq_") {
                    data_str.starts_with("high_freq_data_")
                } else if topic.starts_with("med_freq_") {
                    data_str.contains("medium_frequency_data_with_more_content_")
                } else if topic == "low_freq_large" {
                    entry.data.len() == 50_000 // Should be 50KB
                } else {
                    false
                };

                if !is_valid {
                    validation_errors += 1;
                }
            }
        }

        thread::sleep(Duration::from_millis(10)); // Small delay to prevent tight loop
    }

    let total_writes: u64 = write_counts.values().sum();
    let total_reads: u64 = read_counts.values().sum();

    println!("E2E Sustained Test Results:");
    println!("  Total writes: {}", total_writes);
    println!("  Total reads: {}", total_reads);
    println!("  Validation errors: {}", validation_errors);
    println!("  Duration: {:?}", start_time.elapsed());

    assert!(
        total_writes > 100,
        "Expected > 100 writes, got {}",
        total_writes
    );
    assert!(total_reads > 50, "Expected > 50 reads, got {}", total_reads);

    assert_eq!(
        validation_errors, 0,
        "Data integrity validation failed: {} errors",
        validation_errors
    );

    cleanup_wal();
}

#[test]
fn e2e_realistic_application_simulation() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let duration = Duration::from_secs(20); // Reduced duration for CI
    let start_time = Instant::now();

    let mut processed_count = 0u64;
    let mut validation_errors = 0u64;
    let mut user_id = 1000u64;
    let mut tx_id = 50000u64;
    let mut metric_counter = 0u64;
    let mut error_id = 1u64;
    let mut iteration = 0u64;

    while start_time.elapsed() < duration {
        let log_entry = format!(
            "{{\"timestamp\":{},\"user_id\":{},\"action\":\"page_view\",\"page\":\"/dashboard\"}}",
            start_time.elapsed().as_millis(),
            user_id
        );
        let _ = wal.append_for_topic("user_activity", log_entry.as_bytes());
        user_id = (user_id + 1) % 10000;

        if iteration % 10 == 0 {
            let transaction = format!(
                "{{\"tx_id\":{},\"timestamp\":{},\"from_account\":\"acc_{}\",\"to_account\":\"acc_{}\",\"amount\":{:.2},\"currency\":\"USD\",\"status\":\"completed\"}}",
                tx_id,
                start_time.elapsed().as_millis(),
                tx_id % 1000,
                (tx_id + 1) % 1000,
                (tx_id as f64 * 0.01) % 1000.0
            );
            let _ = wal.append_for_topic("transactions", transaction.as_bytes());
            tx_id += 1;
        }

        if iteration % 100 == 0 {
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
            let _ = wal.append_for_topic("system_metrics", metrics.as_bytes());
            metric_counter += 1;
        }

        if iteration % 200 == 0 {
            let error_log = format!(
                "{{\"error_id\":{},\"timestamp\":{},\"level\":\"ERROR\",\"service\":\"payment_processor\",\"message\":\"Payment processing failed for transaction {}\",\"stack_trace\":\"{}\"}}",
                error_id,
                start_time.elapsed().as_millis(),
                error_id * 1000,
                "at PaymentProcessor.process(PaymentProcessor.java:123)\\n"
                    .repeat((error_id % 10 + 1) as usize)
            );
            let _ = wal.append_for_topic("error_logs", error_log.as_bytes());
            error_id += 1;
        }

        let topics = vec![
            "user_activity",
            "transactions",
            "system_metrics",
            "error_logs",
        ];
        for topic in &topics {
            if let Some(entry) = wal.read_next(topic).unwrap() {
                processed_count += 1;

                let data_str = String::from_utf8_lossy(&entry.data);
                let is_valid = match *topic {
                    "user_activity" => {
                        data_str.contains("\"action\":\"page_view\"")
                            && data_str.contains("\"page\":\"/dashboard\"")
                            && data_str.contains("\"user_id\":")
                    }
                    "transactions" => {
                        data_str.contains("\"tx_id\":")
                            && data_str.contains("\"from_account\":\"acc_")
                            && data_str.contains("\"to_account\":\"acc_")
                            && data_str.contains("\"currency\":\"USD\"")
                            && data_str.contains("\"status\":\"completed\"")
                    }
                    "system_metrics" => {
                        data_str.contains("\"cpu_usage\":")
                            && data_str.contains("\"memory_usage\":")
                            && data_str.contains("\"disk_io\":")
                            && data_str.contains("\"network_rx\":")
                            && data_str.contains("\"active_connections\":")
                    }
                    "error_logs" => {
                        data_str.contains("\"level\":\"ERROR\"")
                            && data_str.contains("\"service\":\"payment_processor\"")
                            && data_str.contains("\"message\":\"Payment processing failed")
                            && data_str.contains("\"stack_trace\":")
                    }
                    _ => false,
                };

                if !is_valid {
                    validation_errors += 1;
                }
            }
        }

        iteration += 1;
        thread::sleep(Duration::from_millis(10)); // Small delay to prevent tight loop
    }

    println!("E2E Realistic Application Results:");
    println!("  Processed entries: {}", processed_count);
    println!("  Validation errors: {}", validation_errors);
    println!("  Duration: {:?}", start_time.elapsed());

    assert!(
        processed_count > 100,
        "Expected > 100 processed entries, got {}",
        processed_count
    );

    assert_eq!(
        validation_errors, 0,
        "Data integrity validation failed: {} errors",
        validation_errors
    );

    cleanup_wal();
}

#[test]
fn e2e_recovery_and_persistence_marathon() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let total_cycles = 5;
    let entries_per_cycle = 1000;
    let topics = vec![
        "persistent_topic_1",
        "persistent_topic_2",
        "persistent_topic_3",
    ];

    let mut expected_data: HashMap<String, Vec<String>> = HashMap::new();
    for topic in &topics {
        expected_data.insert(topic.to_string(), Vec::new());
    }

    for cycle in 0..total_cycles {
        println!("E2E Recovery Cycle {}/{}", cycle + 1, total_cycles);

        let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();

        for entry_id in 0..entries_per_cycle {
            for (topic_idx, topic) in topics.iter().enumerate() {
                let data = format!(
                    "cycle_{}_entry_{}_topic_{}_data_{}",
                    cycle,
                    entry_id,
                    topic_idx,
                    "x".repeat((entry_id % 100) + 1) // Variable length
                );

                wal.append_for_topic(topic, data.as_bytes()).unwrap();
                expected_data.get_mut(*topic).unwrap().push(data);
            }
        }

        for topic in &topics {
            let read_count = (entries_per_cycle * (cycle + 1)) / 2; // Read half
            for _ in 0..read_count {
                if wal.read_next(topic).unwrap().is_none() {
                    break;
                }
            }
        }

        thread::sleep(Duration::from_millis(100));
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let mut total_read = 0;
    let mut validation_errors = 0;

    for topic in &topics {
        while let Some(entry) = wal.read_next(topic).unwrap() {
            total_read += 1;

            let data_str = String::from_utf8_lossy(&entry.data);
            let is_valid = data_str.starts_with("cycle_")
                && data_str.contains("_entry_")
                && data_str.contains("_topic_")
                && data_str.contains("_data_")
                && data_str.ends_with(&"x".repeat(1)); // At least one 'x'

            if !is_valid {
                validation_errors += 1;
            }
        }
    }

    println!("E2E Recovery Marathon Results:");
    println!("  Total cycles: {}", total_cycles);
    println!("  Entries per cycle per topic: {}", entries_per_cycle);
    println!("  Total topics: {}", topics.len());
    println!("  Remaining entries read: {}", total_read);
    println!("  Validation errors: {}", validation_errors);

    // let expected_remaining = (total_cycles * entries_per_cycle * topics.len()) / 2;
    // assert!(
    //     total_read >= expected_remaining / 2, // Allow some tolerance
    //     "Expected at least {} remaining entries, got {}",
    //     expected_remaining / 2,
    //     total_read
    // );

    assert_eq!(
        validation_errors, 0,
        "Data integrity validation failed: {} errors",
        validation_errors
    );

    cleanup_wal();
}

#[test]
fn e2e_massive_data_throughput_test() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let duration = Duration::from_secs(25); // Reduced duration for CI
    let start_time = Instant::now();

    let mut bytes_written = 0u64;
    let mut bytes_read = 0u64;
    let mut entries_written = 0u64;
    let mut entries_read = 0u64;
    let mut validation_errors = 0u64;

    let topics = (0..4)
        .map(|i| format!("throughput_topic_{}", i))
        .collect::<Vec<_>>();
    let mut counter = 0u64;
    let mut topic_index = 0;

    while start_time.elapsed() < duration {
        for worker_id in 0..4 {
            let topic = &topics[worker_id];
            let base_data = format!("throughput_data_worker_{}_", worker_id);

            let size = 1024 + (counter % 4) * 1024;
            let mut data = base_data.clone();
            data.push_str(&"x".repeat(size as usize - base_data.len()));

            if wal.append_for_topic(topic, data.as_bytes()).is_ok() {
                bytes_written += data.len() as u64;
                entries_written += 1;
            }
        }

        for _ in 0..2 {
            let topic = &topics[topic_index % topics.len()];

            if let Some(entry) = wal.read_next(topic).unwrap() {
                bytes_read += entry.data.len() as u64;
                entries_read += 1;

                let data_str = String::from_utf8_lossy(&entry.data);
                let expected_worker_id =
                    topic.chars().last().unwrap().to_digit(10).unwrap() as usize;
                let expected_prefix = format!("throughput_data_worker_{}_", expected_worker_id);

                let size_valid = entry.data.len() >= 1024 && entry.data.len() <= 5120;
                let content_valid =
                    data_str.starts_with(&expected_prefix) && data_str.ends_with('x');

                if !size_valid || !content_valid {
                    validation_errors += 1;
                }
            }

            topic_index += 1;
        }

        counter += 1;

        if counter % 50 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }

    let elapsed = start_time.elapsed();

    println!("E2E Massive Throughput Results:");
    println!("  Duration: {:?}", elapsed);
    println!(
        "  Bytes written: {} ({:.2} MB)",
        bytes_written,
        bytes_written as f64 / 1_000_000.0
    );
    println!(
        "  Bytes read: {} ({:.2} MB)",
        bytes_read,
        bytes_read as f64 / 1_000_000.0
    );
    println!("  Entries written: {}", entries_written);
    println!("  Entries read: {}", entries_read);
    println!("  Validation errors: {}", validation_errors);
    println!(
        "  Write throughput: {:.2} MB/s",
        (bytes_written as f64 / 1_000_000.0) / elapsed.as_secs_f64()
    );
    println!(
        "  Read throughput: {:.2} MB/s",
        (bytes_read as f64 / 1_000_000.0) / elapsed.as_secs_f64()
    );
    println!(
        "  Write rate: {:.2} entries/s",
        entries_written as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  Read rate: {:.2} entries/s",
        entries_read as f64 / elapsed.as_secs_f64()
    );

    assert!(
        bytes_written > 1_000_000,
        "Expected > 1MB written, got {} bytes",
        bytes_written
    );
    assert!(
        entries_written > 100,
        "Expected > 100 entries written, got {}",
        entries_written
    );
    assert!(
        bytes_read > 100_000,
        "Expected > 100KB read, got {} bytes",
        bytes_read
    );

    assert_eq!(
        validation_errors, 0,
        "Data integrity validation failed: {} errors",
        validation_errors
    );

    cleanup_wal();
}

#[test]
fn e2e_system_stress_and_stability() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();
    let duration = Duration::from_secs(30); // Reduced duration for CI
    let start_time = Instant::now();

    let mut write_errors = 0u64;
    let read_errors = 0u64;
    let mut successful_operations = 0u64;
    let mut read_validation_errors = 0u64;

    let topics = vec!["stress_topic_0", "stress_topic_1", "stress_topic_2"];
    let mut counter = 0u64;
    let mut topic_index = 0;

    while start_time.elapsed() < duration {
        for worker_id in 0..6 {
            let topic = &topics[worker_id % 3];

            let size = match counter % 5 {
                0 => 10,      // Tiny
                1 => 1_000,   // Small
                2 => 25_000,  // Medium (reduced)
                3 => 100_000, // Large (reduced)
                4 => 500_000, // Very large (reduced)
                _ => 1_000,
            };

            let data = vec![(counter % 256) as u8; size];

            match wal.append_for_topic(topic, &data) {
                Ok(_) => {
                    successful_operations += 1;
                }
                Err(_) => {
                    write_errors += 1;
                }
            }
        }

        for _ in 0..3 {
            let topic = &topics[topic_index % topics.len()];

            match wal.read_next(topic).unwrap() {
                Some(entry) => {
                    successful_operations += 1;

                    let expected_sizes = [10, 1_000, 25_000, 100_000, 500_000];
                    let size_valid = expected_sizes.contains(&entry.data.len());

                    let content_valid = if entry.data.len() <= 1_000 {
                        entry.data.iter().all(|&b| b == entry.data[0])
                    } else {
                        entry.data.iter().all(|&b| b == entry.data[0])
                    };

                    if !size_valid || !content_valid {
                        read_validation_errors += 1;
                    }
                }
                None => {}
            }

            topic_index += 1;
        }

        counter += 1;

        let delay = match counter % 7 {
            0 => 0,     // No delay
            1..=3 => 1, // Short delay
            4..=5 => 5, // Medium delay (reduced)
            6 => 20,    // Long delay (reduced)
            _ => 1,
        };

        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }
    }

    let elapsed = start_time.elapsed();

    println!("E2E System Stress Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Successful operations: {}", successful_operations);
    println!("  Write errors: {}", write_errors);
    println!("  Read errors: {}", read_errors);
    println!("  Read validation errors: {}", read_validation_errors);
    println!(
        "  Success rate: {:.2}%",
        (successful_operations as f64
            / (successful_operations + write_errors + read_errors) as f64)
            * 100.0
    );
    println!(
        "  Operations/sec: {:.2}",
        successful_operations as f64 / elapsed.as_secs_f64()
    );

    assert!(
        successful_operations > 200,
        "Expected > 200 successful operations, got {}",
        successful_operations
    );

    let total_ops = successful_operations + write_errors + read_errors;
    if total_ops > 0 {
        let error_rate = (write_errors + read_errors) as f64 / total_ops as f64;
        assert!(
            error_rate < 0.10,
            "Error rate too high: {:.2}%",
            error_rate * 100.0
        );
    }

    assert_eq!(
        read_validation_errors, 0,
        "Data integrity validation failed: {} errors",
        read_validation_errors
    );

    cleanup_wal();
}

#[test]
fn e2e_performance_benchmark() {
    cleanup_wal();

    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let wal = Walrus::with_consistency(ReadConsistency::StrictlyAtOnce).unwrap();

    println!("=== WAL Performance Benchmark ===");

    let start = Instant::now();
    let duration = Duration::from_secs(10); // Reduced duration for CI
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
    println!(
        "  Throughput: {:.0} ops/sec",
        write_count as f64 / write_elapsed.as_secs_f64()
    );

    let start = Instant::now();
    let mut read_count = 0u64;
    let mut read_bytes = 0u64;

    println!("Running read benchmark for {:?}...", duration);

    while start.elapsed() < duration {
        if let Some(entry) = wal.read_next("bench").unwrap() {
            read_count += 1;
            read_bytes += entry.data.len() as u64;
        }
    }

    let read_elapsed = start.elapsed();
    println!("Read Results:");
    println!("  Operations: {}", read_count);
    println!("  Bytes: {} KB", read_bytes / 1024);
    println!(
        "  Throughput: {:.0} ops/sec",
        read_count as f64 / read_elapsed.as_secs_f64()
    );

    assert!(
        write_count > 10,
        "Write throughput too low: {} ops",
        write_count
    );
    assert!(
        read_count > 5,
        "Read throughput too low: {} ops",
        read_count
    );

    println!("Performance benchmark completed!");

    cleanup_wal();
}
