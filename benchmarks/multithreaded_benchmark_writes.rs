use rand::Rng;
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use walrus::wal::Walrus;

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    // Give filesystem time to clean up
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn multithreaded_benchmark() {
    cleanup_wal();

    // Enable quiet mode to suppress debug output during benchmark
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    println!("=== Multi-threaded WAL Benchmark ===");
    println!("Configuration: 10 threads, 2 minutes write phase only");

    let wal = Arc::new(
        Walrus::with_consistency(walrus::ReadConsistency::AtLeastOnce { persist_every: 50 })
            .expect("Failed to create Walrus"),
    );
    let num_threads = 10;
    let write_duration = Duration::from_secs(120); // 2 minutes

    // Shared counters for statistics
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));

    // Create CSV file for throughput monitoring
    let csv_path = "benchmark_throughput.csv";
    let mut csv_file = fs::File::create(csv_path).expect("Failed to create CSV file");
    writeln!(
        csv_file,
        "timestamp,elapsed_seconds,writes_per_second,bytes_per_second,total_writes,total_bytes"
    )
    .expect("Failed to write CSV header");

    // Channel for throughput monitoring
    let (throughput_tx, throughput_rx) = mpsc::channel::<()>();

    // Barrier to synchronize thread start
    let start_barrier = Arc::new(Barrier::new(num_threads + 1)); // +1 for main thread
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));

    // Topic names for each thread
    let topics = vec![
        "topic_0".to_string(),
        "topic_1".to_string(),
        "topic_2".to_string(),
        "topic_3".to_string(),
        "topic_4".to_string(),
        "topic_5".to_string(),
        "topic_6".to_string(),
        "topic_7".to_string(),
        "topic_8".to_string(),
        "topic_9".to_string(),
    ];

    println!("Starting {} writer threads...", num_threads);

    // Spawn throughput monitoring thread
    let total_writes_monitor = Arc::clone(&total_writes);
    let total_write_bytes_monitor = Arc::clone(&total_write_bytes);
    let throughput_tx_clone = throughput_tx.clone();

    let monitor_handle = thread::spawn(move || {
        let mut csv_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("benchmark_throughput.csv")
            .expect("Failed to open CSV file");

        let mut start_time = Instant::now();
        let mut last_writes = 0u64;
        let mut last_bytes = 0u64;
        let mut last_time = start_time;
        let mut tick_index: u64 = 0;

        // Wait for benchmark to start
        let _ = throughput_rx.recv();

        // Log initial state at time 0
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        writeln!(
            csv_file,
            "{},{:.2},{:.0},{:.0},{},{}",
            timestamp, 0.0, 0.0, 0.0, 0, 0
        )
        .expect("Failed to write initial CSV entry");
        csv_file.flush().expect("Failed to flush CSV");

        // Reset timing and wait before first measurement
        start_time = Instant::now();
        last_time = start_time;
        thread::sleep(Duration::from_millis(500));

        loop {
            thread::sleep(Duration::from_millis(500)); // Sample every 500ms for better granularity

            // Deterministic time base to avoid duplicate/rounded times
            tick_index += 1;
            let interval_s = 0.5f64;
            let elapsed_total = tick_index as f64 * interval_s;

            let current_time = Instant::now();
            let current_writes = total_writes_monitor.load(Ordering::Relaxed);
            let current_bytes = total_write_bytes_monitor.load(Ordering::Relaxed);

            // Calculate rates over fixed interval
            let writes_per_second = (current_writes - last_writes) as f64 / interval_s;
            let bytes_per_second = (current_bytes - last_bytes) as f64 / interval_s;

            // Only log if there's been some change or every 2 seconds (4 ticks of 0.5s)
            let should_log = (current_writes != last_writes) || (tick_index % 4 == 0);

            if should_log {
                // Write to CSV
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                writeln!(
                    csv_file,
                    "{},{:.2},{:.0},{:.0},{},{}",
                    timestamp,
                    elapsed_total,
                    writes_per_second,
                    bytes_per_second,
                    current_writes,
                    current_bytes
                )
                .expect("Failed to write to CSV");
                csv_file.flush().expect("Failed to flush CSV");

                // Print progress only if there's activity
                if current_writes > last_writes {
                    println!(
                        "[Monitor] {:.1}s: {:.0} writes/sec, {:.2} MB/sec, total: {} writes",
                        elapsed_total,
                        writes_per_second,
                        bytes_per_second / (1024.0 * 1024.0),
                        current_writes
                    );
                }
            }

            last_writes = current_writes;
            last_bytes = current_bytes;
            last_time = current_time;

            // Stop monitoring after write phase (roughly 2 minutes + buffer)
            if elapsed_total > 150.0 {
                break;
            }
        }
    });

    // Spawn writer threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_writes_clone = Arc::clone(&total_writes);
        let total_write_bytes_clone = Arc::clone(&total_write_bytes);
        let write_errors_clone = Arc::clone(&write_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            start_barrier_clone.wait();

            let start_time = Instant::now();
            let mut local_writes = 0u64;
            let mut local_write_bytes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;

            // Write phase - write as fast as possible for 2 minutes
            let mut rng = rand::thread_rng();

            while start_time.elapsed() < write_duration {
                // Random entry size between 500B and 1KB
                let size = rng.gen_range(500..=1024);
                let data = vec![(counter % 256) as u8; size];

                match wal_clone.append_for_topic(&topic, &data) {
                    Ok(_) => {
                        local_writes += 1;
                        local_write_bytes += data.len() as u64;
                        // Update global counters in real-time so the monitor sees progress
                        total_writes_clone.fetch_add(1, Ordering::Relaxed);
                        total_write_bytes_clone.fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        local_errors += 1;
                    }
                }

                counter += 1;

                // Small gap after every 100k writes (50μs) - reduced frequency
                if counter % 100000 == 0 {
                    thread::sleep(Duration::from_micros(50));
                }
            }

            // Persist error count at the end (writes/bytes are already tracked live)
            write_errors_clone.fetch_add(local_errors, Ordering::Relaxed);

            println!(
                "Thread {} ({}): {} writes, {} KB, {} errors",
                thread_id,
                topic,
                local_writes,
                local_write_bytes / 1024,
                local_errors
            );

            // Wait for all threads to finish writing
            write_end_barrier_clone.wait();
        });

        handles.push(handle);
    }

    // Start all threads simultaneously
    let benchmark_start = Instant::now();
    start_barrier.wait();
    println!("All threads started! Write phase beginning...");

    // Signal monitoring thread to start
    let _ = throughput_tx.send(());

    // Wait for write phase to complete
    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();
    println!("Write phase completed in {:?}", write_elapsed);

    // Print write results immediately
    let final_writes = total_writes.load(Ordering::Relaxed);
    let final_write_bytes = total_write_bytes.load(Ordering::Relaxed);
    let final_errors = write_errors.load(Ordering::Relaxed);

    println!("\n=== Write Phase Results ===");
    println!("Write Duration: {:?}", write_elapsed);
    println!("Total Operations: {}", final_writes);
    println!("Total Bytes: {} MB", final_write_bytes / (1024 * 1024));
    println!("Write Errors: {}", final_errors);
    println!(
        "Write Throughput: {:.0} ops/sec",
        final_writes as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "Write Bandwidth: {:.2} MB/sec",
        (final_write_bytes as f64 / (1024.0 * 1024.0)) / write_elapsed.as_secs_f64()
    );
    println!();

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join().unwrap();
    }

    let total_elapsed = benchmark_start.elapsed();

    println!("\n=== Final Summary ===");
    println!("Total Benchmark Duration: {:?}", total_elapsed);

    // Get final write stats for assertions
    let final_writes = total_writes.load(Ordering::Relaxed);
    let final_errors = write_errors.load(Ordering::Relaxed);

    // Performance assertions
    assert!(
        final_writes > 1000,
        "Write throughput too low: {} ops",
        final_writes
    );
    assert!(
        final_errors < final_writes / 10,
        "Too many write errors: {} out of {}",
        final_errors,
        final_writes
    );

    println!("Multi-threaded benchmark completed successfully!");

    // Wait for monitoring thread to finish
    let _ = monitor_handle.join();
    println!("Throughput data saved to: {}", csv_path);

    cleanup_wal();
}
