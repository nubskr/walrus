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
fn multithreaded_read_benchmark() {
    cleanup_wal();

    // Enable quiet mode to suppress debug output during benchmark
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    println!("=== Multi-threaded WAL Read Benchmark ===");
    println!("Configuration: 10 threads, 1 minute write phase + 2 minutes read phase");

    let wal = Arc::new(
        Walrus::with_consistency(walrus::ReadConsistency::AtLeastOnce {
            persist_every: 5000,
        })
        .expect("Failed to create Walrus"),
    );
    let num_threads = 10;
    let write_duration = Duration::from_secs(60); // 1 minute write phase
    let read_duration = Duration::from_secs(120); // 2 minutes read phase

    // Shared counters for statistics
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_read_bytes = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(AtomicU64::new(0));

    // Create CSV file for throughput monitoring
    let csv_path = "read_benchmark_throughput.csv";
    let mut csv_file = fs::File::create(csv_path).expect("Failed to create CSV file");
    writeln!(csv_file, "timestamp,elapsed_seconds,phase,writes_per_second,reads_per_second,write_bytes_per_second,read_bytes_per_second,total_writes,total_reads").expect("Failed to write CSV header");

    // Channel for throughput monitoring
    let (throughput_tx, throughput_rx) = mpsc::channel::<String>();

    // Barriers to synchronize phases
    let write_start_barrier = Arc::new(Barrier::new(num_threads + 1));
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));
    let read_start_barrier = Arc::new(Barrier::new(num_threads + 1));
    let read_end_barrier = Arc::new(Barrier::new(num_threads + 1));

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

    println!("Starting {} writer/reader threads...", num_threads);

    // Spawn throughput monitoring thread
    let total_writes_monitor = Arc::clone(&total_writes);
    let total_write_bytes_monitor = Arc::clone(&total_write_bytes);
    let total_reads_monitor = Arc::clone(&total_reads);
    let total_read_bytes_monitor = Arc::clone(&total_read_bytes);

    let monitor_handle = thread::spawn(move || {
        let mut csv_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("read_benchmark_throughput.csv")
            .expect("Failed to open CSV file");

        let mut start_time = Instant::now();
        let mut last_writes = 0u64;
        let mut last_reads = 0u64;
        let mut last_write_bytes = 0u64;
        let mut last_read_bytes = 0u64;
        let mut last_time = start_time;
        let mut current_phase = "write";
        let mut tick_index: u64 = 0;

        // Wait for explicit start of write phase to avoid pre-start samples
        let _ = throughput_rx.recv(); // expect "write_start"
        // Initial zero entry at t=0 for write phase
        {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            writeln!(
                csv_file,
                "{},{:.2},{},{:.0},{:.0},{:.0},{:.0},{},{}",
                timestamp, 0.0, "write", 0.0, 0.0, 0.0, 0.0, 0, 0
            )
            .expect("Failed to write initial CSV entry");
            csv_file.flush().expect("Failed to flush CSV");
        }
        start_time = Instant::now();
        last_time = start_time;
        last_writes = 0;
        last_write_bytes = 0;
        tick_index = 0;

        loop {
            // Check for phase changes
            if let Ok(phase) = throughput_rx.try_recv() {
                current_phase = match phase.as_str() {
                    "read_start" => {
                        // Log initial state at time 0 for read phase
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let current_writes = total_writes_monitor.load(Ordering::Relaxed);
                        writeln!(
                            csv_file,
                            "{},{:.2},{},{:.0},{:.0},{:.0},{:.0},{},{}",
                            timestamp, 0.0, "read", 0.0, 0.0, 0.0, 0.0, current_writes, 0
                        )
                        .expect("Failed to write initial CSV entry");
                        csv_file.flush().expect("Failed to flush CSV");
                        start_time = Instant::now(); // Reset start time for read phase
                        last_time = start_time;
                        last_reads = 0;
                        last_read_bytes = 0;
                        tick_index = 0;
                        "read"
                    }
                    "end" => break,
                    _ => current_phase,
                };
                // After logging initial state, wait before next measurement
                thread::sleep(Duration::from_millis(500));
                continue;
            } else {
                thread::sleep(Duration::from_millis(500)); // Sample every 500ms
            }

            // Deterministic time base to avoid duplicate/rounded times
            tick_index += 1;
            let interval_s = 0.5f64;
            let elapsed_total = tick_index as f64 * interval_s;

            let current_time = Instant::now();
            let current_writes = total_writes_monitor.load(Ordering::Relaxed);
            let current_reads = total_reads_monitor.load(Ordering::Relaxed);
            let current_write_bytes = total_write_bytes_monitor.load(Ordering::Relaxed);
            let current_read_bytes = total_read_bytes_monitor.load(Ordering::Relaxed);

            // Calculate rates over fixed interval
            let writes_per_second = (current_writes - last_writes) as f64 / interval_s;
            let reads_per_second = (current_reads - last_reads) as f64 / interval_s;
            let write_bytes_per_second =
                (current_write_bytes - last_write_bytes) as f64 / interval_s;
            let read_bytes_per_second = (current_read_bytes - last_read_bytes) as f64 / interval_s;

            // Log if there's activity or every 2 seconds
            let has_activity = (current_writes != last_writes) || (current_reads != last_reads);
            let should_log = has_activity || (elapsed_total as u64 % 2 == 0);

            if should_log {
                // Write to CSV
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                writeln!(
                    csv_file,
                    "{},{:.2},{},{:.0},{:.0},{:.0},{:.0},{},{}",
                    timestamp,
                    elapsed_total,
                    current_phase,
                    writes_per_second,
                    reads_per_second,
                    write_bytes_per_second,
                    read_bytes_per_second,
                    current_writes,
                    current_reads
                )
                .expect("Failed to write to CSV");
                csv_file.flush().expect("Failed to flush CSV");

                // Print progress only if there's activity
                if has_activity {
                    if current_phase == "write" {
                        println!(
                            "[Monitor] {:.1}s [WRITE]: {:.0} writes/sec, {:.2} MB/sec, total: {} writes",
                            elapsed_total,
                            writes_per_second,
                            write_bytes_per_second / (1024.0 * 1024.0),
                            current_writes
                        );
                    } else {
                        println!(
                            "[Monitor] {:.1}s [READ]: {:.0} reads/sec, {:.2} MB/sec, total: {} reads",
                            elapsed_total,
                            reads_per_second,
                            read_bytes_per_second / (1024.0 * 1024.0),
                            current_reads
                        );
                    }
                }
            }

            last_writes = current_writes;
            last_reads = current_reads;
            last_write_bytes = current_write_bytes;
            last_read_bytes = current_read_bytes;
            last_time = current_time;
        }
    });

    // Spawn worker threads (each does both writing and reading)
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_writes_clone = Arc::clone(&total_writes);
        let total_write_bytes_clone = Arc::clone(&total_write_bytes);
        let total_reads_clone = Arc::clone(&total_reads);
        let total_read_bytes_clone = Arc::clone(&total_read_bytes);
        let write_errors_clone = Arc::clone(&write_errors);
        let read_errors_clone = Arc::clone(&read_errors);
        let write_start_barrier_clone = Arc::clone(&write_start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let read_start_barrier_clone = Arc::clone(&read_start_barrier);
        let read_end_barrier_clone = Arc::clone(&read_end_barrier);
        let topic = topics[thread_id].clone();

        let handle = thread::spawn(move || {
            // WRITE PHASE
            write_start_barrier_clone.wait();

            let write_start_time = Instant::now();
            let mut local_writes = 0u64;
            let mut local_write_bytes = 0u64;
            let mut local_write_errors = 0u64;
            let mut counter = 0u64;

            // Write phase - populate data for reading
            let mut rng = rand::thread_rng();

            while write_start_time.elapsed() < write_duration {
                // Random entry size between 500B and 1KB
                let size = rng.gen_range(500..=1024);
                let data = vec![(counter % 256) as u8; size];

                match wal_clone.append_for_topic(&topic, &data) {
                    Ok(_) => {
                        local_writes += 1;
                        local_write_bytes += data.len() as u64;
                        total_writes_clone.fetch_add(1, Ordering::Relaxed);
                        total_write_bytes_clone.fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        local_write_errors += 1;
                    }
                }

                counter += 1;

                // Small gap after every 50k writes for more sustainable writing
                if counter % 50000 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }

            write_errors_clone.fetch_add(local_write_errors, Ordering::Relaxed);

            println!(
                "Thread {} ({}): WRITE PHASE - {} writes, {} KB, {} errors",
                thread_id,
                topic,
                local_writes,
                local_write_bytes / 1024,
                local_write_errors
            );

            write_end_barrier_clone.wait();

            // READ PHASE
            read_start_barrier_clone.wait();

            let read_start_time = Instant::now();
            let mut local_reads = 0u64;
            let mut local_read_bytes = 0u64;
            let mut local_read_errors = 0u64;
            let mut consecutive_nulls = 0u32;

            // Read phase - consume all written data and continue reading
            while read_start_time.elapsed() < read_duration {
                match wal_clone.read_next(&topic) {
                    Ok(Some(entry)) => {
                        local_reads += 1;
                        local_read_bytes += entry.data.len() as u64;
                        total_reads_clone.fetch_add(1, Ordering::Relaxed);
                        total_read_bytes_clone
                            .fetch_add(entry.data.len() as u64, Ordering::Relaxed);
                        consecutive_nulls = 0;
                    }
                    Ok(None) => {
                        consecutive_nulls += 1;
                        // If we've caught up, sleep briefly to avoid spinning
                        if consecutive_nulls > 10 {
                            thread::sleep(Duration::from_micros(100));
                            consecutive_nulls = 0;
                        }
                    }
                    Err(_) => {
                        local_read_errors += 1;
                        consecutive_nulls += 1;
                        if consecutive_nulls > 100 {
                            thread::sleep(Duration::from_millis(1));
                            consecutive_nulls = 0;
                        }
                    }
                }
            }

            read_errors_clone.fetch_add(local_read_errors, Ordering::Relaxed);

            println!(
                "Thread {} ({}): READ PHASE - {} reads, {} KB, {} errors",
                thread_id,
                topic,
                local_reads,
                local_read_bytes / 1024,
                local_read_errors
            );

            read_end_barrier_clone.wait();
        });

        handles.push(handle);
    }

    // Start write phase
    let benchmark_start = Instant::now();
    let _ = throughput_tx.send("write_start".to_string());
    write_start_barrier.wait();
    println!("All threads started! Write phase beginning...");

    // Wait for write phase to complete
    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();
    println!("Write phase completed in {:?}", write_elapsed);

    // Print write results
    let writes_after_write_phase = total_writes.load(Ordering::Relaxed);
    let write_bytes_after_write_phase = total_write_bytes.load(Ordering::Relaxed);
    let write_errors_after_write_phase = write_errors.load(Ordering::Relaxed);

    println!("\n=== Write Phase Results ===");
    println!("Write Duration: {:?}", write_elapsed);
    println!("Total Writes: {}", writes_after_write_phase);
    println!(
        "Total Write Bytes: {} MB",
        write_bytes_after_write_phase / (1024 * 1024)
    );
    println!("Write Errors: {}", write_errors_after_write_phase);
    println!(
        "Write Throughput: {:.0} ops/sec",
        writes_after_write_phase as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "Write Bandwidth: {:.2} MB/sec",
        (write_bytes_after_write_phase as f64 / (1024.0 * 1024.0)) / write_elapsed.as_secs_f64()
    );
    println!();

    // Start read phase
    let read_phase_start = Instant::now();
    let _ = throughput_tx.send("read_start".to_string());
    read_start_barrier.wait();
    println!("Read phase beginning...");

    // Wait for read phase to complete
    read_end_barrier.wait();
    let read_elapsed = read_phase_start.elapsed();
    println!("Read phase completed in {:?}", read_elapsed);

    // Signal monitoring thread to stop
    let _ = throughput_tx.send("end".to_string());

    // Print read results
    let final_reads = total_reads.load(Ordering::Relaxed);
    let final_read_bytes = total_read_bytes.load(Ordering::Relaxed);
    let final_read_errors = read_errors.load(Ordering::Relaxed);

    println!("\n=== Read Phase Results ===");
    println!("Read Duration: {:?}", read_elapsed);
    println!("Total Reads: {}", final_reads);
    println!("Total Read Bytes: {} MB", final_read_bytes / (1024 * 1024));
    println!("Read Errors: {}", final_read_errors);
    println!(
        "Read Throughput: {:.0} ops/sec",
        final_reads as f64 / read_elapsed.as_secs_f64()
    );
    println!(
        "Read Bandwidth: {:.2} MB/sec",
        (final_read_bytes as f64 / (1024.0 * 1024.0)) / read_elapsed.as_secs_f64()
    );
    println!();

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join().unwrap();
    }

    let total_elapsed = benchmark_start.elapsed();

    println!("\n=== Final Summary ===");
    println!("Total Benchmark Duration: {:?}", total_elapsed);
    println!(
        "Data Written: {} MB",
        write_bytes_after_write_phase / (1024 * 1024)
    );
    println!("Data Read: {} MB", final_read_bytes / (1024 * 1024));
    println!(
        "Read/Write Ratio: {:.2}%",
        (final_read_bytes as f64 / write_bytes_after_write_phase as f64) * 100.0
    );

    // Performance assertions
    assert!(
        writes_after_write_phase > 500,
        "Write throughput too low: {} ops",
        writes_after_write_phase
    );
    assert!(
        final_reads > 500,
        "Read throughput too low: {} ops",
        final_reads
    );
    assert!(
        write_errors_after_write_phase < writes_after_write_phase / 10,
        "Too many write errors: {} out of {}",
        write_errors_after_write_phase,
        writes_after_write_phase
    );
    assert!(
        final_read_errors < final_reads / 10,
        "Too many read errors: {} out of {}",
        final_read_errors,
        final_reads
    );

    println!("Multi-threaded read benchmark completed successfully!");

    // Wait for monitoring thread to finish
    let _ = monitor_handle.join();
    println!("Throughput data saved to: {}", csv_path);

    cleanup_wal();
}
