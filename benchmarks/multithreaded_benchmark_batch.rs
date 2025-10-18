use rand::Rng;
use std::env;
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use walrus_rust::wal::{FsyncSchedule, ReadConsistency, Walrus};

// Function to get system memory information including dirty pages
fn get_memory_info() -> (u64, u64, f64) {
    // Returns (total_memory_kb, dirty_pages_kb, dirty_ratio)

    #[cfg(target_os = "macos")]
    {
        // On macOS, we can get memory info from vm_stat and sysctl
        let total_memory = get_macos_total_memory();
        let dirty_pages = get_macos_dirty_pages();
        let dirty_ratio = if total_memory > 0 {
            (dirty_pages as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };
        (total_memory, dirty_pages, dirty_ratio)
    }

    #[cfg(target_os = "linux")]
    {
        // On Linux, read from /proc/meminfo
        get_linux_memory_info()
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        // Fallback for other systems
        (0, 0, 0.0)
    }
}

#[cfg(target_os = "macos")]
fn get_macos_total_memory() -> u64 {
    use std::process::Command;

    if let Ok(output) = Command::new("sysctl").args(&["-n", "hw.memsize"]).output() {
        if let Ok(memsize_str) = String::from_utf8(output.stdout) {
            if let Ok(memsize_bytes) = memsize_str.trim().parse::<u64>() {
                return memsize_bytes / 1024; // Convert to KB
            }
        }
    }
    0
}

#[cfg(target_os = "macos")]
fn get_macos_dirty_pages() -> u64 {
    use std::process::Command;

    if let Ok(output) = Command::new("vm_stat").output() {
        if let Ok(vm_stat_str) = String::from_utf8(output.stdout) {
            // Parse vm_stat output to find dirty pages
            for line in vm_stat_str.lines() {
                if line.contains("Pages modified:") {
                    // Extract the number from "Pages modified: 12345."
                    if let Some(pages_str) = line.split_whitespace().nth(2) {
                        if let Ok(pages) = pages_str.trim_end_matches('.').parse::<u64>() {
                            // vm_stat reports in pages, typically 4KB each on macOS
                            return pages * 4; // Convert to KB
                        }
                    }
                }
            }
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn get_linux_memory_info() -> (u64, u64, f64) {
    let mut total_memory = 0u64;
    let mut dirty_pages = 0u64;

    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    total_memory = kb_str.parse().unwrap_or(0);
                }
            } else if line.starts_with("Dirty:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    dirty_pages = kb_str.parse().unwrap_or(0);
                }
            }
        }
    }

    let dirty_ratio = if total_memory > 0 {
        (dirty_pages as f64 / total_memory as f64) * 100.0
    } else {
        0.0
    };

    (total_memory, dirty_pages, dirty_ratio)
}

fn parse_fsync_schedule() -> FsyncSchedule {
    // Check environment variable first (for Makefile integration)
    if let Ok(fsync_env) = env::var("WALRUS_FSYNC") {
        match fsync_env.as_str() {
            "sync-each" => return FsyncSchedule::SyncEach,
            "no-fsync" | "none" => return FsyncSchedule::NoFsync,
            "async" => return FsyncSchedule::Milliseconds(1000),
            ms_str if ms_str.ends_with("ms") => {
                if let Ok(ms) = ms_str[..ms_str.len() - 2].parse::<u64>() {
                    return FsyncSchedule::Milliseconds(ms);
                }
            }
            ms_str => {
                if let Ok(ms) = ms_str.parse::<u64>() {
                    return FsyncSchedule::Milliseconds(ms);
                }
            }
        }
    }

    // Check command line arguments (for direct cargo test usage)
    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--fsync" && i + 1 < args.len() {
            match args[i + 1].as_str() {
                "sync-each" => return FsyncSchedule::SyncEach,
                "no-fsync" | "none" => return FsyncSchedule::NoFsync,
                "async" => return FsyncSchedule::Milliseconds(1000),
                ms_str if ms_str.ends_with("ms") => {
                    if let Ok(ms) = ms_str[..ms_str.len() - 2].parse::<u64>() {
                        return FsyncSchedule::Milliseconds(ms);
                    }
                }
                ms_str => {
                    if let Ok(ms) = ms_str.parse::<u64>() {
                        return FsyncSchedule::Milliseconds(ms);
                    }
                }
            }
        }
    }

    // Default to async (1000ms)
    FsyncSchedule::Milliseconds(1000)
}

fn parse_batch_size() -> usize {
    // Check environment variable first
    if let Ok(batch_env) = env::var("WALRUS_BATCH_SIZE") {
        if let Ok(size) = batch_env.parse::<usize>() {
            if size > 0 && size <= 10000000 {
                return size;
            }
        }
    }

    // Check command line arguments
    let args: Vec<String> = env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--batch-size" && i + 1 < args.len() {
            if let Ok(size) = args[i + 1].parse::<usize>() {
                if size > 0 && size <= 10000000 {
                    return size;
                }
            }
        }
    }

    // Default batch size (halved again)
    2047
}

fn parse_duration() -> Duration {
    // Check environment variable first (for Makefile integration)
    if let Ok(duration_env) = env::var("WALRUS_DURATION") {
        if let Some(duration) = parse_duration_string(&duration_env) {
            return duration;
        }
    }

    // Check command line arguments (for direct cargo test usage)
    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--duration" && i + 1 < args.len() {
            if let Some(duration) = parse_duration_string(&args[i + 1]) {
                return duration;
            }
        }
    }

    // Default to 2 minutes (120 seconds)
    Duration::from_secs(120)
}

fn parse_duration_string(duration_str: &str) -> Option<Duration> {
    if duration_str.ends_with("s") {
        // Parse seconds: "30s", "120s"
        if let Ok(secs) = duration_str[..duration_str.len() - 1].parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
    } else if duration_str.ends_with("m") {
        // Parse minutes: "2m", "5m"
        if let Ok(mins) = duration_str[..duration_str.len() - 1].parse::<u64>() {
            return Some(Duration::from_secs(mins * 60));
        }
    } else if duration_str.ends_with("h") {
        // Parse hours: "1h", "2h"
        if let Ok(hours) = duration_str[..duration_str.len() - 1].parse::<u64>() {
            return Some(Duration::from_secs(hours * 3600));
        }
    } else if let Ok(secs) = duration_str.parse::<u64>() {
        // Parse raw seconds: "120", "300"
        return Some(Duration::from_secs(secs));
    }
    None
}

fn print_usage() {
    println!(
        "Usage: WALRUS_FSYNC=<schedule> WALRUS_DURATION=<duration> WALRUS_BATCH_SIZE=<size> cargo test multithreaded_benchmark_batch"
    );
    println!(
        "   or: cargo test multithreaded_benchmark_batch -- --fsync <schedule> --duration <duration> --batch-size <size>"
    );
    println!();
    println!("Fsync Schedule Options:");
    println!("  sync-each    Fsync after every write (slowest, most durable)");
    println!("  no-fsync     Disable fsyncing entirely (fastest, no durability)");
    println!("  none         Same as no-fsync");
    println!("  async        Async fsync every 1000ms (default)");
    println!("  <number>ms   Async fsync every N milliseconds (e.g., 500ms)");
    println!("  <number>     Async fsync every N milliseconds (e.g., 500)");
    println!();
    println!("Batch Size Options:");
    println!("  <number>     Number of entries per batch (1-10000000, default: 2047)");
    println!();
    println!("Duration Options:");
    println!("  <number>s    Duration in seconds (e.g., 30s, 120s)");
    println!("  <number>m    Duration in minutes (e.g., 2m, 5m)");
    println!("  <number>h    Duration in hours (e.g., 1h, 2h)");
    println!("  <number>     Duration in seconds (e.g., 120, 300)");
    println!("  Default: 2m (120 seconds)");
    println!();
    println!("Examples:");
    println!(
        "  WALRUS_FSYNC=sync-each WALRUS_DURATION=30s WALRUS_BATCH_SIZE=50 cargo test multithreaded_benchmark_batch"
    );
    println!(
        "  WALRUS_FSYNC=no-fsync WALRUS_DURATION=1m WALRUS_BATCH_SIZE=200 cargo test multithreaded_benchmark_batch"
    );
    println!("  WALRUS_FSYNC=500ms WALRUS_DURATION=5m cargo test multithreaded_benchmark_batch");
    println!("  cargo test multithreaded_benchmark_batch -- --fsync no-fsync --duration 1m --batch-size 150");
    println!();
    println!("Note: This benchmark uses batch_append_for_topic() which requires FD backend on Linux.");
}

fn cleanup_wal() {
    let _ = fs::remove_dir_all("wal_files");
    // Give filesystem time to clean up
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn multithreaded_batch_benchmark() {
    // Check for help flag
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }

    cleanup_wal();

    // Enable FD backend for batch operations (Linux only)
    #[cfg(target_os = "linux")]
    walrus_rust::enable_fd_backend();

    // Enable quiet mode to suppress debug output during benchmark
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    let fsync_schedule = parse_fsync_schedule();
    let write_duration = parse_duration();
    let batch_size = parse_batch_size();

    println!("=== Multi-threaded WAL Batch Benchmark ===");
    println!(
        "Configuration: 10 threads, {:.0}s write phase only, batch size: {} entries/batch",
        write_duration.as_secs(),
        batch_size
    );
    println!("Fsync schedule: {:?}", fsync_schedule);
    println!(
        "Duration: {:?} (batch writes with {}ms delays between batches)",
        write_duration, 500
    );
    println!("Using batch_append_for_topic() for atomic batch writes");

    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 5000 },
            fsync_schedule,
        )
        .expect("Failed to create Walrus"),
    );
    let num_threads = 10; // Scaled up to 10 threads with smaller entries

    // Shared counters for statistics
    let total_batches = Arc::new(AtomicU64::new(0));
    let total_entries = Arc::new(AtomicU64::new(0));
    let total_write_bytes = Arc::new(AtomicU64::new(0));
    let batch_errors = Arc::new(AtomicU64::new(0));

    // Create CSV file for throughput monitoring
    let csv_path = "batch_benchmark_throughput.csv";
    let mut csv_file = fs::File::create(csv_path).expect("Failed to create CSV file");
    writeln!(
        csv_file,
        "timestamp,elapsed_seconds,batches_per_second,entries_per_second,bytes_per_second,total_batches,total_entries,total_bytes,dirty_pages_kb,dirty_ratio_percent"
    )
    .expect("Failed to write CSV header");

    // Channel for throughput monitoring
    let (throughput_tx, throughput_rx) = mpsc::channel::<()>();

    // Barrier to synchronize thread start
    let start_barrier = Arc::new(Barrier::new(num_threads + 1)); // +1 for main thread
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));

    // Topic names for each thread
    let topics: Vec<String> = (0..num_threads).map(|i| format!("batch_topic_{}", i)).collect();

    println!("Starting {} batch writer threads...", num_threads);

    // Spawn throughput monitoring thread
    let total_batches_monitor = Arc::clone(&total_batches);
    let total_entries_monitor = Arc::clone(&total_entries);
    let total_write_bytes_monitor = Arc::clone(&total_write_bytes);
    let throughput_tx_clone = throughput_tx.clone();
    let monitor_duration = write_duration;

    let monitor_handle = thread::spawn(move || {
        let mut csv_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("batch_benchmark_throughput.csv")
            .expect("Failed to open CSV file");

        let mut start_time = Instant::now();
        let mut last_batches = 0u64;
        let mut last_entries = 0u64;
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
        let (_, initial_dirty_kb, initial_dirty_ratio) = get_memory_info();
        writeln!(
            csv_file,
            "{},{:.2},{:.0},{:.0},{:.0},{},{},{},{},{:.2}",
            timestamp, 0.0, 0.0, 0.0, 0.0, 0, 0, 0, initial_dirty_kb, initial_dirty_ratio
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
            let current_batches = total_batches_monitor.load(Ordering::Relaxed);
            let current_entries = total_entries_monitor.load(Ordering::Relaxed);
            let current_bytes = total_write_bytes_monitor.load(Ordering::Relaxed);

            // Calculate rates over fixed interval
            let batches_per_second = (current_batches - last_batches) as f64 / interval_s;
            let entries_per_second = (current_entries - last_entries) as f64 / interval_s;
            let bytes_per_second = (current_bytes - last_bytes) as f64 / interval_s;

            // Get memory information including dirty pages
            let (_, dirty_kb, dirty_ratio) = get_memory_info();

            // Only log if there's been some change or every 2 seconds (4 ticks of 0.5s)
            let should_log = (current_batches != last_batches) || (tick_index % 4 == 0);

            if should_log {
                // Write to CSV
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                writeln!(
                    csv_file,
                    "{},{:.2},{:.0},{:.0},{:.0},{},{},{},{},{:.2}",
                    timestamp,
                    elapsed_total,
                    batches_per_second,
                    entries_per_second,
                    bytes_per_second,
                    current_batches,
                    current_entries,
                    current_bytes,
                    dirty_kb,
                    dirty_ratio
                )
                .expect("Failed to write to CSV");
                csv_file.flush().expect("Failed to flush CSV");

                // Print progress only if there's activity
                if current_batches > last_batches {
                    println!(
                        "[Monitor] {:.1}s: {:.0} batches/sec, {:.0} entries/sec, {:.2} MB/sec, total: {} batches ({} entries), dirty: {:.2}% ({} KB)",
                        elapsed_total,
                        batches_per_second,
                        entries_per_second,
                        bytes_per_second / (1024.0 * 1024.0),
                        current_batches,
                        current_entries,
                        dirty_ratio,
                        dirty_kb
                    );
                }
            }

            last_batches = current_batches;
            last_entries = current_entries;
            last_bytes = current_bytes;
            last_time = current_time;

            // Stop monitoring after write phase (duration + 30s buffer)
            let max_monitor_time = monitor_duration.as_secs_f64() + 30.0;
            if elapsed_total > max_monitor_time {
                break;
            }
        }
    });

    // Spawn writer threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_batches_clone = Arc::clone(&total_batches);
        let total_entries_clone = Arc::clone(&total_entries);
        let total_write_bytes_clone = Arc::clone(&total_write_bytes);
        let batch_errors_clone = Arc::clone(&batch_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();
        let batch_size_local = batch_size;

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            start_barrier_clone.wait();

            let start_time = Instant::now();
            let mut local_batches = 0u64;
            let mut local_entries = 0u64;
            let mut local_write_bytes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;

            // Batch write phase
            let mut rng = rand::thread_rng();

            while start_time.elapsed() < write_duration {
                // Create a batch of entries
                let mut batch_data = Vec::new();
                let mut batch_bytes = 0usize;

                for _ in 0..batch_size_local {
                    // Fixed entry size of 10KB
                    let size = 10 * 1024; // 10KB
                    let data = vec![(counter % 256) as u8; size];
                    batch_data.push(data);
                    batch_bytes += size;
                    counter += 1;
                }

                // Convert to slice of slices for batch_append_for_topic
                let batch_refs: Vec<&[u8]> = batch_data.iter().map(|v| v.as_slice()).collect();

                match wal_clone.batch_append_for_topic(&topic, &batch_refs) {
                    Ok(_) => {
                        local_batches += 1;
                        local_entries += batch_size_local as u64;
                        local_write_bytes += batch_bytes as u64;
                        // Update global counters in real-time so the monitor sees progress
                        total_batches_clone.fetch_add(1, Ordering::Relaxed);
                        total_entries_clone.fetch_add(batch_size_local as u64, Ordering::Relaxed);
                        total_write_bytes_clone.fetch_add(batch_bytes as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        local_errors += 1;
                        // Log the error type for debugging
                        match e.kind() {
                            std::io::ErrorKind::WouldBlock => {
                                // Another batch in progress, this is expected in concurrent scenarios
                                thread::sleep(Duration::from_millis(1));
                            }
                            _ => {
                                eprintln!("Thread {}: Batch write error: {}", thread_id, e);
                            }
                        }
                    }
                }

                // Longer delay between batches to avoid overwhelming io_uring queue
                thread::sleep(Duration::from_millis(500));
            }

            // Persist error count at the end
            batch_errors_clone.fetch_add(local_errors, Ordering::Relaxed);

            println!(
                "Thread {} ({}): {} batches, {} entries, {} KB, {} errors",
                thread_id,
                topic,
                local_batches,
                local_entries,
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
    println!("All threads started! Batch write phase beginning...");

    // Signal monitoring thread to start
    let _ = throughput_tx.send(());

    // Wait for write phase to complete
    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();
    println!("Batch write phase completed in {:?}", write_elapsed);

    // Print write results immediately
    let final_batches = total_batches.load(Ordering::Relaxed);
    let final_entries = total_entries.load(Ordering::Relaxed);
    let final_write_bytes = total_write_bytes.load(Ordering::Relaxed);
    let final_errors = batch_errors.load(Ordering::Relaxed);

    println!("\n=== Write Phase Results ===");
    println!("Write Duration: {:?}", write_elapsed);
    println!("Total Operations: {} batches ({} entries)", final_batches, final_entries);
    println!("Total Bytes: {} MB", final_write_bytes / (1024 * 1024));
    println!("Write Errors: {}", final_errors);
    println!(
        "Batch Throughput: {:.0} batches/sec",
        final_batches as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "Entry Throughput: {:.0} entries/sec", 
        final_entries as f64 / write_elapsed.as_secs_f64()
    );
    println!(
        "Write Bandwidth: {:.2} MB/sec",
        (final_write_bytes as f64 / (1024.0 * 1024.0)) / write_elapsed.as_secs_f64()
    );
    if final_batches > 0 {
        println!(
            "Average Batch Size: {:.1} entries/batch (configured: {})",
            final_entries as f64 / final_batches as f64,
            batch_size
        );
    }
    println!();

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join().unwrap();
    }

    let total_elapsed = benchmark_start.elapsed();

    println!("\n=== Final Summary ===");
    println!("Total Benchmark Duration: {:?}", total_elapsed);

    // Performance assertions
    assert!(
        final_batches > 10,
        "Batch throughput too low: {} batches",
        final_batches
    );
    assert!(
        final_entries > 1000,
        "Entry throughput too low: {} entries",
        final_entries
    );
    assert!(
        final_errors < final_batches / 2,
        "Too many batch errors: {} out of {}",
        final_errors,
        final_batches
    );

    println!("Multi-threaded batch benchmark completed successfully!");

    // Wait for monitoring thread to finish
    let _ = monitor_handle.join();
    println!("Throughput data saved to: {}", csv_path);

    cleanup_wal();
}
