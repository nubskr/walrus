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

fn parse_thread_range() -> (usize, usize) {
    // Check environment variable first (for Makefile integration)
    if let Ok(threads_env) = env::var("WALRUS_THREADS") {
        if let Some((start_str, end_str)) = threads_env.split_once('-') {
            // Range format: "1-16" or "2-8"
            if let (Ok(start), Ok(end)) = (start_str.parse::<usize>(), end_str.parse::<usize>()) {
                if start > 0 && end >= start && end <= 128 {
                    return (start, end);
                }
            }
        } else if let Ok(max_threads) = threads_env.parse::<usize>() {
            // Single number format: "16" means "1-16"
            if max_threads > 0 && max_threads <= 128 {
                return (1, max_threads);
            }
        }
    }

    // Check command line arguments (for direct cargo test usage)
    let args: Vec<String> = env::args().collect();

    for i in 0..args.len() {
        if args[i] == "--threads" && i + 1 < args.len() {
            let threads_arg = &args[i + 1];
            if let Some((start_str, end_str)) = threads_arg.split_once('-') {
                if let (Ok(start), Ok(end)) = (start_str.parse::<usize>(), end_str.parse::<usize>())
                {
                    if start > 0 && end >= start && end <= 128 {
                        return (start, end);
                    }
                }
            } else if let Ok(max_threads) = threads_arg.parse::<usize>() {
                if max_threads > 0 && max_threads <= 128 {
                    return (1, max_threads);
                }
            }
        }
    }

    // Default: test 1 to 10 threads
    (1, 10)
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

fn print_usage() {
    println!("Usage: WALRUS_FSYNC=<schedule> WALRUS_THREADS=<range> cargo test scaling_benchmark");
    println!("   or: cargo test scaling_benchmark -- --fsync <schedule> --threads <range>");
    println!();
    println!("Fsync Schedule Options:");
    println!("  sync-each    Fsync after every write (slowest, most durable)");
    println!("  no-fsync     Disable fsyncing entirely (fastest, no durability)");
    println!("  none         Same as no-fsync");
    println!("  async        Async fsync every 1000ms (default)");
    println!("  <number>ms   Async fsync every N milliseconds (e.g., 500ms)");
    println!("  <number>     Async fsync every N milliseconds (e.g., 500)");
    println!();
    println!("Thread Range Options:");
    println!("  <number>     Test from 1 to N threads (e.g., 16 means 1-16)");
    println!("  <start-end>  Test from start to end threads (e.g., 2-8, 1-32)");
    println!("  Default: 1-10 threads");
    println!();
    println!("Examples:");
    println!("  WALRUS_FSYNC=sync-each WALRUS_THREADS=16 cargo test scaling_benchmark");
    println!("  WALRUS_FSYNC=no-fsync WALRUS_THREADS=32 cargo test scaling_benchmark");
    println!("  WALRUS_THREADS=2-8 cargo test scaling_benchmark");
    println!("  make bench-scaling-sync  # Uses Makefile convenience targets");
    println!("  THREADS=32 make bench-scaling  # Test up to 32 threads");
    println!();
    println!("Makefile targets:");
    println!("  make bench-scaling       # Default (1-10 threads, async 1000ms)");
    println!("  make bench-scaling-sync  # Sync each write");
    println!("  make bench-scaling-fast  # Fast async (100ms)");
    println!("  THREADS=16 make bench-scaling  # Custom thread count");
}

fn cleanup_wal() {
    // Remove WAL files directory
    let _ = fs::remove_dir_all("wal_files");

    // Remove any leftover index files
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db");
    let _ = fs::remove_file("wal_files/read_offset_idx_index.db.tmp");

    // Give filesystem time to clean up completely
    thread::sleep(Duration::from_millis(200));

    // Force garbage collection to clean up any lingering memory
    // This helps ensure each test starts with a clean slate
    std::hint::black_box(());
}

fn run_benchmark_with_threads(num_threads: usize) -> f64 {
    // Thorough cleanup before each test
    cleanup_wal();

    // Enable quiet mode to suppress debug output
    unsafe {
        std::env::set_var("WALRUS_QUIET", "1");
    }

    // Small delay to ensure complete cleanup
    thread::sleep(Duration::from_millis(100));

    let fsync_schedule = parse_fsync_schedule();
    let wal = Arc::new(
        Walrus::with_consistency_and_schedule(
            ReadConsistency::AtLeastOnce { persist_every: 5000 },
            fsync_schedule,
        )
        .expect("Failed to create Walrus"),
    );
    let test_duration = Duration::from_secs(30); // 30 seconds per test

    // Shared counters for statistics
    let total_writes = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));

    // Barrier to synchronize thread start
    let start_barrier = Arc::new(Barrier::new(num_threads + 1)); // +1 for main thread
    let write_end_barrier = Arc::new(Barrier::new(num_threads + 1));

    // Generate topic names for each thread
    let topics: Vec<String> = (0..num_threads).map(|i| format!("topic_{}", i)).collect();

    // Spawn writer threads
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let total_writes_clone = Arc::clone(&total_writes);
        let write_errors_clone = Arc::clone(&write_errors);
        let start_barrier_clone = Arc::clone(&start_barrier);
        let write_end_barrier_clone = Arc::clone(&write_end_barrier);
        let topic = topics[thread_id].clone();

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            start_barrier_clone.wait();

            let start_time = Instant::now();
            let mut local_writes = 0u64;
            let mut local_errors = 0u64;
            let mut counter = 0u64;
            let mut rng = rand::thread_rng();

            // Write phase - start slow and ramp up to simulate realistic workload
            let ramp_up_duration = test_duration.mul_f64(0.15); // 15% of total duration for ramp-up

            while start_time.elapsed() < test_duration {
                // Calculate current write rate based on ramp-up
                let elapsed = start_time.elapsed();
                let delay_ms = if elapsed < ramp_up_duration {
                    // Ramp up from 50ms delay to 0ms delay over ramp_up_duration
                    let ramp_progress = elapsed.as_secs_f64() / ramp_up_duration.as_secs_f64();
                    let max_delay_ms = 50.0;
                    max_delay_ms * (1.0 - ramp_progress)
                } else {
                    // Full speed after ramp-up
                    0.0
                };

                // Apply the delay if we're still ramping up
                if delay_ms > 0.1 {
                    thread::sleep(Duration::from_millis(delay_ms as u64));
                }

                // Random entry size between 500B and 1KB
                let size = rng.gen_range(500..=1024);
                let data = vec![(counter % 256) as u8; size];

                match wal_clone.append_for_topic(&topic, &data) {
                    Ok(_) => {
                        local_writes += 1;
                        // Update global counter in real-time
                        total_writes_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        local_errors += 1;
                        write_errors_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }

                counter += 1;

                // Small gap after every 100k writes
                if counter % 100000 == 0 {
                    thread::sleep(Duration::from_micros(50));
                }
            }

            // Wait for all threads to finish writing
            write_end_barrier_clone.wait();
        });

        handles.push(handle);
    }

    // Start all threads simultaneously
    let benchmark_start = Instant::now();
    start_barrier.wait();

    // Wait for write phase to complete
    write_end_barrier.wait();
    let write_elapsed = benchmark_start.elapsed();

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join().unwrap();
    }

    let final_writes = total_writes.load(Ordering::Relaxed);
    let final_errors = write_errors.load(Ordering::Relaxed);

    // Calculate throughput (ops/sec)
    let throughput = final_writes as f64 / write_elapsed.as_secs_f64();

    println!(
        "{} threads: {} writes, {} errors, {:.0} ops/sec",
        num_threads, final_writes, final_errors, throughput
    );

    // Explicit cleanup after test to ensure no residue
    drop(wal);

    // Additional cleanup to ensure no lingering state
    cleanup_wal();

    throughput
}

fn create_live_plot_script() {
    let plot_script = r#"#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import os
import time

class LiveScalingPlot:
    def __init__(self):
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.ax.set_xlabel('Number of Threads')
        self.ax.set_ylabel('Throughput (ops/sec)')
        self.ax.set_title('WAL Write Throughput Scaling (Live)')
        self.ax.grid(True, alpha=0.3)
        self.ax.set_xlim(0.5, 32.5)  # Support up to 32 threads by default
        self.ax.set_xticks(range(1, 33, 2))  # Show every other tick to avoid crowding
        
        # Format Y-axis to avoid scientific notation
        self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        plt.tight_layout()
        
    def update_plot(self, frame):
        if not os.path.exists('scaling_results_live.csv'):
            return
            
        try:
            df = pd.read_csv('scaling_results_live.csv')
            if df.empty:
                return
                
            self.ax.clear()
            
            # Plot the data
            self.ax.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
            
            # Styling
            self.ax.set_xlabel('Number of Threads')
            self.ax.set_ylabel('Throughput (ops/sec)')
            
            # Dynamic title and limits based on actual data
            min_threads = df['threads'].min()
            max_threads = df['threads'].max()
            total_expected = max_threads - min_threads + 1
            self.ax.set_title(f'WAL Write Throughput Scaling (Live) - {len(df)}/{total_expected} tests complete')
            self.ax.grid(True, alpha=0.3)
            
            # Dynamic X-axis limits and ticks
            x_margin = max(1, (max_threads - min_threads) * 0.05)
            self.ax.set_xlim(min_threads - x_margin, max_threads + x_margin)
            
            # Smart tick spacing
            thread_range = max_threads - min_threads + 1
            if thread_range <= 10:
                tick_step = 1
            elif thread_range <= 20:
                tick_step = 2
            else:
                tick_step = max(1, thread_range // 10)
            
            ticks = list(range(min_threads, max_threads + 1, tick_step))
            if max_threads not in ticks:
                ticks.append(max_threads)
            self.ax.set_xticks(ticks)
            
            # Format Y-axis
            self.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            
            # Set Y-axis limits with some padding
            if len(df) > 0:
                max_throughput = df['throughput'].max()
                self.ax.set_ylim(0, max_throughput * 1.1)
            
            plt.tight_layout()
            
        except Exception as e:
            print(f"Error updating plot: {e}")
    
    def start_monitoring(self):
        print("Starting live scaling visualization...")
        print("Graph will update as each test completes")
        print("Close the plot window to stop monitoring")
        
        ani = animation.FuncAnimation(self.fig, self.update_plot, 
                                    interval=1000, blit=False, cache_frame_data=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

if __name__ == '__main__':
    plotter = LiveScalingPlot()
    plotter.start_monitoring()
"#;

    fs::write("live_scaling_plot.py", plot_script).expect("Failed to write live plot script");
}

#[test]
fn scaling_benchmark() {
    // Check for help flag
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }

    let fsync_schedule = parse_fsync_schedule();
    let (start_threads, end_threads) = parse_thread_range();

    println!("=== WAL Scaling Benchmark ===");
    println!(
        "Testing throughput scaling from {} to {} threads",
        start_threads, end_threads
    );
    println!("Each test runs for 30 seconds with random 500B-1KB entries");
    println!("Fsync schedule: {:?}", fsync_schedule);
    println!();

    // Create the live plotting script
    create_live_plot_script();

    // Initialize CSV file for live updates
    fs::write("scaling_results_live.csv", "threads,throughput\n")
        .expect("Failed to create live CSV");

    // Create channel for non-blocking CSV updates
    let (csv_tx, csv_rx) = mpsc::channel::<(usize, f64)>();

    // Spawn dedicated CSV writer thread
    let csv_handle = thread::spawn(move || {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open("scaling_results_live.csv")
            .expect("Failed to open live CSV");

        while let Ok((threads, throughput)) = csv_rx.recv() {
            if let Err(e) = writeln!(file, "{},{:.0}", threads, throughput) {
                eprintln!("Failed to write to CSV: {}", e);
            }
            if let Err(e) = file.flush() {
                eprintln!("Failed to flush CSV: {}", e);
            }
        }
    });

    println!("Live graph available! Run in another terminal:");
    println!("   python live_scaling_plot.py");
    println!();

    let mut results = Vec::new();

    // Test from start_threads to end_threads
    for num_threads in start_threads..=end_threads {
        println!("Testing with {} thread(s)...", num_threads);
        let throughput = run_benchmark_with_threads(num_threads);
        results.push((num_threads, throughput));

        // Send to CSV writer thread (non-blocking)
        let _ = csv_tx.send((num_threads, throughput));

        println!(
            "{} threads: {:.0} ops/sec (added to live graph)",
            num_threads, throughput
        );

        // Longer delay between tests to ensure complete cleanup
        thread::sleep(Duration::from_millis(1000));
    }

    // Close CSV writer thread
    drop(csv_tx);
    let _ = csv_handle.join();

    // Final cleanup
    cleanup_wal();

    println!("\n=== Scaling Results ===");
    println!("Threads | Throughput (ops/sec)");
    println!("--------|-------------------");
    for (threads, throughput) in &results {
        println!("{:7} | {:>17.0}", threads, throughput);
    }

    // Create a simple CSV for plotting
    let csv_content = results
        .iter()
        .map(|(threads, throughput)| format!("{},{:.0}", threads, throughput))
        .collect::<Vec<_>>()
        .join("\n");

    let csv_data = format!("threads,throughput\n{}", csv_content);
    fs::write("scaling_results.csv", csv_data).expect("Failed to write scaling results");

    // Create a simple Python script to show the graph
    let plot_script = r#"#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('scaling_results.csv')

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(df['threads'], df['throughput'], 'bo-', linewidth=2, markersize=8)
plt.xlabel('Number of Threads')
plt.ylabel('Throughput (ops/sec)')
plt.title('WAL Write Throughput Scaling')
plt.grid(True, alpha=0.3)

# Format Y-axis to avoid scientific notation
ax = plt.gca()
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# Dynamic X-axis ticks based on data
min_threads = df['threads'].min()
max_threads = df['threads'].max()
thread_range = max_threads - min_threads + 1

if thread_range <= 10:
    tick_step = 1
elif thread_range <= 20:
    tick_step = 2
else:
    tick_step = max(1, thread_range // 10)

ticks = list(range(min_threads, max_threads + 1, tick_step))
if max_threads not in ticks:
    ticks.append(max_threads)
plt.xticks(ticks)

# Add some styling
plt.tight_layout()

# Show the plot
plt.show()

print("Scaling benchmark complete!")
print("Data saved to: scaling_results.csv")
"#;

    fs::write("show_scaling_graph.py", plot_script).expect("Failed to write plot script");

    println!("\nResults saved to: scaling_results.csv");
    println!("Run 'python show_scaling_graph.py' to see the scaling graph");

    // Basic performance assertions
    let max_throughput = results.iter().map(|(_, t)| *t).fold(0.0, f64::max);
    let single_thread_throughput = results[0].1;

    assert!(
        max_throughput > single_thread_throughput,
        "Multi-threading should improve performance"
    );
    assert!(
        single_thread_throughput > 1000.0,
        "Single thread throughput too low: {:.0} ops/sec",
        single_thread_throughput
    );

    println!("\nScaling benchmark completed successfully!");
    println!("Best throughput: {:.0} ops/sec", max_throughput);
    println!(
        "Scaling factor: {:.1}x",
        max_throughput / single_thread_throughput
    );
}
