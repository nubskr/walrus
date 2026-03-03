//! Reproduction test for the WALRUS_DATA_DIR race condition.
//!
//! `wal_data_dir()` reads the process-wide `WALRUS_DATA_DIR` env var on every
//! call without synchronization.  When two threads each set `WALRUS_DATA_DIR`
//! to their own temp directory and then call `Walrus::new_for_key`, the second
//! thread's `set_var` overwrites the first's, causing both instances to land in
//! the same directory — breaking isolation.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use walrus_rust::Walrus;
use walrus_rust::WalrusBuilder;

fn make_temp_dir(name: &str) -> PathBuf {
    let dir = env::temp_dir()
        .join(format!("walrus-race-repro-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn test_env_var_race_condition() {
    let dir1 = make_temp_dir("thread1");
    let dir2 = make_temp_dir("thread2");

    let path1 = dir1.clone();
    let path2 = dir2.clone();

    // Barrier ensures both threads have set_var before either calls new_for_key.
    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let handle1 = thread::spawn(move || {
        // 1. Set global env var to OUR directory
        unsafe { env::set_var("WALRUS_DATA_DIR", &path1); }

        // 2. Wait for the other thread to also set its env var
        b1.wait();

        // 3. Small sleep to widen the race window — thread 2's set_var
        //    should have overwritten ours by now.
        thread::sleep(Duration::from_millis(50));

        // 4. Initialise Walrus (reads the env var internally)
        let _wal = Walrus::new_for_key("race_test").unwrap();

        // 5. Check: did data land in OUR directory?
        let expected = path1.join("race_test");
        expected.exists()
    });

    let b2 = barrier.clone();
    let handle2 = thread::spawn(move || {
        // Same sequence, but with the other directory
        unsafe { env::set_var("WALRUS_DATA_DIR", &path2); }
        b2.wait();

        let _wal = Walrus::new_for_key("race_test").unwrap();

        let expected = path2.join("race_test");
        expected.exists()
    });

    let t1_ok = handle1.join().expect("thread 1 panicked");
    let t2_ok = handle2.join().expect("thread 2 panicked");

    // Cleanup
    let _ = fs::remove_dir_all(&dir1);
    let _ = fs::remove_dir_all(&dir2);

    if !t1_ok || !t2_ok {
        panic!(
            "Race condition reproduced: at least one thread's Walrus instance \
             used the other thread's WALRUS_DATA_DIR.\n\
             Thread 1 found its dir: {}\n\
             Thread 2 found its dir: {}",
            t1_ok, t2_ok,
        );
    }
}

/// Proves that `WalrusBuilder::data_dir()` eliminates the race — each thread
/// gets its own directory regardless of env var state.
#[test]
fn test_builder_eliminates_race_condition() {
    let dir1 = make_temp_dir("builder-thread1");
    let dir2 = make_temp_dir("builder-thread2");

    let path1 = dir1.clone();
    let path2 = dir2.clone();

    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let handle1 = thread::spawn(move || {
        b1.wait();
        thread::sleep(Duration::from_millis(50));

        // Use builder with explicit data_dir — no env var involved
        let _wal = WalrusBuilder::new()
            .data_dir(path1.clone())
            .key("race_test")
            .build()
            .unwrap();

        let expected = path1.join("race_test");
        expected.exists()
    });

    let b2 = barrier.clone();
    let handle2 = thread::spawn(move || {
        b2.wait();

        let _wal = WalrusBuilder::new()
            .data_dir(path2.clone())
            .key("race_test")
            .build()
            .unwrap();

        let expected = path2.join("race_test");
        expected.exists()
    });

    let t1_ok = handle1.join().expect("thread 1 panicked");
    let t2_ok = handle2.join().expect("thread 2 panicked");

    // Cleanup
    let _ = fs::remove_dir_all(&dir1);
    let _ = fs::remove_dir_all(&dir2);

    assert!(
        t1_ok && t2_ok,
        "Builder should eliminate the race condition.\n\
         Thread 1 found its dir: {}\n\
         Thread 2 found its dir: {}",
        t1_ok, t2_ok,
    );
}
