//! Durable-write helper binary for the real process kill/restart test
//! (FEAT-003 step 5; SOW §8, §19; Technical-Design §13).
//!
//! The default `distributedb` binary is an in-memory REPL and has no durable
//! mode, so this dedicated harness provides one. It opens a real data
//! directory, performs durable writes in `fsync` mode, and prints one
//! `ACK <lsn> <key>=<value>` line per acknowledged write (flushing stdout each
//! time) so the parent test can observe exactly which writes were
//! acknowledged before it kills the process at an arbitrary point.
//!
//! # Usage
//!
//! ```text
//! wal_kill_harness <data-dir> <count> [delay-micros]
//! ```
//!
//! * `<data-dir>`: the data directory to open (created if fresh).
//! * `<count>`: how many durable SETs to perform (keys `k0..k{count-1}`,
//!   values `v0..`). The process runs until killed or until `count` writes
//!   complete.
//! * `[delay-micros]`: optional sleep between writes so the parent test has a
//!   window in which to kill the process mid-run.
//!
//! The harness intentionally never exits cleanly during the kill window: after
//! finishing its writes it flushes and loops sleeping so the parent controls
//! termination timing.

use distributedb::{Db, DurabilityMode, RealFs};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: wal_kill_harness <data-dir> <count> [delay-micros]");
        std::process::exit(2);
    }
    let data_dir = Path::new(&args[1]);
    let count: u64 = args[2].parse().unwrap_or(0);
    let delay_micros: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

    let mut db = match Db::open(RealFs::new(), data_dir, DurabilityMode::Fsync) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open failed: {e}");
            std::process::exit(3);
        }
    };

    let stdout = std::io::stdout();
    for i in 0..count {
        let key = format!("k{i}").into_bytes();
        let value = format!("v{i}").into_bytes();
        match db.set(key, value) {
            Ok(lsn) => {
                let mut lock = stdout.lock();
                // ACK is printed only AFTER the durable set returned Ok, so any
                // ACK the parent reads is a genuinely acknowledged write.
                let _ = writeln!(lock, "ACK {lsn} k{i}=v{i}");
                let _ = lock.flush();
            }
            Err(e) => {
                eprintln!("set {i} failed: {e}");
                std::process::exit(4);
            }
        }
        if delay_micros > 0 {
            std::thread::sleep(Duration::from_micros(delay_micros));
        }
    }

    // Signal completion, then idle so the parent can kill us within a
    // deterministic window regardless of timing.
    {
        let mut lock = stdout.lock();
        let _ = writeln!(lock, "DONE");
        let _ = lock.flush();
    }
    loop {
        std::thread::sleep(Duration::from_millis(50));
    }
}
