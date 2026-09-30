//! Real process kill/restart durability test (FEAT-003 step 5; SOW §8, §19;
//! Technical-Design §13 acceptance gate: 100 process-kill/restart runs).
//!
//! This spawns the `wal_kill_harness` binary as a SEPARATE OS process. The
//! harness opens a real data directory, performs durable `fsync`-mode writes,
//! and prints one `ACK <lsn> k<i>=v<i>` line per acknowledged write. The test
//! reads those ACK lines, kills the process at a randomized point (spanning
//! the append/footer/sync/apply/response window across runs), then reopens the
//! same directory with [`Db`] and asserts EVERY acknowledged write survived.
//!
//! # Run count
//!
//! `cargo test` stays fast by defaulting to a small number of runs. Set the
//! `DISTRIBUTEDB_KILL_RUNS` environment variable to run the full §13 gate:
//!
//! ```text
//! DISTRIBUTEDB_KILL_RUNS=100 cargo test --test wal_kill_restart -- --nocapture
//! ```

mod common;

use common::TempDir;
use distributedb::{Db, DurabilityMode, GetResult, RealFs};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Path to the harness binary provided by Cargo for integration tests.
const HARNESS: &str = env!("CARGO_BIN_EXE_wal_kill_harness");

/// How many kill/restart runs to perform. Small by default for CI speed; the
/// full 100-run acceptance gate is enabled via `DISTRIBUTEDB_KILL_RUNS`.
fn run_count() -> usize {
    std::env::var("DISTRIBUTEDB_KILL_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6)
}

/// Reopen the data dir and collect the recovered database.
fn reopen(root: &Path) -> Db<RealFs> {
    Db::open(RealFs::new(), root, DurabilityMode::Fsync).expect("reopen after kill")
}

/// Kill and reap a child, ignoring "already exited" races.
fn kill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn acknowledged_writes_survive_process_kill() {
    let runs = run_count();
    for run in 0..runs {
        let dir = TempDir::new(&format!("kill-{run}"));
        // Vary the write count and inter-write delay per run so the kill lands
        // at different points relative to append/sync/apply/response.
        let count = 8 + (run % 5);
        let delay_micros = 200 + (run as u64 % 4) * 300;

        let mut child = Command::new(HARNESS)
            .arg(dir.path())
            .arg(count.to_string())
            .arg(delay_micros.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn harness");

        // Read ACK lines until we have seen a few, then kill mid-run. This
        // guarantees the kill lands while writes are still being acknowledged
        // for at least some runs, and after DONE for others.
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let mut acked: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();

        // Kill after observing this many ACKs (varies per run, spanning the
        // whole window: sometimes 0 -> kill before any durable write).
        let kill_after = run % (count + 1);

        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break, // EOF: child exited or pipe closed
                Ok(_) => {
                    let trimmed = line.trim();
                    if let Some(rest) = trimmed.strip_prefix("ACK ") {
                        // Format: "<lsn> k<i>=v<i>"
                        if let Some((lsn_str, kv)) = rest.split_once(' ') {
                            if let Some((k, v)) = kv.split_once('=') {
                                if let Ok(lsn) = lsn_str.parse::<u64>() {
                                    acked.push((lsn, k.as_bytes().to_vec(), v.as_bytes().to_vec()));
                                }
                            }
                        }
                        if acked.len() >= kill_after {
                            break;
                        }
                    } else if trimmed == "DONE" {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        // Give the harness a beat, then kill it hard (SIGKILL via Child::kill).
        std::thread::sleep(Duration::from_millis(2));
        kill(&mut child);
        // Ensure the OS released the child's LOCK file handle before reopening.
        std::thread::sleep(Duration::from_millis(5));

        // The child holds an exclusive LOCK via a LOCK file created with
        // create_new. On SIGKILL the RealLockGuard Drop does not run, so the
        // stale LOCK file may remain. Remove it so recovery can reacquire, the
        // same cleanup a supervisor would perform on restart.
        let lock_path = dir.path().join("LOCK");
        let _ = std::fs::remove_file(&lock_path);

        // Reopen and verify every acknowledged write survived.
        let db = reopen(dir.path());
        for (lsn, key, value) in &acked {
            assert!(
                db.last_applied_lsn() >= *lsn,
                "run {run}: last_applied_lsn {} < acknowledged lsn {lsn}",
                db.last_applied_lsn()
            );
            assert_eq!(
                db.get(key),
                GetResult::Found(value.clone()),
                "run {run}: acknowledged write lsn={lsn} lost after kill"
            );
        }
    }
}
