//! Seeded simulated-power-loss suite (FEAT-003 step 2; Technical-Design §6.3,
//! §6.4, §13; SOW §19).
//!
//! Phase 2's acceptance gate demands "zero lost OK writes across at least
//! 1,000 seeded randomized simulated crashes". This suite drives the durable
//! [`Db`] write path over the deterministic [`SimFs`] power-loss layer with
//! seeded fault injection (short writes, failed syncs, crash-at-step), then
//! injects an explicit crash, reopens from the STABLE image, and asserts the
//! single safety invariant:
//!
//! > **Every acknowledged durable-OK write survives.** A write is
//! > *acknowledged* iff `db.set`/`db.delete` returned `Ok` (append + fsync +
//! > apply all succeeded). Unknown-outcome writes (the call errored, so the
//! > caller never observed OK) MAY or may not survive and are NEVER asserted
//! > absent.
//!
//! # Reference model
//!
//! We maintain a `HashMap<key, Option<value>>` of the LAST acknowledged
//! outcome per key (`Some(v)` for an acknowledged SET, `None` for an
//! acknowledged DELETE). After the crash + reopen, every acknowledged SET key
//! must read back its acknowledged value and every acknowledged DELETE key
//! must be absent -- UNLESS a later, unknown-outcome write for that key MIGHT
//! have superseded it. To keep the invariant crisp we stop issuing new writes
//! for a key once any write to it has an unknown outcome (see `poisoned`).
//!
//! # Reproducibility
//!
//! Every run is fully determined by its seed. On any invariant violation the
//! test prints the seed plus the compact operation/fault trace so the failure
//! is a one-line regression (`SEED=<n>`), per §13 ("publish seeds and failure
//! traces").

use distributedb::{Db, DurabilityMode, FsError, GetResult, SimConfig, SimFs, WalError};
use std::collections::HashMap;
use std::path::Path;

const ROOT: &str = "/data";

/// A compact record of one attempted operation and its observed outcome, used
/// to reconstruct a failing run from its seed.
#[derive(Debug, Clone)]
enum TraceStep {
    Set { key: u8, value: u8, ack: bool },
    Delete { key: u8, ack: bool },
    Crash,
}

impl std::fmt::Display for TraceStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceStep::Set { key, value, ack } => {
                write!(f, "SET k{key}=v{value} {}", if *ack { "OK" } else { "ERR" })
            }
            TraceStep::Delete { key, ack } => {
                write!(f, "DEL k{key} {}", if *ack { "OK" } else { "ERR" })
            }
            TraceStep::Crash => write!(f, "CRASH"),
        }
    }
}

/// Render a trace as a compact one-line string for failure reporting.
fn render_trace(trace: &[TraceStep]) -> String {
    trace
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Outcome of one seeded run: the reference model of acknowledged writes plus
/// the trace for diagnostics.
struct RunResult {
    /// Last acknowledged outcome per key: Some(value) = SET, None = DELETE.
    acked: HashMap<u8, Option<u8>>,
    /// Keys whose acknowledged state is ambiguous (a write to them had an
    /// unknown outcome), so we make no assertion about them.
    poisoned: std::collections::HashSet<u8>,
    trace: Vec<TraceStep>,
}

/// Run a single seeded scenario end-to-end and verify the invariant.
///
/// Returns `Err((message, trace))` describing the first violated invariant and
/// the compact operation/fault trace (the caller turns that into a panic that
/// includes the seed for reproduction, per §13).
fn run_seed(seed: u64) -> Result<(), (String, Vec<TraceStep>)> {
    // A tiny local PRNG so the SCENARIO (which ops, when to crash) is also
    // seeded independently of the SimFs fault PRNG. We derive it from the seed.
    let mut scenario = Lcg::new(seed ^ 0x9E37_79B9_7F4A_7C15);

    // The SimFs fault PRNG uses the seed directly, with a mix of fault classes.
    let config = SimConfig::new(seed)
        .with_short_writes(60)
        .with_sync_failures(60)
        .with_crashes(40);
    let fs = SimFs::new(config);

    let mut result = RunResult {
        acked: HashMap::new(),
        poisoned: std::collections::HashSet::new(),
        trace: Vec::new(),
    };

    // Number of ops before we force an explicit crash (1..=12).
    let op_count = 1 + (scenario.next() % 12) as usize;

    // Phase 1: open a fresh db and issue a seeded sequence of writes. The db
    // handle may become fail-closed or the fs may auto-crash on an injected
    // fault; in either case we stop issuing writes and move to reopen.
    let mut crashed_early = false;
    {
        let mut db = match Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync) {
            Ok(db) => db,
            Err(_) => {
                // A fault during fresh-init: nothing acknowledged; just crash.
                fs.crash();
                result.trace.push(TraceStep::Crash);
                return verify(&fs, &result).map_err(|m| (m, result.trace.clone()));
            }
        };

        for _ in 0..op_count {
            let key = (scenario.next() % 5) as u8; // small key space => overwrites
            let is_delete = scenario.next().is_multiple_of(4);
            if is_delete {
                match db.delete(vec![key]) {
                    Ok(_) => {
                        result.trace.push(TraceStep::Delete { key, ack: true });
                        if !result.poisoned.contains(&key) {
                            result.acked.insert(key, None);
                        }
                    }
                    Err(e) => {
                        result.trace.push(TraceStep::Delete { key, ack: false });
                        result.poisoned.insert(key);
                        if is_crash_like(&e) {
                            crashed_early = true;
                            break;
                        }
                    }
                }
            } else {
                let value = (scenario.next() % 250) as u8;
                match db.set(vec![key], vec![value]) {
                    Ok(_) => {
                        result.trace.push(TraceStep::Set {
                            key,
                            value,
                            ack: true,
                        });
                        if !result.poisoned.contains(&key) {
                            result.acked.insert(key, Some(value));
                        }
                    }
                    Err(e) => {
                        result.trace.push(TraceStep::Set {
                            key,
                            value,
                            ack: false,
                        });
                        result.poisoned.insert(key);
                        if is_crash_like(&e) {
                            crashed_early = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Phase 2: force an explicit crash (unless a fault already crashed the fs).
    if !crashed_early {
        fs.crash();
        result.trace.push(TraceStep::Crash);
    }

    verify(&fs, &result).map_err(|m| (m, result.trace.clone()))
}

/// After the crash, reopen from the stable image and check every acknowledged
/// write survives.
fn verify(fs: &SimFs, result: &RunResult) -> Result<(), String> {
    // Any key with an unambiguous acknowledged outcome to assert about.
    let has_unambiguous_ack = result.acked.keys().any(|k| !result.poisoned.contains(k));

    // Recovery reads/truncates/syncs the WAL, so the still-armed fault PRNG can
    // inject a transient fault (a crash during recovery). The stable image is
    // never mutated by such a fault, so a real supervisor simply restarts
    // recovery. We model that by retrying reopen a bounded number of times,
    // treating injected I/O faults as transient. Interior corruption is NOT
    // transient and fails immediately.
    let mut last_err: Option<String> = None;
    let mut opened: Option<Db<SimFs>> = None;
    for _ in 0..64 {
        match Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync) {
            Ok(db) => {
                opened = Some(db);
                break;
            }
            Err(WalError::Corruption(m)) => {
                // The core SimFs model never alters synced bytes, so recovery
                // must not see interior corruption. If it does, that is real.
                return Err(format!("recovery failed closed unexpectedly: {m}"));
            }
            Err(WalError::Io(_)) => {
                // Transient injected fault during recovery; a crash resets
                // volatile back to the (unchanged) stable image, so retry.
                fs.crash();
                last_err = Some("transient recovery fault".into());
                continue;
            }
            Err(e) => {
                last_err = Some(e.to_string());
                break;
            }
        }
    }
    let db = match opened {
        Some(db) => db,
        None => {
            // Never recovered. Acceptable ONLY if nothing was acknowledged
            // (e.g. a crash during fresh-init left a partial data dir). If any
            // unambiguous ack happened, this is a real durability violation.
            if has_unambiguous_ack {
                return Err(format!(
                    "recovery never succeeded after acknowledged write(s): {}",
                    last_err.unwrap_or_else(|| "unknown".into())
                ));
            }
            return Ok(());
        }
    };

    for (key, outcome) in &result.acked {
        if result.poisoned.contains(key) {
            continue; // ambiguous; make no assertion
        }
        match outcome {
            Some(value) => match db.get(&[*key]) {
                GetResult::Found(v) if v == vec![*value] => {}
                other => {
                    return Err(format!(
                        "acknowledged SET key={key} value={value} lost: got {other:?}"
                    ));
                }
            },
            None => {
                if db.exists(&[*key]) {
                    return Err(format!("acknowledged DELETE key={key} resurrected"));
                }
            }
        }
    }
    Ok(())
}

/// True if the error indicates the SimFs already crashed / the writer is now
/// fail-closed (so we should stop issuing writes).
fn is_crash_like(e: &WalError) -> bool {
    matches!(
        e,
        WalError::FailClosed
            | WalError::Io(FsError::InjectedFault(_))
            | WalError::Io(FsError::Locked(_))
    )
}

/// A tiny linear-congruential generator for the SCENARIO choices (which ops,
/// how many, delete vs set). Independent of the SimFs fault PRNG.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed.wrapping_add(0x1234_5678_9ABC_DEF0))
    }
    fn next(&mut self) -> u64 {
        // Numerical Recipes LCG constants.
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

/// The main gate: run at least 1,000 distinct seeds and assert no acknowledged
/// write is ever lost. On the first failure, panic with the reproducing seed
/// and the trace.
#[test]
fn no_acknowledged_write_lost_across_seeded_crashes() {
    // At least 1,000 seeds (Technical-Design §13). This is in-memory and fast.
    let seed_count: u64 = 2_000;
    let mut failures = 0u64;
    for seed in 1..=seed_count {
        if let Err((msg, trace)) = run_seed(seed) {
            // Publish the seed AND the compact fault/cut-point trace so the
            // failure is reproducible (Technical-Design §13).
            eprintln!("SEED={seed} FAILED: {msg}");
            eprintln!("SEED={seed} TRACE: {}", render_trace(&trace));
            eprintln!("--- reproduce: run this test with a debugger pinned to seed {seed} ---");
            failures += 1;
        }
    }
    assert_eq!(
        failures, 0,
        "{failures} seed(s) lost an acknowledged write (see SEED=... lines above)"
    );
}

/// A focused determinism check: the same seed yields the same verification
/// outcome twice (the whole pipeline is reproducible).
#[test]
fn seeded_runs_are_deterministic() {
    for seed in [1u64, 7, 42, 123, 9999] {
        let a = run_seed(seed).is_ok();
        let b = run_seed(seed).is_ok();
        assert_eq!(a, b, "seed {seed} not deterministic: {a} vs {b}");
    }
}
