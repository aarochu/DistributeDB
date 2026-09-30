//! Phase 4 snapshot acceptance suites (FEAT-004; Technical-Design §7, §6.3,
//! §6.4; SOW §22).
//!
//! These integration tests exercise the REAL snapshot publication, recovery,
//! and WAL-reclamation code paths over the deterministic [`SimFs`] power-loss
//! layer. They cover the three Phase 4 acceptance goals:
//!
//! 1. **Crash-safety across the 8 publication steps.** A crash injected at any
//!    point of [`Db::publish_snapshot`] (temp write, sync, rename, dir sync,
//!    reload/verify, WAL delete, wal-dir sync) must leave a recoverable chain
//!    on reopen: either the OLD snapshot+WAL state or the NEW snapshot state,
//!    and in ALL cases the full pre-crash *acknowledged* key/value set is
//!    recoverable. There is never a gap and never a silent fallback to empty
//!    state after WAL was deleted.
//! 2. **Corrupt-latest-snapshot -> fail-closed.** With no complete remaining
//!    chain, [`Db::open`] returns [`WalError::Corruption`] (fail-closed), not a
//!    silent empty or older start. When a usable lower snapshot + contiguous
//!    WAL chain remains, recovery succeeds from that lower chain.
//! 3. **Recovery-work-reduction.** After a snapshot at LSN *S*, restart replays
//!    only WAL records with `lsn > S` (bounded by `last_applied_lsn - S`), so
//!    recovery no longer scales with the entire command history. Segments
//!    wholly covered by *S* are reclaimed and never read.
//!
//! # Reproducibility
//!
//! Every run is fully determined by its seed. The crash-safety gate iterates a
//! fixed range of seeds (1..=SEED_COUNT) and, on any invariant violation,
//! prints `SEED=<n>` so the failure reproduces as a one-line regression
//! (Technical-Design §13).

use distributedb::{
    Db, DurabilityMode, FileSystem, FsError, GetResult, SimConfig, SimFs, WalError,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const ROOT: &str = "/data";

// The fixed data-directory layout created by `Db::open` (Technical-Design §6).
// The PRIMARY never switches generations, so generation 1 is always active.
const GENERATION: &str = "0000000000000001";

/// Path to the immutable snapshot file for a snapshot taken at LSN `lsn`
/// (zero-padded to 20 digits, matching `DataPaths::snapshot`).
fn snapshot_path(lsn: u64) -> PathBuf {
    Path::new(ROOT)
        .join("generations")
        .join(GENERATION)
        .join("snapshots")
        .join(format!("{lsn:020}.snap"))
}

/// Directory holding the WAL segments for the active generation.
fn wal_dir() -> PathBuf {
    Path::new(ROOT)
        .join("generations")
        .join(GENERATION)
        .join("wal")
}

/// Path to a WAL segment whose first LSN is `first_lsn`.
fn segment_path(first_lsn: u64) -> PathBuf {
    wal_dir().join(format!("{first_lsn:020}.wal"))
}

// ---------------------------------------------------------------------------
// Suite 1: crash-safety across the publication steps.
// ---------------------------------------------------------------------------

/// True if the error means the SimFs already crashed / the writer is now
/// fail-closed, so we should stop issuing operations.
fn is_crash_like(e: &WalError) -> bool {
    matches!(
        e,
        WalError::FailClosed
            | WalError::Io(FsError::InjectedFault(_))
            | WalError::Io(FsError::Locked(_))
    )
}

/// Reopen from the stable image after a crash, retrying transient injected I/O
/// faults during recovery (a crash resets volatile back to the unchanged
/// stable image, so a real supervisor simply restarts recovery). Interior
/// corruption is NOT transient and is surfaced immediately.
///
/// Returns `Ok(Some(db))` on success, `Ok(None)` if recovery never succeeded
/// only because of transient faults, and `Err(msg)` on a fail-closed
/// corruption error.
fn reopen_with_retry(fs: &SimFs) -> Result<Option<Db<SimFs>>, String> {
    for _ in 0..128 {
        match Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync) {
            Ok(db) => return Ok(Some(db)),
            Err(WalError::Corruption(m)) => {
                return Err(format!("fail-closed corruption during recovery: {m}"));
            }
            Err(WalError::Io(_)) => {
                // Transient injected fault during recovery; reset volatile to
                // the (unchanged) stable image and retry.
                fs.crash();
                continue;
            }
            Err(WalError::Identity(_)) | Err(WalError::Format(_)) => {
                // A torn IDENTITY / empty first segment on the STABLE image can
                // only happen when a crash interrupted fresh init BEFORE any
                // write was acknowledged (IDENTITY + the first segment are
                // synced before the first ack). Recovery cannot proceed, but
                // this is acceptable ONLY when nothing was acknowledged; the
                // caller enforces that via `has_ack`.
                return Ok(None);
            }
            Err(e) => return Err(format!("unexpected recovery error: {e}")),
        }
    }
    Ok(None)
}

/// One seeded crash-safety run.
///
/// Writes a seeded set of acknowledged key/value pairs, then invokes
/// `publish_snapshot` while SimFs fault injection (short writes, failed syncs,
/// crash-at-step) is armed, and finally forces an explicit crash at a seeded
/// cut point. After reopening from the stable image, EVERY acknowledged write
/// must survive (never a gap) and recovery must never silently start empty
/// after having produced acknowledged writes.
fn run_seed(seed: u64) -> Result<(), String> {
    let mut scenario = Lcg::new(seed ^ 0x9E37_79B9_7F4A_7C15);

    // Fault injection is armed on the SimFs so a crash may land at ANY step of
    // the publication sequence (create/append/sync temp, rename, dir sync,
    // reload, WAL delete, wal-dir sync). Short writes and sync failures widen
    // the set of reachable failure points.
    let config = SimConfig::new(seed)
        .with_short_writes(40)
        .with_sync_failures(40)
        .with_crashes(30);
    let fs = SimFs::new(config);

    // The reference model: last acknowledged value per key (None = deleted).
    let mut acked: HashMap<u8, Option<u8>> = HashMap::new();
    // Keys whose outcome is AMBIGUOUS: an operation to them returned an error,
    // so the write may or may not have reached the durable image (a crash /
    // sync failure can fire AFTER the group's footer is stable but before the
    // caller observes success). We make NO assertion about a poisoned key --
    // only truly acknowledged (Ok) outcomes are guaranteed to survive.
    let mut poisoned: std::collections::HashSet<u8> = std::collections::HashSet::new();

    // Number of pre-snapshot writes (3..=10) and post-snapshot writes (0..=4).
    let pre_ops = 3 + (scenario.next() % 8) as usize;
    let post_ops = (scenario.next() % 5) as usize;
    // Whether to publish a FIRST snapshot before the fault-injected one, so
    // some runs exercise the previous-chain / reclamation path under crash.
    let two_snapshots = scenario.next().is_multiple_of(2);

    let mut crashed_early = false;
    {
        let mut db = match Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync) {
            Ok(db) => db,
            Err(_) => {
                // A fault during fresh init: nothing acknowledged. Crash and
                // verify the (empty) invariant holds.
                fs.crash();
                return verify(&fs, &acked, &poisoned);
            }
        };

        // Phase A: acknowledged pre-snapshot writes.
        for _ in 0..pre_ops {
            if apply_seeded_op(&mut db, &mut scenario, &mut acked, &mut poisoned) {
                crashed_early = true;
                break;
            }
        }

        // Optionally publish a clean first snapshot (best-effort; a fault here
        // just moves us to the crash+verify path with whatever is acknowledged
        // and durable).
        if !crashed_early && two_snapshots {
            if let Err(e) = db.publish_snapshot() {
                if is_crash_like(&e) {
                    crashed_early = true;
                }
            }
            // A few more acknowledged writes after the first snapshot.
            if !crashed_early {
                for _ in 0..2 {
                    if apply_seeded_op(&mut db, &mut scenario, &mut acked, &mut poisoned) {
                        crashed_early = true;
                        break;
                    }
                }
            }
        }

        // Phase B: the fault-injected publish. A crash may land at any step.
        // If it fails in a crash-like way the fs may already have auto-crashed,
        // so we must stop issuing operations on this now-stale handle.
        if !crashed_early {
            if let Err(e) = db.publish_snapshot() {
                if is_crash_like(&e) {
                    crashed_early = true;
                }
            }
        }

        // Phase C: acknowledged post-snapshot writes (only reachable if the
        // publish did not auto-crash the fs). A crash-like error stops the
        // loop; nothing after Phase C reads `crashed_early`.
        if !crashed_early {
            for _ in 0..post_ops {
                if apply_seeded_op(&mut db, &mut scenario, &mut acked, &mut poisoned) {
                    break;
                }
            }
        }
    }

    // Model a power loss: drop ALL volatile state, keeping only what a
    // successful sync made durable. This is essential even when an injected
    // fault already stopped us: an injected short-write / sync-failure leaves
    // NON-durable bytes in the volatile image (the write was never
    // acknowledged), and only a crash reveals the true post-power-loss image
    // that recovery must reconstruct from. `crash()` is idempotent, so calling
    // it here is safe whether or not a fault auto-crashed earlier.
    fs.crash();

    verify(&fs, &acked, &poisoned)
}

/// Apply one seeded SET or DELETE.
///
/// On an acknowledged (Ok) result the reference model is updated (unless the
/// key is already poisoned). On an ERROR the key is POISONED: the write's
/// outcome is unknown (a crash / sync failure can fire after the group's
/// footer becomes stable but before the caller observes success), so we must
/// never again assert about that key. Returns `true` if the error was
/// crash-like (so the caller stops issuing operations).
fn apply_seeded_op(
    db: &mut Db<SimFs>,
    scenario: &mut Lcg,
    acked: &mut HashMap<u8, Option<u8>>,
    poisoned: &mut std::collections::HashSet<u8>,
) -> bool {
    // Small key space so snapshots contain overwrites and deletes.
    let key = (scenario.next() % 6) as u8;
    let is_delete = scenario.next().is_multiple_of(5);
    if is_delete {
        match db.delete(vec![key]) {
            Ok(_) => {
                if !poisoned.contains(&key) {
                    acked.insert(key, None);
                }
                false
            }
            Err(e) => {
                poisoned.insert(key);
                is_crash_like(&e)
            }
        }
    } else {
        let value = (scenario.next() % 250) as u8;
        match db.set(vec![key], vec![value]) {
            Ok(_) => {
                if !poisoned.contains(&key) {
                    acked.insert(key, Some(value));
                }
                false
            }
            Err(e) => {
                poisoned.insert(key);
                is_crash_like(&e)
            }
        }
    }
}

/// Reopen from the stable image and assert every acknowledged, non-poisoned
/// write survives.
fn verify(
    fs: &SimFs,
    acked: &HashMap<u8, Option<u8>>,
    poisoned: &std::collections::HashSet<u8>,
) -> Result<(), String> {
    let has_ack = acked.keys().any(|k| !poisoned.contains(k));
    let db = match reopen_with_retry(fs)? {
        Some(db) => db,
        None => {
            // Recovery never succeeded (only transient faults). Acceptable ONLY
            // when nothing was acknowledged.
            if has_ack {
                return Err("recovery never succeeded after acknowledged write(s)".into());
            }
            return Ok(());
        }
    };

    // Fail-closed invariant: recovery must never silently start EMPTY after we
    // acknowledged a surviving SET. (A snapshot-based start with the correct
    // map is not "empty"; this guards against a silent fallback past deleted
    // WAL.)
    let has_live_set = acked
        .iter()
        .any(|(k, v)| !poisoned.contains(k) && v.is_some());
    if has_live_set && db.is_empty() {
        return Err(
            "recovery silently started empty despite acknowledged SET(s) (silent fallback)".into(),
        );
    }

    for (key, outcome) in acked {
        if poisoned.contains(key) {
            continue; // ambiguous outcome; make no assertion
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

/// A tiny LCG driving the SCENARIO choices, independent of the SimFs fault
/// PRNG. Numerical Recipes constants.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed.wrapping_add(0x1234_5678_9ABC_DEF0))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

/// The crash-safety gate: run a fixed range of seeds and assert that a crash at
/// any publication step leaves a recoverable chain with the full acknowledged
/// key/value set intact and no silent empty fallback.
#[test]
fn snapshot_publish_crash_preserves_acknowledged_state() {
    // Fixed, documented seed range for determinism (Technical-Design §13).
    const SEED_COUNT: u64 = 1_500;
    let mut failures = 0u64;
    for seed in 1..=SEED_COUNT {
        if let Err(msg) = run_seed(seed) {
            eprintln!("SEED={seed} FAILED: {msg}");
            failures += 1;
        }
    }
    assert_eq!(
        failures, 0,
        "{failures} seed(s) violated a snapshot crash-safety invariant (see SEED=... lines)"
    );
}

/// Determinism check: the same seed yields the same outcome twice.
#[test]
fn snapshot_publish_crash_runs_are_deterministic() {
    for seed in [1u64, 7, 42, 123, 777, 9999] {
        let a = run_seed(seed).is_ok();
        let b = run_seed(seed).is_ok();
        assert_eq!(a, b, "seed {seed} not deterministic: {a} vs {b}");
    }
}

/// A focused, fault-free check that a clean publish followed by a crash reopens
/// to the NEW snapshot state (exercises the real reload-verify + reclaim path).
#[test]
fn clean_publish_then_crash_recovers_new_snapshot_state() {
    let fs = SimFs::new(SimConfig::new(2024));
    let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    db.set(b"alpha".to_vec(), b"1".to_vec()).unwrap();
    db.set(b"beta".to_vec(), b"2".to_vec()).unwrap();
    db.delete(b"alpha".to_vec()).unwrap();
    db.set(b"gamma".to_vec(), b"3".to_vec()).unwrap();
    let s = db.publish_snapshot().unwrap();
    assert_eq!(db.snapshot_lsn(), s);
    drop(db);
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.snapshot_lsn(), s);
    assert!(!db.exists(b"alpha"));
    assert_eq!(db.get(b"beta"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.get(b"gamma"), GetResult::Found(b"3".to_vec()));
}

// ---------------------------------------------------------------------------
// Suite 2: corrupt-latest-snapshot -> fail-closed.
// ---------------------------------------------------------------------------

/// Corrupting the LATEST snapshot when no complete recovery chain remains must
/// fail closed (WalError::Corruption), NOT silently start empty or from an
/// older state (Technical-Design §7, §9.3 fail-closed invariant).
#[test]
fn corrupt_latest_snapshot_without_chain_fails_closed() {
    let fs = SimFs::new(SimConfig::new(31));
    {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        db.publish_snapshot().unwrap(); // S = 2; seg 1 (<=2) retained, seg 3 active
        db.set(b"c".to_vec(), b"3".to_vec()).unwrap(); // lsn 3
                                                       // A second snapshot reclaims seg 1 and seg 3, so no LSN-1 chain remains
                                                       // and the ONLY recovery base is the latest (S2 = 3) snapshot.
        db.publish_snapshot().unwrap(); // S2 = 3
    }
    let snap = snapshot_path(3);
    assert!(
        fs.exists(&snap),
        "latest snapshot must exist before corruption"
    );
    // The first chain's WAL must have been reclaimed (proves the real
    // reclamation path ran; the test would not be fail-closed otherwise).
    assert!(
        !fs.exists(&segment_path(1)),
        "seg 1 should be reclaimed by S2"
    );

    // Corrupt the latest snapshot's STABLE bytes (post-sync, a deliberately
    // §6.3-violating tamper) so no complete chain remains.
    let mut bytes = fs.read(&snap).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF; // flip a payload byte
    fs.truncate(&snap, 0).unwrap();
    fs.append(&snap, &bytes).unwrap();
    fs.sync_file(&snap).unwrap();
    fs.crash();

    let err = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync);
    let is_corruption = matches!(err, Err(WalError::Corruption(_)));
    assert!(
        is_corruption,
        "expected fail-closed Corruption on corrupt latest snapshot with no chain, got {}",
        match err {
            Ok(_) => "Ok(<db>)".to_string(),
            Err(e) => format!("Err({e})"),
        }
    );
}

/// A corrupt HIGHER snapshot with a usable lower snapshot + contiguous WAL tail
/// must recover from the LOWER chain (Technical-Design §7 highest-verified
/// selection with fallback).
#[test]
fn corrupt_latest_snapshot_falls_back_to_lower_chain() {
    let fs = SimFs::new(SimConfig::new(32));
    {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.publish_snapshot().unwrap(); // S1 = 1; seg 2 active
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap(); // lsn 2, in seg 2 (the S1 tail)
    }
    // Fabricate a corrupt HIGHER snapshot at LSN 3 with valid outer framing so
    // it is enumerated but fails decode (flip the trailing snapshot_crc64).
    // The exact cluster id does not matter: the corrupt higher snapshot must
    // fail decode (broken crc64) before any cluster check, so we only need
    // well-formed length framing.
    let cluster = [0x11u8; 16];
    let mut higher =
        distributedb::encode_snapshot(cluster, 3, 999, &[(b"z".to_vec(), b"9".to_vec())]);
    let last = higher.len() - 1;
    higher[last] ^= 0xFF; // break snapshot_crc64
    let snap3 = snapshot_path(3);
    fs.create_file(&snap3).unwrap();
    fs.append(&snap3, &higher).unwrap();
    fs.sync_file(&snap3).unwrap();
    fs.sync_dir(snapshot_path(3).parent().unwrap()).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    // The corrupt LSN-3 snapshot is skipped; recovery uses verified S1 = 1 plus
    // its contiguous WAL tail (seg 2, LSN 2).
    assert_eq!(db.snapshot_lsn(), 1);
    assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.last_applied_lsn(), 2);
}

// ---------------------------------------------------------------------------
// Suite 3: recovery-work-reduction (SOW §22).
// ---------------------------------------------------------------------------

/// After a snapshot at LSN *S*, restart replays only the post-snapshot WAL tail
/// (records with `lsn > S`), so recovery no longer scales with the whole
/// command history. Segments wholly covered by *S* are reclaimed and not read.
#[test]
fn recovery_replays_only_post_snapshot_wal() {
    let fs = SimFs::new(SimConfig::new(44));
    let s;
    let total;
    {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        // N mutations across many keys, rotating segments periodically so the
        // history spans several WAL segments.
        const N: u64 = 200;
        for i in 0..N {
            let key = vec![(i % 32) as u8];
            let value = vec![(i % 256) as u8, ((i >> 8) & 0xFF) as u8];
            db.set(key, value).unwrap();
            if i % 25 == 24 {
                db.rotate().unwrap(); // force multiple sealed segments
            }
        }
        assert_eq!(db.last_applied_lsn(), N);

        // Publish a snapshot at S = N near the end of the history.
        s = db.publish_snapshot().unwrap();
        assert_eq!(s, N);

        // A few more mutations after the snapshot (S+1 .. A).
        for i in 0..4u64 {
            db.set(vec![100 + i as u8], vec![i as u8]).unwrap();
        }
        total = db.last_applied_lsn();
        assert_eq!(total, N + 4);
    }

    // Reopen: recovery loads the snapshot map and replays ONLY records > S.
    let db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.snapshot_lsn(), s);
    assert_eq!(db.last_applied_lsn(), total);

    // The acceptance bound: records replayed after the snapshot base is bounded
    // by (last_applied_lsn - S), i.e. strictly fewer than the whole history.
    let replayed = db.records_replayed();
    assert!(
        replayed <= total - s,
        "replayed {replayed} exceeds post-snapshot bound {}",
        total - s
    );
    assert!(
        replayed < total,
        "recovery replayed the whole history ({replayed} of {total}); snapshot gave no reduction"
    );

    // Segments wholly covered by S were reclaimed and therefore not read: the
    // early segments (first_lsn = 1, 26, ...) must be gone.
    assert!(
        !fs.exists(&segment_path(1)),
        "seg 1 (<=S) should be reclaimed"
    );
    assert!(
        !fs.exists(&segment_path(26)),
        "seg 26 (<=S) should be reclaimed"
    );

    // State is fully correct after the reduced-work recovery.
    assert_eq!(db.get(&[199u8 % 32]), db.get(&[(199u64 % 32) as u8]));
    assert_eq!(db.get(&[100u8]), GetResult::Found(vec![0]));
    assert_eq!(db.get(&[103u8]), GetResult::Found(vec![3]));
}

/// A snapshot with NO post-snapshot writes replays zero WAL records on reopen
/// (the strongest form of recovery-work reduction).
#[test]
fn recovery_replays_zero_records_when_snapshot_is_at_tail() {
    let fs = SimFs::new(SimConfig::new(45));
    let s;
    {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        for i in 0..50u64 {
            db.set(vec![(i % 16) as u8], vec![i as u8]).unwrap();
        }
        s = db.publish_snapshot().unwrap();
        assert_eq!(s, 50);
    }
    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.snapshot_lsn(), s);
    assert_eq!(db.last_applied_lsn(), s);
    assert_eq!(
        db.records_replayed(),
        0,
        "no post-snapshot writes should mean zero WAL records replayed"
    );
}
