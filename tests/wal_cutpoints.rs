//! Deterministic cut-point and torn-write recovery tests (FEAT-003 step 3;
//! Technical-Design §6.3, §13; SOW §19).
//!
//! Phase 2's acceptance gate requires exercising EVERY deterministic cut point
//! in a short trace and asserting the §6.3 recovery outcome:
//!
//! * crash **before append** — nothing new persisted, prior state intact;
//! * crash **during a short/partial write** — the torn record and its group
//!   are discarded;
//! * crash **before the group sync** — the whole unsynced group is discarded;
//! * crash **after the group sync but before map apply** — the group IS
//!   durable and replays on restart (write-ahead order: durable-then-apply);
//! * crash **after apply but before response** — identical on-disk state to
//!   the previous case, so it also replays.
//!
//! It also requires the specific TORN-WRITE case: SimFs persisted an intact
//! later record after a torn earlier record in the same unsynced group;
//! recovery must discard the WHOLE group after the last valid footer and must
//! NOT resurrect the later intact record.
//!
//! Rather than depending on private WAL internals, these tests reconstruct the
//! exact on-disk layout using the crate's public byte-format types
//! ([`SegmentHeader`], [`MutationRecord`], [`GroupFooter`]) written through a
//! [`SimFs`], then reopen with [`Db`] and assert the outcome. The segment path
//! is the documented layout: `<root>/generations/0000000000000001/wal/`
//! with segments named by their zero-padded (20-digit) first LSN.

use distributedb::{
    Db, DurabilityMode, FileSystem, GetResult, GroupFooter, MutationRecord, RecordType, SimConfig,
    SimFs,
};
use std::path::{Path, PathBuf};

const ROOT: &str = "/data";

/// The documented WAL segment path for a segment whose first LSN is `first`.
fn segment_path(first: u64) -> PathBuf {
    Path::new(ROOT)
        .join("generations")
        .join("0000000000000001")
        .join("wal")
        .join(format!("{first:020}.wal"))
}

/// A no-fault SimFs (all randomness disabled; we drive crashes explicitly).
fn sim() -> SimFs {
    SimFs::new(SimConfig::new(0xC0FFEE))
}

/// Open a fresh db, run a fresh-init so the layout/segment header exist.
fn fresh(fs: &SimFs) -> Db<SimFs> {
    Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).expect("open fresh")
}

/// Encode a SET record at `lsn` chaining `prev_hash`; returns (bytes, hash).
fn set_record(lsn: u64, prev_hash: u64, key: &[u8], value: &[u8]) -> (Vec<u8>, u64) {
    let rec = MutationRecord {
        lsn,
        rtype: RecordType::Set,
        key: key.to_vec(),
        value: value.to_vec(),
        prev_hash,
    };
    let bytes = rec.encode();
    let hash = rec.record_hash();
    (bytes, hash)
}

/// Encode the footer that closes a group of `count` records.
fn footer(first_lsn: u64, last_lsn: u64, count: u32, last_record_hash: u64) -> Vec<u8> {
    GroupFooter {
        first_lsn,
        last_lsn,
        count,
        last_record_hash,
    }
    .encode()
}

// ---------------------------------------------------------------------------
// Cut point: crash BEFORE append.
// ---------------------------------------------------------------------------

#[test]
fn crash_before_append_keeps_prior_state() {
    let fs = sim();
    {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap(); // durable group, synced
    }
    // A crash happens with no further append attempted.
    fs.crash();
    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(!db.tail_truncated());
}

// ---------------------------------------------------------------------------
// Cut point: crash BEFORE the group sync (records appended, not synced).
// ---------------------------------------------------------------------------

#[test]
fn crash_before_sync_discards_unsynced_group() {
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    // Determine the prev_hash after the first committed group.
    let prev_hash = last_committed_hash(&fs, &seg);
    // Append a full, valid group-2 (records + footer) but DO NOT sync it.
    let (rec, hash) = set_record(2, prev_hash, b"B", b"2");
    let mut group = rec;
    group.extend_from_slice(&footer(2, 2, 1, hash));
    fs.append(&seg, &group).unwrap();
    // No sync_file => these bytes are volatile only.
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    // The unsynced group vanished; only the synced group-1 survives.
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(!db.tail_truncated());
}

// ---------------------------------------------------------------------------
// Cut point: crash AFTER the group sync but before map apply / before response.
// ---------------------------------------------------------------------------

#[test]
fn crash_after_sync_before_apply_replays_group() {
    // Write-ahead order (Technical-Design §3, §6.2): a group that is appended
    // AND synced is durable even if the process dies before applying it to the
    // map or before returning OK. Recovery must replay it.
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    let prev_hash = last_committed_hash(&fs, &seg);
    let (rec, hash) = set_record(2, prev_hash, b"B", b"2");
    let mut group = rec;
    group.extend_from_slice(&footer(2, 2, 1, hash));
    fs.append(&seg, &group).unwrap();
    // Sync makes the group durable, THEN we crash before any "apply"/response.
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    // The synced group survives and replays.
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.last_applied_lsn(), 2);
    assert!(!db.tail_truncated());
}

// ---------------------------------------------------------------------------
// Cut point: crash DURING a short/partial write (torn tail record).
// ---------------------------------------------------------------------------

#[test]
fn crash_during_short_write_discards_torn_record() {
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    let prev_hash = last_committed_hash(&fs, &seg);
    let (rec, _hash) = set_record(2, prev_hash, b"B", b"2");
    // Persist only a prefix of the record (a short write), no footer.
    let torn = &rec[..rec.len() - 5];
    fs.append(&seg, torn).unwrap();
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(db.tail_truncated());
}

// ---------------------------------------------------------------------------
// The TORN-WRITE case: an intact LATER record after a torn EARLIER record in
// the same unsynced group must NOT be resurrected (Technical-Design §6.3).
// ---------------------------------------------------------------------------

#[test]
fn torn_earlier_then_intact_later_record_discards_whole_group() {
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    let prev_hash = last_committed_hash(&fs, &seg);

    // Group 2 would be [rec(lsn=2 "B"), rec(lsn=3 "C"), footer]. Model SimFs
    // persisting the FIRST record only partially (torn) but the SECOND record
    // fully intact after it. Recovery scans left-to-right, hits the torn first
    // record, and must discard everything after the last valid footer -- it
    // must NOT skip ahead and resurrect the intact "C".
    let (rec_b, hash_b) = set_record(2, prev_hash, b"B", b"2");
    let (rec_c, _hash_c) = set_record(3, hash_b, b"C", b"3");

    // Torn first record: drop its trailing bytes (including its CRC).
    let torn_b = &rec_b[..rec_b.len() - 6];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(torn_b);
    bytes.extend_from_slice(&rec_c); // intact later record right after the tear
    fs.append(&seg, &bytes).unwrap();
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    // Neither the torn "B" nor the intact-but-orphaned "C" may appear.
    assert_eq!(db.get(b"B"), GetResult::NotFound);
    assert_eq!(db.get(b"C"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(db.tail_truncated());
}

#[test]
fn missing_footer_discards_valid_looking_records() {
    // A whole group's records persisted intact but its FOOTER never did.
    // Only footer-closed groups are replayable, so the group is discarded.
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    let prev_hash = last_committed_hash(&fs, &seg);
    let (rec_b, hash_b) = set_record(2, prev_hash, b"B", b"2");
    let (rec_c, _hash_c) = set_record(3, hash_b, b"C", b"3");
    let mut bytes = rec_b;
    bytes.extend_from_slice(&rec_c);
    // Intentionally no footer.
    fs.append(&seg, &bytes).unwrap();
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::NotFound);
    assert_eq!(db.get(b"C"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(db.tail_truncated());
}

// ---------------------------------------------------------------------------
// The §6.3 core guarantee, exercised head-on: an acknowledged (synced,
// footer-CLOSED) group survives a torn group that is appended AFTER it in the
// SAME active segment. Complements the torn-tail-only and sealed-segment
// fail-closed cases: here the surviving group is a genuine MULTI-record group
// and it is followed by a fully-synced-but-unclosed group.
// ---------------------------------------------------------------------------

#[test]
fn synced_closed_group_survives_following_torn_group_in_active_segment() {
    let fs = sim();
    let seg = {
        let mut db = fresh(&fs);
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap(); // group 1 (lsn 1), synced
        segment_path(1)
    };

    // Group A (the acknowledged group under test): TWO records (lsn 2 and 3)
    // closed by a valid footer, then synced -> durable & acknowledged.
    let prev_hash = last_committed_hash(&fs, &seg);
    let (rec_b, hash_b) = set_record(2, prev_hash, b"B", b"2");
    let (rec_c, hash_c) = set_record(3, hash_b, b"C", b"3");
    let mut group_a = rec_b;
    group_a.extend_from_slice(&rec_c);
    group_a.extend_from_slice(&footer(2, 3, 2, hash_c));
    fs.append(&seg, &group_a).unwrap();
    fs.sync_file(&seg).unwrap(); // group A is now acknowledged (durable)

    // Group B: records for lsn 4 and 5 but NO valid footer (torn / unclosed).
    // It is even synced to disk, so this is not merely an unsynced tail: the
    // bytes are stable, yet the group is not footer-closed and must not replay.
    let (rec_d, hash_d) = set_record(4, hash_c, b"D", b"4");
    let (rec_e, _hash_e) = set_record(5, hash_d, b"E", b"5");
    let mut group_b = rec_d;
    group_b.extend_from_slice(&rec_e);
    // Intentionally no footer for group B.
    fs.append(&seg, &group_b).unwrap();
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync).unwrap();
    // (a) Every record of the acknowledged closed groups replays.
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.get(b"C"), GetResult::Found(b"3".to_vec()));
    // (c) None of the torn, unclosed group B is resurrected.
    assert_eq!(db.get(b"D"), GetResult::NotFound);
    assert_eq!(db.get(b"E"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 3);
    // (b) The trailing torn group was truncated away.
    assert!(db.tail_truncated());
}

/// Recover the record hash of the last footer-closed record in `seg` by
/// decoding the stable bytes. Used to chain the synthetic trailing group so
/// only the footer/tear (not the chain) determines the outcome.
fn last_committed_hash(fs: &SimFs, seg: &Path) -> u64 {
    // Sync so we read the committed bytes; the first group is already synced by
    // the durable set() above, so stable == what we appended.
    let bytes = fs.stable_bytes(seg).expect("segment exists");
    // Walk footer-closed records: the segment header is 64 bytes; decode
    // records until we consume the whole synced region, tracking the last hash.
    let mut off = 64usize;
    let mut last_hash = 0u64;
    while off < bytes.len() {
        // Footer magic marks a group boundary; skip 40-byte footers.
        if bytes.len() - off >= 8 && &bytes[off..off + 8] == b"DDBGRP01" {
            off += 40;
            continue;
        }
        match MutationRecord::decode(&bytes[off..]) {
            Ok(d) => {
                last_hash = d.record_hash;
                off += d.consumed;
            }
            Err(_) => break,
        }
    }
    last_hash
}
