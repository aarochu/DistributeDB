//! Corrupt-final-WAL-entry and fail-closed corruption tests (FEAT-003 step 4;
//! SOW §19; Technical-Design §6.3).
//!
//! Two boundaries from §6.3 are documented here:
//!
//! * A corrupt / incomplete record in the FINAL (active) group is safely
//!   ignored: recovery discards the torn final group (`tail_truncated`) and
//!   preserves every earlier acknowledged (footer-closed, synced) group.
//! * Corruption inside a SEALED (non-final) segment, or inside an already
//!   acknowledged synced group, makes recovery FAIL CLOSED: it returns
//!   [`WalError::Corruption`] and refuses to serve, rather than silently
//!   dropping or misapplying data.

use distributedb::{
    Db, DurabilityMode, FileSystem, GetResult, MutationRecord, RecordType, SimConfig, SimFs,
    WalError,
};
use std::path::{Path, PathBuf};

const ROOT: &str = "/data";

fn segment_path(first: u64) -> PathBuf {
    Path::new(ROOT)
        .join("generations")
        .join("0000000000000001")
        .join("wal")
        .join(format!("{first:020}.wal"))
}

fn sim() -> SimFs {
    SimFs::new(SimConfig::new(0xBADF00D))
}

fn open(fs: SimFs) -> Result<Db<SimFs>, WalError> {
    Db::open(fs, Path::new(ROOT), DurabilityMode::Fsync)
}

/// Overwrite `seg`'s stable+volatile bytes with `bytes` (models on-disk
/// corruption: replace the whole file, then sync so it is durable).
fn overwrite(fs: &SimFs, seg: &Path, bytes: &[u8]) {
    fs.truncate(seg, 0).unwrap();
    fs.append(seg, bytes).unwrap();
    fs.sync_file(seg).unwrap();
}

#[test]
fn corrupt_final_record_is_ignored_and_earlier_groups_preserved() {
    // Write two synced groups, then append an intact-looking third record with
    // a valid CRC but corrupt its bytes so its CRC no longer matches. Because
    // it is in the FINAL (active) group with no closing footer, recovery must
    // treat it as a torn tail and preserve the earlier acknowledged groups.
    let fs = sim();
    let seg = {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
        segment_path(1)
    };

    // Append a standalone record (no footer) for lsn=3, then flip a byte in it.
    let rec = MutationRecord {
        lsn: 3,
        rtype: RecordType::Set,
        key: b"C".to_vec(),
        value: b"3".to_vec(),
        prev_hash: 0, // chain value irrelevant: the corruption is what matters
    };
    let mut encoded = rec.encode();
    let mid = encoded.len() / 2;
    encoded[mid] ^= 0xff; // corrupt the payload; stored CRC now mismatches
    fs.append(&seg, &encoded).unwrap();
    fs.sync_file(&seg).unwrap();
    fs.crash();

    let db = open(fs).unwrap();
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.get(b"C"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 2);
    assert!(db.tail_truncated());
}

#[test]
fn interior_corruption_in_active_segment_prefix_fails_closed_after_rotation() {
    // The strongest form of "never discard an acknowledged synced group"
    // (§6.3) is enforced by making the damaged, footer-closed group live in a
    // SEALED segment. Here we corrupt a record inside segment 1's committed
    // group AND write a later group into segment 2 (so segment 1 is sealed);
    // recovery must fail closed rather than drop the acknowledged data.
    //
    // (When the damaged footer-closed group is instead in the *active*
    // segment, the current recovery treats the earliest parse failure as a
    // torn tail and truncates from there -- that documented active-segment
    // behavior is covered by
    // `active_segment_corruption_truncates_the_tail` below.)
    let fs = sim();
    let seg1 = {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
        db.rotate().unwrap(); // seal segment 1
        db.set(b"C".to_vec(), b"3".to_vec()).unwrap();
        segment_path(1)
    };
    let mut bytes = fs.stable_bytes(&seg1).unwrap();
    let idx = 64 + 30; // inside sealed segment 1's first committed record
    bytes[idx] ^= 0xff;
    overwrite(&fs, &seg1, &bytes);
    fs.crash();

    assert_fails_closed(open(fs), "sealed footer-closed group corruption");
}

#[test]
fn active_segment_corruption_truncates_the_tail() {
    // Documented active-segment boundary (§6.3): in the LAST active segment,
    // the earliest parse failure is treated as the start of a torn tail. The
    // footer-closed prefix BEFORE the corruption is preserved; everything from
    // the corruption onward is discarded and the segment is truncated
    // (`tail_truncated`). This is why fail-closed corruption tests target a
    // SEALED segment (see above).
    let fs = sim();
    let seg = {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
        segment_path(1)
    };
    // Corrupt a byte inside the SECOND committed group's record ("B").
    let mut bytes = fs.stable_bytes(&seg).unwrap();
    // Group 1 = header(64) + record A + footer; corrupt inside record B which
    // follows. Locate record B by scanning past the first record + footer.
    let a_len = MutationRecord {
        lsn: 1,
        rtype: RecordType::Set,
        key: b"A".to_vec(),
        value: b"1".to_vec(),
        prev_hash: 0,
    }
    .encoded_len();
    let b_payload = 64 + a_len + 40 + 30; // header + recA + footer + into recB
    bytes[b_payload] ^= 0xff;
    overwrite(&fs, &seg, &bytes);
    fs.crash();

    let db = open(fs).expect("active-segment tail truncation, not fail-closed");
    // The acknowledged prefix (A) survives; the corrupted tail (B) is dropped.
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::NotFound);
    assert_eq!(db.last_applied_lsn(), 1);
    assert!(db.tail_truncated());
}

#[test]
fn corruption_in_sealed_segment_fails_closed() {
    // After rotation, segment 1 is SEALED. Any damage in it is interior
    // corruption and recovery must fail closed (not treat it as an active
    // torn tail).
    let fs = sim();
    let seg1 = {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.rotate().unwrap(); // seal segment 1, open segment 2
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        segment_path(1)
    };
    let mut bytes = fs.stable_bytes(&seg1).unwrap();
    let idx = 64 + 25; // inside sealed segment 1's first record
    bytes[idx] ^= 0xff;
    overwrite(&fs, &seg1, &bytes);
    fs.crash();

    assert_fails_closed(open(fs), "sealed-segment damage");
}

#[test]
fn corrupt_segment_header_fails_closed() {
    // Damaging the fixed segment header (magic/version/crc) is unrecoverable
    // interior corruption regardless of position.
    let fs = sim();
    let seg = {
        let mut db = Db::open(fs.clone(), Path::new(ROOT), DurabilityMode::Fsync).unwrap();
        db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        segment_path(1)
    };
    let mut bytes = fs.stable_bytes(&seg).unwrap();
    bytes[0] ^= 0xff; // corrupt the magic
    overwrite(&fs, &seg, &bytes);
    fs.crash();

    match open(fs) {
        Err(WalError::Corruption(_)) | Err(WalError::Format(_)) => {}
        Err(other) => panic!("expected header corruption failure, got error {other}"),
        Ok(_) => panic!("expected header corruption failure, but recovery succeeded"),
    }
}

/// Assert that a `Db::open` result failed closed with a corruption error.
/// Avoids `{:?}`-formatting the `Ok(Db<..>)` arm (which is not `Debug`).
fn assert_fails_closed(result: Result<Db<SimFs>, WalError>, context: &str) {
    match result {
        Err(WalError::Corruption(_)) => {}
        Err(other) => panic!("{context}: expected Corruption, got error {other}"),
        Ok(_) => panic!("{context}: expected Corruption, but recovery succeeded"),
    }
}
