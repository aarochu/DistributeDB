//! Durable write + restart-and-recover integration tests over the REAL
//! file-I/O adapter (FEAT-003 step 1; SOW §8, §20; Technical-Design §9).
//!
//! These exercise the full durable path on a real temp data directory: open a
//! fresh data dir, perform durable writes through [`Db`], drop the handle
//! (releasing the LOCK), reopen the same directory, and assert the
//! reconstructed [`StorageEngine`] state and `last_applied_lsn` match what was
//! acknowledged before the restart. This is the automated form of the SOW §8
//! "SET A 1 / SET B 2 / SET C 3 / restart / GET" worked example.

mod common;

use common::TempDir;
use distributedb::{Db, DurabilityMode, GetResult, RealFs};

/// Open a durable `Db` on the real filesystem at `root` in `fsync` mode.
fn open(root: &std::path::Path) -> Db<RealFs> {
    Db::open(RealFs::new(), root, DurabilityMode::Fsync).expect("open db")
}

#[test]
fn sow_set_a_b_c_restart_recovers_on_real_fs() {
    // SOW §8 worked example, driven over RealFs with a genuine reopen.
    let dir = TempDir::new("recovery-abc");
    {
        let mut db = open(dir.path());
        assert_eq!(db.set(b"A".to_vec(), b"1".to_vec()).unwrap(), 1);
        assert_eq!(db.set(b"B".to_vec(), b"2".to_vec()).unwrap(), 2);
        assert_eq!(db.set(b"C".to_vec(), b"3".to_vec()).unwrap(), 3);
        assert_eq!(db.last_applied_lsn(), 3);
        assert_eq!(db.last_durable_lsn(), 3);
        // Drop releases the LOCK so we can reopen the same directory.
    }

    let db = open(dir.path());
    assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
    assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
    assert_eq!(db.get(b"C"), GetResult::Found(b"3".to_vec()));
    assert_eq!(db.last_applied_lsn(), 3);
    // A clean restart of a fully-synced log truncates nothing.
    assert!(!db.tail_truncated());
}

#[test]
fn delete_then_recover_over_real_fs() {
    let dir = TempDir::new("recovery-delete");
    {
        let mut db = open(dir.path());
        db.set(b"x".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"y".to_vec(), b"2".to_vec()).unwrap();
        db.delete(b"x".to_vec()).unwrap();
        // DELETE of an absent key still advances the LSN (Technical-Design §3).
        assert_eq!(db.delete(b"absent".to_vec()).unwrap(), 4);
    }

    let db = open(dir.path());
    assert_eq!(db.get(b"x"), GetResult::NotFound);
    assert_eq!(db.get(b"y"), GetResult::Found(b"2".to_vec()));
    assert!(!db.exists(b"x"));
    assert_eq!(db.last_applied_lsn(), 4);
}

#[test]
fn empty_value_round_trips_across_restart() {
    // An empty stored value must recover as Found(b""), never NotFound
    // (matches StorageEngine::get semantics).
    let dir = TempDir::new("recovery-empty");
    {
        let mut db = open(dir.path());
        db.set(b"k".to_vec(), Vec::new()).unwrap();
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k"), GetResult::Found(Vec::new()));
    assert!(db.exists(b"k"));
    assert_eq!(db.last_applied_lsn(), 1);
}

#[test]
fn reopen_fresh_empty_dir_is_empty() {
    // Opening a brand-new directory twice with no writes recovers an empty,
    // LSN-0 database each time.
    let dir = TempDir::new("recovery-fresh");
    {
        let db = open(dir.path());
        assert!(db.is_empty());
        assert_eq!(db.last_applied_lsn(), 0);
    }
    let db = open(dir.path());
    assert!(db.is_empty());
    assert_eq!(db.last_applied_lsn(), 0);
    assert!(!db.tail_truncated());
}

#[test]
fn binary_keys_and_values_recover() {
    // Non-UTF8 keys/values must survive a restart byte-for-byte.
    let dir = TempDir::new("recovery-binary");
    let key = vec![0u8, 255, 1, 254, 2];
    let value = vec![7u8, 0, 9, 0, 42];
    {
        let mut db = open(dir.path());
        db.set(key.clone(), value.clone()).unwrap();
    }
    let db = open(dir.path());
    assert_eq!(db.get(&key), GetResult::Found(value));
}

#[test]
fn many_groups_recover_in_order() {
    // Write a longer sequence of single-mutation groups and confirm the final
    // value for each key survives (later SET wins) after a restart.
    let dir = TempDir::new("recovery-many");
    {
        let mut db = open(dir.path());
        for i in 0..50u32 {
            db.set(b"counter".to_vec(), i.to_le_bytes().to_vec())
                .unwrap();
        }
        assert_eq!(db.last_applied_lsn(), 50);
    }
    let db = open(dir.path());
    assert_eq!(
        db.get(b"counter"),
        GetResult::Found(49u32.to_le_bytes().to_vec())
    );
    assert_eq!(db.last_applied_lsn(), 50);
}
