mod common;

use std::process::Command;

use common::TempDir;
use distributedb::{Db, DurabilityMode, GetResult, RealFs};

#[test]
fn offline_snapshot_preserves_data_and_reduces_replay() {
    let dir = TempDir::new("snapshot-cli");
    {
        let mut db = Db::open(RealFs, dir.path(), DurabilityMode::Fsync).unwrap();
        for index in 0..100u32 {
            db.set(format!("key:{index}").into_bytes(), format!("value:{index}").into_bytes())
                .unwrap();
        }
    }

    let snapshot = Command::new(env!("CARGO_BIN_EXE_distributedb"))
        .args(["snapshot", "--data"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(snapshot.status.success(), "{}", String::from_utf8_lossy(&snapshot.stderr));
    assert_eq!(String::from_utf8_lossy(&snapshot.stdout).trim(), "snapshot LSN: 100");

    let mut db = Db::open(RealFs, dir.path(), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.snapshot_lsn(), 100);
    assert_eq!(db.records_replayed(), 0);
    assert_eq!(db.get(b"key:37"), GetResult::Found(b"value:37".to_vec()));
    db.set(b"tail".to_vec(), b"after".to_vec()).unwrap();
    drop(db);

    let db = Db::open(RealFs, dir.path(), DurabilityMode::Fsync).unwrap();
    assert_eq!(db.snapshot_lsn(), 100);
    assert_eq!(db.records_replayed(), 1);
    assert_eq!(db.get(b"tail"), GetResult::Found(b"after".to_vec()));
}

#[test]
fn snapshot_refuses_an_active_data_directory() {
    let dir = TempDir::new("snapshot-locked");
    let mut db = Db::open(RealFs, dir.path(), DurabilityMode::Fsync).unwrap();
    db.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    let attempt = Command::new(env!("CARGO_BIN_EXE_distributedb"))
        .args(["snapshot", "--data"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!attempt.status.success());
    assert!(String::from_utf8_lossy(&attempt.stderr).contains("already locked"));
    assert_eq!(db.get(b"key"), GetResult::Found(b"value".to_vec()));
}
