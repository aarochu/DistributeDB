//! The LSM storage engine behind `Db` (SOW §16 Option B, Phase 8): recovery
//! from tables plus the WAL tail, WAL reclamation after flushes, replication
//! with table images, and the server path.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use common::TempDir;
use distributedb::{
    Client, Db, DurabilityMode, FileSystem, GetResult, LsmConfig, NodeRole, OpenConfig,
    PrimaryListener, RealFs, ReplicaRunner, ReplicationStats, Server, ServerConfig, SimConfig,
    SimFs, Status, StorageKind, WalError,
};

fn lsm_storage() -> StorageKind {
    StorageKind::Lsm(LsmConfig {
        memtable_bytes: 8 * 1024,
        l0_compaction_trigger: 3,
        target_table_bytes: 16 * 1024,
        durable: true,
    })
}

fn lsm() -> OpenConfig {
    OpenConfig {
        storage: lsm_storage(),
        ..OpenConfig::default()
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}").into_bytes()
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(Instant::now() < deadline, "did not converge in 20 seconds");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Overwrites and deletes over 1,000 keys, with the expected final state.
fn workload(db: &mut Db<SimFs>) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut model = HashMap::new();
    for i in 0..3000u32 {
        db.set(key(i % 1000), value(i)).unwrap();
        model.insert(key(i % 1000), value(i));
        if i % 7 == 0 {
            let victim = key((i * 3) % 1000);
            db.delete(victim.clone()).unwrap();
            model.remove(&victim);
        }
    }
    model
}

fn assert_matches(db: &Db<SimFs>, model: &HashMap<Vec<u8>, Vec<u8>>) {
    for i in 0..1000u32 {
        let expected = match model.get(&key(i)) {
            Some(value) => GetResult::Found(value.clone()),
            None => GetResult::NotFound,
        };
        assert_eq!(db.get(&key(i)), expected, "key {i}");
    }
    assert_eq!(db.len(), model.len());
}

#[test]
fn recovers_from_tables_and_the_wal_tail_and_reclaims_wal() {
    let fs = SimFs::new(SimConfig::new(1));
    let root = Path::new("/lsm");
    let mut db = Db::open_configured(fs.clone(), root, DurabilityMode::Fsync, lsm()).unwrap();
    let model = workload(&mut db);
    assert_eq!(db.storage_engine(), "lsm");
    let stats = db.lsm_stats().unwrap();
    assert!(stats.flushes >= 3, "{stats:?}");
    assert!(stats.compactions >= 1, "{stats:?}");
    assert_matches(&db, &model);
    let total_lsn = db.last_applied_lsn();

    // Flushed WAL is deleted: only segments after the last flush remain.
    let wal_dir = root.join("generations/0000000000000001/wal");
    let segments = fs.list_dir(&wal_dir).unwrap().len();
    assert!(segments <= 2, "{segments} WAL segments remain");
    // Replication history holds only records after the last flush.
    assert!(db.snapshot_lsn() > 0);
    assert!(db.record_hash_at(1).is_none());

    drop(db);
    fs.crash();
    let reopened = Db::open_configured(fs, root, DurabilityMode::Fsync, lsm()).unwrap();
    assert_eq!(reopened.last_applied_lsn(), total_lsn);
    assert_matches(&reopened, &model);
    assert!(
        reopened.records_replayed() < 500,
        "replayed {} of {total_lsn} records",
        reopened.records_replayed()
    );
}

#[test]
fn engine_choice_is_persisted_and_snapshots_are_memory_only() {
    let fs = SimFs::new(SimConfig::new(2));
    let mut db =
        Db::open_configured(fs.clone(), Path::new("/a"), DurabilityMode::Fsync, lsm()).unwrap();
    db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
    assert!(matches!(db.publish_snapshot(), Err(WalError::Identity(_))));
    drop(db);
    assert!(matches!(
        Db::open(fs.clone(), Path::new("/a"), DurabilityMode::Fsync),
        Err(WalError::Identity(_))
    ));

    drop(Db::open(fs.clone(), Path::new("/b"), DurabilityMode::Fsync).unwrap());
    assert!(matches!(
        Db::open_configured(fs, Path::new("/b"), DurabilityMode::Fsync, lsm()),
        Err(WalError::Identity(_))
    ));
}

#[test]
fn replicas_behind_the_flush_boundary_catch_up_from_a_table_image() {
    let primary_fs = SimFs::new(SimConfig::new(3));
    let mut primary = Db::open_configured(
        primary_fs,
        Path::new("/primary"),
        DurabilityMode::Fsync,
        lsm(),
    )
    .unwrap();
    let cluster_id = primary.identity().cluster_id;
    let model = workload(&mut primary);
    assert!(primary.snapshot_lsn() > 0, "history was trimmed by flushes");
    let primary = Arc::new(RwLock::new(primary));
    let mut listener = PrimaryListener::start(
        "127.0.0.1:0",
        Arc::clone(&primary),
        Arc::new(ReplicationStats::default()),
    )
    .unwrap();

    for (name, storage) in [("lsm", lsm_storage()), ("memory", StorageKind::Memory)] {
        let replica = Arc::new(RwLock::new(
            Db::open_configured(
                SimFs::new(SimConfig::new(4)),
                Path::new("/replica"),
                DurabilityMode::Fsync,
                OpenConfig {
                    role: NodeRole::Replica,
                    cluster_id: Some(cluster_id),
                    allow_snapshot_rebootstrap: true,
                    storage,
                    ..OpenConfig::default()
                },
            )
            .unwrap(),
        ));
        let mut runner = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica)).unwrap();
        let target = primary.read().unwrap().last_applied_lsn();
        wait_until(|| replica.read().unwrap().last_applied_lsn() == target);

        // New writes stream as records after the image.
        for i in 0..300u32 {
            primary
                .write()
                .unwrap()
                .set(key(i), format!("{name}-{i}").into_bytes())
                .unwrap();
        }
        let target = primary.read().unwrap().last_applied_lsn();
        wait_until(|| replica.read().unwrap().last_applied_lsn() == target);
        assert!(runner.fatal_error().is_none(), "{name} replica failed");
        runner.shutdown();

        let replica = replica.read().unwrap();
        let primary = primary.read().unwrap();
        assert_eq!(replica.storage_engine(), name);
        assert_eq!(
            replica.record_hash_at(target),
            primary.record_hash_at(target)
        );
        for i in 0..1000u32 {
            assert_eq!(replica.get(&key(i)), primary.get(&key(i)), "{name} key {i}");
        }
        let expected_tail = model.get(&key(999)).cloned();
        assert_eq!(
            replica.get(&key(999)),
            expected_tail.map_or(GetResult::NotFound, GetResult::Found)
        );
    }
    listener.shutdown();
}

#[test]
fn server_writes_flush_and_survive_restart() {
    let temp = TempDir::new("lsm-server");
    let open = || Db::open_configured(RealFs::new(), temp.path(), DurabilityMode::Fsync, lsm());
    let mut server =
        Server::start("127.0.0.1:0", open().unwrap(), ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();
    for i in 0..3000u32 {
        assert_eq!(client.set(key(i % 1500), value(i)).unwrap(), Status::Ok);
    }
    for i in 0..1500u32 {
        assert_eq!(
            client.get(key(i)).unwrap(),
            Some(value(i + 1500)),
            "key {i}"
        );
    }
    let stats = client.stats().unwrap();
    assert!(
        stats.lines().any(|line| line == "storage_engine=lsm"),
        "{stats}"
    );
    let flushes: u64 = stats
        .lines()
        .find_map(|line| line.strip_prefix("lsm_flushes_total="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(flushes >= 1, "{stats}");
    drop(client);
    server.shutdown();
    drop(server);

    let db = open().unwrap();
    for i in 0..1500u32 {
        assert_eq!(
            db.get(&key(i)),
            GetResult::Found(value(i + 1500)),
            "key {i}"
        );
    }
}
