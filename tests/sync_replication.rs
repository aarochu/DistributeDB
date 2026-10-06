//! Synchronous replication (SOW §11): with `sync_replicas`, a write answers
//! `OK` only after that many replicas have synced and applied it.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, GetResult, NodeRole, OpenConfig, PrimaryListener, ReplicaRunner,
    ReplicationStats, Server, ServerConfig, SimConfig, SimFs, Status,
};

struct Primary {
    db: Arc<RwLock<Db<SimFs>>>,
    server: Server,
    listener: PrimaryListener,
}

fn primary(seed: u64, sync_replicas: usize, sync_timeout: Duration) -> Primary {
    let fs = SimFs::new(SimConfig::new(seed));
    let db = Db::open(fs, Path::new("/primary"), DurabilityMode::Fsync).unwrap();
    let db = Arc::new(RwLock::new(db));
    let stats = Arc::new(ReplicationStats::default());
    let config = ServerConfig {
        replication_stats: Some(Arc::clone(&stats)),
        sync_replicas,
        sync_timeout,
        ..ServerConfig::default()
    };
    let server = Server::start_shared("127.0.0.1:0", Arc::clone(&db), config).unwrap();
    let listener = PrimaryListener::start("127.0.0.1:0", Arc::clone(&db), stats).unwrap();
    Primary {
        db,
        server,
        listener,
    }
}

fn replica(primary: &Primary, seed: u64) -> (Arc<RwLock<Db<SimFs>>>, ReplicaRunner) {
    let cluster_id = primary.db.read().unwrap().identity().cluster_id;
    let config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        ..OpenConfig::default()
    };
    let fs = SimFs::new(SimConfig::new(seed));
    let db = Db::open_configured(fs, Path::new("/replica"), DurabilityMode::Fsync, config).unwrap();
    let db = Arc::new(RwLock::new(db));
    let runner = ReplicaRunner::start(primary.listener.local_addr(), Arc::clone(&db)).unwrap();
    (db, runner)
}

#[test]
fn ok_means_the_replica_already_has_the_write() {
    let mut primary = primary(61, 1, Duration::from_secs(10));
    let (replica_db, mut runner) = replica(&primary, 62);
    let mut client = Client::connect(primary.server.local_addr()).unwrap();
    for i in 0..50u32 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        assert_eq!(client.set(key.clone(), value.clone()).unwrap(), Status::Ok);
        // No waiting: OK was sent only after the replica's ACK.
        assert_eq!(
            replica_db.read().unwrap().get(&key),
            GetResult::Found(value),
            "key-{i} missing on the replica when OK arrived"
        );
    }
    // A transaction is acknowledged the same way, as one group.
    assert_eq!(client.begin().unwrap(), Status::Ok);
    assert_eq!(
        client.set(b"t1".to_vec(), b"a".to_vec()).unwrap(),
        Status::Queued
    );
    assert_eq!(
        client.set(b"t2".to_vec(), b"b".to_vec()).unwrap(),
        Status::Queued
    );
    assert_eq!(client.commit().unwrap(), Status::Ok);
    assert_eq!(
        replica_db.read().unwrap().get(b"t2"),
        GetResult::Found(b"b".to_vec())
    );
    let stats = client.stats().unwrap();
    assert!(
        stats.lines().any(|line| line == "sync_replicas=1"),
        "{stats}"
    );
    assert!(
        stats
            .lines()
            .any(|line| line == "sync_ack_timeouts_total=0"),
        "{stats}"
    );

    drop(client);
    runner.shutdown();
    primary.server.shutdown();
    primary.listener.shutdown();
}

#[test]
fn missing_acks_time_out_as_unavailable_but_the_write_is_durable() {
    let timeout = Duration::from_millis(200);
    let mut primary = primary(63, 1, timeout);
    let mut client = Client::connect(primary.server.local_addr()).unwrap();
    let started = Instant::now();
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Unavailable
    );
    assert!(started.elapsed() >= timeout);
    // The outcome is unknown to the client, but the primary committed it,
    // and a replica that connects later receives it.
    assert_eq!(client.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    let stats = client.stats().unwrap();
    assert!(
        stats
            .lines()
            .any(|line| line == "sync_ack_timeouts_total=1"),
        "{stats}"
    );

    let (replica_db, mut runner) = replica(&primary, 64);
    let deadline = Instant::now() + Duration::from_secs(10);
    while replica_db.read().unwrap().get(b"k") != GetResult::Found(b"v".to_vec()) {
        assert!(Instant::now() < deadline, "late replica did not catch up");
        std::thread::sleep(Duration::from_millis(20));
    }
    // With the replica connected, writes are acknowledged normally again.
    assert_eq!(
        client.set(b"k2".to_vec(), b"v".to_vec()).unwrap(),
        Status::Ok
    );

    drop(client);
    runner.shutdown();
    primary.server.shutdown();
    primary.listener.shutdown();
}

#[test]
fn sync_mode_without_a_replication_listener_is_rejected() {
    let fs = SimFs::new(SimConfig::new(65));
    let db = Db::open(fs, Path::new("/db"), DurabilityMode::Fsync).unwrap();
    let config = ServerConfig {
        sync_replicas: 1,
        ..ServerConfig::default()
    };
    let error = Server::start("127.0.0.1:0", db, config)
        .err()
        .expect("sync_replicas without replication must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
