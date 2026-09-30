//! Two-node and three-node asynchronous replication over loopback TCP.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, GetResult, NodeRole, OpenConfig, PrimaryListener, ReplicaRunner,
    ReplicationStats, Server, ServerConfig, SimConfig, SimFs, Status,
};

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "replication did not converge in 10 seconds");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn two_replicas_catch_up_after_one_disconnects() {
    let primary_fs = SimFs::new(SimConfig::new(1));
    let primary_db = Db::open(primary_fs, Path::new("/primary"), DurabilityMode::Fsync).unwrap();
    let cluster_id = primary_db.identity().cluster_id;
    let primary_db = Arc::new(RwLock::new(primary_db));
    let stats = Arc::new(ReplicationStats::default());
    let config = ServerConfig {
        replication_stats: Some(Arc::clone(&stats)),
        ..ServerConfig::default()
    };
    let mut server = Server::start_shared("127.0.0.1:0", Arc::clone(&primary_db), config).unwrap();
    let mut listener = PrimaryListener::start("127.0.0.1:0", Arc::clone(&primary_db), stats).unwrap();

    let replica_config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        ..OpenConfig::default()
    };
    let replica_a_fs = SimFs::new(SimConfig::new(2));
    let replica_b_fs = SimFs::new(SimConfig::new(3));
    let replica_a = Arc::new(RwLock::new(
        Db::open_configured(replica_a_fs, Path::new("/replica-a"), DurabilityMode::Fsync, replica_config).unwrap(),
    ));
    let replica_b = Arc::new(RwLock::new(
        Db::open_configured(replica_b_fs, Path::new("/replica-b"), DurabilityMode::Fsync, replica_config).unwrap(),
    ));
    let mut runner_a = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_a)).unwrap();
    let mut runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();

    for index in 0..40 {
        assert_eq!(client.set(format!("key-{index}").into_bytes(), format!("value-{index}").into_bytes()).unwrap(), Status::Ok);
    }
    wait_until(|| replica_a.read().unwrap().last_applied_lsn() == 40 && replica_b.read().unwrap().last_applied_lsn() == 40);
    assert_eq!(replica_a.read().unwrap().record_hash_at(40), primary_db.read().unwrap().record_hash_at(40));
    runner_b.shutdown();

    for index in 40..80 {
        assert_eq!(client.set(format!("key-{index}").into_bytes(), format!("value-{index}").into_bytes()).unwrap(), Status::Ok);
    }
    wait_until(|| replica_a.read().unwrap().last_applied_lsn() == 80);
    assert_eq!(replica_b.read().unwrap().last_applied_lsn(), 40);
    runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    wait_until(|| replica_b.read().unwrap().last_applied_lsn() == 80);
    assert_eq!(replica_b.read().unwrap().get(b"key-79"), GetResult::Found(b"value-79".to_vec()));
    assert_eq!(replica_b.read().unwrap().record_hash_at(80), primary_db.read().unwrap().record_hash_at(80));
    let response = client.stats().unwrap();
    assert!(response.contains("replicas_connected=2"));
    assert!(response.contains("_lag=0"));
    assert!(runner_a.fatal_error().is_none());
    assert!(runner_b.fatal_error().is_none());

    drop(client);
    runner_a.shutdown();
    runner_b.shutdown();
    listener.shutdown();
    server.shutdown();
}

#[test]
fn mismatched_cluster_stops_replica_without_rewriting_history() {
    let primary_fs = SimFs::new(SimConfig::new(4));
    let primary_db = Db::open(primary_fs, Path::new("/primary"), DurabilityMode::Fsync).unwrap();
    let primary_db = Arc::new(RwLock::new(primary_db));
    let mut listener = PrimaryListener::start("127.0.0.1:0", primary_db, Arc::new(ReplicationStats::default())).unwrap();
    let replica_fs = SimFs::new(SimConfig::new(5));
    let replica = Db::open_configured(
        replica_fs,
        Path::new("/replica"),
        DurabilityMode::Fsync,
        OpenConfig { role: NodeRole::Replica, cluster_id: Some([0xee; 16]), ..OpenConfig::default() },
    ).unwrap();
    let replica = Arc::new(RwLock::new(replica));
    let mut runner = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica)).unwrap();
    wait_until(|| runner.fatal_error().is_some());
    assert_eq!(replica.read().unwrap().last_applied_lsn(), 0);
    runner.shutdown();
    listener.shutdown();
}
