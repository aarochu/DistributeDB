//! Two-node and three-node asynchronous replication over loopback TCP.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, FileSystem, GetResult, NodeRole, OpenConfig, PrimaryListener,
    ReplicaRunner, ReplicationStats, Server, ServerConfig, SimConfig, SimFs, Status,
};

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "replication did not converge in 10 seconds"
        );
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
    let mut listener =
        PrimaryListener::start("127.0.0.1:0", Arc::clone(&primary_db), stats).unwrap();

    let replica_config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        ..OpenConfig::default()
    };
    let replica_a_fs = SimFs::new(SimConfig::new(2));
    let replica_b_fs = SimFs::new(SimConfig::new(3));
    let replica_a = Arc::new(RwLock::new(
        Db::open_configured(
            replica_a_fs,
            Path::new("/replica-a"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap(),
    ));
    let replica_b = Arc::new(RwLock::new(
        Db::open_configured(
            replica_b_fs,
            Path::new("/replica-b"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap(),
    ));
    let mut runner_a = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_a)).unwrap();
    let mut runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    let mut replica_read_server = Server::start_shared(
        "127.0.0.1:0",
        Arc::clone(&replica_a),
        ServerConfig::default(),
    )
    .unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();

    for index in 0..40 {
        assert_eq!(
            client
                .set(
                    format!("key-{index}").into_bytes(),
                    format!("value-{index}").into_bytes()
                )
                .unwrap(),
            Status::Ok
        );
    }
    wait_until(|| {
        replica_a.read().unwrap().last_applied_lsn() == 40
            && replica_b.read().unwrap().last_applied_lsn() == 40
    });
    let mut replica_client = Client::connect(replica_read_server.local_addr()).unwrap();
    assert_eq!(
        replica_client.get(b"key-37".to_vec()).unwrap(),
        Some(b"value-37".to_vec())
    );
    assert!(replica_client.exists(b"key-37".to_vec()).unwrap());
    assert_eq!(
        replica_client
            .set(b"replica-write".to_vec(), b"denied".to_vec())
            .unwrap(),
        Status::NotPrimary
    );
    assert_eq!(replica_client.get(b"replica-write".to_vec()).unwrap(), None);
    assert_eq!(
        replica_a.read().unwrap().record_hash_at(40),
        primary_db.read().unwrap().record_hash_at(40)
    );
    runner_b.shutdown();

    for index in 40..80 {
        assert_eq!(
            client
                .set(
                    format!("key-{index}").into_bytes(),
                    format!("value-{index}").into_bytes()
                )
                .unwrap(),
            Status::Ok
        );
    }
    wait_until(|| replica_a.read().unwrap().last_applied_lsn() == 80);
    assert_eq!(replica_b.read().unwrap().last_applied_lsn(), 40);
    runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    wait_until(|| replica_b.read().unwrap().last_applied_lsn() == 80);
    assert_eq!(
        replica_b.read().unwrap().get(b"key-79"),
        GetResult::Found(b"value-79".to_vec())
    );
    assert_eq!(
        replica_b.read().unwrap().record_hash_at(80),
        primary_db.read().unwrap().record_hash_at(80)
    );
    wait_until(|| {
        let response = client.stats().unwrap();
        response.contains("replicas_connected=2") && response.matches("_lag=0").count() == 2
    });
    assert!(runner_a.fatal_error().is_none());
    assert!(runner_b.fatal_error().is_none());

    drop(client);
    runner_a.shutdown();
    runner_b.shutdown();
    replica_read_server.shutdown();
    listener.shutdown();
    server.shutdown();
}

#[test]
fn mismatched_cluster_stops_replica_without_rewriting_history() {
    let primary_fs = SimFs::new(SimConfig::new(4));
    let primary_db = Db::open(primary_fs, Path::new("/primary"), DurabilityMode::Fsync).unwrap();
    let primary_db = Arc::new(RwLock::new(primary_db));
    let mut listener = PrimaryListener::start(
        "127.0.0.1:0",
        primary_db,
        Arc::new(ReplicationStats::default()),
    )
    .unwrap();
    let replica_fs = SimFs::new(SimConfig::new(5));
    let replica = Db::open_configured(
        replica_fs,
        Path::new("/replica"),
        DurabilityMode::Fsync,
        OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some([0xee; 16]),
            ..OpenConfig::default()
        },
    )
    .unwrap();
    let replica = Arc::new(RwLock::new(replica));
    let mut runner = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica)).unwrap();
    wait_until(|| runner.fatal_error().is_some());
    assert_eq!(replica.read().unwrap().last_applied_lsn(), 0);
    runner.shutdown();
    listener.shutdown();
}

#[test]
fn known_history_mismatch_stops_even_a_rebootstrap_provisioned_replica() {
    let mut primary = Db::open(
        SimFs::new(SimConfig::new(60)),
        Path::new("/primary"),
        DurabilityMode::Fsync,
    )
    .unwrap();
    let cluster_id = primary.identity().cluster_id;
    primary.set(b"key".to_vec(), b"primary".to_vec()).unwrap();

    // Build a different, durable record at the same LSN and cluster ID. A
    // provisioned snapshot policy must not turn a known mismatch into an
    // implicit reset of this replica's local history.
    let mut divergent_source = Db::open_configured(
        SimFs::new(SimConfig::new(61)),
        Path::new("/divergent-source"),
        DurabilityMode::Fsync,
        OpenConfig {
            cluster_id: Some(cluster_id),
            ..OpenConfig::default()
        },
    )
    .unwrap();
    divergent_source
        .set(b"key".to_vec(), b"replica".to_vec())
        .unwrap();
    let divergent_record = divergent_source
        .durable_records_after(0, 1)
        .unwrap()
        .remove(0);
    let replica_fs = SimFs::new(SimConfig::new(62));
    let replica_config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        allow_snapshot_rebootstrap: true,
        ..OpenConfig::default()
    };
    let mut replica = Db::open_configured(
        replica_fs.clone(),
        Path::new("/replica"),
        DurabilityMode::Fsync,
        replica_config,
    )
    .unwrap();
    replica.apply_replicated_record(&divergent_record).unwrap();
    let divergent_hash = replica.record_hash_at(1);
    assert_ne!(divergent_hash, primary.record_hash_at(1));
    let replica = Arc::new(RwLock::new(replica));
    let primary = Arc::new(RwLock::new(primary));
    let mut listener = PrimaryListener::start(
        "127.0.0.1:0",
        Arc::clone(&primary),
        Arc::new(ReplicationStats::default()),
    )
    .unwrap();
    let mut runner = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica)).unwrap();
    wait_until(|| runner.fatal_error().is_some());
    assert!(runner
        .fatal_error()
        .unwrap()
        .contains("history hash mismatch"));
    assert_eq!(replica.read().unwrap().last_applied_lsn(), 1);
    assert_eq!(replica.read().unwrap().record_hash_at(1), divergent_hash);
    assert_eq!(
        replica.read().unwrap().get(b"key"),
        GetResult::Found(b"replica".to_vec())
    );
    runner.shutdown();
    listener.shutdown();
    drop(replica);
    replica_fs.crash();
    let reopened = Db::open_configured(
        replica_fs,
        Path::new("/replica"),
        DurabilityMode::Fsync,
        replica_config,
    )
    .unwrap();
    assert_eq!(reopened.last_applied_lsn(), 1);
    assert_eq!(reopened.record_hash_at(1), divergent_hash);
    assert_eq!(reopened.get(b"key"), GetResult::Found(b"replica".to_vec()));
}

#[test]
fn snapshot_rebootstrap_is_explicit_and_recovers_after_crash() {
    let primary_fs = SimFs::new(SimConfig::new(40));
    let mut primary = Db::open(primary_fs, Path::new("/primary"), DurabilityMode::Fsync).unwrap();
    let cluster_id = primary.identity().cluster_id;
    for index in 0..20 {
        primary
            .set(format!("key-{index}").into_bytes(), vec![index as u8])
            .unwrap();
    }
    assert_eq!(primary.publish_snapshot().unwrap(), 20);
    for index in 20..30 {
        primary
            .set(format!("key-{index}").into_bytes(), vec![index as u8])
            .unwrap();
    }
    let primary = Arc::new(RwLock::new(primary));
    let mut listener = PrimaryListener::start(
        "127.0.0.1:0",
        Arc::clone(&primary),
        Arc::new(ReplicationStats::default()),
    )
    .unwrap();

    let denied_fs = SimFs::new(SimConfig::new(41));
    let denied = Arc::new(RwLock::new(
        Db::open_configured(
            denied_fs,
            Path::new("/denied"),
            DurabilityMode::Fsync,
            OpenConfig {
                role: NodeRole::Replica,
                cluster_id: Some(cluster_id),
                ..OpenConfig::default()
            },
        )
        .unwrap(),
    ));
    let mut denied_runner =
        ReplicaRunner::start(listener.local_addr(), Arc::clone(&denied)).unwrap();
    wait_until(|| denied_runner.fatal_error().is_some());
    assert_eq!(denied.read().unwrap().last_applied_lsn(), 0);
    denied_runner.shutdown();

    let replica_fs = SimFs::new(SimConfig::new(42));
    let config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        allow_snapshot_rebootstrap: true,
        ..OpenConfig::default()
    };
    let replica = Arc::new(RwLock::new(
        Db::open_configured(
            replica_fs.clone(),
            Path::new("/replica"),
            DurabilityMode::Fsync,
            config,
        )
        .unwrap(),
    ));
    let mut runner = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica)).unwrap();
    wait_until(|| replica.read().unwrap().last_applied_lsn() == 30);
    assert!(runner.fatal_error().is_none());
    assert_eq!(replica.read().unwrap().generation_id(), 2);
    // The generation replaced by the installed snapshot is garbage collected.
    let old_generation = Path::new("/replica/generations/0000000000000001");
    assert_eq!(replica.read().unwrap().generations_removed(), 1);
    assert!(!replica_fs.exists(old_generation));
    assert_eq!(
        replica.read().unwrap().get(b"key-29"),
        GetResult::Found(vec![29])
    );
    runner.shutdown();
    drop(replica);
    replica_fs.crash();
    assert!(!replica_fs.exists(old_generation));
    let reopened = Db::open_configured(
        replica_fs,
        Path::new("/replica"),
        DurabilityMode::Fsync,
        OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster_id),
            ..OpenConfig::default()
        },
    )
    .unwrap();
    assert!(reopened.allows_snapshot_rebootstrap());
    assert_eq!(reopened.generation_id(), 2);
    assert_eq!(reopened.last_applied_lsn(), 30);
    assert_eq!(reopened.get(b"key-0"), GetResult::Found(vec![0]));
    assert_eq!(reopened.get(b"key-29"), GetResult::Found(vec![29]));
    listener.shutdown();
}
