//! Numeric replication acceptance gate from Technical-Design §13.
//! The SOW requires ordered replication, replica catch-up, and continued
//! primary service during temporary replica unavailability (§§10–14, 20).

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, NodeRole, OpenConfig, PrimaryListener, ReplicaRunner,
    ReplicationStats, Server, ServerConfig, SimConfig, SimFs, Status,
};

fn wait_for_lsn(db: &Arc<RwLock<Db<SimFs>>>, expected: u64, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let actual = db.read().unwrap().last_applied_lsn();
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label} did not reach LSN {expected}; current LSN is {actual}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn two_replicas_recover_ten_thousand_writes_after_sixty_second_outage() {
    const INITIAL_WRITES: u64 = 10_000;
    const OUTAGE: Duration = Duration::from_secs(60);

    let primary = Db::open(
        SimFs::new(SimConfig::new(0xacc3_5501)),
        Path::new("/primary"),
        DurabilityMode::Fsync,
    )
    .unwrap();
    let cluster_id = primary.identity().cluster_id;
    let primary = Arc::new(RwLock::new(primary));
    let stats = Arc::new(ReplicationStats::default());
    let mut server = Server::start_shared(
        "127.0.0.1:0",
        Arc::clone(&primary),
        ServerConfig {
            replication_stats: Some(Arc::clone(&stats)),
            ..ServerConfig::default()
        },
    )
    .unwrap();
    let mut listener = PrimaryListener::start("127.0.0.1:0", Arc::clone(&primary), stats).unwrap();
    let replica_config = OpenConfig {
        role: NodeRole::Replica,
        cluster_id: Some(cluster_id),
        ..OpenConfig::default()
    };
    let replica_a = Arc::new(RwLock::new(
        Db::open_configured(
            SimFs::new(SimConfig::new(0xacc3_5502)),
            Path::new("/replica-a"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap(),
    ));
    let replica_b = Arc::new(RwLock::new(
        Db::open_configured(
            SimFs::new(SimConfig::new(0xacc3_5503)),
            Path::new("/replica-b"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap(),
    ));
    let mut runner_a = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_a)).unwrap();
    let mut runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();

    // Take one replica offline. Every write below must be acknowledged by the
    // primary without waiting for it; the other replica remains connected.
    runner_b.shutdown();
    let outage_started = Instant::now();
    for index in 0..INITIAL_WRITES {
        assert_eq!(
            client
                .set(
                    format!("key:{index:05}").into_bytes(),
                    index.to_le_bytes().to_vec()
                )
                .unwrap(),
            Status::Ok,
            "write {index} was not acknowledged during the outage"
        );
    }
    let mut written = INITIAL_WRITES;
    while outage_started.elapsed() < OUTAGE {
        assert_eq!(
            client
                .set(format!("pulse:{written:05}").into_bytes(), vec![1])
                .unwrap(),
            Status::Ok,
            "write {written} was not acknowledged during the outage"
        );
        written += 1;
        thread::sleep(Duration::from_millis(250));
    }
    assert!(outage_started.elapsed() >= OUTAGE);
    assert_eq!(replica_b.read().unwrap().last_applied_lsn(), 0);
    wait_for_lsn(&replica_a, written, "connected replica");

    runner_b = ReplicaRunner::start(listener.local_addr(), Arc::clone(&replica_b)).unwrap();
    wait_for_lsn(&replica_b, written, "reconnected replica");
    let primary_hash = primary.read().unwrap().record_hash_at(written);
    assert!(
        primary_hash.is_some(),
        "primary lacks the final history hash"
    );
    assert_eq!(
        replica_a.read().unwrap().record_hash_at(written),
        primary_hash
    );
    assert_eq!(
        replica_b.read().unwrap().record_hash_at(written),
        primary_hash
    );
    assert!(runner_a.fatal_error().is_none());
    assert!(runner_b.fatal_error().is_none());

    drop(client);
    runner_a.shutdown();
    runner_b.shutdown();
    listener.shutdown();
    server.shutdown();
}
