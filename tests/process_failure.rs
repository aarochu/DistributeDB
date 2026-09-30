//! End-to-end kill/restart exercise using the compiled server binary and real
//! filesystem. This models process termination, not a physical power cut.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distributedb::{Client, Db, DurabilityMode, GetResult, NodeRole, OpenConfig, RealFs, Status};

struct Process {
    child: Option<Child>,
}

impl Process {
    fn spawn(args: &[&str]) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_distributedb"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn DistributeDB process");
        Self { child: Some(child) }
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            child.wait().expect("reap DistributeDB process");
        }
    }

    fn shutdown(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(b"shutdown\n");
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if child.try_wait().expect("poll child").is_some() {
                    break;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    child.wait().expect("reap timed-out child");
                    panic!("DistributeDB process did not shut down in 5 seconds");
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

fn wait_client(addr: SocketAddr) -> Client {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(client) = Client::connect(addr) {
            return client;
        }
        assert!(Instant::now() < deadline, "primary failed to start");
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_replica(client: &mut Client, replica: &mut Process, expected_lsn: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = client.stats().expect("primary STATS");
        if stats.contains("replicas_connected=1")
            && stats.contains(&format!("_applied_lsn={expected_lsn}"))
            && stats.contains("_lag=0")
        {
            return;
        }
        let child = replica.child.as_mut().expect("replica process exists");
        if let Some(status) = child.try_wait().expect("poll replica") {
            let mut stderr = String::new();
            if let Some(pipe) = child.stderr.as_mut() {
                pipe.read_to_string(&mut stderr).unwrap();
            }
            panic!("replica exited {status} before LSN {expected_lsn}: {stderr}");
        }
        assert!(
            Instant::now() < deadline,
            "replica did not converge: {stats}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn write_range(client: &mut Client, begin: usize, end: usize) {
    for index in begin..end {
        assert_eq!(
            client
                .set(
                    format!("key-{index}").into_bytes(),
                    vec![(index % 251) as u8],
                )
                .unwrap(),
            Status::Ok
        );
    }
}

fn data_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("ddb-failure-{name}-{}-{nanos}", std::process::id()))
}

fn provision(primary_dir: &Path, replica_dir: &Path) -> String {
    let primary = Db::open(RealFs, primary_dir, DurabilityMode::Fsync).unwrap();
    let cluster = primary.identity().cluster_id;
    drop(primary);
    let replica = Db::open_configured(
        RealFs,
        replica_dir,
        DurabilityMode::Fsync,
        OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        },
    )
    .unwrap();
    drop(replica);
    distributedb::replication::format_id(&cluster)
}

#[test]
fn replica_and_primary_process_kill_restart_converges() {
    let seed: usize = std::env::var("DDB_FAILURE_SEED")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1);
    let first = 20 + seed % 31;
    let second = first + 20 + (seed / 7) % 31;
    let final_lsn = second + 20 + (seed / 13) % 31;
    let root = data_dir("cluster");
    let primary_dir = root.join("primary");
    let replica_dir = root.join("replica");
    let cluster_id = provision(&primary_dir, &replica_dir);
    let client_reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let replication_reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_addr = client_reservation.local_addr().unwrap();
    let replication_addr = replication_reservation.local_addr().unwrap();
    drop(client_reservation);
    drop(replication_reservation);
    let primary_path = primary_dir.to_string_lossy().into_owned();
    let replica_path = replica_dir.to_string_lossy().into_owned();
    let client_address = client_addr.to_string();
    let replication_address = replication_addr.to_string();
    let primary_args = [
        "serve",
        "--addr",
        client_address.as_str(),
        "--replication-addr",
        replication_address.as_str(),
        "--data",
        primary_path.as_str(),
    ];
    let replica_args = [
        "replica",
        "--primary-addr",
        replication_address.as_str(),
        "--cluster-id",
        cluster_id.as_str(),
        "--data",
        replica_path.as_str(),
    ];

    let mut primary = Process::spawn(&primary_args);
    let mut replica = Process::spawn(&replica_args);
    let mut client = wait_client(client_addr);
    write_range(&mut client, 0, first);
    wait_replica(&mut client, &mut replica, first);

    replica.kill();
    write_range(&mut client, first, second);
    replica = Process::spawn(&replica_args);
    wait_replica(&mut client, &mut replica, second);

    primary.kill();
    drop(client);
    primary = Process::spawn(&primary_args);
    client = wait_client(client_addr);
    write_range(&mut client, second, final_lsn);
    wait_replica(&mut client, &mut replica, final_lsn);
    drop(client);

    replica.shutdown();
    primary.shutdown();
    let mut cluster = [0u8; 16];
    for (index, byte) in cluster.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&cluster_id[index * 2..index * 2 + 2], 16).unwrap();
    }
    let reopened = Db::open_configured(
        RealFs,
        &replica_dir,
        DurabilityMode::Fsync,
        OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        },
    )
    .unwrap();
    assert_eq!(reopened.last_applied_lsn(), final_lsn as u64);
    for index in 0..final_lsn {
        assert_eq!(
            reopened.get(format!("key-{index}").as_bytes()),
            GetResult::Found(vec![(index % 251) as u8])
        );
    }
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
