//! End-to-end kill/restart exercise using the compiled server binary and real
//! filesystem. This models process termination, not a physical power cut.
//!
//! The replica is killed while the primary is streaming writes to it, and the
//! primary is killed while a client is issuing writes. Every write the primary
//! acknowledged before it was killed must survive its restart and reach the
//! replica.

use std::fs::File;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distributedb::{Client, Db, DurabilityMode, GetResult, NodeRole, OpenConfig, RealFs, Status};

struct Process {
    child: Option<Child>,
    log: PathBuf,
}

impl Process {
    /// Starts the server binary with stderr appended to `log`, so diagnostics
    /// survive a failed trial and a full pipe cannot block the process.
    fn spawn(args: &[&str], log: &Path) -> Self {
        let stderr = File::options()
            .create(true)
            .append(true)
            .open(log)
            .expect("open process log");
        let child = Command::new(env!("CARGO_BIN_EXE_distributedb"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("spawn DistributeDB process");
        Self {
            child: Some(child),
            log: log.to_path_buf(),
        }
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

/// Returns the value of the first STATS line whose name ends with `suffix`.
fn stat(stats: &str, suffix: &str) -> Option<String> {
    stats.lines().find_map(|line| {
        let (name, value) = line.split_once('=')?;
        name.ends_with(suffix).then(|| value.to_string())
    })
}

fn wait_replica(client: &mut Client, replica: &mut Process, expected_lsn: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let expected = expected_lsn.to_string();
    loop {
        let stats = client.stats().expect("primary STATS");
        if stat(&stats, "replicas_connected").as_deref() == Some("1")
            && stat(&stats, "_applied_lsn").as_deref() == Some(expected.as_str())
            && stat(&stats, "_lag").as_deref() == Some("0")
        {
            return;
        }
        let child = replica.child.as_mut().expect("replica process exists");
        if let Some(status) = child.try_wait().expect("poll replica") {
            let log = std::fs::read_to_string(&replica.log).unwrap_or_default();
            panic!("replica exited {status} before LSN {expected_lsn}: {log}");
        }
        assert!(
            Instant::now() < deadline,
            "replica did not converge to LSN {expected_lsn}: {stats}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn value(index: usize) -> Vec<u8> {
    vec![(index % 251) as u8]
}

fn write_range(client: &mut Client, begin: usize, end: usize) {
    for index in begin..end {
        assert_eq!(
            client
                .set(format!("key-{index}").into_bytes(), value(index))
                .unwrap(),
            Status::Ok
        );
    }
}

/// Writes keys from `begin` on a separate client until `end` or until the
/// connection fails. `acked` holds one past the last acknowledged index.
fn spawn_writer(
    addr: SocketAddr,
    begin: usize,
    end: usize,
    acked: Arc<AtomicUsize>,
) -> thread::JoinHandle<()> {
    acked.store(begin, Ordering::SeqCst);
    thread::spawn(move || {
        let mut client = Client::connect(addr).expect("writer connects");
        for index in begin..end {
            match client.set(format!("key-{index}").into_bytes(), value(index)) {
                Ok(Status::Ok) => acked.store(index + 1, Ordering::SeqCst),
                _ => return,
            }
        }
    })
}

fn wait_acked(acked: &AtomicUsize, target: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while acked.load(Ordering::SeqCst) < target {
        assert!(Instant::now() < deadline, "writer stalled before {target}");
        thread::sleep(Duration::from_millis(1));
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
    // Seeded boundaries: baseline writes, replica kill point, primary kill
    // point, and writes issued after the primary restarts.
    let first = 20 + seed % 31;
    let replica_kill = first + 5 + (seed / 3) % 21;
    let second = replica_kill + 20 + (seed / 7) % 31;
    let primary_kill = second + 5 + (seed / 11) % 21;
    let tail = 20 + (seed / 13) % 31;
    let root = data_dir("cluster");
    println!("process failure seed {seed}: data under {}", root.display());
    let primary_dir = root.join("primary");
    let replica_dir = root.join("replica");
    let primary_log = root.join("primary.log");
    let replica_log = root.join("replica.log");
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

    let mut primary = Process::spawn(&primary_args, &primary_log);
    let mut replica = Process::spawn(&replica_args, &replica_log);
    let mut client = wait_client(client_addr);
    write_range(&mut client, 0, first);
    wait_replica(&mut client, &mut replica, first);

    // Kill the replica while the primary is streaming new writes to it.
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = spawn_writer(client_addr, first, second, Arc::clone(&acked));
    wait_acked(&acked, replica_kill);
    replica.kill();
    writer.join().expect("writer thread");
    assert_eq!(acked.load(Ordering::SeqCst), second);
    replica = Process::spawn(&replica_args, &replica_log);
    wait_replica(&mut client, &mut replica, second);

    // Kill the primary while a client is writing. The write in flight at the
    // kill may or may not be durable; every acknowledged write must be.
    drop(client);
    let writer = spawn_writer(client_addr, second, usize::MAX, Arc::clone(&acked));
    wait_acked(&acked, primary_kill);
    primary.kill();
    writer.join().expect("writer thread");
    let acknowledged = acked.load(Ordering::SeqCst);

    primary = Process::spawn(&primary_args, &primary_log);
    client = wait_client(client_addr);
    let stats = client.stats().expect("primary STATS");
    let recovered: usize = stat(&stats, "current_lsn").unwrap().parse().unwrap();
    assert!(
        recovered == acknowledged || recovered == acknowledged + 1,
        "primary recovered LSN {recovered} after {acknowledged} acknowledged writes"
    );
    for index in 0..recovered {
        assert_eq!(
            client.get(format!("key-{index}").into_bytes()).unwrap(),
            Some(value(index)),
            "primary lost key-{index} (acknowledged through {acknowledged})"
        );
    }
    let final_lsn = recovered + tail;
    write_range(&mut client, recovered, final_lsn);
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
            GetResult::Found(value(index))
        );
    }
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
