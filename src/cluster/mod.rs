//! Local cluster launcher behind `distributedb cluster`.
//!
//! Starts one primary and N replicas as child processes of the same binary,
//! each with its own data directory, ports, and log file. The launcher reads
//! the primary's cluster ID from its log and passes it to every replica, so
//! nothing has to be copied by hand. Replicas can be stopped and restarted to
//! show catch-up; the primary runs until the cluster shuts down.
//!
//! Every node binds to loopback. Stopping a node sends `shutdown` on its
//! stdin, which `serve` and `replica` treat as a clean stop.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crate::dashboard::NodeControl;

/// How long a node may take to open its client port.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a node may take to exit after `shutdown` before it is killed.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// Replicas one launcher supports, so ports stay in a small fixed range.
pub const MAX_REPLICAS: usize = 8;

/// Launcher settings.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// The `distributedb` binary to run for each node.
    pub exe: PathBuf,
    /// Parent of the per-node directories; reused across runs.
    pub data_dir: PathBuf,
    pub replicas: usize,
    /// The primary's client port. The replication port is `base_port + 1`,
    /// and replica *i* (from 1) serves reads on `base_port + 1 + i`.
    pub base_port: u16,
    /// Flags for every node, such as `--storage lsm`.
    pub storage_args: Vec<String>,
    /// Flags for the primary only, such as `--sync-replicas 1`.
    pub primary_args: Vec<String>,
}

/// Whether a node is the primary or a replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary,
    Replica,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Primary => "primary",
            Role::Replica => "replica",
        }
    }
}

/// A node as the launcher sees it.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub name: String,
    pub role: Role,
    /// The client port: writes and reads on the primary, reads on a replica.
    pub client_addr: SocketAddr,
    pub pid: Option<u32>,
    pub log_path: PathBuf,
}

struct Node {
    info: NodeInfo,
    data_dir: PathBuf,
    child: Option<Child>,
}

/// A running local cluster. Dropping it stops every node.
pub struct Cluster {
    config: ClusterConfig,
    cluster_id: String,
    nodes: Vec<Node>,
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

impl Cluster {
    /// Start the primary, read its cluster ID, then start each replica and
    /// wait until every node accepts connections.
    pub fn start(config: ClusterConfig) -> io::Result<Cluster> {
        if config.replicas > MAX_REPLICAS {
            return Err(other(format!("at most {MAX_REPLICAS} replicas")));
        }
        let last_port = config.base_port as usize + 1 + config.replicas;
        if last_port > u16::MAX as usize {
            return Err(other("base port leaves no room for the replica ports"));
        }
        fs::create_dir_all(&config.data_dir)?;
        let mut nodes = vec![Node {
            info: NodeInfo {
                name: "primary".into(),
                role: Role::Primary,
                client_addr: loopback(config.base_port),
                pid: None,
                log_path: config.data_dir.join("primary.log"),
            },
            data_dir: config.data_dir.join("primary"),
            child: None,
        }];
        for index in 1..=config.replicas {
            let name = format!("replica-{index:02}");
            nodes.push(Node {
                info: NodeInfo {
                    client_addr: loopback(config.base_port + 1 + index as u16),
                    log_path: config.data_dir.join(format!("{name}.log")),
                    name: name.clone(),
                    role: Role::Replica,
                    pid: None,
                },
                data_dir: config.data_dir.join(name),
                child: None,
            });
        }
        let mut cluster = Cluster {
            config,
            cluster_id: String::new(),
            nodes,
        };
        cluster.spawn(0)?;
        cluster.cluster_id = read_cluster_id(&cluster.nodes[0].info.log_path)?;
        for index in 1..cluster.nodes.len() {
            cluster.spawn(index)?;
        }
        Ok(cluster)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn replication_addr(&self) -> SocketAddr {
        loopback(self.config.base_port + 1)
    }

    /// Every node, primary first.
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|node| node.info.clone()).collect()
    }

    fn args_for(&self, index: usize) -> Vec<String> {
        let node = &self.nodes[index];
        let data = node.data_dir.to_string_lossy().into_owned();
        let mut args: Vec<String> = match node.info.role {
            Role::Primary => vec![
                "serve".into(),
                "--addr".into(),
                node.info.client_addr.to_string(),
                "--replication-addr".into(),
                self.replication_addr().to_string(),
                "--data".into(),
                data,
            ],
            Role::Replica => vec![
                "replica".into(),
                "--primary-addr".into(),
                self.replication_addr().to_string(),
                "--cluster-id".into(),
                self.cluster_id.clone(),
                "--data".into(),
                data,
                "--read-addr".into(),
                node.info.client_addr.to_string(),
                "--allow-snapshot-rebootstrap".into(),
            ],
        };
        args.extend(self.config.storage_args.iter().cloned());
        if node.info.role == Role::Primary {
            args.extend(self.config.primary_args.iter().cloned());
        }
        args
    }

    fn spawn(&mut self, index: usize) -> io::Result<()> {
        let args = self.args_for(index);
        let node = &mut self.nodes[index];
        if node.child.is_some() {
            return Err(other(format!("{} is already running", node.info.name)));
        }
        // Refuse a port someone else holds rather than mistaking that
        // process for the node.
        TcpListener::bind(node.info.client_addr).map_err(|error| {
            other(format!(
                "{} port {} is unavailable: {error}",
                node.info.name, node.info.client_addr
            ))
        })?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&node.info.log_path)?;
        let child = Command::new(&self.config.exe)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        node.info.pid = Some(child.id());
        node.child = Some(child);
        wait_until_listening(node)
    }

    fn index_of(&self, name: &str) -> Result<usize, String> {
        self.nodes
            .iter()
            .position(|node| node.info.name == name)
            .ok_or_else(|| format!("no node named {name}"))
    }

    /// Stop a replica cleanly. The primary is stopped only by
    /// [`Cluster::shutdown`].
    pub fn stop_node(&mut self, name: &str) -> Result<(), String> {
        let index = self.index_of(name)?;
        if self.nodes[index].info.role == Role::Primary {
            return Err("the primary stops only with the whole cluster".into());
        }
        if self.nodes[index].child.is_none() {
            return Err(format!("{name} is not running"));
        }
        stop(&mut self.nodes[index]);
        Ok(())
    }

    /// Restart a stopped replica on its existing data directory; it catches
    /// up from the primary's retained WAL.
    pub fn start_node(&mut self, name: &str) -> Result<(), String> {
        let index = self.index_of(name)?;
        if self.nodes[index].info.role == Role::Primary {
            return Err("the primary starts only with the whole cluster".into());
        }
        if self.nodes[index].child.is_some() {
            return Err(format!("{name} is already running"));
        }
        self.spawn(index).map_err(|error| error.to_string())
    }

    /// Stop the replicas, then the primary.
    pub fn shutdown(&mut self) {
        for node in self.nodes.iter_mut().rev() {
            stop(node);
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl NodeControl for Mutex<Cluster> {
    fn stop(&self, name: &str) -> Result<(), String> {
        self.lock().expect("cluster lock poisoned").stop_node(name)
    }

    fn start(&self, name: &str) -> Result<(), String> {
        self.lock().expect("cluster lock poisoned").start_node(name)
    }
}

fn stop(node: &mut Node) {
    let Some(mut child) = node.child.take() else {
        return;
    };
    node.info.pid = None;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(b"shutdown\n");
        let _ = stdin.flush();
    }
    let deadline = Instant::now() + STOP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

fn wait_until_listening(node: &mut Node) -> io::Result<()> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(child) = node.child.as_mut() {
            if let Some(status) = child.try_wait()? {
                node.child = None;
                node.info.pid = None;
                return Err(other(format!(
                    "{} exited during startup ({status}); see {}",
                    node.info.name,
                    node.info.log_path.display()
                )));
            }
        }
        if TcpStream::connect_timeout(&node.info.client_addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            stop(node);
            return Err(other(format!(
                "{} did not open {} within {} s; see {}",
                node.info.name,
                node.info.client_addr,
                START_TIMEOUT.as_secs(),
                node.info.log_path.display()
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// The last `cluster ID:` line in the primary's log.
fn read_cluster_id(log_path: &Path) -> io::Result<String> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        let text = fs::read_to_string(log_path).unwrap_or_default();
        let found = text
            .lines()
            .rev()
            .find_map(|line| line.strip_prefix("cluster ID: "))
            .map(str::trim)
            .filter(|id| id.len() == 32 && id.chars().all(|ch| ch.is_ascii_hexdigit()));
        if let Some(id) = found {
            return Ok(id.to_string());
        }
        if Instant::now() >= deadline {
            return Err(other(format!(
                "the primary did not print its cluster ID; see {}",
                log_path.display()
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_newest_cluster_id_from_a_log() {
        let dir = std::env::temp_dir().join(format!("ddb-cluster-id-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("primary.log");
        let mut file = fs::File::create(&log).unwrap();
        writeln!(file, "cluster ID: {}", "a".repeat(32)).unwrap();
        writeln!(file, "cluster ID: short").unwrap();
        writeln!(file, "cluster ID: {}", "b".repeat(32)).unwrap();
        drop(file);
        assert_eq!(read_cluster_id(&log).unwrap(), "b".repeat(32));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rejects_too_many_replicas_and_port_overflow() {
        let config = ClusterConfig {
            exe: PathBuf::from("distributedb"),
            data_dir: std::env::temp_dir().join("ddb-cluster-unused"),
            replicas: MAX_REPLICAS + 1,
            base_port: 5555,
            storage_args: Vec::new(),
            primary_args: Vec::new(),
        };
        assert!(Cluster::start(config.clone()).is_err());
        let overflow = ClusterConfig {
            replicas: 2,
            base_port: u16::MAX - 1,
            ..config
        };
        assert!(Cluster::start(overflow).is_err());
    }
}
