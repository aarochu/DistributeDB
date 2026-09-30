//! Static-primary asynchronous WAL replication (SOW §§10–12, Phase 5).
//!
//! The primary sends one locally durable mutation at a time on a distinct
//! loopback port. A replica validates the record, syncs its own WAL, applies
//! it, then ACKs. This one-record window bounds in-flight data and makes an
//! ACK's durability meaning explicit. The primary never waits for ACK before
//! answering a client write. Reconnect begins with a fresh history handshake.
//! Snapshot transfer is a Phase 6 extension; a reclaimed prefix currently
//! fails explicitly with REBOOTSTRAP_REQUIRED.

pub mod protocol;

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::fileio::FileSystem;
use crate::wal::{Db, DurabilityMode};
use protocol::{read_message, write_message, Message};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_REPLICAS: usize = 16;

pub fn format_id(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Copy)]
pub struct PeerProgress {
    pub applied_lsn: u64,
    pub connected: bool,
}

/// Bounded per-replica state for the primary's STATS response. A disconnected
/// peer's lag is unknown; its last ACK is historical, not a current claim.
#[derive(Debug, Default)]
pub struct ReplicationStats {
    peers: Mutex<HashMap<[u8; 16], PeerProgress>>,
}

impl ReplicationStats {
    fn connect(&self, id: [u8; 16], applied_lsn: u64) -> bool {
        let mut peers = self.peers.lock().expect("replication stats poisoned");
        if peers.get(&id).is_some_and(|progress| progress.connected) {
            return false;
        }
        if !peers.contains_key(&id) && peers.len() >= MAX_REPLICAS {
            return false;
        }
        peers.insert(id, PeerProgress { applied_lsn, connected: true });
        true
    }

    fn ack(&self, id: [u8; 16], applied_lsn: u64) {
        let mut peers = self.peers.lock().expect("replication stats poisoned");
        if let Some(progress) = peers.get_mut(&id) {
            progress.applied_lsn = applied_lsn;
        }
    }

    fn disconnect(&self, id: [u8; 16]) {
        let mut peers = self.peers.lock().expect("replication stats poisoned");
        if let Some(progress) = peers.get_mut(&id) {
            progress.connected = false;
        }
    }

    pub fn snapshot(&self) -> Vec<([u8; 16], PeerProgress)> {
        let peers = self.peers.lock().expect("replication stats poisoned");
        let mut snapshot: Vec<_> = peers.iter().map(|(&id, &progress)| (id, progress)).collect();
        snapshot.sort_by_key(|(id, _)| *id);
        snapshot
    }
}

struct PeerGuard {
    id: [u8; 16],
    stats: Arc<ReplicationStats>,
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        self.stats.disconnect(self.id);
    }
}

pub struct PrimaryListener {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
}

impl PrimaryListener {
    /// Bind a distinct replication port. The Phase 5 demo only accepts
    /// loopback peers; endpoint matching is not authentication.
    pub fn start<A, F>(addr: A, db: Arc<RwLock<Db<F>>>, stats: Arc<ReplicationStats>) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        F: FileSystem + Clone + Send + Sync + 'static,
    {
        {
            let guard = db.read().expect("db lock poisoned");
            if guard.identity().role != "primary" || guard.durability_mode() != DurabilityMode::Fsync {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "replication requires an fsync primary"));
            }
        }
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        if !local_addr.ip().is_loopback() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Phase 5 replication must bind loopback"));
        }
        listener.set_nonblocking(true)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&shutdown);
        let acceptor = thread::Builder::new().name("ddb-repl-accept".into()).spawn(move || {
            let mut workers: Vec<JoinHandle<()>> = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, peer)) if peer.ip().is_loopback() => {
                        let db = Arc::clone(&db);
                        let stats = Arc::clone(&stats);
                        let stop = Arc::clone(&stop);
                        workers.push(thread::spawn(move || {
                            let _ = handle_primary_connection(stream, db, stats, stop);
                        }));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL_INTERVAL),
                    Err(_) => thread::sleep(POLL_INTERVAL),
                }
                let mut index = 0;
                while index < workers.len() {
                    if workers[index].is_finished() {
                        let _ = workers.swap_remove(index).join();
                    } else { index += 1; }
                }
            }
            for worker in workers { let _ = worker.join(); }
        })?;
        Ok(Self { local_addr, shutdown, acceptor: Some(acceptor) })
    }

    pub fn local_addr(&self) -> SocketAddr { self.local_addr }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(acceptor) = self.acceptor.take() { let _ = acceptor.join(); }
    }
}

impl Drop for PrimaryListener {
    fn drop(&mut self) { self.shutdown(); }
}

fn send_error(stream: &mut TcpStream, code: u16, diagnostic: &str) {
    let _ = write_message(stream, &Message::Error { code, diagnostic: diagnostic.into() });
}

fn handle_primary_connection<F>(
    mut stream: TcpStream,
    db: Arc<RwLock<Db<F>>>,
    stats: Arc<ReplicationStats>,
    shutdown: Arc<AtomicBool>,
) -> protocol::Result<()>
where F: FileSystem + Clone,
{
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let (cluster_id, replica_id, mut cursor, hash, wal_version, snapshot_version) =
        match read_message(&mut stream)? {
            Some(Message::Hello { cluster_id, replica_id, durable_lsn, record_hash, wal_version, snapshot_version }) =>
                (cluster_id, replica_id, durable_lsn, record_hash, wal_version, snapshot_version),
            _ => { send_error(&mut stream, protocol::ERROR_BAD_FRAME, "expected HELLO"); return Err(protocol::Error::Invalid("expected HELLO")); }
        };
    if wal_version != 1 || snapshot_version != 1 {
        send_error(&mut stream, protocol::ERROR_BAD_FRAME, "unsupported format version");
        return Err(protocol::Error::Invalid("unsupported format version"));
    }
    let (primary_id, latest_lsn, latest_hash, earliest) = {
        let guard = db.read().expect("db lock poisoned");
        if cluster_id != guard.identity().cluster_id || replica_id == guard.identity().node_id {
            send_error(&mut stream, protocol::ERROR_CLUSTER_MISMATCH, "cluster or node ID mismatch");
            return Err(protocol::Error::Invalid("cluster or node ID mismatch"));
        }
        if cursor > guard.last_durable_lsn() {
            send_error(&mut stream, protocol::ERROR_DIVERGED, "replica is ahead of primary");
            return Err(protocol::Error::Invalid("replica ahead of primary"));
        }
        match guard.record_hash_at(cursor) {
            Some(known) if known != hash => {
                send_error(&mut stream, protocol::ERROR_DIVERGED, "history hash mismatch");
                return Err(protocol::Error::Invalid("history hash mismatch"));
            }
            None => {
                send_error(&mut stream, protocol::ERROR_REBOOTSTRAP_REQUIRED, "history prefix is no longer retained");
                return Err(protocol::Error::Invalid("history prefix unavailable"));
            }
            Some(_) => {}
        }
        (guard.identity().node_id, guard.last_durable_lsn(), guard.record_hash_at(guard.last_durable_lsn()).unwrap_or(0), guard.snapshot_lsn() + 1)
    };
    if !stats.connect(replica_id, cursor) {
        send_error(&mut stream, protocol::ERROR_UNAVAILABLE, "duplicate or excessive replica connection");
        return Err(protocol::Error::Invalid("replica connection rejected"));
    }
    let _peer = PeerGuard { id: replica_id, stats: Arc::clone(&stats) };
    write_message(&mut stream, &Message::HelloAck { cluster_id, primary_id, durable_lsn: latest_lsn, record_hash: latest_hash, earliest_retained_lsn: earliest, wal_version: 1, snapshot_version: 1 })?;
    let started = Instant::now();
    let mut last_heartbeat = Instant::now();
    while !shutdown.load(Ordering::SeqCst) {
        let next = {
            let guard = db.read().expect("db lock poisoned");
            guard.durable_records_after(cursor, 1)
        };
        let Some(records) = next else {
            send_error(&mut stream, protocol::ERROR_REBOOTSTRAP_REQUIRED, "required WAL was reclaimed");
            return Err(protocol::Error::Invalid("required WAL was reclaimed"));
        };
        if let Some(record) = records.into_iter().next() {
            let expected_hash = record.record_hash();
            write_message(&mut stream, &Message::Record { record: record.clone(), record_hash: expected_hash })?;
            match read_message(&mut stream)? {
                Some(Message::Ack { durable_lsn, applied_lsn, record_hash })
                    if durable_lsn == record.lsn && applied_lsn == record.lsn && record_hash == expected_hash => {
                        cursor = record.lsn;
                        stats.ack(replica_id, cursor);
                    }
                _ => {
                    send_error(&mut stream, protocol::ERROR_DIVERGED, "ACK does not match sent record");
                    return Err(protocol::Error::Invalid("invalid ACK"));
                }
            }
            continue;
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let durable_lsn = db.read().expect("db lock poisoned").last_durable_lsn();
            let send_monotonic_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            write_message(&mut stream, &Message::Heartbeat { durable_lsn, send_monotonic_ns })?;
            last_heartbeat = Instant::now();
        }
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

pub struct ReplicaRunner {
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    fatal_error: Arc<Mutex<Option<String>>>,
}

impl ReplicaRunner {
    pub fn start<F>(primary: SocketAddr, db: Arc<RwLock<Db<F>>>) -> io::Result<Self>
    where F: FileSystem + Clone + Send + Sync + 'static,
    {
        {
            let guard = db.read().expect("db lock poisoned");
            if guard.identity().role != "replica" || guard.durability_mode() != DurabilityMode::Fsync {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "replica runner requires an fsync replica"));
            }
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let fatal_error = Arc::new(Mutex::new(None));
        let stop = Arc::clone(&shutdown);
        let error_slot = Arc::clone(&fatal_error);
        let worker = thread::Builder::new().name("ddb-replica".into()).spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match replica_session(primary, &db, &stop) {
                    Ok(()) => {}
                    Err(SessionError::Retry) => {}
                    Err(SessionError::Fatal(error)) => {
                        *error_slot.lock().expect("replica error lock poisoned") = Some(error);
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
        })?;
        Ok(Self { shutdown, worker: Some(worker), fatal_error })
    }

    pub fn fatal_error(&self) -> Option<String> {
        self.fatal_error.lock().expect("replica error lock poisoned").clone()
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}

impl Drop for ReplicaRunner {
    fn drop(&mut self) { self.shutdown(); }
}

enum SessionError { Retry, Fatal(String) }

fn replica_session<F>(primary: SocketAddr, db: &Arc<RwLock<Db<F>>>, stop: &AtomicBool) -> std::result::Result<(), SessionError>
where F: FileSystem + Clone,
{
    let mut stream = TcpStream::connect_timeout(&primary, Duration::from_millis(500)).map_err(|_| SessionError::Retry)?;
    stream.set_read_timeout(Some(IO_TIMEOUT)).map_err(|_| SessionError::Retry)?;
    stream.set_write_timeout(Some(IO_TIMEOUT)).map_err(|_| SessionError::Retry)?;
    stream.set_nodelay(true).map_err(|_| SessionError::Retry)?;
    let (cluster_id, replica_id, durable_lsn, record_hash) = {
        let guard = db.read().expect("db lock poisoned");
        (guard.identity().cluster_id, guard.identity().node_id, guard.last_durable_lsn(), guard.record_hash_at(guard.last_durable_lsn()).unwrap_or(0))
    };
    write_message(&mut stream, &Message::Hello { cluster_id, replica_id, durable_lsn, record_hash, wal_version: 1, snapshot_version: 1 }).map_err(|_| SessionError::Retry)?;
    match read_message(&mut stream).map_err(|_| SessionError::Retry)? {
        Some(Message::HelloAck { cluster_id: response_cluster, durable_lsn: primary_lsn, wal_version: 1, snapshot_version: 1, .. })
            if response_cluster == cluster_id && primary_lsn >= durable_lsn => {}
        Some(Message::Error { code, diagnostic }) if code == protocol::ERROR_DIVERGED || code == protocol::ERROR_REBOOTSTRAP_REQUIRED || code == protocol::ERROR_CLUSTER_MISMATCH =>
            return Err(SessionError::Fatal(diagnostic)),
        _ => return Err(SessionError::Fatal("invalid primary HELLO_ACK".into())),
    }
    while !stop.load(Ordering::SeqCst) {
        match read_message(&mut stream) {
            Ok(Some(Message::Record { record, record_hash })) => {
                if record.record_hash() != record_hash {
                    return Err(SessionError::Fatal("record hash mismatch".into()));
                }
                let mut guard = db.write().expect("db lock poisoned");
                if let Err(error) = guard.apply_replicated_record(&record) {
                    return Err(SessionError::Fatal(error.to_string()));
                }
                let durable_lsn = guard.last_durable_lsn();
                let applied_lsn = guard.last_applied_lsn();
                let hash = guard.record_hash_at(durable_lsn).unwrap_or(0);
                drop(guard);
                write_message(&mut stream, &Message::Ack { durable_lsn, applied_lsn, record_hash: hash }).map_err(|_| SessionError::Retry)?;
            }
            Ok(Some(Message::Heartbeat { .. })) => {}
            Ok(Some(Message::Error { code, diagnostic })) if code == protocol::ERROR_DIVERGED || code == protocol::ERROR_REBOOTSTRAP_REQUIRED =>
                return Err(SessionError::Fatal(diagnostic)),
            Ok(None) | Err(_) => return Err(SessionError::Retry),
            _ => return Err(SessionError::Fatal("unexpected primary message".into())),
        }
    }
    Ok(())
}
