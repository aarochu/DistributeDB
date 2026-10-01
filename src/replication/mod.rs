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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::checksum::crc64_ecma;
use crate::fileio::FileSystem;
use crate::wal::{Db, DurabilityMode};
use protocol::{read_message, write_message, Message};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_REPLICAS: usize = 16;
const MAX_CONNECTIONS: usize = 32;
const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024 - 26;

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
        peers.insert(
            id,
            PeerProgress {
                applied_lsn,
                connected: true,
            },
        );
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
        let mut snapshot: Vec<_> = peers
            .iter()
            .map(|(&id, &progress)| (id, progress))
            .collect();
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
    pub fn start<A, F>(
        addr: A,
        db: Arc<RwLock<Db<F>>>,
        stats: Arc<ReplicationStats>,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        F: FileSystem + Clone + Send + Sync + 'static,
    {
        Self::start_with_network(addr, db, stats, false)
    }

    /// Explicitly permit a non-loopback listener and peers for an isolated
    /// local container network. The wire protocol has no authentication.
    pub fn start_with_network<A, F>(
        addr: A,
        db: Arc<RwLock<Db<F>>>,
        stats: Arc<ReplicationStats>,
        allow_non_loopback: bool,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        F: FileSystem + Clone + Send + Sync + 'static,
    {
        {
            let guard = db.read().expect("db lock poisoned");
            if guard.identity().role != "primary"
                || guard.durability_mode() != DurabilityMode::Fsync
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replication requires an fsync primary",
                ));
            }
        }
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        if !allow_non_loopback && !local_addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Phase 5 replication must bind loopback",
            ));
        }
        listener.set_nonblocking(true)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&shutdown);
        let active_connections = Arc::new(AtomicUsize::new(0));
        let acceptor = thread::Builder::new()
            .name("ddb-repl-accept".into())
            .spawn(move || {
                let mut workers: Vec<JoinHandle<()>> = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, peer)) if allow_non_loopback || peer.ip().is_loopback() => {
                            if active_connections.load(Ordering::SeqCst) >= MAX_CONNECTIONS {
                                continue;
                            }
                            active_connections.fetch_add(1, Ordering::SeqCst);
                            let db = Arc::clone(&db);
                            let stats = Arc::clone(&stats);
                            let stop = Arc::clone(&stop);
                            let active_connections = Arc::clone(&active_connections);
                            workers.push(thread::spawn(move || {
                                let _ = handle_primary_connection(stream, db, stats, stop);
                                active_connections.fetch_sub(1, Ordering::SeqCst);
                            }));
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(POLL_INTERVAL)
                        }
                        Err(_) => thread::sleep(POLL_INTERVAL),
                    }
                    let mut index = 0;
                    while index < workers.len() {
                        if workers[index].is_finished() {
                            let _ = workers.swap_remove(index).join();
                        } else {
                            index += 1;
                        }
                    }
                }
                for worker in workers {
                    let _ = worker.join();
                }
            })?;
        Ok(Self {
            local_addr,
            shutdown,
            acceptor: Some(acceptor),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
    }
}

impl Drop for PrimaryListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn send_error(stream: &mut TcpStream, code: u16, diagnostic: &str) {
    let _ = write_message(
        stream,
        &Message::Error {
            code,
            diagnostic: diagnostic.into(),
        },
    );
}

fn handle_primary_connection<F>(
    mut stream: TcpStream,
    db: Arc<RwLock<Db<F>>>,
    stats: Arc<ReplicationStats>,
    shutdown: Arc<AtomicBool>,
) -> protocol::Result<()>
where
    F: FileSystem + Clone,
{
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let (cluster_id, replica_id, mut cursor, hash, wal_version, snapshot_version) =
        match read_message(&mut stream)? {
            Some(Message::Hello {
                cluster_id,
                replica_id,
                durable_lsn,
                record_hash,
                wal_version,
                snapshot_version,
            }) => (
                cluster_id,
                replica_id,
                durable_lsn,
                record_hash,
                wal_version,
                snapshot_version,
            ),
            _ => {
                send_error(&mut stream, protocol::ERROR_BAD_FRAME, "expected HELLO");
                return Err(protocol::Error::Invalid("expected HELLO"));
            }
        };
    if wal_version != 1 || snapshot_version != 1 {
        send_error(
            &mut stream,
            protocol::ERROR_BAD_FRAME,
            "unsupported format version",
        );
        return Err(protocol::Error::Invalid("unsupported format version"));
    }
    let history = {
        let guard = db.read().expect("db lock poisoned");
        if cluster_id != guard.identity().cluster_id || replica_id == guard.identity().node_id {
            Err((
                protocol::ERROR_CLUSTER_MISMATCH,
                "cluster or node ID mismatch",
            ))
        } else if cursor > guard.last_durable_lsn() {
            Err((protocol::ERROR_DIVERGED, "replica is ahead of primary"))
        } else {
            match guard.record_hash_at(cursor) {
                Some(known) if known != hash => {
                    Err((protocol::ERROR_DIVERGED, "history hash mismatch"))
                }
                known => {
                    let snapshot = if known.is_none() {
                        match guard.replication_snapshot() {
                            Ok(Some(snapshot)) if snapshot.bytes.len() <= MAX_SNAPSHOT_BYTES => {
                                Ok(Some(snapshot))
                            }
                            _ => Err((
                                protocol::ERROR_REBOOTSTRAP_REQUIRED,
                                "snapshot unavailable for reclaimed prefix",
                            )),
                        }
                    } else {
                        Ok(None)
                    };
                    snapshot.map(|snapshot| {
                        (
                            guard.identity().node_id,
                            guard.last_durable_lsn(),
                            guard.record_hash_at(guard.last_durable_lsn()).unwrap_or(0),
                            guard.snapshot_lsn() + 1,
                            snapshot,
                        )
                    })
                }
            }
        }
    };
    let (primary_id, latest_lsn, latest_hash, earliest, snapshot) = match history {
        Ok(history) => history,
        Err((code, diagnostic)) => {
            send_error(&mut stream, code, diagnostic);
            return Err(protocol::Error::Invalid(diagnostic));
        }
    };
    if !stats.connect(replica_id, cursor) {
        send_error(
            &mut stream,
            protocol::ERROR_UNAVAILABLE,
            "duplicate or excessive replica connection",
        );
        return Err(protocol::Error::Invalid("replica connection rejected"));
    }
    let _peer = PeerGuard {
        id: replica_id,
        stats: Arc::clone(&stats),
    };
    write_message(
        &mut stream,
        &Message::HelloAck {
            cluster_id,
            primary_id,
            durable_lsn: latest_lsn,
            record_hash: latest_hash,
            earliest_retained_lsn: earliest,
            wal_version: 1,
            snapshot_version: 1,
        },
    )?;
    if let Some(snapshot) = snapshot {
        let bytes = snapshot.bytes;
        let snapshot_lsn = snapshot.lsn;
        let snapshot_hash = snapshot.record_hash;
        let snapshot_crc64 = snapshot.crc64;
        write_message(
            &mut stream,
            &Message::SnapshotOffer {
                snapshot_lsn,
                record_hash: snapshot_hash,
                snapshot_bytes: bytes.len() as u64,
                snapshot_crc64,
            },
        )?;
        for (index, chunk) in bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
            write_message(
                &mut stream,
                &Message::SnapshotChunk {
                    snapshot_lsn,
                    offset: (index * SNAPSHOT_CHUNK_BYTES) as u64,
                    bytes: chunk.to_vec(),
                },
            )?;
        }
        write_message(
            &mut stream,
            &Message::SnapshotDone {
                snapshot_lsn,
                snapshot_crc64,
            },
        )?;
        match read_message(&mut stream)? {
            Some(Message::Ack {
                durable_lsn,
                applied_lsn,
                record_hash,
            }) if durable_lsn == snapshot_lsn
                && applied_lsn == snapshot_lsn
                && record_hash == snapshot_hash =>
            {
                cursor = snapshot_lsn;
                stats.ack(replica_id, cursor);
            }
            _ => return Err(protocol::Error::Invalid("invalid snapshot ACK")),
        }
    }
    let started = Instant::now();
    let mut last_heartbeat = Instant::now();
    while !shutdown.load(Ordering::SeqCst) {
        let next = {
            let guard = db.read().expect("db lock poisoned");
            guard.durable_records_after(cursor, 1)
        };
        let Some(records) = next else {
            send_error(
                &mut stream,
                protocol::ERROR_REBOOTSTRAP_REQUIRED,
                "required WAL was reclaimed",
            );
            return Err(protocol::Error::Invalid("required WAL was reclaimed"));
        };
        if let Some(record) = records.into_iter().next() {
            let expected_hash = record.record_hash();
            write_message(
                &mut stream,
                &Message::Record {
                    record: record.clone(),
                    record_hash: expected_hash,
                },
            )?;
            match read_message(&mut stream)? {
                Some(Message::Ack {
                    durable_lsn,
                    applied_lsn,
                    record_hash,
                }) if durable_lsn == record.lsn
                    && applied_lsn == record.lsn
                    && record_hash == expected_hash =>
                {
                    cursor = record.lsn;
                    stats.ack(replica_id, cursor);
                }
                _ => {
                    send_error(
                        &mut stream,
                        protocol::ERROR_DIVERGED,
                        "ACK does not match sent record",
                    );
                    return Err(protocol::Error::Invalid("invalid ACK"));
                }
            }
            continue;
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let durable_lsn = db.read().expect("db lock poisoned").last_durable_lsn();
            let send_monotonic_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            write_message(
                &mut stream,
                &Message::Heartbeat {
                    durable_lsn,
                    send_monotonic_ns,
                },
            )?;
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
    where
        F: FileSystem + Clone + Send + Sync + 'static,
    {
        Self::start_with_host(primary.to_string(), db)
    }

    /// Resolve the configured primary host on each retry so a restarted
    /// container can be reached if its network address changes.
    pub fn start_with_host<F>(primary: String, db: Arc<RwLock<Db<F>>>) -> io::Result<Self>
    where
        F: FileSystem + Clone + Send + Sync + 'static,
    {
        {
            let guard = db.read().expect("db lock poisoned");
            if guard.identity().role != "replica"
                || guard.durability_mode() != DurabilityMode::Fsync
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replica runner requires an fsync replica",
                ));
            }
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let fatal_error = Arc::new(Mutex::new(None));
        let stop = Arc::clone(&shutdown);
        let error_slot = Arc::clone(&fatal_error);
        let worker = thread::Builder::new()
            .name("ddb-replica".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match replica_session(&primary, &db, &stop) {
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
        Ok(Self {
            shutdown,
            worker: Some(worker),
            fatal_error,
        })
    }

    pub fn fatal_error(&self) -> Option<String> {
        self.fatal_error
            .lock()
            .expect("replica error lock poisoned")
            .clone()
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for ReplicaRunner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum SessionError {
    Retry,
    Fatal(String),
}

fn replica_session<F>(
    primary: &str,
    db: &Arc<RwLock<Db<F>>>,
    stop: &AtomicBool,
) -> std::result::Result<(), SessionError>
where
    F: FileSystem + Clone,
{
    let addresses = primary
        .to_socket_addrs()
        .map_err(|_| SessionError::Retry)?;
    let mut stream = addresses
        .filter_map(|address| TcpStream::connect_timeout(&address, Duration::from_millis(500)).ok())
        .next()
        .ok_or(SessionError::Retry)?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|_| SessionError::Retry)?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|_| SessionError::Retry)?;
    stream.set_nodelay(true).map_err(|_| SessionError::Retry)?;
    let (cluster_id, replica_id, durable_lsn, record_hash) = {
        let guard = db.read().expect("db lock poisoned");
        (
            guard.identity().cluster_id,
            guard.identity().node_id,
            guard.last_durable_lsn(),
            guard.record_hash_at(guard.last_durable_lsn()).unwrap_or(0),
        )
    };
    write_message(
        &mut stream,
        &Message::Hello {
            cluster_id,
            replica_id,
            durable_lsn,
            record_hash,
            wal_version: 1,
            snapshot_version: 1,
        },
    )
    .map_err(|_| SessionError::Retry)?;
    match read_message(&mut stream).map_err(|_| SessionError::Retry)? {
        Some(Message::HelloAck {
            cluster_id: response_cluster,
            durable_lsn: primary_lsn,
            wal_version: 1,
            snapshot_version: 1,
            ..
        }) if response_cluster == cluster_id && primary_lsn >= durable_lsn => {}
        Some(Message::Error { code, diagnostic })
            if code == protocol::ERROR_DIVERGED
                || code == protocol::ERROR_REBOOTSTRAP_REQUIRED
                || code == protocol::ERROR_CLUSTER_MISMATCH =>
        {
            return Err(SessionError::Fatal(diagnostic))
        }
        Some(Message::Error { code, .. }) if code == protocol::ERROR_UNAVAILABLE => {
            return Err(SessionError::Retry)
        }
        _ => return Err(SessionError::Fatal("invalid primary HELLO_ACK".into())),
    }
    while !stop.load(Ordering::SeqCst) {
        match read_message(&mut stream) {
            Ok(Some(Message::Record {
                record,
                record_hash,
            })) => {
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
                write_message(
                    &mut stream,
                    &Message::Ack {
                        durable_lsn,
                        applied_lsn,
                        record_hash: hash,
                    },
                )
                .map_err(|_| SessionError::Retry)?;
            }
            Ok(Some(Message::Heartbeat { .. })) => {}
            Ok(Some(Message::SnapshotOffer {
                snapshot_lsn,
                record_hash,
                snapshot_bytes,
                snapshot_crc64,
            })) => {
                if !db
                    .read()
                    .expect("db lock poisoned")
                    .allows_snapshot_rebootstrap()
                {
                    send_error(
                        &mut stream,
                        protocol::ERROR_REBOOTSTRAP_REQUIRED,
                        "replica was not provisioned for snapshot rebootstrap",
                    );
                    return Err(SessionError::Fatal(
                        "snapshot rebootstrap requires explicit provisioning".into(),
                    ));
                }
                if snapshot_bytes > MAX_SNAPSHOT_BYTES as u64 || snapshot_bytes < 72 {
                    return Err(SessionError::Fatal("snapshot size is invalid".into()));
                }
                let mut bytes = Vec::with_capacity(snapshot_bytes as usize);
                while bytes.len() < snapshot_bytes as usize {
                    match read_message(&mut stream) {
                        Ok(Some(Message::SnapshotChunk {
                            snapshot_lsn: chunk_lsn,
                            offset,
                            bytes: chunk,
                        })) if chunk_lsn == snapshot_lsn
                            && offset == bytes.len() as u64
                            && !chunk.is_empty()
                            && chunk.len() <= snapshot_bytes as usize - bytes.len() =>
                        {
                            bytes.extend_from_slice(&chunk);
                        }
                        Ok(None) | Err(_) => return Err(SessionError::Retry),
                        _ => return Err(SessionError::Fatal("invalid snapshot chunk".into())),
                    }
                }
                match read_message(&mut stream) {
                    Ok(Some(Message::SnapshotDone {
                        snapshot_lsn: done_lsn,
                        snapshot_crc64: done_crc,
                    })) if done_lsn == snapshot_lsn && done_crc == snapshot_crc64 => {}
                    Ok(None) | Err(_) => return Err(SessionError::Retry),
                    _ => return Err(SessionError::Fatal("invalid snapshot completion".into())),
                }
                let embedded_crc = u64::from_le_bytes(
                    bytes[bytes.len() - 8..].try_into().expect("length checked"),
                );
                if embedded_crc != snapshot_crc64
                    || crc64_ecma(&bytes[..bytes.len() - 8]) != snapshot_crc64
                {
                    return Err(SessionError::Fatal("snapshot checksum mismatch".into()));
                }
                let mut guard = db.write().expect("db lock poisoned");
                guard
                    .install_replica_snapshot(&bytes, snapshot_lsn, record_hash)
                    .map_err(|error| SessionError::Fatal(error.to_string()))?;
                drop(guard);
                write_message(
                    &mut stream,
                    &Message::Ack {
                        durable_lsn: snapshot_lsn,
                        applied_lsn: snapshot_lsn,
                        record_hash,
                    },
                )
                .map_err(|_| SessionError::Retry)?;
            }
            Ok(Some(Message::Error { code, diagnostic }))
                if code == protocol::ERROR_DIVERGED
                    || code == protocol::ERROR_REBOOTSTRAP_REQUIRED =>
            {
                return Err(SessionError::Fatal(diagnostic))
            }
            Ok(None) | Err(_) => return Err(SessionError::Retry),
            _ => return Err(SessionError::Fatal("unexpected primary message".into())),
        }
    }
    Ok(())
}
