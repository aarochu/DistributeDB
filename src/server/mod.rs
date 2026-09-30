//! Threaded TCP server with the §5 mutation-sequencer architecture
//! (Technical-Design §4, §5, §16; SOW §5, §13, §17).
//!
//! Phase 3 puts the Phase 2 durable [`Db`](crate::wal::Db) behind a TCP wire
//! protocol (the FEAT-001 codec in [`crate::protocol`]). The design follows
//! Technical-Design §5:
//!
//! * An **acceptor** thread owns the [`TcpListener`]. It accepts connections
//!   and spawns one worker thread per connection, up to
//!   [`ServerConfig::max_connections`] (default 128). When the live-connection
//!   gauge is at the cap the acceptor **rejects the excess** connection: it
//!   writes a single `UNAVAILABLE` response frame and closes it (§5 "acceptor
//!   rejects excess").
//! * Each **connection worker** reads request frames with
//!   [`protocol::read_frame`](crate::protocol::read_frame), serves reads
//!   (`GET`/`EXISTS`/`STATS`) directly under a shared read lock, and submits
//!   writes (`SET`/`DELETE`) to the sequencer over a bounded queue, blocking on
//!   a per-request one-shot for the result. A worker **never holds the map
//!   lock while waiting on the sequencer** (the deadlock rule).
//! * A single **sequencer** thread owns the WAL append + group `fsync` + map
//!   apply + LSN assignment. It drains up to [`wal::MAX_GROUP_RECORDS`] (64)
//!   mutations / [`wal::MAX_GROUP_BYTES`] (8 MiB) into one group and calls
//!   [`Db::apply_group`](crate::wal::Db::apply_group) once per group, so a
//!   single `fsync` covers the whole batch (§5 group commit). The sequencer
//!   **never performs network I/O**; it only touches the WAL, the map, and the
//!   per-request response slots.
//!
//! The shared engine is a [`RwLock<Db<F>>`]: the sequencer takes the write
//! lock only while applying an already-synced group; readers take the read
//! lock for `GET`/`EXISTS`/`STATS`. Because the durable `fsync` happens before
//! the write lock is taken, the linearization point is the map apply after the
//! WAL sync (§5).
//!
//! The bounded write queue is a `Mutex<VecDeque<..>>` + [`Condvar`] with an
//! explicit `max_queue_depth` (std has no bounded channel); an overflow returns
//! `RESOURCE_EXHAUSTED` to that connection (§5).
//!
//! # Durability / retry limitation (Technical-Design §4.2)
//!
//! A `SET`/`DELETE` is durable once its group `fsync` completes and the server
//! returns `OK`. If the connection drops *before* the client reads the `OK`,
//! the outcome is **UNKNOWN** to the client: the write may or may not have been
//! committed. There is no request de-duplication, so a client that retries may
//! apply the mutation twice. `SET` is idempotent (last write wins);
//! `DELETE` is idempotent; a retried non-idempotent sequence is the client's
//! responsibility. This is the documented §4.2 no-exactly-once-on-disconnect
//! contract.
//!
//! This is a loopback-only, unauthenticated demo server: it binds `127.0.0.1`
//! and performs no authentication or encryption (out of scope for Phase 3).

use std::collections::VecDeque;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::fileio::FileSystem;
use crate::protocol::{
    self, Request, Response, Status, KIND_DELETE, KIND_EXISTS, KIND_GET, KIND_SET, KIND_STATS,
    PROTOCOL_VERSION,
};
use crate::storage::{GetResult, Mutation};
use crate::wal::{Db, MAX_GROUP_BYTES, MAX_GROUP_RECORDS};

/// Default maximum number of concurrent connections (Technical-Design §16, §5).
pub const DEFAULT_MAX_CONNECTIONS: usize = 128;
/// Default idle-read timeout for a client connection (Technical-Design §16).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Default bounded write-queue depth (sequencer backpressure, §5).
pub const DEFAULT_MAX_QUEUE_DEPTH: usize = 1024;
/// Default intentional group-commit wait: none (Technical-Design §5).
pub const DEFAULT_GROUP_WAIT: Duration = Duration::from_millis(0);

/// Configuration for a [`Server`] (Technical-Design §5, §16).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum concurrent connections; the acceptor rejects the excess.
    pub max_connections: usize,
    /// Per-connection idle read timeout.
    pub idle_timeout: Duration,
    /// Bounded write-queue depth before the sequencer returns
    /// `RESOURCE_EXHAUSTED`.
    pub max_queue_depth: usize,
    /// Intentional wait the sequencer allows for a group to fill (default 0).
    pub group_wait: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            group_wait: DEFAULT_GROUP_WAIT,
        }
    }
}

/// Server-side operational counters for `STATS` (Technical-Design §17).
#[derive(Debug, Default)]
struct Metrics {
    requests_total: AtomicU64,
    reads_total: AtomicU64,
    writes_total: AtomicU64,
    connected_clients: AtomicUsize,
}

/// The outcome the sequencer returns for a submitted write: the assigned LSN on
/// success, or a failure [`Status`] to send back to the client.
type WriteOutcome = Result<u64, Status>;

/// A submitted write awaiting sequencing: the mutation plus a one-shot sender
/// for the sequencer to return the outcome.
struct WriteJob {
    mutation: Mutation,
    /// Encoded length used to enforce the 8 MiB per-group byte cap.
    encoded_len: usize,
    respond: Sender<WriteOutcome>,
}

/// The bounded write queue shared between connection workers and the
/// sequencer. `closed` is set on shutdown so the sequencer drains and exits.
struct WriteQueue {
    inner: Mutex<QueueState>,
    not_empty: Condvar,
    max_depth: usize,
}

struct QueueState {
    jobs: VecDeque<WriteJob>,
    closed: bool,
}

impl WriteQueue {
    fn new(max_depth: usize) -> Self {
        WriteQueue {
            inner: Mutex::new(QueueState {
                jobs: VecDeque::new(),
                closed: false,
            }),
            not_empty: Condvar::new(),
            max_depth,
        }
    }

    /// Try to enqueue a job. Returns the job back (as `Err`) when the queue is
    /// full or closed, so the caller can answer `RESOURCE_EXHAUSTED`.
    fn try_push(&self, job: WriteJob) -> Result<(), WriteJob> {
        let mut state = self.inner.lock().expect("write queue poisoned");
        if state.closed || state.jobs.len() >= self.max_depth {
            return Err(job);
        }
        state.jobs.push_back(job);
        drop(state);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Signal shutdown: no more jobs are accepted and the sequencer wakes to
    /// drain whatever remains and then exit.
    fn close(&self) {
        let mut state = self.inner.lock().expect("write queue poisoned");
        state.closed = true;
        drop(state);
        self.not_empty.notify_all();
    }
}

/// Drain up to `MAX_GROUP_RECORDS` jobs / `MAX_GROUP_BYTES` encoded bytes from
/// the front of `jobs` into a batch, respecting the §5 group-commit bounds.
///
/// Pure function over the queue so it is unit-testable without sockets. At
/// least one job is always taken when `jobs` is non-empty (a single oversized
/// mutation is already bounded by the codec's mutation limit, which is well
/// under 8 MiB). Returns the drained jobs in submission order.
fn drain_group(jobs: &mut VecDeque<WriteJob>) -> Vec<WriteJob> {
    let mut batch: Vec<WriteJob> = Vec::new();
    let mut bytes = 0usize;
    while let Some(front) = jobs.front() {
        if !batch.is_empty() {
            // Stop before exceeding either bound; leave the rest for the next
            // group. The footer overhead is small and bounded; reserving a
            // little headroom keeps us safely under MAX_GROUP_BYTES.
            if batch.len() >= MAX_GROUP_RECORDS || bytes + front.encoded_len + 64 > MAX_GROUP_BYTES
            {
                break;
            }
        }
        let job = jobs.pop_front().expect("front exists");
        bytes += job.encoded_len;
        batch.push(job);
    }
    batch
}

/// A running server. Dropping or calling [`Server::shutdown`] stops the
/// acceptor, drains in-flight work, and joins all threads.
pub struct Server {
    local_addr: std::net::SocketAddr,
    shutdown: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    sequencer: Option<JoinHandle<()>>,
    queue: Arc<WriteQueue>,
    /// A clone of the listener address we can self-connect to in order to
    /// unblock the blocking `accept()` during shutdown.
    listener_addr: std::net::SocketAddr,
}

impl Server {
    /// The address the listener is bound to (with the OS-assigned port when
    /// bound to `:0`). Tests use this to connect.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// A [`ShutdownHandle`] the CLI or tests can use to trigger a graceful
    /// shutdown from another thread.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            shutdown: Arc::clone(&self.shutdown),
            addr: self.listener_addr,
        }
    }

    /// Gracefully shut down: stop accepting, close the write queue so the
    /// sequencer drains and exits, and join the acceptor and sequencer.
    /// In-flight connection workers finish their current request; the acceptor
    /// stops spawning new ones. Idempotent.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Unblock the blocking accept() by self-connecting to the listener.
        let _ = TcpStream::connect(self.listener_addr);
        if let Some(handle) = self.acceptor.take() {
            let _ = handle.join();
        }
        // With the acceptor stopped, no new writes are queued; close the queue
        // so the sequencer drains the remainder and exits.
        self.queue.close();
        if let Some(handle) = self.sequencer.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A cloneable handle to trigger [`Server::shutdown`] from another thread
/// (e.g. a signal handler or the CLI).
#[derive(Clone)]
pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
    addr: std::net::SocketAddr,
}

impl ShutdownHandle {
    /// Signal the acceptor to stop and unblock its `accept()`.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

/// Shared state handed to each connection worker and the sequencer.
struct Shared<F: FileSystem + Clone + Send + 'static> {
    db: RwLock<Db<F>>,
    metrics: Metrics,
    start: Instant,
    queue: Arc<WriteQueue>,
    config: ServerConfig,
    live_connections: AtomicUsize,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    /// Bind and start a server on `addr`, driving all durable writes through
    /// `db` (Technical-Design §5). Spawns the acceptor and the sequencer and
    /// returns immediately; the server runs until [`Server::shutdown`] (or
    /// drop).
    ///
    /// `addr` accepts anything [`std::net::ToSocketAddrs`] does; tests pass
    /// `"127.0.0.1:0"` and read the OS-assigned port from
    /// [`Server::local_addr`].
    pub fn start<A, FS>(addr: A, db: Db<FS>, config: ServerConfig) -> std::io::Result<Server>
    where
        A: std::net::ToSocketAddrs,
        FS: FileSystem + Clone + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let queue = Arc::new(WriteQueue::new(config.max_queue_depth));

        let shared = Arc::new(Shared {
            db: RwLock::new(db),
            metrics: Metrics::default(),
            start: Instant::now(),
            queue: Arc::clone(&queue),
            config: config.clone(),
            live_connections: AtomicUsize::new(0),
            shutdown: Arc::clone(&shutdown),
        });

        // Sequencer thread: single owner of the WAL append + group sync + apply.
        let seq_shared = Arc::clone(&shared);
        let sequencer = thread::Builder::new()
            .name("ddb-sequencer".to_string())
            .spawn(move || sequencer_loop(seq_shared))
            .expect("spawn sequencer");

        // Acceptor thread.
        let acc_shared = Arc::clone(&shared);
        let acceptor = thread::Builder::new()
            .name("ddb-acceptor".to_string())
            .spawn(move || acceptor_loop(listener, acc_shared))
            .expect("spawn acceptor");

        Ok(Server {
            local_addr,
            shutdown,
            acceptor: Some(acceptor),
            sequencer: Some(sequencer),
            queue,
            listener_addr: local_addr,
        })
    }
}

/// The acceptor loop: accept connections, enforce the connection cap, and spawn
/// one worker thread per accepted connection (Technical-Design §5).
fn acceptor_loop<F>(listener: TcpListener, shared: Arc<Shared<F>>)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    for incoming in listener.incoming() {
        if shared.shutdown.load(Ordering::SeqCst) {
            break;
        }
        let stream = match incoming {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Enforce the connection cap: the acceptor rejects the excess (§5).
        let live = shared.live_connections.fetch_add(1, Ordering::SeqCst);
        if live >= shared.config.max_connections {
            shared.live_connections.fetch_sub(1, Ordering::SeqCst);
            reject_overflow(stream);
            continue;
        }

        let worker_shared = Arc::clone(&shared);
        let spawned = thread::Builder::new()
            .name("ddb-conn".to_string())
            .spawn(move || {
                worker_shared
                    .metrics
                    .connected_clients
                    .fetch_add(1, Ordering::SeqCst);
                handle_connection(stream, &worker_shared);
                worker_shared
                    .metrics
                    .connected_clients
                    .fetch_sub(1, Ordering::SeqCst);
                worker_shared
                    .live_connections
                    .fetch_sub(1, Ordering::SeqCst);
            });
        if spawned.is_err() {
            // Could not spawn: release the reserved slot.
            shared.live_connections.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Reject a connection that exceeds the cap by sending one `UNAVAILABLE`
/// response frame, then closing (§5).
fn reject_overflow(mut stream: TcpStream) {
    let resp = Response::new(
        0,
        Status::Unavailable,
        b"server at connection capacity".to_vec(),
    );
    let _ = stream.write_all(&resp.encode());
    let _ = stream.flush();
    // Stream drops -> connection closes.
}

/// Handle one connection: loop reading request frames and dispatching them
/// until the client disconnects, a fatal decode error occurs, or shutdown.
fn handle_connection<F>(stream: TcpStream, shared: &Arc<Shared<F>>)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let _ = stream.set_read_timeout(Some(shared.config.idle_timeout));
    let _ = stream.set_nodelay(true);
    let mut reader = stream.try_clone().expect("clone stream for reader");
    let mut writer = stream;

    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            break;
        }
        match protocol::read_frame(&mut reader) {
            Ok(None) => break, // Clean disconnect between frames.
            Ok(Some(body)) => {
                shared
                    .metrics
                    .requests_total
                    .fetch_add(1, Ordering::Relaxed);
                let (response, keep_open) = dispatch(&body, shared);
                if writer.write_all(&response.encode()).is_err() {
                    break;
                }
                if writer.flush().is_err() || !keep_open {
                    break;
                }
            }
            Err(err) => {
                // A decode error before/within a frame. Send the mapped status
                // where we can, then close (§4.1: unknown version and malformed
                // frames get a response then the connection closes).
                let status = err.to_status();
                let resp = Response::new(0, status, err.to_string().into_bytes());
                let _ = writer.write_all(&resp.encode());
                let _ = writer.flush();
                break;
            }
        }
    }
}

/// Dispatch one decoded frame body to a [`Response`]. Returns `(response,
/// keep_open)`; `keep_open` is `false` for a fatal condition (bad version)
/// after which the connection must close (§4.1).
fn dispatch<F>(body: &[u8], shared: &Arc<Shared<F>>) -> (Response, bool)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let request = match protocol::decode_request_body(body) {
        Ok(req) => req,
        Err(err) => {
            let status = err.to_status();
            // A bad version is fatal: respond then close (§4.1).
            let keep_open = status != Status::UnsupportedVersion;
            let resp = Response::new(0, status, err.to_string().into_bytes());
            return (resp, keep_open);
        }
    };

    match request {
        Request::Get { key } => {
            shared.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
            let db = shared.db.read().expect("db read lock poisoned");
            match db.get(&key) {
                GetResult::Found(value) => (Response::new(KIND_GET, Status::Ok, value), true),
                GetResult::NotFound => {
                    (Response::new(KIND_GET, Status::NotFound, Vec::new()), true)
                }
            }
        }
        Request::Exists { key } => {
            shared.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
            let db = shared.db.read().expect("db read lock poisoned");
            let byte = if db.exists(&key) { 1u8 } else { 0u8 };
            (Response::new(KIND_EXISTS, Status::Ok, vec![byte]), true)
        }
        Request::Stats => {
            shared.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
            let stats = render_stats(shared);
            (
                Response::new(KIND_STATS, Status::Ok, stats.into_bytes()),
                true,
            )
        }
        Request::Set { key, value } => {
            shared.metrics.writes_total.fetch_add(1, Ordering::Relaxed);
            let encoded_len = crate::command::SET_FIXED_OVERHEAD + key.len() + value.len();
            let mutation = Mutation::Set { key, value };
            (submit_write(shared, mutation, encoded_len, KIND_SET), true)
        }
        Request::Delete { key } => {
            shared.metrics.writes_total.fetch_add(1, Ordering::Relaxed);
            let encoded_len = crate::command::SET_FIXED_OVERHEAD + key.len();
            let mutation = Mutation::Delete { key };
            (
                submit_write(shared, mutation, encoded_len, KIND_DELETE),
                true,
            )
        }
    }
}

/// Submit a write to the sequencer and block (without holding the map lock) on
/// its one-shot result (Technical-Design §5). Maps queue overflow to
/// `RESOURCE_EXHAUSTED` and a WAL failure to `INTERNAL_ERROR`.
fn submit_write<F>(
    shared: &Arc<Shared<F>>,
    mutation: Mutation,
    encoded_len: usize,
    kind: u8,
) -> Response
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let (tx, rx): (Sender<WriteOutcome>, Receiver<WriteOutcome>) = mpsc::channel();
    let job = WriteJob {
        mutation,
        encoded_len,
        respond: tx,
    };
    if let Err(_full) = shared.queue.try_push(job) {
        return Response::new(kind, Status::ResourceExhausted, Vec::new());
    }
    // Block on the one-shot. We hold NO map lock here (deadlock rule, §5).
    match rx.recv() {
        Ok(Ok(_lsn)) => Response::new(kind, Status::Ok, Vec::new()),
        Ok(Err(status)) => Response::new(kind, status, Vec::new()),
        // The sequencer dropped the sender without responding (shutdown mid
        // flight): report unavailable.
        Err(_) => Response::new(kind, Status::Unavailable, Vec::new()),
    }
}

/// The sequencer loop: the single writer. Waits for jobs, drains a group up to
/// the §5 bounds, group-commits it through [`Db::apply_group`], and releases
/// each waiting worker's one-shot with the assigned LSN or an error status.
fn sequencer_loop<F>(shared: Arc<Shared<F>>)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let queue = &shared.queue;
    loop {
        // Wait for at least one job (or shutdown-drain).
        let batch = {
            let mut state = queue.inner.lock().expect("write queue poisoned");
            while state.jobs.is_empty() && !state.closed {
                state = queue
                    .not_empty
                    .wait(state)
                    .expect("write queue condvar poisoned");
            }
            if state.jobs.is_empty() && state.closed {
                break;
            }
            // Optional intentional wait to let a group fill (default 0 ms).
            if !shared.config.group_wait.is_zero() && state.jobs.len() < MAX_GROUP_RECORDS {
                let wait = shared.config.group_wait;
                drop(state);
                thread::sleep(wait);
                state = queue.inner.lock().expect("write queue poisoned");
            }
            drain_group(&mut state.jobs)
        };

        if batch.is_empty() {
            continue;
        }

        // Apply the whole batch as one durable group commit (one fsync).
        let muts: Vec<Mutation> = batch.iter().map(|j| j.mutation.clone()).collect();
        let result = {
            let mut db = shared.db.write().expect("db write lock poisoned");
            db.apply_group(&muts)
        };

        match result {
            Ok(lsns) => {
                for (job, lsn) in batch.into_iter().zip(lsns.into_iter()) {
                    let _ = job.respond.send(Ok(lsn));
                }
            }
            Err(_e) => {
                // The whole group failed to commit durably; no mutation was
                // applied. Report INTERNAL_ERROR to every waiter.
                for job in batch {
                    let _ = job.respond.send(Err(Status::InternalError));
                }
            }
        }
    }
}

/// Render the `STATS` payload as bounded UTF-8 `name=value` lines with the
/// version FIRST (Technical-Design §4.1).
///
/// NOTE: SOW §17 shows a `name: value` (colon) example, but Technical-Design
/// §4.1 is the binding wire spec and specifies `name=value` with version
/// first; this resolved discrepancy uses the §4.1 form. Replica fields
/// (`replicas_connected`, per-replica lag) are Phase 5 and reported as
/// `replicas_connected=0` here. The output is a small fixed set of lines, so it
/// is inherently bounded.
fn render_stats<F>(shared: &Arc<Shared<F>>) -> String
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let (keys, current_lsn) = {
        let db = shared.db.read().expect("db read lock poisoned");
        (db.len(), db.last_applied_lsn())
    };
    let snapshot = StatsSnapshot {
        uptime_seconds: shared.start.elapsed().as_secs(),
        keys,
        requests_total: shared.metrics.requests_total.load(Ordering::Relaxed),
        reads_total: shared.metrics.reads_total.load(Ordering::Relaxed),
        writes_total: shared.metrics.writes_total.load(Ordering::Relaxed),
        current_lsn,
        connected_clients: shared.metrics.connected_clients.load(Ordering::Relaxed),
    };
    render_stats_lines(&snapshot)
}

/// A point-in-time snapshot of the metrics rendered into `STATS` output.
///
/// Split out so the `name=value` rendering is a pure function testable without
/// a live server (Technical-Design §17).
struct StatsSnapshot {
    uptime_seconds: u64,
    keys: usize,
    requests_total: u64,
    reads_total: u64,
    writes_total: u64,
    current_lsn: u64,
    connected_clients: usize,
}

/// Render a [`StatsSnapshot`] as bounded UTF-8 `name=value` lines with
/// `version` FIRST (Technical-Design §4.1).
fn render_stats_lines(s: &StatsSnapshot) -> String {
    let mut out = String::new();
    // version FIRST (§4.1).
    out.push_str(&format!("version={}\n", PROTOCOL_VERSION));
    out.push_str(&format!("uptime_seconds={}\n", s.uptime_seconds));
    out.push_str(&format!("keys={}\n", s.keys));
    out.push_str(&format!("requests_total={}\n", s.requests_total));
    out.push_str(&format!("reads_total={}\n", s.reads_total));
    out.push_str(&format!("writes_total={}\n", s.writes_total));
    out.push_str(&format!("current_lsn={}\n", s.current_lsn));
    out.push_str(&format!("connected_clients={}\n", s.connected_clients));
    // Replication is Phase 5; report zero for now.
    out.push_str("replicas_connected=0\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(encoded_len: usize) -> WriteJob {
        let (tx, _rx) = mpsc::channel();
        WriteJob {
            mutation: Mutation::Delete { key: b"k".to_vec() },
            encoded_len,
            respond: tx,
        }
    }

    #[test]
    fn drain_group_takes_at_most_max_records() {
        let mut jobs: VecDeque<WriteJob> = (0..(MAX_GROUP_RECORDS + 10)).map(|_| job(10)).collect();
        let batch = drain_group(&mut jobs);
        assert_eq!(batch.len(), MAX_GROUP_RECORDS);
        assert_eq!(jobs.len(), 10);
    }

    #[test]
    fn drain_group_stops_at_byte_cap() {
        // Each job ~1 MiB; 8 should exceed the 8 MiB group cap (with footer
        // headroom), so we stop before the 9th.
        let per = 1024 * 1024;
        let mut jobs: VecDeque<WriteJob> = (0..16).map(|_| job(per)).collect();
        let batch = drain_group(&mut jobs);
        let total: usize = batch.iter().map(|j| j.encoded_len).sum();
        assert!(
            total + 64 <= MAX_GROUP_BYTES,
            "batch bytes {total} within cap"
        );
        assert!(!batch.is_empty());
        assert!(batch.len() < 16);
    }

    #[test]
    fn drain_group_always_takes_at_least_one() {
        // A single job larger than the cap is still taken (bounded elsewhere by
        // the codec's mutation-size limit, well under 8 MiB).
        let mut jobs: VecDeque<WriteJob> = VecDeque::new();
        jobs.push_back(job(MAX_GROUP_BYTES * 2));
        let batch = drain_group(&mut jobs);
        assert_eq!(batch.len(), 1);
        assert!(jobs.is_empty());
    }

    #[test]
    fn drain_group_empty_returns_empty() {
        let mut jobs: VecDeque<WriteJob> = VecDeque::new();
        let batch = drain_group(&mut jobs);
        assert!(batch.is_empty());
    }

    #[test]
    fn bounded_queue_rejects_when_full() {
        let q = WriteQueue::new(2);
        assert!(q.try_push(job(1)).is_ok());
        assert!(q.try_push(job(1)).is_ok());
        // Third push exceeds depth 2.
        assert!(q.try_push(job(1)).is_err());
    }

    #[test]
    fn bounded_queue_rejects_when_closed() {
        let q = WriteQueue::new(8);
        q.close();
        assert!(q.try_push(job(1)).is_err());
    }

    #[test]
    fn stats_render_version_first_and_name_equals_value() {
        let snapshot = StatsSnapshot {
            uptime_seconds: 42,
            keys: 3,
            requests_total: 100,
            reads_total: 70,
            writes_total: 30,
            current_lsn: 30,
            connected_clients: 2,
        };
        let text = render_stats_lines(&snapshot);
        let lines: Vec<&str> = text.lines().collect();
        // version is the FIRST line (§4.1).
        assert_eq!(lines[0], format!("version={PROTOCOL_VERSION}"));
        // Every line is a bounded `name=value` pair (no colons).
        for line in &lines {
            assert!(line.contains('='), "line missing '=': {line}");
            assert!(!line.contains(": "), "line uses colon form: {line}");
        }
        assert!(text.contains("uptime_seconds=42"));
        assert!(text.contains("keys=3"));
        assert!(text.contains("requests_total=100"));
        assert!(text.contains("reads_total=70"));
        assert!(text.contains("writes_total=30"));
        assert!(text.contains("current_lsn=30"));
        assert!(text.contains("connected_clients=2"));
        assert!(text.contains("replicas_connected=0"));
        // Bounded: a small fixed number of lines.
        assert_eq!(lines.len(), 9);
    }
}
