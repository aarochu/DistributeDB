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
//! lock for a group's durable append + `fsync` + map apply, and readers take
//! the read lock for `GET`/`EXISTS`/`STATS`. The linearization point is the
//! map apply after the WAL sync (§5): a reader never observes an un-synced
//! write.
//!
//! # Intentional divergence from §5: fsync under the write lock
//!
//! Technical-Design §5 describes readers running against the prior applied
//! state *while* a batch waits for its WAL sync ("a read may run while a batch
//! waits for WAL sync and will see the prior applied state"), i.e. the
//! sequencer would hold the exclusive lock only around the map apply, doing
//! the append + `fsync` outside it. This implementation instead holds the
//! `Db` write lock across the whole [`Db::apply_group`] call (append + group
//! `fsync` + map apply), so concurrent reads block for the duration of an
//! in-flight group's disk sync rather than proceeding against prior state.
//!
//! This is a deliberate, correctness-preserving simplification, not a bug:
//!
//! * **Correctness is unchanged.** The linearization point is still the map
//!   apply that happens only after a successful `fsync`; a reader either sees
//!   the state before the group or the state after it, never a partially
//!   applied or un-synced group.
//! * **Why not split the phases.** [`Db::apply_group`] performs the durable
//!   append and the map apply behind a single `&mut self`, and the engine is
//!   shared as one `RwLock<Db<F>>`. Splitting append+sync (outside the lock)
//!   from map apply (under it) safely would require the WAL and the in-memory
//!   map to sit behind *separate* locks so the append could proceed while
//!   readers hold the map's read lock. That is a larger structural change with
//!   its own ordering hazards; for a loopback demo server the read-latency
//!   cost of syncing under the lock is acceptable and keeps the exactly-once,
//!   one-LSN-per-mutation, write-ahead guarantees trivially intact.
//!
//! The trade-off is read latency (a `GET` can block for one group's `fsync`),
//! not correctness. If Phase 5 needs read-during-sync concurrency, the fix is
//! to put the WAL behind its own lock and take the map write lock only for the
//! apply step.
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
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::fileio::FileSystem;
use crate::protocol::{
    self, Request, Response, Status, KIND_DELETE, KIND_EXISTS, KIND_GET, KIND_SCAN, KIND_SET,
    KIND_STATS, PROTOCOL_VERSION,
};
use crate::replication::{PeerProgress, ReplicationStats};
use crate::storage::{GetResult, Mutation};
use crate::wal::{Db, DurabilityMode, LsmStats, WalSyncStats, MAX_GROUP_BYTES, MAX_GROUP_RECORDS};

mod latency;
use latency::LatencyHistogram;

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
    /// Optional primary-side replica ACK state included in STATS.
    pub replication_stats: Option<Arc<ReplicationStats>>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            group_wait: DEFAULT_GROUP_WAIT,
            replication_stats: None,
        }
    }
}

/// Server-side operational counters for `STATS` (Technical-Design §17).
///
/// # Counter semantics: dispatched requests, not successful operations
///
/// All three request counters are incremented at **dispatch time**, i.e. once
/// per well-framed request the server decodes, *before* the operation runs and
/// regardless of its outcome. They count *requests dispatched*, not
/// *operations that succeeded*. Concretely:
///
/// * `requests_total` counts every decoded frame (any kind).
/// * `reads_total` counts every `GET`/`EXISTS`/`STATS` dispatched — including
///   the `STATS` request that reads these very counters (a `STATS` call is
///   itself a read, so it is reflected in the snapshot it returns).
/// * `writes_total` counts every `SET`/`DELETE` dispatched — including writes
///   that are later rejected with `RESOURCE_EXHAUSTED` (queue overflow) or
///   `UNAVAILABLE` (WAL durability failure). It is therefore an *attempt*
///   count, not a *committed-write* count; the count of durably committed
///   writes is `current_lsn` (one LSN per applied mutation).
///
/// This "requests dispatched" definition is deliberate and consistent across
/// the three counters: it makes `requests_total` a clean total-load gauge and
/// keeps the read/write counters cheap (no post-outcome accounting on the hot
/// path). Callers that need committed-write throughput read `current_lsn`.
#[derive(Debug, Default)]
struct Metrics {
    requests_total: AtomicU64,
    reads_total: AtomicU64,
    writes_total: AtomicU64,
    connected_clients: AtomicUsize,
    /// Server-side service time of `GET`/`EXISTS`, from a decoded frame to
    /// its encoded response. Network transit is excluded.
    read_latency: LatencyHistogram,
    /// Server-side service time of `SET`/`DELETE`, including sequencer queueing
    /// and the group commit that made the write durable.
    write_latency: LatencyHistogram,
    /// Time a `GET`/`EXISTS` waited to acquire the database read lock: the
    /// direct cost of contention with the sequencer's write lock.
    read_lock_wait: LatencyHistogram,
    /// Time the sequencer held the database write lock for one group commit.
    write_lock_hold: LatencyHistogram,
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
    acceptor: Option<JoinHandle<Vec<ConnectionWorker>>>,
    sequencer: Option<JoinHandle<()>>,
    /// LSM flush and compaction threads, with the signals that stop them.
    maintenance: Vec<(Arc<Signal>, JoinHandle<()>)>,
    queue: Arc<WriteQueue>,
    /// Listener address used to prompt the acceptor during shutdown.
    listener_addr: std::net::SocketAddr,
}

/// Keep a socket handle so shutdown can interrupt a worker blocked in a read
/// or write, then join it before releasing the database's directory lock.
struct ConnectionWorker {
    socket: TcpStream,
    handle: JoinHandle<()>,
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

    /// Stop accepting, drain queued writes, and join the sequencer and all
    /// connection workers before releasing the database. Idempotent.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Prompt the acceptor to observe the stop flag without waiting for its
        // next poll interval.
        let _ = TcpStream::connect(self.listener_addr);
        let workers = self
            .acceptor
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        // With the acceptor stopped, no new writes are queued; close the queue
        // so the sequencer drains the remainder and exits.
        self.queue.close();
        if let Some(handle) = self.sequencer.take() {
            let _ = handle.join();
        }
        // No more groups will be committed. Let maintenance finish the job it
        // is on, if any, and stop. An unflushed memtable stays in the WAL.
        for (signal, _) in &self.maintenance {
            signal.stop();
        }
        for (_, handle) in self.maintenance.drain(..) {
            let _ = handle.join();
        }
        // Once durable writes have drained, disconnect clients to release
        // workers blocked on idle or partial frames (or on socket writes).
        // A lost response after a durable write has an unknown outcome to the
        // client, as specified by the protocol.
        for worker in &workers {
            let _ = worker.socket.shutdown(Shutdown::Both);
        }
        for worker in workers {
            let _ = worker.handle.join();
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
    /// Signal the acceptor to stop promptly.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

/// Shared state handed to each connection worker and the sequencer.
struct Shared<F: FileSystem + Clone + Send + 'static> {
    db: Arc<RwLock<Db<F>>>,
    metrics: Metrics,
    start: Instant,
    queue: Arc<WriteQueue>,
    config: ServerConfig,
    live_connections: AtomicUsize,
    shutdown: Arc<AtomicBool>,
    /// Successful mutations answer `OK_VOLATILE` because the database runs in
    /// benchmark-only `os` durability mode (Technical-Design §6.1).
    volatile: bool,
    /// The database uses the LSM engine; the sequencer wakes the flush and
    /// compaction threads after each group.
    lsm: bool,
    flush_signal: Arc<Signal>,
    compaction_signal: Arc<Signal>,
}

/// Wakes a background maintenance thread. Notifications coalesce: one wake-up
/// covers every notify since the thread last woke.
#[derive(Debug, Default)]
struct Signal {
    state: Mutex<SignalState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct SignalState {
    pending: bool,
    stopped: bool,
}

impl Signal {
    fn notify(&self) {
        self.state.lock().expect("signal poisoned").pending = true;
        self.wake.notify_one();
    }

    fn stop(&self) {
        self.state.lock().expect("signal poisoned").stopped = true;
        self.wake.notify_all();
    }

    fn stopped(&self) -> bool {
        self.state.lock().expect("signal poisoned").stopped
    }

    /// Block until notified; `false` once stopped.
    fn wait(&self) -> bool {
        let mut state = self.state.lock().expect("signal poisoned");
        while !state.pending && !state.stopped {
            state = self.wake.wait(state).expect("signal poisoned");
        }
        state.pending = false;
        !state.stopped
    }
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
        Self::start_shared(addr, Arc::new(RwLock::new(db)), config)
    }

    /// Start a client server with a database also held by the replication
    /// listener. Each subsystem shares the same map/WAL lock and write path.
    pub fn start_shared<A, FS>(
        addr: A,
        db: Arc<RwLock<Db<FS>>>,
        config: ServerConfig,
    ) -> std::io::Result<Server>
    where
        A: std::net::ToSocketAddrs,
        FS: FileSystem + Clone + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let queue = Arc::new(WriteQueue::new(config.max_queue_depth));

        let (mode, lsm) = {
            let db = db.read().expect("db read lock poisoned");
            (db.durability_mode(), db.storage_engine() == "lsm")
        };
        let volatile = mode == DurabilityMode::Os;
        let shared = Arc::new(Shared {
            db,
            metrics: Metrics::default(),
            start: Instant::now(),
            queue: Arc::clone(&queue),
            config: config.clone(),
            live_connections: AtomicUsize::new(0),
            shutdown: Arc::clone(&shutdown),
            volatile,
            lsm,
            flush_signal: Arc::new(Signal::default()),
            compaction_signal: Arc::new(Signal::default()),
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

        let mut maintenance = Vec::new();
        if lsm {
            let flush_shared = Arc::clone(&shared);
            let flush = thread::Builder::new()
                .name("ddb-lsm-flush".to_string())
                .spawn(move || flush_loop(flush_shared))
                .expect("spawn flush thread");
            maintenance.push((Arc::clone(&shared.flush_signal), flush));
            let compaction_shared = Arc::clone(&shared);
            let compaction = thread::Builder::new()
                .name("ddb-lsm-compaction".to_string())
                .spawn(move || compaction_loop(compaction_shared))
                .expect("spawn compaction thread");
            maintenance.push((Arc::clone(&shared.compaction_signal), compaction));
        }

        Ok(Server {
            local_addr,
            shutdown,
            acceptor: Some(acceptor),
            sequencer: Some(sequencer),
            maintenance,
            queue,
            listener_addr: local_addr,
        })
    }
}

/// The acceptor loop: accept connections, enforce the connection cap, and spawn
/// one worker thread per accepted connection (Technical-Design §5). Polling
/// also reaps finished workers promptly so their control sockets do not keep
/// closed client connections alive.
fn acceptor_loop<F>(listener: TcpListener, shared: Arc<Shared<F>>) -> Vec<ConnectionWorker>
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    let mut workers: Vec<ConnectionWorker> = Vec::new();
    loop {
        // Reap disconnected clients during normal operation so the handle
        // registry does not grow with the lifetime connection count.
        let mut index = 0;
        while index < workers.len() {
            if workers[index].handle.is_finished() {
                let worker: ConnectionWorker = workers.swap_remove(index);
                let _ = worker.handle.join();
            } else {
                index += 1;
            }
        }
        if shared.shutdown.load(Ordering::SeqCst) {
            break;
        }
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(_) => {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        // Accepted sockets inherit the listener's non-blocking mode on macOS
        // and Windows (not on Linux); workers rely on blocking reads.
        if stream.set_nonblocking(false).is_err() {
            continue;
        }

        // Enforce the connection cap: the acceptor rejects the excess (§5).
        let live = shared.live_connections.fetch_add(1, Ordering::SeqCst);
        if live >= shared.config.max_connections {
            shared.live_connections.fetch_sub(1, Ordering::SeqCst);
            reject_overflow(stream);
            continue;
        }

        let socket = match stream.try_clone() {
            Ok(socket) => socket,
            Err(_) => {
                shared.live_connections.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
        };

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
        match spawned {
            Ok(handle) => workers.push(ConnectionWorker { socket, handle }),
            Err(_) => {
                // Could not spawn: release the reserved slot.
                shared.live_connections.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    workers
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
                let started = Instant::now();
                let (response, keep_open) = dispatch(&body, shared);
                match response.kind {
                    KIND_GET | KIND_EXISTS | KIND_SCAN => {
                        shared.metrics.read_latency.record(started.elapsed())
                    }
                    KIND_SET | KIND_DELETE => {
                        shared.metrics.write_latency.record(started.elapsed())
                    }
                    _ => {}
                }
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
            let waiting = Instant::now();
            let db = shared.db.read().expect("db read lock poisoned");
            shared.metrics.read_lock_wait.record(waiting.elapsed());
            match db.try_get(&key) {
                Ok(GetResult::Found(value)) => (Response::new(KIND_GET, Status::Ok, value), true),
                Ok(GetResult::NotFound) => {
                    (Response::new(KIND_GET, Status::NotFound, Vec::new()), true)
                }
                Err(error) => (read_failure(KIND_GET, &error), true),
            }
        }
        Request::Exists { key } => {
            shared.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
            let waiting = Instant::now();
            let db = shared.db.read().expect("db read lock poisoned");
            shared.metrics.read_lock_wait.record(waiting.elapsed());
            match db.try_exists(&key) {
                Ok(exists) => (
                    Response::new(KIND_EXISTS, Status::Ok, vec![u8::from(exists)]),
                    true,
                ),
                Err(error) => (read_failure(KIND_EXISTS, &error), true),
            }
        }
        Request::Scan { start, end, limit } => {
            shared.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
            let waiting = Instant::now();
            let db = shared.db.read().expect("db read lock poisoned");
            shared.metrics.read_lock_wait.record(waiting.elapsed());
            match db.scan(&start, end.as_deref(), limit as usize) {
                Ok((mut pairs, mut more)) => {
                    // Cut the page where it would exceed one response frame;
                    // the client continues after the last key it received.
                    let mut len = 5;
                    if let Some(fit) = pairs.iter().position(|(key, value)| {
                        len += protocol::scan_pair_len(key, value);
                        len > protocol::MAX_SCAN_DATA_LEN
                    }) {
                        pairs.truncate(fit);
                        more = true;
                    }
                    let data = protocol::encode_scan_page(&pairs, more);
                    (Response::new(KIND_SCAN, Status::Ok, data), true)
                }
                Err(error) => (read_failure(KIND_SCAN, &error), true),
            }
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

/// A read the storage engine could not complete, such as an LSM table that
/// failed its checksum. The client sees `UNAVAILABLE`; the node keeps serving
/// other keys.
fn read_failure(kind: u8, error: &crate::wal::WalError) -> Response {
    Response::new(kind, Status::Unavailable, error.to_string().into_bytes())
}

/// Map a durable-write [`WalError`] to the client-facing [`Status`]
/// (Technical-Design §6.2).
///
/// §6.2 specifies that disk-full / sync / append failures are surfaced as
/// `UNAVAILABLE` for the current request, and that subsequent mutations are
/// rejected until operator recovery. After the first such failure the WAL is
/// fail-closed and returns [`WalError::FailClosed`] for every later write, so
/// that maps to `UNAVAILABLE` as well: the node has become read-only and the
/// client should treat writes as unavailable, not as an internal bug.
///
/// * [`WalError::Io`], [`WalError::FailClosed`], [`WalError::Corruption`] —
///   durability failures and the fail-closed state that follows them: mapped
///   to `UNAVAILABLE` (§6.2).
/// * [`WalError::MutationTooLarge`] — the request itself is too big; this is a
///   client error, `BAD_REQUEST`. (The codec bounds mutation size before it
///   reaches here, so this is defensive.)
/// * [`WalError::ResourceExhausted`] — retained recovery data would exceed the
///   configured disk budget, so new writes are paused: `RESOURCE_EXHAUSTED`
///   (Technical-Design §7).
/// * [`WalError::ReadOnlyReplica`] — a client write reached a replica:
///   `NOT_PRIMARY`.
/// * [`WalError::Format`] / [`WalError::Identity`] — genuinely unexpected
///   internal faults: `INTERNAL_ERROR`.
fn wal_error_to_status(err: &crate::wal::WalError) -> Status {
    use crate::wal::WalError;
    match err {
        WalError::Io(_) | WalError::FailClosed | WalError::Corruption(_) => Status::Unavailable,
        WalError::ReadOnlyReplica => Status::NotPrimary,
        WalError::MutationTooLarge { .. } => Status::BadRequest,
        WalError::ResourceExhausted { .. } => Status::ResourceExhausted,
        WalError::Format(_) | WalError::Identity(_) | WalError::GroupInProgress => {
            Status::InternalError
        }
    }
}

/// Submit a write to the sequencer and block (without holding the map lock) on
/// its one-shot result (Technical-Design §5). Maps queue overflow to
/// `RESOURCE_EXHAUSTED` and a WAL durability failure to `UNAVAILABLE` (§6.2,
/// via [`wal_error_to_status`]).
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
        Ok(Ok(_lsn)) => {
            let status = if shared.volatile {
                Status::OkVolatile
            } else {
                Status::Ok
            };
            Response::new(kind, status, Vec::new())
        }
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
    // Whether the last group left the LSM memtable at its stall size. Read
    // under the write lock the group already holds, so the common case costs
    // no extra lock acquisition.
    let mut stalled = false;
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

        // Back-pressure: while the memtable is far past its flush size and
        // the previous one is still being written, wait for that flush.
        if stalled {
            while shared
                .db
                .read()
                .expect("db read lock poisoned")
                .lsm_write_stall()
            {
                thread::sleep(Duration::from_millis(1));
            }
        }

        // Apply the whole batch as one durable group commit (one fsync).
        let muts: Vec<Mutation> = batch.iter().map(|j| j.mutation.clone()).collect();
        // Append under the write lock, sync with no database lock held, then
        // apply under the write lock. Readers keep using the pre-group map
        // during the sync, which is correct because none of these writes has
        // been acknowledged yet (Phase 7 lock-scope change).
        let mut flush_due = false;
        let (begun, first_hold) = {
            let mut db = shared.db.write().expect("db write lock poisoned");
            let held = Instant::now();
            (db.begin_group(&muts), held.elapsed())
        };
        let result = match begun {
            Err(e) => {
                shared.metrics.write_lock_hold.record(first_hold);
                Err(e)
            }
            Ok(pending) => {
                let synced = pending.sync();
                let mut db = shared.db.write().expect("db write lock poisoned");
                let held = Instant::now();
                let result = db.finish_group(pending, &muts, synced);
                if shared.lsm {
                    flush_due = db.lsm_flush_due();
                    stalled = db.lsm_write_stall();
                }
                shared
                    .metrics
                    .write_lock_hold
                    .record(first_hold + held.elapsed());
                result
            }
        };

        match result {
            Ok(lsns) => {
                for (job, lsn) in batch.into_iter().zip(lsns) {
                    let _ = job.respond.send(Ok(lsn));
                }
            }
            Err(e) => {
                // The whole group failed to commit durably; no mutation was
                // applied (apply_group leaves the engine untouched on error).
                // Surface the §6.2 status to every waiter: a durability failure
                // (and the fail-closed state that follows it) is UNAVAILABLE,
                // not INTERNAL_ERROR.
                let status = wal_error_to_status(&e);
                for job in batch {
                    let _ = job.respond.send(Err(status));
                }
            }
        }
        // Wake the flush thread only when a flush is due, so it does not
        // take the write lock after every group.
        if flush_due {
            shared.flush_signal.notify();
        }
    }
}

/// Background LSM flushes. Freezing and installing take the write lock;
/// writing the table does not, so the sequencer keeps committing groups and
/// reads continue. A failure makes the node fail closed (see
/// [`Db::finish_lsm_flush`]), which clients see as `UNAVAILABLE` on their
/// next write.
fn flush_loop<F>(shared: Arc<Shared<F>>)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    while shared.flush_signal.wait() {
        while !shared.flush_signal.stopped() {
            let begun = shared
                .db
                .write()
                .expect("db write lock poisoned")
                .begin_lsm_flush();
            match begun {
                Ok(Some(flush)) => {
                    let written = flush.write();
                    // An error is recorded as the fail-closed state.
                    let _ = shared
                        .db
                        .write()
                        .expect("db write lock poisoned")
                        .finish_lsm_flush(written);
                    shared.compaction_signal.notify();
                }
                // Not due, or deferred because a group was between append and
                // apply: retry shortly only in the second case.
                Ok(None) => {
                    let due = shared
                        .db
                        .read()
                        .expect("db read lock poisoned")
                        .lsm_flush_due();
                    if !due {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }
    }
}

/// Background LSM compactions, run beside flushes so a long merge does not
/// delay the next flush.
fn compaction_loop<F>(shared: Arc<Shared<F>>)
where
    F: FileSystem + Clone + Send + Sync + 'static,
{
    while shared.compaction_signal.wait() {
        while !shared.compaction_signal.stopped() {
            let planned = shared
                .db
                .write()
                .expect("db write lock poisoned")
                .begin_lsm_compaction();
            let Some(compaction) = planned else {
                break;
            };
            let compacted = compaction.write();
            let _ = shared
                .db
                .write()
                .expect("db write lock poisoned")
                .finish_lsm_compaction(compacted);
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
    let (
        keys,
        current_lsn,
        durable_lsn,
        snapshot_lsn,
        role,
        durability,
        wal_sync,
        recovery,
        storage,
    ) = {
        let db = shared.db.read().expect("db read lock poisoned");
        let durability = match db.durability_mode() {
            DurabilityMode::Fsync => "fsync",
            DurabilityMode::Os => "os",
        };
        (
            db.approximate_len(),
            db.last_applied_lsn(),
            db.last_durable_lsn(),
            db.snapshot_lsn(),
            db.identity().role.clone(),
            durability,
            db.wal_sync_stats(),
            (db.recovery_duration(), db.records_replayed()),
            (db.storage_engine(), db.lsm_stats()),
        )
    };
    let percentiles = |histogram: &LatencyHistogram| {
        [500, 950, 990].map(|per_mille| histogram.percentile_nanos(per_mille))
    };
    let replicas = shared
        .config
        .replication_stats
        .as_ref()
        .map(|stats| stats.snapshot())
        .unwrap_or_default();
    let snapshot = StatsSnapshot {
        uptime_seconds: shared.start.elapsed().as_secs(),
        keys,
        requests_total: shared.metrics.requests_total.load(Ordering::Relaxed),
        reads_total: shared.metrics.reads_total.load(Ordering::Relaxed),
        writes_total: shared.metrics.writes_total.load(Ordering::Relaxed),
        current_lsn,
        durable_lsn,
        snapshot_lsn,
        connected_clients: shared.metrics.connected_clients.load(Ordering::Relaxed),
        replicas,
        role,
        durability,
        read_latency: percentiles(&shared.metrics.read_latency),
        write_latency: percentiles(&shared.metrics.write_latency),
        read_lock_wait: percentiles(&shared.metrics.read_lock_wait),
        write_lock_hold: percentiles(&shared.metrics.write_lock_hold),
        wal_sync,
        recovery_duration: recovery.0,
        records_replayed: recovery.1,
        storage_engine: storage.0,
        lsm: storage.1,
    };
    render_stats_lines(&snapshot)
}

/// A point-in-time snapshot of the metrics rendered into `STATS` output.
///
/// Split out so the `name=value` rendering is a pure function testable without
/// a live server (Technical-Design §17).
struct StatsSnapshot {
    uptime_seconds: u64,
    /// Exact for the in-memory engine; an upper bound for the LSM engine.
    keys: u64,
    requests_total: u64,
    reads_total: u64,
    writes_total: u64,
    current_lsn: u64,
    durable_lsn: u64,
    snapshot_lsn: u64,
    connected_clients: usize,
    replicas: Vec<([u8; 16], PeerProgress)>,
    role: String,
    durability: &'static str,
    /// p50/p95/p99 in nanoseconds; `None` before the first sample.
    read_latency: [Option<u64>; 3],
    write_latency: [Option<u64>; 3],
    read_lock_wait: [Option<u64>; 3],
    write_lock_hold: [Option<u64>; 3],
    wal_sync: WalSyncStats,
    recovery_duration: Duration,
    records_replayed: u64,
    storage_engine: &'static str,
    lsm: Option<LsmStats>,
}

/// Render nanoseconds as whole microseconds, rounded up so a nonzero latency
/// never reads as zero; `unknown` before the first sample.
fn micros(nanos: Option<u64>) -> String {
    match nanos {
        Some(nanos) => nanos.div_ceil(1000).to_string(),
        None => "unknown".to_string(),
    }
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
    out.push_str(&format!("durable_lsn={}\n", s.durable_lsn));
    out.push_str(&format!("snapshot_lsn={}\n", s.snapshot_lsn));
    out.push_str(&format!("connected_clients={}\n", s.connected_clients));
    out.push_str(&format!("role={}\n", s.role));
    out.push_str(&format!("durability={}\n", s.durability));
    out.push_str(&format!("storage_engine={}\n", s.storage_engine));
    if let Some(lsm) = &s.lsm {
        out.push_str(&format!("lsm_memtable_bytes={}\n", lsm.memtable_bytes));
        out.push_str(&format!("lsm_tables={}\n", lsm.tables));
        out.push_str(&format!("lsm_level0_tables={}\n", lsm.level_zero_tables));
        out.push_str(&format!("lsm_table_bytes={}\n", lsm.table_bytes));
        out.push_str(&format!("lsm_flushes_total={}\n", lsm.flushes));
        out.push_str(&format!("lsm_compactions_total={}\n", lsm.compactions));
    }
    // Records recovery would replay after the snapshot boundary.
    let wal_entries = s.current_lsn.saturating_sub(s.snapshot_lsn);
    out.push_str(&format!("wal_entries={wal_entries}\n"));
    let histograms = [
        ("read_latency", s.read_latency),
        ("write_latency", s.write_latency),
        ("read_lock_wait", s.read_lock_wait),
        ("write_lock_hold", s.write_lock_hold),
    ];
    for (class, values) in histograms {
        for (name, value) in ["p50", "p95", "p99"].into_iter().zip(values) {
            out.push_str(&format!("{class}_{name}_us={}\n", micros(value)));
        }
    }
    let sync = &s.wal_sync;
    out.push_str(&format!("wal_syncs_total={}\n", sync.syncs));
    out.push_str(&format!("wal_sync_errors_total={}\n", sync.errors));
    let syncs = u128::from(sync.syncs);
    let average = sync.total.as_nanos().checked_div(syncs);
    let average = micros(average.map(|nanos| nanos as u64));
    out.push_str(&format!("wal_sync_avg_us={average}\n"));
    let max = (sync.syncs > 0).then_some(sync.max.as_nanos() as u64);
    out.push_str(&format!("wal_sync_max_us={}\n", micros(max)));
    let recovery_us = micros(Some(s.recovery_duration.as_nanos() as u64));
    out.push_str(&format!("recovery_us={recovery_us}\n"));
    let replayed = s.records_replayed;
    out.push_str(&format!("recovery_records_replayed={replayed}\n"));
    let connected = s.replicas.iter().filter(|(_, peer)| peer.connected).count();
    out.push_str(&format!("replicas_connected={connected}\n"));
    for (id, peer) in &s.replicas {
        let hex = crate::replication::format_id(id);
        out.push_str(&format!("replica_{hex}_applied_lsn={}\n", peer.applied_lsn));
        if peer.connected && peer.applied_lsn <= s.durable_lsn {
            out.push_str(&format!(
                "replica_{hex}_lag={}\n",
                s.durable_lsn - peer.applied_lsn
            ));
        } else {
            out.push_str(&format!("replica_{hex}_lag=unknown\n"));
        }
    }
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
            durable_lsn: 30,
            snapshot_lsn: 20,
            connected_clients: 2,
            replicas: Vec::new(),
            role: "primary".into(),
            durability: "fsync",
            read_latency: [Some(1_500), Some(9_000), None],
            write_latency: [None; 3],
            read_lock_wait: [Some(200), Some(2_000), Some(20_000)],
            write_lock_hold: [None; 3],
            wal_sync: WalSyncStats {
                syncs: 4,
                errors: 0,
                total: Duration::from_micros(400),
                max: Duration::from_micros(250),
            },
            recovery_duration: Duration::from_millis(12),
            records_replayed: 10,
            storage_engine: "memory",
            lsm: None,
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
        assert!(text.contains("snapshot_lsn=20"));
        assert!(text.contains("connected_clients=2"));
        assert!(text.contains("replicas_connected=0"));
        for expected in [
            "role=primary",
            "durability=fsync",
            "storage_engine=memory",
            "wal_entries=10",
            "read_latency_p50_us=2",
            "read_latency_p95_us=9",
            "read_latency_p99_us=unknown",
            "write_latency_p50_us=unknown",
            "read_lock_wait_p50_us=1",
            "read_lock_wait_p99_us=20",
            "write_lock_hold_p99_us=unknown",
            "wal_syncs_total=4",
            "wal_sync_errors_total=0",
            "wal_sync_avg_us=100",
            "wal_sync_max_us=250",
            "recovery_us=12000",
            "recovery_records_replayed=10",
        ] {
            assert!(lines.contains(&expected), "missing {expected}");
        }
        // Bounded: a small fixed number of lines.
        assert_eq!(lines.len(), 33);
    }
}
