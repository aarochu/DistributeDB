//! Socket-level integration tests for the Phase 3 TCP server + client
//! (SOW §5, §13, §20; Technical-Design §4, §4.2, §5).
//!
//! These tests drive the real `distributedb` server over loopback TCP:
//! * single-client happy path via the typed [`Client`],
//! * many concurrent clients on their own connections,
//! * malformed / oversized / unknown-version / partial-message framing via
//!   raw [`TcpStream`] writes (bypassing the typed client),
//! * client disconnect mid-request,
//! * graceful shutdown, and
//! * durability-through-server (write over TCP, restart the `Db`, recover).
//!
//! Every server binds `127.0.0.1:0` (OS-assigned port, read via
//! [`Server::local_addr`]) and is shut down + joined before the test returns
//! so no threads leak. Any on-disk data lives under a unique temp directory
//! that is removed at the end of the test.

use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use distributedb::protocol::MAX_BODY_LEN;
use distributedb::{
    decode_response_body, read_frame, Client, Db, DurabilityMode, GetResult, RealFs, Request,
    Response, Server, ServerConfig, SimConfig, SimFs, Status, PROTOCOL_VERSION,
};

/// A process-unique counter so concurrent tests never share a temp directory.
static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// A unique temp data directory for a RealFs-backed server; removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "distributedb-net-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        // Start from a clean slate in case a previous run left something.
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp data dir");
        TempDir { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Start a server on `127.0.0.1:0` backed by a deterministic in-memory
/// [`SimFs`] (no disk touched) with the given config.
fn start_sim_server(config: ServerConfig) -> Server {
    start_sim_server_with_fs(config).0
}

/// Like [`start_sim_server`] but also returns a handle to the backing
/// [`SimFs`], so a test can arm fault injection after the `Db` has opened
/// cleanly (e.g. to force a WAL durability failure on the first write).
fn start_sim_server_with_fs(config: ServerConfig) -> (Server, SimFs) {
    let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
    let fs = SimFs::new(SimConfig::new(0x5eed_0000 ^ n));
    let db = Db::open(fs.clone(), Path::new("/ddb"), DurabilityMode::Fsync).expect("open sim Db");
    let server = Server::start("127.0.0.1:0", db, config).expect("start server");
    (server, fs)
}

/// Connect a raw stream to the server with sensible read/write timeouts so a
/// misbehaving assertion can never hang the whole test suite.
fn raw_connect(server: &Server) -> TcpStream {
    let stream = TcpStream::connect(server.local_addr()).expect("connect raw stream");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set write timeout");
    stream.set_nodelay(true).ok();
    stream
}

/// Read exactly one response frame from a raw stream.
fn read_one_response(stream: &mut TcpStream) -> Response {
    let body = read_frame(stream)
        .expect("read frame")
        .expect("a response frame (not a clean EOF)");
    decode_response_body(&body).expect("decode response body")
}

// ---------------------------------------------------------------------------
// 1. Single-client happy path over TCP.
// ---------------------------------------------------------------------------

#[test]
fn single_client_happy_path() {
    let mut server = start_sim_server(ServerConfig::default());
    let mut client = Client::connect(server.local_addr()).expect("connect client");

    // SET user:123 Aaron.
    assert_eq!(
        client
            .set(b"user:123".to_vec(), b"Aaron".to_vec())
            .expect("set"),
        Status::Ok
    );

    // GET returns the value.
    assert_eq!(
        client.get(b"user:123".to_vec()).expect("get"),
        Some(b"Aaron".to_vec())
    );

    // EXISTS is true.
    assert!(client.exists(b"user:123".to_vec()).expect("exists"));

    // DELETE then GET returns NOT_FOUND (None).
    assert_eq!(
        client.delete(b"user:123".to_vec()).expect("delete"),
        Status::Ok
    );
    assert_eq!(client.get(b"user:123".to_vec()).expect("get deleted"), None);

    // GET of a never-set key returns NOT_FOUND (None).
    assert_eq!(client.get(b"missing".to_vec()).expect("get missing"), None);

    // Empty-value SET then GET returns Found(empty), which is distinct from
    // NOT_FOUND (None): Some(empty) vs None.
    assert_eq!(
        client
            .set(b"empty".to_vec(), Vec::new())
            .expect("set empty"),
        Status::Ok
    );
    assert_eq!(
        client.get(b"empty".to_vec()).expect("get empty"),
        Some(Vec::new())
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 2. Many concurrent clients.
// ---------------------------------------------------------------------------

#[test]
fn many_concurrent_clients() {
    let mut server = start_sim_server(ServerConfig::default());
    let addr = server.local_addr();

    const CLIENTS: u64 = 24;
    const PER_CLIENT: u64 = 5;

    let handles: Vec<_> = (0..CLIENTS)
        .map(|c| {
            thread::spawn(move || {
                let mut client = Client::connect(addr).expect("connect concurrent client");
                for i in 0..PER_CLIENT {
                    let key = format!("c{c}:k{i}").into_bytes();
                    let value = format!("v{c}-{i}").into_bytes();
                    assert_eq!(
                        client.set(key.clone(), value.clone()).expect("set"),
                        Status::Ok
                    );
                    // Interleave a read of the just-written key on the same
                    // connection.
                    assert_eq!(client.get(key).expect("get own key"), Some(value));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("join concurrent client");
    }

    // Every key is readable with its expected value from a fresh connection.
    let mut verifier = Client::connect(addr).expect("connect verifier");
    for c in 0..CLIENTS {
        for i in 0..PER_CLIENT {
            let key = format!("c{c}:k{i}").into_bytes();
            let value = format!("v{c}-{i}").into_bytes();
            assert_eq!(verifier.get(key).expect("verify get"), Some(value));
        }
    }

    // STATS current_lsn advanced by exactly the number of writes.
    let total_writes = CLIENTS * PER_CLIENT;
    let stats = verifier.stats().expect("stats");
    let current_lsn = parse_stat(&stats, "current_lsn");
    assert_eq!(
        current_lsn, total_writes,
        "current_lsn should equal number of writes\nstats:\n{stats}"
    );

    server.shutdown();
}

/// Parse a `name=value` u64 line out of a STATS payload.
fn parse_stat(stats: &str, name: &str) -> u64 {
    for line in stats.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{name}=")) {
            return rest.parse().expect("numeric stat value");
        }
    }
    panic!("stat {name} not found in:\n{stats}");
}

// ---------------------------------------------------------------------------
// 3. Malformed frame handling.
// ---------------------------------------------------------------------------

#[test]
fn malformed_frame_gets_bad_request_and_server_stays_usable() {
    let mut server = start_sim_server(ServerConfig::default());

    // A syntactically complete frame with an UNKNOWN request kind (7). The
    // frame is well-formed at the framing layer (valid length prefix, valid
    // version byte) but the body is semantically invalid.
    let mut stream = raw_connect(&server);
    let body: Vec<u8> = vec![PROTOCOL_VERSION, 7]; // version=1, kind=7 (unknown)
    let mut frame = (body.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&body);
    stream.write_all(&frame).expect("write malformed frame");
    stream.flush().expect("flush");

    let resp = read_one_response(&mut stream);
    assert_eq!(
        resp.status,
        Status::BadRequest,
        "unknown kind -> BAD_REQUEST"
    );
    // A decode error on a well-framed body (unknown kind) is answered with
    // BAD_REQUEST and the connection stays OPEN (only a bad version or a
    // framing-level error closes it, §4.1). Prove the same connection is still
    // usable for a subsequent valid request.
    let follow_up = Request::Set {
        key: b"still".to_vec(),
        value: b"alive".to_vec(),
    }
    .encode();
    stream.write_all(&follow_up).expect("write valid follow-up");
    stream.flush().expect("flush follow-up");
    let resp = read_one_response(&mut stream);
    assert_eq!(
        resp.status,
        Status::Ok,
        "connection stays usable after a BAD_REQUEST on a well-framed body"
    );
    drop(stream);

    // The server also remains usable for a fresh, valid connection.
    let mut client = Client::connect(server.local_addr()).expect("fresh client");
    assert_eq!(
        client.get(b"still".to_vec()).expect("get"),
        Some(b"alive".to_vec())
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 4. Oversized frame.
// ---------------------------------------------------------------------------

#[test]
fn oversized_frame_is_rejected_without_hanging() {
    let mut server = start_sim_server(ServerConfig::default());

    // Declare a body length just over the MAX_BODY_LEN cap. The server must
    // reject it BEFORE allocating a body buffer (§4.1, §3.9) and must not
    // block waiting for a body we will never send.
    let mut stream = raw_connect(&server);
    let bogus_len = (MAX_BODY_LEN as u32) + 1;
    stream
        .write_all(&bogus_len.to_le_bytes())
        .expect("write oversized length prefix");
    stream.flush().expect("flush");

    // The server responds BAD_REQUEST (oversized maps to BadRequest) then
    // closes; it does not hang waiting for the never-sent body.
    let resp = read_one_response(&mut stream);
    assert_eq!(resp.status, Status::BadRequest, "oversized -> BAD_REQUEST");
    assert!(
        read_frame(&mut stream).expect("read after close").is_none(),
        "server should close after rejecting an oversized frame"
    );
    drop(stream);

    // The server stays up for other clients.
    let mut client = Client::connect(server.local_addr()).expect("fresh client");
    assert_eq!(
        client
            .set(b"ok".to_vec(), b"still-up".to_vec())
            .expect("set"),
        Status::Ok
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 5. Unknown version.
// ---------------------------------------------------------------------------

#[test]
fn unknown_version_gets_unsupported_version_then_close() {
    let mut server = start_sim_server(ServerConfig::default());

    // A well-formed frame whose version byte is 2 (not PROTOCOL_VERSION=1).
    // Body: version=2, kind=STATS(5) -> a valid shape apart from the version.
    let mut stream = raw_connect(&server);
    let body: Vec<u8> = vec![2, 5];
    let mut frame = (body.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&body);
    stream.write_all(&frame).expect("write bad-version frame");
    stream.flush().expect("flush");

    let resp = read_one_response(&mut stream);
    assert_eq!(
        resp.status,
        Status::UnsupportedVersion,
        "version 2 -> UNSUPPORTED_VERSION"
    );
    // The server closes the connection after an unsupported version (§4.1).
    assert!(
        read_frame(&mut stream).expect("read after close").is_none(),
        "server should close the connection after UNSUPPORTED_VERSION"
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 6. Partial message / TCP has no boundaries.
// ---------------------------------------------------------------------------

#[test]
fn partial_message_is_reassembled() {
    let mut server = start_sim_server(ServerConfig::default());

    // Encode a valid SET request, then write it in several separate TCP writes
    // with pauses between them: the 4 length bytes, then the body in two
    // chunks. The server's read_frame loop must reassemble it.
    let frame = Request::Set {
        key: b"partial".to_vec(),
        value: b"assembled-across-writes".to_vec(),
    }
    .encode();

    let mut stream = raw_connect(&server);
    // Length prefix first.
    stream.write_all(&frame[0..4]).expect("write length prefix");
    stream.flush().expect("flush len");
    thread::sleep(Duration::from_millis(20));
    // Body split across two writes.
    let mid = 4 + (frame.len() - 4) / 2;
    stream.write_all(&frame[4..mid]).expect("write body part 1");
    stream.flush().expect("flush body 1");
    thread::sleep(Duration::from_millis(20));
    stream.write_all(&frame[mid..]).expect("write body part 2");
    stream.flush().expect("flush body 2");

    let resp = read_one_response(&mut stream);
    assert_eq!(resp.status, Status::Ok, "reassembled SET should succeed");
    drop(stream);

    // Two requests back-to-back in one write on a single connection are
    // answered in order (one outstanding at a time).
    let mut stream = raw_connect(&server);
    let mut two = Request::Get {
        key: b"partial".to_vec(),
    }
    .encode();
    two.extend_from_slice(
        &Request::Exists {
            key: b"partial".to_vec(),
        }
        .encode(),
    );
    stream.write_all(&two).expect("write two frames");
    stream.flush().expect("flush two");

    let first = read_one_response(&mut stream);
    assert_eq!(first.status, Status::Ok);
    assert_eq!(first.data, b"assembled-across-writes".to_vec());
    let second = read_one_response(&mut stream);
    assert_eq!(second.status, Status::Ok);
    assert_eq!(second.data, vec![1u8], "EXISTS should report present");

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 7. Client disconnect mid-request.
// ---------------------------------------------------------------------------

#[test]
fn client_disconnect_mid_request_is_handled_gracefully() {
    let mut server = start_sim_server(ServerConfig::default());

    // Open a connection, send only a partial frame (a length prefix promising
    // a body, but not the body), then drop the socket. The server's connection
    // worker must handle the truncated read without panicking.
    //
    // Per Technical-Design §4.2, a disconnect before the OK response leaves the
    // mutation outcome UNKNOWN to the client: there is no exactly-once /
    // de-duplication guarantee, and a retry may apply the mutation twice. This
    // test asserts only server STABILITY, not any rollback of in-flight work.
    {
        let mut stream = raw_connect(&server);
        let promised_body_len: u32 = 32;
        stream
            .write_all(&promised_body_len.to_le_bytes())
            .expect("write partial length prefix");
        // Write two body bytes, far fewer than promised, then drop.
        stream
            .write_all(&[PROTOCOL_VERSION, 1])
            .expect("write partial body");
        stream.flush().expect("flush partial");
        // Dropping `stream` here closes the socket mid-request.
    }

    // Also do the most abrupt case: only two of the four length bytes.
    {
        let mut stream = raw_connect(&server);
        stream
            .write_all(&[0x08, 0x00])
            .expect("write 2 length bytes");
        stream.flush().expect("flush");
    }

    // Give the server a moment to observe the disconnects.
    thread::sleep(Duration::from_millis(50));

    // The server keeps serving other clients.
    let mut client = Client::connect(server.local_addr()).expect("fresh client after disconnects");
    assert_eq!(
        client
            .set(b"after".to_vec(), b"disconnect".to_vec())
            .expect("set after disconnect"),
        Status::Ok
    );
    assert_eq!(
        client.get(b"after".to_vec()).expect("get after disconnect"),
        Some(b"disconnect".to_vec())
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 8. Graceful shutdown.
// ---------------------------------------------------------------------------

#[test]
fn graceful_shutdown_stops_accepting_and_joins_cleanly() {
    let mut server = start_sim_server(ServerConfig::default());
    let addr = server.local_addr();

    // A live client that issues a request before shutdown.
    let mut client = Client::connect(addr).expect("connect client");
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).expect("set"),
        Status::Ok
    );

    // Trigger shutdown from another thread via the shutdown handle, then join
    // the server threads by calling shutdown() (idempotent) on this thread.
    let handle = server.shutdown_handle();
    let t = thread::spawn(move || handle.shutdown());
    t.join().expect("join shutdown trigger");

    // shutdown() stops accepting and joins the acceptor + sequencer threads
    // without panicking. If a thread had panicked, join() inside shutdown()
    // would have surfaced it; reaching here means a clean join.
    server.shutdown();

    // After shutdown the server no longer accepts new connections. Connecting
    // may still succeed at the TCP layer briefly, but a request must not get a
    // normal reply: either connect fails or the request errors / sees EOF.
    thread::sleep(Duration::from_millis(50));
    match Client::connect(addr) {
        Err(_) => { /* refused: expected once the listener is gone */ }
        Ok(mut c) => {
            // The acceptor is stopped, so no worker will service this: the
            // request must not return Ok.
            assert!(
                c.get(b"k".to_vec()).is_err(),
                "server should not service requests after shutdown"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 9. Durability through the server (write via TCP -> restart -> recover).
// ---------------------------------------------------------------------------

#[test]
fn durability_through_the_server_survives_restart() {
    let temp = TempDir::new("durable");
    let keys: [(&[u8], &[u8]); 4] = [
        (b"alpha", b"one"),
        (b"beta", b"two"),
        (b"gamma", b"three"),
        (b"delta", b"four"),
    ];

    // Phase 1: start a RealFs-backed server on a unique temp dir, write keys
    // over TCP with Fsync durability, and confirm OK.
    {
        let db =
            Db::open(RealFs::new(), temp.path(), DurabilityMode::Fsync).expect("open durable Db");
        let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default())
            .expect("start durable server");
        let mut client = Client::connect(server.local_addr()).expect("connect durable client");
        for (k, v) in keys {
            assert_eq!(
                client.set(k.to_vec(), v.to_vec()).expect("durable set"),
                Status::Ok,
                "SET over TCP should report OK (durable)"
            );
        }
        // Drop the client first so its connection worker sees EOF and exits,
        // releasing its Arc<Shared>. Then shut the server down (joins the
        // acceptor + sequencer) and drop it: dropping the Server releases the
        // last Arc to the shared Db, which drops the Db and releases the LOCK
        // so we can reopen the same data directory below.
        drop(client);
        server.shutdown();
        drop(server);
    }

    // Phase 2: reopen a Db on the SAME data directory and assert every key
    // recovered with the correct value, proving the writes went through the
    // Phase 2 WAL group-commit durable path and survive restart. A lingering
    // connection worker may hold its Arc<Shared> (and thus the Db LOCK) for a
    // brief moment after shutdown; retry the open until the LOCK is released.
    {
        let db = reopen_with_retry(temp.path());
        for (k, v) in keys {
            assert_eq!(
                db.get(k),
                GetResult::Found(v.to_vec()),
                "key should survive server restart"
            );
        }
        assert_eq!(
            db.last_applied_lsn(),
            keys.len() as u64,
            "one LSN per durable write"
        );
    }

    // `temp` is dropped here, removing the temp directory (test hygiene).
}

/// Reopen a RealFs-backed [`Db`] on `path`, retrying briefly while a lingering
/// connection worker still holds the data-directory LOCK after shutdown.
fn reopen_with_retry(path: &Path) -> Db<RealFs> {
    let mut last_err = None;
    for _ in 0..100 {
        match Db::open(RealFs::new(), path, DurabilityMode::Fsync) {
            Ok(db) => return db,
            Err(e) => {
                last_err = Some(e);
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    panic!("reopen durable Db never succeeded: {last_err:?}");
}

// ---------------------------------------------------------------------------
// 10. WAL durability failure -> UNAVAILABLE, then the node stays fail-closed.
// ---------------------------------------------------------------------------

/// A write whose WAL sync fails must be reported to the client as `UNAVAILABLE`
/// (Technical-Design §6.2), not `INTERNAL_ERROR`. After the first such failure
/// the WAL is fail-closed, so every later write is rejected too -- also as
/// `UNAVAILABLE`, since the node has become read-only rather than hit an
/// internal bug. Reads of state committed before the failure keep working.
#[test]
fn wal_sync_failure_is_unavailable_and_node_stays_fail_closed() {
    let (mut server, fs) = start_sim_server_with_fs(ServerConfig::default());
    let mut client = Client::connect(server.local_addr()).expect("connect client");

    // A clean write before any fault is armed commits durably.
    assert_eq!(
        client
            .set(b"before".to_vec(), b"ok".to_vec())
            .expect("clean set"),
        Status::Ok
    );

    // Arm deterministic sync failures: every subsequent WAL fsync fails.
    fs.arm_sync_failures(1000);

    // The next write's group fsync fails. Per §6.2 the client sees UNAVAILABLE
    // (mapped from WalError::Io), NOT INTERNAL_ERROR.
    assert_eq!(
        client
            .set(b"boom".to_vec(), b"nope".to_vec())
            .expect("set with failing sync"),
        Status::Unavailable,
        "a WAL sync failure must surface as UNAVAILABLE (§6.2), not INTERNAL_ERROR"
    );

    // The WAL is now fail-closed. Even if the disk "recovers", the writer keeps
    // rejecting mutations until operator recovery -- and that rejection is also
    // UNAVAILABLE (WalError::FailClosed), not INTERNAL_ERROR.
    fs.arm_sync_failures(0);
    assert_eq!(
        client
            .set(b"after".to_vec(), b"still-no".to_vec())
            .expect("set after fail-closed"),
        Status::Unavailable,
        "a fail-closed node must keep reporting writes as UNAVAILABLE"
    );
    assert_eq!(
        client
            .delete(b"before".to_vec())
            .expect("delete after fail-closed"),
        Status::Unavailable,
        "DELETE is a mutation too and must be rejected while fail-closed"
    );

    // Reads still work and reflect state committed before the failure: no
    // failed write was ever applied to the map.
    assert_eq!(
        client.get(b"before".to_vec()).expect("get committed key"),
        Some(b"ok".to_vec()),
        "the pre-failure committed write is still readable"
    );
    assert_eq!(
        client.get(b"boom".to_vec()).expect("get failed key"),
        None,
        "a write whose sync failed was never applied"
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 11. Queue overflow -> RESOURCE_EXHAUSTED.
// ---------------------------------------------------------------------------

/// When the bounded write queue is full the server returns `RESOURCE_EXHAUSTED`
/// to the overflowing writer (Technical-Design §5 backpressure). We hold the
/// sequencer with a nonzero `group_wait` and a tiny `max_queue_depth`, then
/// fire many concurrent writes so at least one is rejected.
#[test]
fn queue_overflow_returns_resource_exhausted() {
    // Tiny queue + a group_wait long enough that the sequencer, once it has
    // drained the first job, sleeps while the rest of the flood piles up and
    // overflows the depth-1 queue.
    let config = ServerConfig {
        max_queue_depth: 1,
        group_wait: Duration::from_millis(400),
        ..ServerConfig::default()
    };
    let mut server = start_sim_server(config);
    let addr = server.local_addr();

    // Fire many concurrent writes on their own connections. With depth 1 and
    // the sequencer asleep in group_wait, most of these cannot be enqueued and
    // must come back RESOURCE_EXHAUSTED.
    const FLOOD: usize = 64;
    let handles: Vec<_> = (0..FLOOD)
        .map(|i| {
            thread::spawn(move || {
                let mut client = Client::connect(addr).expect("connect flood client");
                let key = format!("flood:{i}").into_bytes();
                client.set(key, b"x".to_vec()).expect("set flood")
            })
        })
        .collect();

    let mut ok = 0usize;
    let mut exhausted = 0usize;
    let mut other = 0usize;
    for h in handles {
        match h.join().expect("join flood client") {
            Status::Ok => ok += 1,
            Status::ResourceExhausted => exhausted += 1,
            _ => other += 1,
        }
    }

    assert_eq!(ok + exhausted + other, FLOOD);
    assert_eq!(other, 0, "unexpected non-OK/non-exhausted status");
    assert!(
        exhausted > 0,
        "expected at least one RESOURCE_EXHAUSTED under a depth-1 flood (ok={ok}, exhausted={exhausted})"
    );

    // The server is still healthy: with the flood over and the queue drained,
    // a fresh write succeeds.
    let mut client = Client::connect(addr).expect("connect after flood");
    assert_eq!(
        client
            .set(b"post".to_vec(), b"flood".to_vec())
            .expect("set after flood"),
        Status::Ok
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// 12. Connection cap -> the excess connection gets one UNAVAILABLE, then close.
// ---------------------------------------------------------------------------

/// With `max_connections=1`, one live connection occupies the only slot; a
/// second connection is rejected by the acceptor, which writes exactly one
/// `UNAVAILABLE` response frame and closes it (Technical-Design §5 "acceptor
/// rejects excess").
#[test]
fn connection_cap_rejects_excess_with_unavailable() {
    let config = ServerConfig {
        max_connections: 1,
        ..ServerConfig::default()
    };
    let mut server = start_sim_server(config);

    // Hold the single allowed connection open and busy so its worker keeps the
    // slot occupied while we attempt the second connection.
    let mut live = Client::connect(server.local_addr()).expect("connect first (allowed) client");
    assert_eq!(
        live.set(b"hold".to_vec(), b"slot".to_vec())
            .expect("first client set"),
        Status::Ok
    );

    // The second connection exceeds the cap. The acceptor sends a single
    // UNAVAILABLE frame and closes. It may be accepted at the TCP layer, so we
    // read a raw frame off it rather than assuming connect() fails.
    let mut second = raw_connect(&server);
    let resp = read_one_response(&mut second);
    assert_eq!(
        resp.status,
        Status::Unavailable,
        "an over-cap connection must receive one UNAVAILABLE frame"
    );
    // After that single frame the server closes the connection: the next read
    // is a clean EOF (no further frame).
    assert!(
        read_frame(&mut second)
            .expect("read after cap reject")
            .is_none(),
        "server must close the connection after the UNAVAILABLE reject"
    );
    drop(second);

    // The first connection is unaffected and still usable.
    assert_eq!(
        live.get(b"hold".to_vec())
            .expect("first client still works"),
        Some(b"slot".to_vec())
    );

    // Once the first client disconnects, a new connection can take the freed
    // slot.
    drop(live);
    // Give the worker a moment to observe EOF and release the slot.
    thread::sleep(Duration::from_millis(100));
    let mut reuse = Client::connect(server.local_addr()).expect("connect after slot freed");
    assert_eq!(
        reuse.get(b"hold".to_vec()).expect("reuse client get"),
        Some(b"slot".to_vec())
    );

    server.shutdown();
}

#[test]
fn stats_report_latency_wal_sync_and_recovery() {
    let temp = TempDir::new("stats");
    {
        let db = Db::open(RealFs::new(), temp.path(), DurabilityMode::Fsync).expect("open Db");
        let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
        let mut client = Client::connect(server.local_addr()).unwrap();
        let stats = client.stats().unwrap();
        assert!(stats.lines().any(|line| line == "read_latency_p50_us=unknown"));
        assert!(stats.lines().any(|line| line == "wal_sync_avg_us=unknown"));
        for index in 0..5u8 {
            assert_eq!(client.set(vec![index], vec![index]).unwrap(), Status::Ok);
            assert_eq!(client.get(vec![index]).unwrap(), Some(vec![index]));
        }
        let stats = client.stats().unwrap();
        assert!(stats.lines().any(|line| line == "role=primary"));
        assert!(stats.lines().any(|line| line == "durability=fsync"));
        assert_eq!(parse_stat(&stats, "wal_entries"), 5);
        assert!(parse_stat(&stats, "wal_syncs_total") >= 1);
        assert_eq!(parse_stat(&stats, "wal_sync_errors_total"), 0);
        let p50 = parse_stat(&stats, "write_latency_p50_us");
        let p99 = parse_stat(&stats, "write_latency_p99_us");
        assert!(p50 >= 1 && p50 <= p99, "write p50 {p50} p99 {p99}");
        assert!(parse_stat(&stats, "read_latency_p99_us") >= 1);
        assert_eq!(parse_stat(&stats, "recovery_records_replayed"), 0);
        drop(client);
        server.shutdown();
    }
    // A restarted node reports the WAL records it replayed and how long that
    // took (SOW §17, §26 "recovery time").
    let db = reopen_with_retry(temp.path());
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();
    let stats = client.stats().unwrap();
    assert_eq!(parse_stat(&stats, "recovery_records_replayed"), 5);
    assert!(parse_stat(&stats, "recovery_us") >= 1);
    assert_eq!(parse_stat(&stats, "wal_syncs_total"), 0);
    drop(client);
    server.shutdown();
}
