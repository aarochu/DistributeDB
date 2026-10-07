//! `ddb_dashboard`: a loopback HTTP bridge between a running cluster and the
//! static dashboard page in `web/`.
//!
//! The database never serves HTTP. This bridge is an ordinary client: it
//! polls `STATS` from every node over the client protocol, keeps a short
//! rolling history, proxies the read commands the Key Explorer needs, and,
//! only when enabled, forwards writes and node start/stop requests.
//!
//! Safety for a local tool: it binds to loopback, rejects requests whose
//! `Host` is not a loopback name (DNS rebinding), and requires a custom
//! request header on every `POST`, which a cross-site page cannot send
//! without a CORS preflight that this server never answers.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::client::{Client, ClientError};
use crate::command::MAX_SCAN_LIMIT;
use crate::protocol::Status;

const INDEX_HTML: &str = include_str!("../../web/index.html");
const STYLE_CSS: &str = include_str!("../../web/style.css");
const APP_JS: &str = include_str!("../../web/app.js");
const TOPOLOGY_JS: &str = include_str!("../../web/topology.js");

/// Header every state-changing request must carry.
pub const REQUEST_HEADER: &str = "x-ddb-request";
/// Largest accepted request head plus body.
const MAX_REQUEST_BYTES: usize = 1 << 20;
/// Largest transaction the write console sends, matching one WAL group.
const MAX_TRANSACTION_OPS: usize = 64;
/// How long a proxied command may take; covers a synchronous-replication wait.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
/// How long one STATS poll may take before the node is reported unavailable.
const POLL_TIMEOUT: Duration = Duration::from_millis(800);

/// A node the bridge polls. The first target is the primary.
#[derive(Debug, Clone)]
pub struct NodeTarget {
    pub name: String,
    pub addr: SocketAddr,
}

/// Start and stop control over local node processes, provided by the
/// cluster launcher. Only replicas are controllable.
pub trait NodeControl: Send + Sync {
    fn stop(&self, name: &str) -> Result<(), String>;
    fn start(&self, name: &str) -> Result<(), String>;
}

/// Bridge configuration.
pub struct DashboardConfig {
    pub bind: SocketAddr,
    pub nodes: Vec<NodeTarget>,
    /// Enables the write console endpoints.
    pub allow_writes: bool,
    /// Enables the node start/stop endpoints.
    pub control: Option<Arc<dyn NodeControl>>,
    pub poll_interval: Duration,
    /// Samples kept for the charts.
    pub history_len: usize,
}

impl DashboardConfig {
    /// Read-only bridge for `nodes`, polling once per second with five
    /// minutes of history.
    pub fn new(bind: SocketAddr, nodes: Vec<NodeTarget>) -> Self {
        DashboardConfig {
            bind,
            nodes,
            allow_writes: false,
            control: None,
            poll_interval: Duration::from_secs(1),
            history_len: 300,
        }
    }
}

/// Parsed `STATS` lines, in server order.
type Stats = Vec<(String, String)>;

#[derive(Default, Clone)]
struct Observation {
    up: bool,
    error: Option<String>,
    stats: Stats,
    /// Wall-clock time of the last successful poll.
    observed_ms: Option<u64>,
}

struct Sample {
    t_ms: u64,
    nodes: Vec<Option<Stats>>,
}

type Observed = (Vec<Observation>, VecDeque<Sample>);

struct Shared {
    config: DashboardConfig,
    observed: Mutex<Observed>,
    shutdown: AtomicBool,
    started_ms: u64,
}

/// A running bridge.
pub struct Dashboard {
    local_addr: SocketAddr,
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

impl Dashboard {
    /// Bind, start polling, and serve until [`Dashboard::shutdown`].
    pub fn start(config: DashboardConfig) -> io::Result<Dashboard> {
        if config.nodes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the dashboard needs at least the primary's address",
            ));
        }
        if !config.bind.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the dashboard binds to a loopback address only",
            ));
        }
        let listener = TcpListener::bind(config.bind)?;
        let local_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let node_count = config.nodes.len();
        let shared = Arc::new(Shared {
            config,
            observed: Mutex::new((vec![Observation::default(); node_count], VecDeque::new())),
            shutdown: AtomicBool::new(false),
            started_ms: now_ms(),
        });
        let poller = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("ddb-dashboard-poll".into())
                .spawn(move || poll_loop(&shared))?
        };
        let acceptor = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("ddb-dashboard-http".into())
                .spawn(move || accept_loop(listener, &shared))?
        };
        Ok(Dashboard {
            local_addr,
            shared,
            threads: vec![poller, acceptor],
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop polling and serving, and join the bridge threads.
    pub fn shutdown(&mut self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Parse `name=value` lines, skipping anything malformed.
pub fn parse_stats(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .filter(|(name, _)| !name.is_empty())
        .collect()
}

fn poll_loop(shared: &Shared) {
    let mut clients: Vec<Option<Client>> = shared.config.nodes.iter().map(|_| None).collect();
    while !shared.shutdown.load(Ordering::SeqCst) {
        let started = Instant::now();
        let results: Vec<Result<Stats, String>> = shared
            .config
            .nodes
            .iter()
            .zip(clients.iter_mut())
            .map(|(target, client)| poll_node(target, client))
            .collect();
        let t_ms = now_ms();
        {
            let mut observed = shared.observed.lock().expect("dashboard state poisoned");
            let (nodes, history) = &mut *observed;
            let mut sample = Vec::with_capacity(results.len());
            for (node, result) in nodes.iter_mut().zip(results) {
                match result {
                    Ok(stats) => {
                        sample.push(Some(stats.clone()));
                        *node = Observation {
                            up: true,
                            error: None,
                            stats,
                            observed_ms: Some(t_ms),
                        };
                    }
                    Err(error) => {
                        sample.push(None);
                        node.up = false;
                        node.error = Some(error);
                    }
                }
            }
            history.push_back(Sample {
                t_ms,
                nodes: sample,
            });
            while history.len() > shared.config.history_len.max(1) {
                history.pop_front();
            }
        }
        while started.elapsed() < shared.config.poll_interval {
            if shared.shutdown.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

fn poll_node(target: &NodeTarget, client: &mut Option<Client>) -> Result<Stats, String> {
    if client.is_none() {
        let connected = Client::connect_timeout(&target.addr, POLL_TIMEOUT);
        *client = Some(connected.map_err(|error| error.to_string())?);
    }
    let result = client.as_mut().expect("connected above").stats();
    match result {
        Ok(text) => Ok(parse_stats(&text)),
        Err(error) => {
            *client = None;
            Err(error.to_string())
        }
    }
}

fn accept_loop(listener: TcpListener, shared: &Arc<Shared>) {
    let mut workers: Vec<JoinHandle<()>> = Vec::new();
    while !shared.shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let shared = Arc::clone(shared);
                if let Ok(worker) = thread::Builder::new()
                    .name("ddb-dashboard-request".into())
                    .spawn(move || handle_connection(stream, &shared))
                {
                    workers.push(worker);
                }
            }
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
        workers.retain(|worker| !worker.is_finished());
    }
    for worker in workers {
        let _ = worker.join();
    }
}

/// One parsed HTTP request.
#[derive(Debug)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Read one HTTP/1.1 request: the head up to a blank line, then a body of
/// `Content-Length` bytes. Bounded by [`MAX_REQUEST_BYTES`].
pub fn read_request<R: Read>(reader: &mut R) -> io::Result<HttpRequest> {
    let invalid = |message: &str| io::Error::new(io::ErrorKind::InvalidData, message.to_string());
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err(invalid("request head too large"));
        }
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            return Err(invalid("connection closed before the request head ended"));
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head =
        std::str::from_utf8(&buffer[..head_end]).map_err(|_| invalid("head is not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| invalid("missing request line"))?;
    let mut parts = request_line.split(' ');
    let (method, target) = match (parts.next(), parts.next(), parts.next()) {
        (Some(method), Some(target), Some(version)) if version.starts_with("HTTP/1.") => {
            (method.to_string(), target)
        }
        _ => return Err(invalid("malformed request line")),
    };
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| {
            value
                .parse::<usize>()
                .map_err(|_| invalid("bad Content-Length"))
        })
        .transpose()?
        .unwrap_or(0);
    if head_end + 4 + content_length > MAX_REQUEST_BYTES {
        return Err(invalid("request body too large"));
    }
    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < content_length {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            return Err(invalid("connection closed before the body ended"));
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, parse_query(query)),
        None => (target, Vec::new()),
    };
    Ok(HttpRequest {
        method,
        path: path.to_string(),
        query,
        headers,
        body,
    })
}

/// Split `a=1&b=2`, percent-decoding names and values.
pub fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name), percent_decode(value))
        })
        .collect()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let escaped = (byte == b'%')
            .then(|| {
                let high = hex_digit(*bytes.get(index + 1)?)?;
                let low = hex_digit(*bytes.get(index + 2)?)?;
                Some(high << 4 | low)
            })
            .flatten();
        match (escaped, byte) {
            (Some(decoded), _) => {
                out.push(decoded);
                index += 3;
                continue;
            }
            (None, b'+') => out.push(b' '),
            (None, byte) => out.push(byte),
        }
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Decode lowercase or uppercase hex; an empty string is zero bytes.
pub fn decode_hex(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks(2)
        .map(|pair| Some(hex_digit(pair[0])? << 4 | hex_digit(pair[1])?))
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A JSON string literal for `text`.
pub fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn stats_json(stats: &Stats) -> String {
    let fields: Vec<String> = stats
        .iter()
        .map(|(name, value)| format!("{}:{}", json_string(name), json_string(value)))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// The wire name of a status, as the protocol documentation spells it.
pub fn status_name(status: Status) -> &'static str {
    match status {
        Status::Ok => "OK",
        Status::NotFound => "NOT_FOUND",
        Status::BadRequest => "BAD_REQUEST",
        Status::NotPrimary => "NOT_PRIMARY",
        Status::Unavailable => "UNAVAILABLE",
        Status::InternalError => "INTERNAL_ERROR",
        Status::UnsupportedVersion => "UNSUPPORTED_VERSION",
        Status::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Status::OkVolatile => "OK_VOLATILE",
        Status::Queued => "QUEUED",
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Reply {
    fn json(status: u16, body: String) -> Reply {
        Reply {
            status,
            content_type: "application/json",
            body,
        }
    }

    fn error(status: u16, message: &str) -> Reply {
        Reply::json(status, format!("{{\"error\":{}}}", json_string(message)))
    }

    fn asset(content_type: &'static str, body: &str) -> Reply {
        Reply {
            status: 200,
            content_type,
            body: body.to_string(),
        }
    }
}

fn handle_connection(stream: TcpStream, shared: &Shared) {
    // Windows and macOS pass the listener's nonblocking mode to accepted
    // sockets; requests are read with ordinary blocking calls.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    let mut reader = match stream.try_clone() {
        Ok(reader) => reader,
        Err(_) => return,
    };
    let reply = match read_request(&mut reader) {
        Ok(request) => route(&request, shared),
        Err(error) => Reply::error(400, &error.to_string()),
    };
    let reason = match reply.status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Bad Gateway",
    };
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        reply.status,
        reply.content_type,
        reply.body.len()
    );
    let mut writer = stream;
    let _ = writer.write_all(head.as_bytes());
    let _ = writer.write_all(reply.body.as_bytes());
    let _ = writer.flush();
}

/// Whether `Host` names this machine's loopback interface.
fn loopback_host(host: Option<&str>) -> bool {
    let Some(host) = host else {
        return false;
    };
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host.rsplit_once(':').map_or(host, |(name, _)| name)
    };
    matches!(name, "127.0.0.1" | "localhost" | "::1")
}

fn route(request: &HttpRequest, shared: &Shared) -> Reply {
    if !loopback_host(request.header("host")) {
        return Reply::error(403, "the dashboard answers loopback host names only");
    }
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") | ("GET", "/index.html") => Reply::asset("text/html", INDEX_HTML),
        ("GET", "/style.css") => Reply::asset("text/css", STYLE_CSS),
        ("GET", "/app.js") => Reply::asset("text/javascript", APP_JS),
        ("GET", "/topology.js") => Reply::asset("text/javascript", TOPOLOGY_JS),
        ("GET", "/api/state") => Reply::json(200, state_json(shared)),
        ("GET", "/api/history") => Reply::json(200, history_json(shared)),
        ("GET", "/api/get") => read_command(request, shared, ReadKind::Get),
        ("GET", "/api/exists") => read_command(request, shared, ReadKind::Exists),
        ("GET", "/api/scan") => read_command(request, shared, ReadKind::Scan),
        ("POST", path) => {
            if request.header(REQUEST_HEADER) != Some("1") {
                return Reply::error(403, "POST requests need the x-ddb-request: 1 header");
            }
            match path {
                "/api/set" | "/api/delete" | "/api/transaction" => write_command(request, shared),
                "/api/node/stop" | "/api/node/start" => node_command(request, shared),
                _ => Reply::error(404, "unknown endpoint"),
            }
        }
        (_, path) if path.starts_with("/api/") => Reply::error(405, "method not allowed"),
        _ => Reply::error(404, "not found"),
    }
}

fn state_json(shared: &Shared) -> String {
    let observed = shared.observed.lock().expect("dashboard state poisoned");
    let (nodes, history) = &*observed;
    let entries: Vec<String> = shared
        .config
        .nodes
        .iter()
        .zip(nodes)
        .enumerate()
        .map(|(index, (target, node))| {
            format!(
                "{{\"name\":{},\"addr\":{},\"primary\":{},\"up\":{},\"error\":{},\"observed_ms\":{},\"stats\":{}}}",
                json_string(&target.name),
                json_string(&target.addr.to_string()),
                index == 0,
                node.up,
                node.error.as_deref().map_or("null".to_string(), json_string),
                node.observed_ms.map_or("null".to_string(), |ms| ms.to_string()),
                stats_json(&node.stats)
            )
        })
        .collect();
    format!(
        "{{\"time_ms\":{},\"started_ms\":{},\"samples\":{},\"poll_ms\":{},\"bridge\":{{\"addr\":{},\"writes\":{},\"control\":{}}},\"nodes\":[{}]}}",
        now_ms(),
        shared.started_ms,
        history.len(),
        shared.config.poll_interval.as_millis(),
        json_string(&shared.config.bind.to_string()),
        shared.config.allow_writes,
        shared.config.control.is_some(),
        entries.join(",")
    )
}

fn history_json(shared: &Shared) -> String {
    let observed = shared.observed.lock().expect("dashboard state poisoned");
    let samples: Vec<String> = observed
        .1
        .iter()
        .map(|sample| {
            let nodes: Vec<String> = sample
                .nodes
                .iter()
                .map(|stats| stats.as_ref().map_or("null".to_string(), stats_json))
                .collect();
            format!("{{\"t\":{},\"nodes\":[{}]}}", sample.t_ms, nodes.join(","))
        })
        .collect();
    let names: Vec<String> = shared
        .config
        .nodes
        .iter()
        .map(|target| json_string(&target.name))
        .collect();
    format!(
        "{{\"names\":[{}],\"samples\":[{}]}}",
        names.join(","),
        samples.join(",")
    )
}

enum ReadKind {
    Get,
    Exists,
    Scan,
}

fn target_node<'a>(request: &HttpRequest, shared: &'a Shared) -> Result<&'a NodeTarget, Reply> {
    match request.param("node") {
        None => Ok(&shared.config.nodes[0]),
        Some(name) => shared
            .config
            .nodes
            .iter()
            .find(|target| target.name == name)
            .ok_or_else(|| Reply::error(400, "unknown node")),
    }
}

fn hex_param(request: &HttpRequest, name: &str) -> Result<Vec<u8>, Reply> {
    let text = request.param(name).unwrap_or("");
    decode_hex(text).ok_or_else(|| Reply::error(400, &format!("{name} must be hexadecimal")))
}

fn connect(target: &NodeTarget) -> Result<Client, Reply> {
    Client::connect_timeout(&target.addr, COMMAND_TIMEOUT)
        .map_err(|error| Reply::error(502, &format!("{} unavailable: {error}", target.name)))
}

fn upstream(error: ClientError) -> Reply {
    Reply::error(502, &error.to_string())
}

fn read_command(request: &HttpRequest, shared: &Shared, kind: ReadKind) -> Reply {
    match read_body(request, shared, kind) {
        Ok(body) => Reply::json(200, body),
        Err(reply) => reply,
    }
}

fn read_body(request: &HttpRequest, shared: &Shared, kind: ReadKind) -> Result<String, Reply> {
    let target = target_node(request, shared)?;
    let node = json_string(&target.name);
    let mut client = connect(target)?;
    Ok(match kind {
        ReadKind::Get => {
            let key = hex_param(request, "key")?;
            match client.get(key).map_err(upstream)? {
                Some(value) => format!(
                    "{{\"node\":{node},\"found\":true,\"value\":{}}}",
                    json_string(&encode_hex(&value))
                ),
                None => format!("{{\"node\":{node},\"found\":false}}"),
            }
        }
        ReadKind::Exists => {
            let key = hex_param(request, "key")?;
            let exists = client.exists(key).map_err(upstream)?;
            format!("{{\"node\":{node},\"exists\":{exists}}}")
        }
        ReadKind::Scan => {
            let start = hex_param(request, "start")?;
            let end = match request.param("end") {
                Some(text) if !text.is_empty() => Some(
                    decode_hex(text).ok_or_else(|| Reply::error(400, "end must be hexadecimal"))?,
                ),
                _ => None,
            };
            let limit = request
                .param("limit")
                .unwrap_or("50")
                .parse::<u32>()
                .ok()
                .filter(|limit| (1..=MAX_SCAN_LIMIT as u32).contains(limit))
                .ok_or_else(|| Reply::error(400, "limit must be 1..=10000"))?;
            let (pairs, more) = client.scan(start, end, limit).map_err(upstream)?;
            let rows: Vec<String> = pairs
                .iter()
                .map(|(key, value)| {
                    format!(
                        "[{},{}]",
                        json_string(&encode_hex(key)),
                        json_string(&encode_hex(value))
                    )
                })
                .collect();
            format!(
                "{{\"node\":{node},\"pairs\":[{}],\"more\":{more}}}",
                rows.join(",")
            )
        }
    })
}

/// One write the console sends: `SET key value` or `DELETE key`.
enum WriteOp {
    Set(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Parse transaction lines `SET <hex> <hex>` and `DELETE <hex>`; `-` is an
/// empty value.
fn parse_ops(body: &[u8]) -> Result<Vec<WriteOp>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "body is not UTF-8".to_string())?;
    let mut ops = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let value = |field: &str| {
            if field == "-" {
                Some(Vec::new())
            } else {
                decode_hex(field)
            }
        };
        let op = match fields.as_slice() {
            ["SET", key, data] => WriteOp::Set(
                decode_hex(key).ok_or("key must be hexadecimal")?,
                value(data).ok_or("value must be hexadecimal")?,
            ),
            ["DELETE", key] => WriteOp::Delete(decode_hex(key).ok_or("key must be hexadecimal")?),
            _ => return Err(format!("unrecognized operation: {line}")),
        };
        ops.push(op);
    }
    if ops.is_empty() || ops.len() > MAX_TRANSACTION_OPS {
        return Err(format!(
            "a transaction holds 1..={MAX_TRANSACTION_OPS} operations"
        ));
    }
    Ok(ops)
}

fn write_command(request: &HttpRequest, shared: &Shared) -> Reply {
    if !shared.config.allow_writes {
        return Reply::error(
            403,
            "writes are disabled; start the bridge with --allow-writes",
        );
    }
    match write_body(request, shared) {
        Ok(body) => Reply::json(200, body),
        Err(reply) => reply,
    }
}

fn write_body(request: &HttpRequest, shared: &Shared) -> Result<String, Reply> {
    let target = &shared.config.nodes[0];
    let mut client = connect(target)?;
    let started = Instant::now();
    let (status, operations) = match request.path.as_str() {
        "/api/set" => {
            let key = hex_param(request, "key")?;
            let value = hex_param(request, "value")?;
            (client.set(key, value).map_err(upstream)?, 1)
        }
        "/api/delete" => {
            let key = hex_param(request, "key")?;
            (client.delete(key).map_err(upstream)?, 1)
        }
        _ => {
            // The write console queues operations in the page; COMMIT sends
            // them here as BEGIN, the writes, and COMMIT on one connection.
            let ops = parse_ops(&request.body).map_err(|error| Reply::error(400, &error))?;
            let begun = client.begin().map_err(upstream)?;
            if begun != Status::Ok {
                return Ok(write_result(begun, 0, started, Some("BEGIN")));
            }
            for (index, op) in ops.iter().enumerate() {
                let status = match op {
                    WriteOp::Set(key, value) => client.set(key.clone(), value.clone()),
                    WriteOp::Delete(key) => client.delete(key.clone()),
                }
                .map_err(upstream)?;
                if status != Status::Queued {
                    let _ = client.rollback();
                    let step = format!("operation {}", index + 1);
                    return Ok(write_result(status, ops.len(), started, Some(&step)));
                }
            }
            (client.commit().map_err(upstream)?, ops.len())
        }
    };
    Ok(write_result(status, operations, started, None))
}

fn write_result(
    status: Status,
    operations: usize,
    started: Instant,
    failed_at: Option<&str>,
) -> String {
    format!(
        "{{\"status\":{},\"operations\":{operations},\"round_trip_us\":{},\"failed_at\":{}}}",
        json_string(status_name(status)),
        started.elapsed().as_micros(),
        failed_at.map_or("null".to_string(), json_string)
    )
}

fn node_command(request: &HttpRequest, shared: &Shared) -> Reply {
    let Some(control) = shared.config.control.as_ref() else {
        return Reply::error(
            403,
            "node control needs the cluster launcher (distributedb cluster --dashboard)",
        );
    };
    let Some(name) = request.param("name") else {
        return Reply::error(400, "name is required");
    };
    let result = if request.path == "/api/node/stop" {
        control.stop(name)
    } else {
        control.start(name)
    };
    match result {
        Ok(()) => Reply::json(
            200,
            format!("{{\"node\":{},\"ok\":true}}", json_string(name)),
        ),
        Err(error) => Reply::error(400, &error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_request_with_query_headers_and_body() {
        let raw = b"POST /api/transaction?node=primary&x=a%20b HTTP/1.1\r\nHost: 127.0.0.1:8090\r\nContent-Length: 11\r\nX-DDB-Request: 1\r\n\r\nDELETE 6b00";
        let request = read_request(&mut &raw[..]).unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/api/transaction");
        assert_eq!(request.param("x"), Some("a b"));
        assert_eq!(request.header("x-ddb-request"), Some("1"));
        assert_eq!(request.body, b"DELETE 6b00");
    }

    #[test]
    fn rejects_malformed_and_oversized_requests() {
        assert!(read_request(&mut &b"GET /\r\n\r\n"[..]).is_err());
        assert!(read_request(&mut &b"GET / HTTP/1.1\r\nContent-Length: x\r\n\r\n"[..]).is_err());
        let huge = format!("POST / HTTP/1.1\r\nContent-Length: {MAX_REQUEST_BYTES}\r\n\r\n");
        assert!(read_request(&mut huge.as_bytes()).is_err());
        assert!(read_request(&mut &b"GET / HTTP/1.1\r\nHost: x"[..]).is_err());
    }

    #[test]
    fn decodes_hex_percent_and_stats() {
        assert_eq!(decode_hex("6B00"), Some(vec![0x6b, 0]));
        assert_eq!(decode_hex(""), Some(Vec::new()));
        assert_eq!(decode_hex("abc"), None);
        assert_eq!(decode_hex("zz"), None);
        assert_eq!(decode_hex("é0"), None);
        assert_eq!(percent_decode("a%2Fb%"), "a/b%");
        assert_eq!(percent_decode("%é"), "%é");
        let stats = parse_stats("version=1\nrole=primary\nbroken\nreplica_ab_lag=unknown\n");
        assert_eq!(stats.len(), 3);
        assert_eq!(stats[2], ("replica_ab_lag".into(), "unknown".into()));
    }

    #[test]
    fn json_strings_escape_control_characters() {
        assert_eq!(json_string("a\"b\\c\n\u{1}"), "\"a\\\"b\\\\c\\n\\u0001\"");
    }

    #[test]
    fn host_check_accepts_only_loopback_names() {
        assert!(loopback_host(Some("127.0.0.1:8090")));
        assert!(loopback_host(Some("localhost")));
        assert!(loopback_host(Some("[::1]:8090")));
        assert!(!loopback_host(Some("evil.example:8090")));
        assert!(!loopback_host(None));
    }

    #[test]
    fn transaction_bodies_are_bounded_and_validated() {
        assert_eq!(
            parse_ops(b"SET 61 62\nDELETE 61\nSET 61 -\n")
                .unwrap()
                .len(),
            3
        );
        assert!(parse_ops(b"").is_err());
        assert!(parse_ops(b"GET 61").is_err());
        assert!(parse_ops(b"SET 6 62").is_err());
        let many = "DELETE 61\n".repeat(MAX_TRANSACTION_OPS + 1);
        assert!(parse_ops(many.as_bytes()).is_err());
    }
}
