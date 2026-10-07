//! The dashboard bridge: page assets, polled state, read proxies, and the
//! guards on writes, against an in-process server on a simulated disk.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

use distributedb::dashboard::{Dashboard, DashboardConfig, NodeTarget};
use distributedb::{Client, Db, DurabilityMode, Server, ServerConfig, SimConfig, SimFs, Status};

fn http(addr: SocketAddr, request: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    http(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n"),
    )
}

fn post(addr: SocketAddr, path: &str, header: bool, body: &str) -> (u16, String) {
    let extra = if header { "x-ddb-request: 1\r\n" } else { "" };
    http(
        addr,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\n{extra}Content-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
}

fn primary(seed: u64) -> Server {
    let fs = SimFs::new(SimConfig::new(seed));
    let db = Db::open(fs, Path::new("/db"), DurabilityMode::Fsync).unwrap();
    Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap()
}

fn bridge(nodes: Vec<NodeTarget>, allow_writes: bool) -> Dashboard {
    let mut config = DashboardConfig::new("127.0.0.1:0".parse().unwrap(), nodes);
    config.allow_writes = allow_writes;
    config.poll_interval = Duration::from_millis(50);
    Dashboard::start(config).unwrap()
}

fn wait_for_state(addr: SocketAddr, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = get(addr, "/api/state");
        if status == 200 && body.contains(needle) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "state never contained {needle}: {body}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn serves_the_page_state_and_proxied_reads() {
    let mut server = primary(71);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Ok
    );
    let nodes = vec![NodeTarget {
        name: "primary".into(),
        addr: server.local_addr(),
    }];
    let mut dashboard = bridge(nodes, false);
    let addr = dashboard.local_addr();

    let state = wait_for_state(addr, "\"role\":\"primary\"");
    assert!(state.contains("\"node_id\":"), "{state}");
    assert!(state.contains("\"writes\":false"), "{state}");
    let (status, page) = get(addr, "/");
    assert_eq!(status, 200);
    assert!(page.contains("DISTRIBUTEDB"));
    for asset in ["/style.css", "/app.js", "/topology.js"] {
        assert_eq!(get(addr, asset).0, 200, "{asset}");
    }
    assert!(get(addr, "/api/history").1.contains("\"samples\":[{"));

    assert_eq!(
        get(addr, "/api/get?key=6b").1,
        "{\"node\":\"primary\",\"found\":true,\"value\":\"76\"}"
    );
    assert!(get(addr, "/api/get?key=00").1.contains("\"found\":false"));
    assert!(get(addr, "/api/exists?key=6b")
        .1
        .contains("\"exists\":true"));
    let (_, scan) = get(addr, "/api/scan?start=&limit=10");
    assert!(scan.contains("\"pairs\":[[\"6b\",\"76\"]]"), "{scan}");
    assert_eq!(get(addr, "/api/get?key=zz").0, 400);
    assert_eq!(get(addr, "/api/scan?limit=0").0, 400);
    assert_eq!(get(addr, "/api/get?key=6b&node=nope").0, 400);
    assert_eq!(get(addr, "/missing").0, 404);

    dashboard.shutdown();
    drop(client);
    server.shutdown();
}

#[test]
fn writes_need_the_flag_and_the_request_header() {
    let mut server = primary(72);
    let nodes = vec![NodeTarget {
        name: "primary".into(),
        addr: server.local_addr(),
    }];
    let mut read_only = bridge(nodes.clone(), false);
    assert_eq!(
        post(read_only.local_addr(), "/api/set?key=61&value=31", true, "").0,
        403
    );
    read_only.shutdown();

    let mut dashboard = bridge(nodes, true);
    let addr = dashboard.local_addr();
    assert_eq!(post(addr, "/api/set?key=61&value=31", false, "").0, 403);
    let (status, body) = post(addr, "/api/set?key=61&value=31", true, "");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"status\":\"OK\""), "{body}");
    assert!(get(addr, "/api/get?key=61").1.contains("\"value\":\"31\""));

    let (_, committed) = post(addr, "/api/transaction", true, "SET 62 32\nDELETE 61\n");
    assert!(committed.contains("\"status\":\"OK\""), "{committed}");
    assert!(committed.contains("\"operations\":2"), "{committed}");
    assert!(get(addr, "/api/exists?key=61")
        .1
        .contains("\"exists\":false"));
    assert!(get(addr, "/api/get?key=62").1.contains("\"value\":\"32\""));
    assert_eq!(post(addr, "/api/transaction", true, "GET 61").0, 400);
    // Node control needs the cluster launcher.
    assert_eq!(post(addr, "/api/node/stop?name=primary", true, "").0, 403);

    dashboard.shutdown();
    server.shutdown();
}

#[test]
fn rejects_foreign_hosts_and_reports_unavailable_nodes() {
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let nodes = vec![NodeTarget {
        name: "primary".into(),
        addr: closed,
    }];
    let mut dashboard = bridge(nodes, false);
    let addr = dashboard.local_addr();
    let (status, _) = http(
        addr,
        "GET /api/state HTTP/1.1\r\nHost: evil.example:8090\r\n\r\n",
    );
    assert_eq!(status, 403);
    let state = wait_for_state(addr, "\"error\":\"");
    assert!(state.contains("\"up\":false"), "{state}");
    assert_eq!(get(addr, "/api/get?key=61").0, 502);
    dashboard.shutdown();
}
