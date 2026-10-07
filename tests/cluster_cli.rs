//! `distributedb cluster`: one command starts a primary and a replica as
//! child processes, a stopped replica catches up after restart, and the
//! dashboard can stop and start replicas but never the primary.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use distributedb::Client;

/// Four consecutive free ports below the operating systems' ephemeral
/// ranges, so no outgoing connection can be handed one of them as its source
/// port (which would let a probe connect to itself).
fn free_ports() -> u16 {
    let start = 20_000 + (std::process::id() % 2_000) as u16 * 4;
    (0..2_000u16)
        .map(|step| 20_000 + (start - 20_000 + step * 4) % 8_000)
        .find(|&base| (0..4).all(|i| TcpListener::bind(("127.0.0.1", base + i)).is_ok()))
        .expect("no free port range")
}

fn http(addr: SocketAddr, request: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response.get(9..12).and_then(|code| code.parse().ok());
    let body = response
        .split_once("\r\n\r\n")
        .map_or("", |(_, b)| b)
        .to_string();
    (
        status.unwrap_or_else(|| panic!("not an HTTP response: {response:?}")),
        body,
    )
}

#[test]
fn launches_a_cluster_and_restarts_a_replica_that_catches_up() {
    let base = free_ports();
    let dashboard: SocketAddr = format!("127.0.0.1:{}", base + 3).parse().unwrap();
    let replica_read: SocketAddr = format!("127.0.0.1:{}", base + 2).parse().unwrap();
    let data = std::env::temp_dir().join(format!("ddb-cluster-cli-{}-{base}", std::process::id()));
    let mut child = Command::new(env!("CARGO_BIN_EXE_distributedb"))
        .args([
            "cluster",
            "--replicas",
            "1",
            "--base-port",
            &base.to_string(),
        ])
        .args(["--dashboard-addr", &dashboard.to_string()])
        .arg("--data")
        .arg(&data)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let (lines, output) = mpsc::channel();
    let stdout = child.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = lines.send(line);
        }
    });

    // The launcher prints the dashboard address once every node is up.
    let mut printed: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !printed
        .iter()
        .any(|line| line.starts_with("dashboard: http://"))
    {
        match output.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(line) => printed.push(line),
            Err(_) => panic!("cluster did not start: {printed:?}"),
        }
    }

    let (status, body) = http(
        dashboard,
        &format!(
            "POST /api/node/stop?name=primary HTTP/1.1\r\nHost: {dashboard}\r\nx-ddb-request: 1\r\n\r\n"
        ),
    );
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("primary"), "{body}");

    writeln!(stdin, "SET a 1").unwrap();
    writeln!(stdin, "stop replica-01").unwrap();
    writeln!(stdin, "SET b 2").unwrap();
    writeln!(stdin, "start replica-01").unwrap();
    writeln!(stdin, "nodes").unwrap();
    stdin.flush().unwrap();

    // The restarted replica receives the write made while it was down.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let found = Client::connect(replica_read)
            .ok()
            .and_then(|mut client| client.get(b"b".to_vec()).ok().flatten());
        if found.as_deref() == Some(b"2".as_slice()) {
            break;
        }
        assert!(Instant::now() < deadline, "replica did not catch up");
        std::thread::sleep(Duration::from_millis(100));
    }

    writeln!(stdin, "shutdown").unwrap();
    drop(stdin);
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("cluster did not shut down");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    reader.join().unwrap();
    printed.extend(output.try_iter());
    let text = printed.join("\n");
    assert!(status.success(), "{text}");
    for expected in [
        "cluster ID: ",
        "OK",
        "replica-01 stopped",
        "replica-01 started",
        "cluster stopped",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
    // Every node exited: the primary's client port is free again.
    assert!(TcpListener::bind(("127.0.0.1", base)).is_ok());
    let _ = std::fs::remove_dir_all(&data);
}
