//! `ddb_dashboard`: serve the dashboard page for a cluster that is already
//! running. The first `--nodes` address is the primary; the rest are replica
//! read addresses (`replica --read-addr`).
//!
//! ```text
//! ddb_dashboard --nodes 127.0.0.1:5555,127.0.0.1:5557 [--addr 127.0.0.1:8090] [--allow-writes]
//! ```
//!
//! For a cluster whose replicas the page can stop and restart, use
//! `distributedb cluster --dashboard` instead.

use std::error::Error;
use std::io::{self, BufRead};
use std::net::{SocketAddr, ToSocketAddrs};

use distributedb::dashboard::{Dashboard, DashboardConfig, NodeTarget};

fn resolve(addr: &str) -> Result<SocketAddr, Box<dyn Error>> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("{addr} did not resolve").into())
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1).cloned())
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let nodes = flag(&args, "--nodes").ok_or("--nodes PRIMARY[,REPLICA...] is required")?;
    let targets = nodes
        .split(',')
        .filter(|addr| !addr.trim().is_empty())
        .enumerate()
        .map(|(index, addr)| {
            let name = if index == 0 {
                "primary".to_string()
            } else {
                format!("replica-{index:02}")
            };
            resolve(addr.trim()).map(|addr| NodeTarget { name, addr })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bind = resolve(&flag(&args, "--addr").unwrap_or_else(|| "127.0.0.1:8090".into()))?;
    let mut config = DashboardConfig::new(bind, targets);
    config.allow_writes = args.iter().any(|arg| arg == "--allow-writes");
    let mut dashboard = Dashboard::start(config)?;
    println!("dashboard: http://{}", dashboard.local_addr());
    println!("press Ctrl-D (EOF) or enter shutdown to stop");
    let mut line = String::new();
    loop {
        line.clear();
        match io::stdin().lock().read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line.trim() == "shutdown" => break,
            Ok(_) => {}
        }
    }
    dashboard.shutdown();
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ddb_dashboard: {error}");
        std::process::exit(1);
    }
}
