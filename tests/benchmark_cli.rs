//! Smoke test for the benchmark's network path and CSV output.

use std::process::Command;

use distributedb::{Client, Db, DurabilityMode, Server, ServerConfig, SimConfig, SimFs, Status};

#[test]
fn benchmark_records_successful_measured_operations() {
    let fs = SimFs::new(SimConfig::new(91));
    let db = Db::open(
        fs,
        std::path::Path::new("/benchmark"),
        DurabilityMode::Fsync,
    )
    .unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let output_path = std::env::temp_dir().join(format!(
        "ddb-benchmark-test-{}-{}.csv",
        std::process::id(),
        server.local_addr().port()
    ));
    let address = server.local_addr().to_string();
    let output_name = output_path.to_string_lossy().into_owned();
    let result = Command::new(env!("CARGO_BIN_EXE_ddb_bench"))
        .args([
            "--addr",
            &address,
            "--clients",
            "2",
            "--operations",
            "100",
            "--warmup",
            "10",
            "--keys",
            "10",
            "--value-bytes",
            "4",
            "--read-ratio",
            "0.9",
            "--output",
            &output_name,
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let csv = std::fs::read_to_string(&output_path).unwrap();
    let mut lines = csv.lines();
    let columns: Vec<&str> = lines.next().unwrap().split(',').collect();
    let values: Vec<&str> = lines.next().unwrap().split(',').collect();
    assert_eq!(columns.len(), values.len());
    assert!(lines.next().is_none());
    let value = |name: &str| values[columns.iter().position(|column| *column == name).unwrap()];
    assert_eq!(value("operations_requested"), "100");
    assert_eq!(value("successful_ops"), "100");
    assert_eq!(value("failed_ops"), "0");
    assert_eq!(value("skipped_ops"), "0");
    assert!(value("p95_latency_ns").parse::<u64>().unwrap() > 0);
    std::fs::remove_file(output_path).unwrap();
    server.shutdown();
}

#[test]
fn os_durability_server_reports_volatile_writes() {
    let fs = SimFs::new(SimConfig::new(92));
    let db = Db::open(
        fs,
        std::path::Path::new("/benchmark-os"),
        DurabilityMode::Os,
    )
    .unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::OkVolatile
    );
    assert_eq!(client.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(client.delete(b"k".to_vec()).unwrap(), Status::OkVolatile);
    drop(client);
    server.shutdown();
}
