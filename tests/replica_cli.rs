//! Replica command-line validation.

use std::process::{Command, Stdio};

#[test]
fn replica_rejects_malformed_primary_address_at_startup() {
    let data = std::env::temp_dir().join(format!("ddb-replica-cli-{}", std::process::id()));
    for address in ["primary", "primary:", ":5556", "primary:notaport"] {
        // stdin is closed, so a replica that started instead of rejecting the
        // address would exit successfully on EOF.
        let output = Command::new(env!("CARGO_BIN_EXE_distributedb"))
            .args([
                "replica",
                "--primary-addr",
                address,
                "--cluster-id",
                "00000000000000000000000000000000",
                "--data",
                &data.to_string_lossy(),
            ])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{address} was accepted");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("HOST:PORT"),
            "{address}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(
        !data.exists(),
        "rejected replica created its data directory"
    );
}
