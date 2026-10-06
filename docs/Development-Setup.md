# Development setup and repository policy

**Current state:** The repository contains a Rust primary, replica, client, benchmark harness, tests, and the project documents. The database is under development and is not production ready. See the [README](../README.md) for current commands and the [SOW conformance audit](SOW-Conformance.md) for tested scope.

## Requirements

- Rust 1.92 or newer with Cargo. The current crate uses the Rust standard library only, so the build and tests can run offline after the toolchain is installed.
- Git for source control and review.
- A 64-bit Linux host with a local ext4 filesystem for the initial durability profile. Windows development can run many unit tests, but it is not a validated durability profile; see [ADR-001](ADR-001-Language-and-Filesystem.md).
- Docker with Compose only for the optional local three-node demonstration.

From the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --offline --all-targets -- -D warnings
cargo test --offline --all-targets
cargo build --offline
```

Use a separate data directory for each node. The [README](../README.md#run-the-current-server) has the primary, client, and replica commands; [the Docker guide](docker-cluster.md) has the three-node demonstration. The demo listeners are unauthenticated and should remain on loopback or an isolated local test network.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/` | Storage engines, WAL, protocol, server, replication, CLI, and benchmark binaries. |
| `tests/` | Integration, crash, recovery, networking, and replication checks. |
| `benchmarks/` | Workload scripts and checked-in result artifacts with their environment notes. |
| `scripts/` | Local demonstration and seeded failure-trial runners. |
| `docs/` | SOW, technical design, ADR, operating guides, and evidence audit. |
| `.github/workflows/` | Documentation, Rust, Docker, failure, and benchmark CI workflows. |
| `compose.yaml` | Local three-node container configuration. |

The `.gitignore` excludes local build output and runtime data. Published benchmark results remain tracked so claims can be inspected with their measurement context.

## CI and release policy

Pull requests run documentation and Rust format, lint, and test checks. Source changes also trigger the applicable Docker cluster, process-failure, and performance comparison workflows. Passing CI supports those test scenarios; it does not establish physical power-loss durability or production suitability. The [failure-testing guide](failure-testing.md) distinguishes process termination, simulated power loss, and hardware power loss.

There is no deployment or release workflow. A local Docker demonstration is an integration exercise, not a published service or package. Release automation requires a separate versioning and release policy.

## Contribution policy

Keep requirements traceable to the [SOW](SOW.md), and document new design choices in the [technical design](Technical-Design.md) or an ADR. State the workload, platform, and limits behind benchmark or durability claims. Commit messages should describe the change and its verification.
