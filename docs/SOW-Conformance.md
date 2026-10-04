# SOW conformance audit

This audit maps the implemented core to [the Statement of Work](SOW.md). It
records code and automated checks that support each claim; it is not a proof
of correctness under every operating-system or hardware failure. The SOW
defines the required scope. [The technical design](Technical-Design.md) and
[ADR-001](ADR-001-Language-and-Filesystem.md) add concrete choices where the
SOW leaves behavior open.

| SOW area | Implementation evidence | Verification and limit |
| --- | --- | --- |
| §4, §6: `SET`, `GET`, `DELETE`, `EXISTS` and in-memory map | `src/command`, `src/storage`, `src/wal` | Module unit tests and `tests/networking.rs` exercise the four operations. The local REPL is intentionally volatile. |
| §5, §13: concurrent TCP clients, framing, malformed/partial requests, disconnects, shutdown | `src/protocol`, `src/client`, `src/server` | `tests/networking.rs` covers concurrent clients and adverse framing. Shutdown now interrupts and joins connection workers before releasing the database lock; reopening no longer depends on a retry. |
| §7–8: WAL, acknowledged write recovery, restart after kill | `src/wal`, `src/fileio`, `src/bin/wal_kill_harness.rs` | `tests/wal_recovery.rs`, `tests/wal_crash_sim.rs`, `tests/wal_cutpoints.rs`, and `tests/process_failure.rs` exercise recovery. That last file includes SOW §19's example at full size: a primary killed during 100,000 concurrent writes, with every acknowledged write checked after restart. Simulated torn writes and process kills do not establish physical power-loss guarantees. |
| §9: snapshots and post-snapshot WAL replay | `src/wal/snapshot.rs`, `src/wal/current.rs`, `src/main.rs` snapshot command | Snapshot publication/recovery tests and `tests/snapshot_publish_crash.rs` cover the local path. Snapshot creation is manual and offline. |
| §10–12, §14: ordered primary/replica stream, ACKs, reconnect, lag, failure handling | `src/replication`, `src/main.rs` | `tests/replication.rs` and `tests/replica_cli.rs` cover ordinary replication and catch-up. Asynchronous primary `OK` guarantees local durability only; replica reads may lag. Reclaimed history needs explicitly provisioned snapshot rebootstrap. There is no automatic failover. |
| §17: runtime statistics | `src/server`, `src/server/latency.rs`, [observability](observability.md) | `tests/networking.rs` checks counters, latency, WAL sync, recovery-time, and lock fields. Disconnected replica lag is unknown; measurements are process-local samples. |
| §18, Phase 7: benchmarks, published results, contention analysis | `src/bin/ddb_bench.rs`, `benchmarks/`, [performance analysis](performance.md) | Three 90/10, 50/50, and 10/90 mixes, CSV output, and published CI-runner results exist. Lock wait and hold are measured, and one optimization (syncing outside the database lock) was compared base-versus-head on the same runner. The shared runner is not a controlled hardware benchmark. |
| §19–20: unit, integration, and failure testing | `src` module tests, `tests/`, `scripts/run_failure_trials.sh` | Rust CI runs format, lint, and all targets; separate workflows run failure trials and a Docker cluster. Test coverage does not imply exhaustive fault tolerance. |
| §25–27: architecture docs, local cluster, three-node demo | `README.md`, `docs/Technical-Design.md`, `compose.yaml`, `scripts/demo.sh` | The Docker workflow runs the cluster smoke/demo path. It does not exercise deployment outside its isolated CI environment. |

## SOW scope boundaries

- SOW §11 permits asynchronous replication first. Synchronous or semi-synchronous
  acknowledgment is not a core acceptance condition and is not implemented.
- SOW §15 makes transactions optional; they are not implemented. SOW §16 and
  §22 Phase 8 ask for one advanced index: the LSM tree is implemented as an
  alternative storage engine ([LSM storage engine](lsm.md)), and the in-memory
  map remains the default.
- SOW §23–24 exclude or defer Raft, automatic failover, sharding, distributed
  transactions, and production authentication. Their absence is intentional.
- SOW §21 is a recommended example layout, not a language or path contract.
  The Rust layout follows ADR-001 and retains the named project components.

## Remaining validation

Physical power-loss durability requires validation on the stated filesystem profile and storage hardware. Client-visible throughput claims need longer measurement windows on dedicated hardware; the CI-runner comparisons show server-side effects, not hardware-independent throughput.
