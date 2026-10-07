# Local cluster launcher and dashboard plan

Status: implemented. `distributedb cluster` (`src/cluster`), the `ddb_dashboard` bridge (`src/dashboard`, `src/bin/ddb_dashboard.rs`), and the page (`web/`) follow [the design document](frontend-design-doc.md). Differences from this plan:

- There is no `--fresh` flag; a new `--data` directory starts an empty cluster, so the launcher never deletes data.
- A launcher-started dashboard enables the write console and node control without `--allow-writes`, because the cluster is local and disposable. The standalone bridge stays read-only unless started with `--allow-writes`.
- The write-path model uses WebGL directly rather than Three.js, so the page loads nothing from the network and needs no vendored library.
- `STATS` gained a `node_id` line so the page can match a replica process to the primary's `replica_<id>_*` fields.
- Every `POST` must carry `x-ddb-request: 1` and every request a loopback `Host`, so another web page cannot drive the bridge.

The SOW does not require a user interface. Every SOW item is implemented and verified through the CLI, `STATS`, tests, and benchmarks. This plan covers two optional tools for running and demonstrating DistributeDB locally:

1. a **cluster launcher** that starts a primary and replicas with one command, and
2. a **read-only dashboard** that shows the cluster's behavior live.

The launcher comes first. It removes most of the effort of running locally, and the dashboard needs the node addresses it knows.

## Constraints

- **Std only.** Like the rest of the crate, both tools use the Rust standard library and no external crates.
- **No HTTP in the database.** The server and replication protocols stay unchanged. The dashboard is a separate process that talks to nodes over the existing client protocol, so it cannot affect durability, recovery, or the tests.
- **Loopback only.** Both tools bind to `127.0.0.1` and have no authentication, matching the demo listeners (SOW §23–24 exclude production authentication).
- **Cross-platform.** Both must work on Linux, macOS, and Windows without Docker. The Docker cluster in [docker-cluster.md](docker-cluster.md) stays as the reproducible SOW §26 demonstration.

## 1. Cluster launcher

```sh
cargo run -- cluster --replicas 2
```

Today a local cluster takes four terminals: start the primary, copy its printed cluster ID, start each replica with that ID and its own ports and data directory, then start a client. The launcher does this in one process tree:

- Starts the primary and *N* replicas as child processes of the same `distributedb` binary, each with its own data directory and ports.
- Reads the primary's cluster ID and passes it to each replica, so there is nothing to copy.
- Prints every node's client, replication, and read address, then opens the client prompt connected to the primary.
- On exit (EOF, `shutdown`, or Ctrl-C), stops replicas and then the primary, and waits for each process.
- Accepts the relevant `serve` flags and passes them through: `--storage lsm`, `--sync-replicas N`, `--sync-timeout-ms`.
- Uses `--data DIR` as a parent directory (`DIR/primary`, `DIR/replica-01`, …), so a second run reuses the same cluster; a new directory starts an empty one.
- Exposes each replica's eventually consistent read port (`--read-addr`), so replica reads can be tried directly.
- Supports `stop replica-1` and `start replica-1` at the prompt, so catch-up after downtime can be shown without a second terminal.

Tests: start a two-replica cluster, write through the prompt, stop and restart a replica, and check that it converges; check that exit leaves no child processes running.

## 2. Dashboard

```sh
cargo run --bin ddb_dashboard -- --nodes 127.0.0.1:5555,127.0.0.1:5557,127.0.0.1:5559
```

The launcher can start the dashboard itself with `--dashboard`. The dashboard has a small bridge and one static page:

- **Bridge** (`src/bin/ddb_dashboard.rs`): polls `STATS` from each node about once per second, keeps a few minutes of history in memory, and serves the page plus a JSON endpoint on a loopback HTTP port.
- **Page:** one HTML file with inline JavaScript and charts. It needs no build step and no external network access.

### Must have

1. **Cluster overview.** One card per node: role, up or down, `uptime_seconds`, `storage_engine`, and `durability`. Replicas are coloured caught up, lagging, or disconnected.
2. **Replication.**
   - The primary's `durable_lsn` against each `replica_<id>_applied_lsn`, as a live chart.
   - `replica_<id>_lag` (shown as unknown while disconnected) and `replicas_connected`.
   - In synchronous mode, `sync_replicas` and `sync_ack_timeouts_total`.
3. **Throughput and latency.**
   - Operations per second, derived from changes in `reads_total` and `writes_total` between polls.
   - `read_latency_*` and `write_latency_*` at p50, p95, and p99.
4. **Durability and WAL.**
   - `current_lsn` against `durable_lsn`.
   - `wal_syncs_total`, `wal_sync_avg_us`, `wal_sync_max_us`, and `wal_sync_errors_total`, highlighted if ever non-zero.
   - `wal_entries` and `snapshot_lsn`.
5. **Key explorer** (read-only): `GET` and `EXISTS` for a key, and paged `SCAN` over a range, with a hex toggle for binary keys and values.
6. **Recovery.** `recovery_us` and `recovery_records_replayed` from each node's last start, which show what a snapshot saves.

### Should have

7. **LSM panel**, shown only with `--storage lsm`:
   - `lsm_memtable_bytes`, `lsm_tables`, `lsm_level0_tables`, and `lsm_table_bytes`.
   - `lsm_flushes_total` and `lsm_compactions_total` over time.
8. **Lock contention.** `read_lock_wait_*` and `write_lock_hold_*` percentiles: the metrics behind the lock-scope change in [performance.md](performance.md).
9. **Guided demonstration.** Mirrors `scripts/demo.sh`: write keys, stop a replica, keep writing, restart it, and watch it catch up. Stopping and starting nodes goes through the launcher.
10. **Write console**: `SET`, `DELETE`, and a transaction box (`BEGIN`, queued writes, `COMMIT` or `ROLLBACK`). This is the only feature that writes, so it is a separate step and is off unless the dashboard is started with `--allow-writes`.

### Out of scope

- Authentication, user accounts, cluster reconfiguration, and failover controls. The SOW excludes them.
- Serving the dashboard beyond loopback.
- Persisting metric history; restarting the dashboard starts a fresh history.

## Delivery

| Step | Contents |
|---|---|
| 1 | `cluster` subcommand with start, stop, and restart of nodes, plus its tests. |
| 2 | Dashboard bridge and page with must-have items 1–6, plus `--dashboard` on the launcher. |
| 3 | Should-have items 7–9. |
| 4 | Write console (item 10), behind `--allow-writes`. |

Each step is a separate pull request and is useful on its own.
