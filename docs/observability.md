# Observability

`STATS` returns versioned `name=value` lines, with `version` first. Read it with `distributedb client --addr HOST:PORT --stats`. Counters and latency samples reset when the process restarts; LSNs persist.

| Field | Meaning |
|---|---|
| `uptime_seconds` | Seconds since this server started. |
| `keys` | Keys in the in-memory map. |
| `requests_total`, `reads_total`, `writes_total` | Requests dispatched. Writes are counted when attempted, including ones later rejected. `current_lsn` counts committed writes. |
| `current_lsn`, `durable_lsn` | Highest applied LSN, and highest LSN synced to the WAL. |
| `snapshot_lsn` | LSN of the snapshot used as the recovery base (0 if none). |
| `connected_clients` | Open client connections, including the one issuing `STATS`. |
| `role`, `durability` | `primary` or `replica`, and `fsync` or the benchmark-only `os` mode. |
| `wal_entries` | WAL records after the snapshot boundary: the records a restart would replay. |
| `read_latency_p50_us`, `_p95_us`, `_p99_us` | Server-side service time of `GET` and `EXISTS`, in microseconds. |
| `write_latency_p50_us`, `_p95_us`, `_p99_us` | Server-side service time of `SET` and `DELETE`, including sequencer queueing and the group commit. |
| `read_lock_wait_p50_us`, `_p95_us`, `_p99_us` | Time a `GET` or `EXISTS` waited for the database read lock. A high tail here means reads are queued behind the sequencer's write lock. |
| `write_lock_hold_p50_us`, `_p95_us`, `_p99_us` | Time the sequencer held the database write lock for one group commit. |
| `wal_syncs_total`, `wal_sync_errors_total` | Group-commit syncs completed and failed since startup. The first failure makes the node fail closed. |
| `wal_sync_avg_us`, `wal_sync_max_us` | Mean and longest group-commit sync time. |
| `recovery_us` | Time the last startup spent recovering: from taking the data-directory lock through loading the snapshot and replaying the WAL tail. |
| `recovery_records_replayed` | WAL records replayed after the snapshot during that recovery. |
| `storage_engine` | `memory` or `lsm`. For `lsm`, `keys` is an upper bound that counts versions and tombstones not yet compacted. |
| `lsm_memtable_bytes`, `lsm_tables`, `lsm_level0_tables`, `lsm_table_bytes` | LSM engine only: memtable size, installed tables, level-0 tables, and table bytes on disk. |
| `lsm_flushes_total`, `lsm_compactions_total` | LSM engine only: flushes and compactions installed since startup. See [the LSM engine](lsm.md). |
| `replicas_connected` | Replicas currently streaming from this primary. |
| `sync_replicas` | Replica ACKs each write waits for before `OK`; `0` is asynchronous replication. |
| `sync_ack_timeouts_total` | Writes answered `UNAVAILABLE` because their replica ACKs did not arrive within the sync timeout. Each is durable on the primary. |
| `replica_<id>_applied_lsn`, `replica_<id>_lag` | Each known replica's last acknowledged LSN, and its distance from `durable_lsn`. Lag is `unknown` while the replica is disconnected. |

## Latency measurement

Service time runs from a decoded request frame to its encoded response. It excludes network transit and the client, so client-observed latency (for example from `ddb_bench`) is higher. Error responses that cannot be attributed to a request kind are not sampled.

Samples go into a lock-free histogram with quarter-octave buckets. A reported percentile is the upper bound of the bucket that contains it. It never understates the sampled latency and overstates it by at most about 19%. Percentiles cover all requests since startup, not a sliding window. A latency field reads `unknown` until its first sample, as do the WAL sync fields until the first sync. In `os` durability mode no sync occurs, so the sync fields stay `unknown`.

## Recovery time

`recovery_us` and `recovery_records_replayed` show how much a snapshot reduces restart work (SOW §9, §27). After a snapshot, `recovery_records_replayed` is bounded by `current_lsn - snapshot_lsn` at the time of the restart. `recovery_us` is wall time on the host, so compare values only on the same host and storage.
