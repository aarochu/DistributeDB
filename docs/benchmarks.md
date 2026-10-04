# Benchmark method

`ddb_bench` measures client-observed GET/SET completion time against an already running server. It prepopulates a fixed number of keys, so measured reads should hit and measured writes overwrite keys. The key distribution is uniform from a recorded seed. Warm-up operations run before the timed interval. Client connections operate concurrently; the requested operation count is divided across them. The timed result counts successful operations, failures, and unattempted operations separately. A network error can leave the last write's outcome unknown.

Use `benchmarks/run_benchmarks.sh` for three repetitions of each SOW mix: 90/10, 50/50, and 10/90 GET/SET. Each invocation writes rows under `benchmarks/results/` unless `DDB_OUTPUT` specifies another path. Set `DDB_ADDR`, `DDB_DURABILITY`, `DDB_REPLICAS`, `DDB_DATA_DIR`, and `DDB_SERVER_PID` to match the running server. Supply `DDB_FILESYSTEM`, `DDB_STORAGE_MEDIUM`, and `DDB_TOPOLOGY` so the result can be interpreted. Compare `os` on a disposable single primary with `fsync` on one primary and with one or two asynchronous replicas. Do not use the same data directory for the `os` and `fsync` runs.

Example:

```sh
DDB_ADDR=127.0.0.1:5555 \
DDB_DURABILITY=fsync DDB_REPLICAS=0 \
DDB_DATA_DIR=./data/primary DDB_SERVER_PID=12345 \
DDB_FILESYSTEM=ext4 DDB_STORAGE_MEDIUM=local-ssd \
DDB_TOPOLOGY=one-primary \
bash benchmarks/run_benchmarks.sh
```

The CSV includes revision, build mode, operating system, architecture, kernel, selected metadata, workload parameters, elapsed time, throughput, average and p50/p95/p99 successful-operation latency, failure counts, replica catch-up status, and data/WAL sizes. On Linux, `--server-pid` also samples server CPU ticks and peak RSS from `/proc`; those columns are empty when unavailable. CPU percent uses `getconf CLK_TCK`. Peak RSS is the process lifetime high-water mark, not an interval-only peak. File sizes are sampled after measurement and may include concurrent replica or snapshot activity. The `server_*` columns copy the server's own `STATS` after the run: read p99, read-lock wait p99, write-lock hold p99, and mean WAL sync time (see [observability](observability.md)). They are cumulative since the server started, so they describe one run only when each run uses a fresh server. `replica_catch_up_ms` is the time from the end of the measured window until every replica had applied the run's writes, which shows how far replication trailed the workload. No throughput target is implied by the harness.

`benchmarks/ci_benchmark.sh` starts each configuration locally (fsync primary, disposable `os` primary, fsync primary with one asynchronous replica, and fsync primary with two asynchronous replicas) and runs the three mixes against it; the **Benchmarks** GitHub workflow runs it and uploads the CSV. Client, server, and replicas share one runner, so these rows measure the whole stack on that runner rather than a networked deployment.

Publish individual CSV rows and a summary of median and variability for each configuration only after the runs have been executed. State the host, storage device, filesystem and mount options, node placement, and whether any result was excluded. A CI runner result is useful for regression comparison on that runner class but is not a hardware-independent performance claim.

## Snapshot and WAL recovery comparison

`benchmarks/run_recovery_benchmark.sh` measures the SOW §9 claim that a
snapshot reduces WAL replay work. It creates two temporary `fsync` data
directories and applies the same ordered SET mutations to both. One keeps a
full WAL; the other publishes a snapshot after the initial records. Both then
receive the same short tail. The benchmark verifies every final key/value,
the final LSN, and the expected replay count before writing its CSV. Temporary
data is removed; the CSV and context file remain.

```sh
DDB_RECOVERY_RECORDS=10000 DDB_RECOVERY_TAIL=500 \
  DDB_RECOVERY_KEYS=2000 DDB_RECOVERY_TRIALS=7 \
  bash benchmarks/run_recovery_benchmark.sh
```

Each path is opened once to warm the filesystem cache. The seven measured
opens alternate which path goes first. CSV rows contain the internal recovery
duration, whole `Db::open` duration, replayed records, snapshot LSN, and data
size. The companion context file records revision, UTC time, Rust version,
kernel, reported filesystem, and runner. The GitHub recovery benchmark workflow
runs this procedure and uploads both files. The two directories have distinct
node identities but identical mutations and final key/value state. A snapshot
adds a separate file and changes the disk layout, so elapsed times compare
these two recovery strategies rather than isolating the cost of replay alone.
The replay counts are the direct evidence for the bounded-work claim. Timing
on a shared runner is a diagnostic sample, not a fixed restart-time guarantee
or evidence of power-loss durability.
