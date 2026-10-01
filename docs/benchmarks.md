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

The CSV includes revision, build mode, operating system, architecture, kernel, selected metadata, workload parameters, elapsed time, throughput, average and p50/p95/p99 successful-operation latency, failure counts, replica catch-up status, and data/WAL sizes. On Linux, `--server-pid` also samples server CPU ticks and peak RSS from `/proc`; those columns are empty when unavailable. CPU percent uses `getconf CLK_TCK`. Peak RSS is the process lifetime high-water mark, not an interval-only peak. File sizes are sampled after measurement and may include concurrent replica or snapshot activity. No throughput target is implied by the harness.

`benchmarks/ci_benchmark.sh` starts each configuration locally (fsync primary, disposable `os` primary, fsync primary with one asynchronous replica) and runs the three mixes against it; the **Benchmarks** GitHub workflow runs it and uploads the CSV. Client, server, and replica share one runner, so these rows measure the whole stack on that runner rather than a networked deployment.

Publish individual CSV rows and a summary of median and variability for each configuration only after the runs have been executed. State the host, storage device, filesystem and mount options, node placement, and whether any result was excluded. A CI runner result is useful for regression comparison on that runner class but is not a hardware-independent performance claim.
