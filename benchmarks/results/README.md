# Benchmark results

Each CSV in this directory holds one row per `ddb_bench` run, in the format described in [the benchmark method](../../docs/benchmarks.md). Summaries below are computed from those rows; nothing has been excluded.

## 2026-10-01 — GitHub Actions runner

Source: [`20261001T051711Z-ci.csv`](20261001T051711Z-ci.csv), produced by [workflow run 36819023051](https://github.com/aarochu/DistributeDB/actions/runs/36819023051) with `benchmarks/ci_benchmark.sh`.

- Host: GitHub-hosted `ubuntu-24.04` runner, Linux `6.17.0-1022-azure`, x86_64, ext4 on the runner's disk. Release build of revision `81be019` (the pull-request merge commit for pull request #10's head `5737b3d`).
- Placement: the client, primary and replica all ran on the same runner over loopback.
- Workload: 32 clients, 10,000 prepopulated keys, 128-byte values, uniform keys, 2,000 warm-up and 20,000 measured operations per run, seeds 1–3 for each mix. Every run completed with zero failed or skipped operations, and the replica had caught up after every replicated run.

| Configuration | GET/SET | ops/s median (min–max) | p50 µs | p95 µs | p99 µs | Server CPU % | Peak RSS KiB | WAL bytes |
|---|---|---|---|---|---|---|---|---|
| One primary, `fsync` | 90/10 | 38,033 (37,876–40,637) | 550 | 2,474 | 3,623 | 108 | 11,796 | 4,996,237 |
| One primary, `fsync` | 50/50 | 29,707 (29,198–30,085) | 1,088 | 2,160 | 3,066 | 104 | 26,864 | 15,524,601 |
| One primary, `fsync` | 10/90 | 22,823 (21,662–23,030) | 1,386 | 1,992 | 3,757 | 96 | 44,996 | 30,627,136 |
| One primary, `os` | 90/10 | 50,985 (50,674–51,740) | 494 | 1,610 | 2,702 | 119 | 11,676 | 5,004,157 |
| One primary, `os` | 50/50 | 41,824 (41,607–42,058) | 659 | 1,590 | 1,959 | 127 | 26,676 | 15,562,201 |
| One primary, `os` | 10/90 | 36,638 (36,555–37,741) | 859 | 1,354 | 1,748 | 136 | 45,644 | 30,747,176 |
| Primary + 1 replica, `fsync` | 90/10 | 37,280 (36,968–38,033) | 573 | 2,747 | 4,033 | 118 | 11,804 | 4,995,317 |
| Primary + 1 replica, `fsync` | 50/50 | 26,834 (20,352–27,359) | 1,201 | 2,411 | 3,590 | 153 | 26,752 | 15,521,561 |
| Primary + 1 replica, `fsync` | 10/90 | 21,331 (20,958–21,502) | 1,506 | 2,122 | 3,882 | 171 | 44,868 | 30,629,456 |

Limits on interpretation:

- Each measured interval lasted roughly 0.4–1 s, so these numbers are short-window samples. Three repetitions per cell bound run-to-run noise on this runner only.
- One data directory per configuration served all nine runs in the order 90/10, 50/50, 10/90. Data and WAL sizes therefore accumulate across runs, and peak RSS is the server's lifetime high-water mark.
- Server CPU above 100% means more than one core was busy. The client shared the runner's cores with the server and replica.
- CI-runner disks do not represent production storage, and `os` mode makes no restart-durability promise. Use these rows to compare revisions on the same runner class, not as a hardware-independent performance claim.

## 2026-10-01 — four-topology CI run

Source: [`20261001T053120Z-ci.csv`](20261001T053120Z-ci.csv), produced by [workflow run 36820141192](https://github.com/aarochu/DistributeDB/actions/runs/36820141192). This is a separate runner allocation from the preceding table; compare topologies **within this run**, not throughput values across the two runs.

- Host: GitHub-hosted `ubuntu-24.04` runner, Linux `6.17.0-1022-azure`, x86_64, ext4 reported by `df -T`. The storage device and mount options were not captured. Client, primary, and replicas shared one host and loopback network.
- Build: release at recorded revision `41a2a69851f5839467a7658b2784df39dc888f71`, the pull-request test merge commit for the two-replica benchmark change before it was rebased onto `main`. The rebase added only the offline `snapshot` CLI command; the benchmark binary, runner, and client/replication request paths were unchanged. Each topology used a separate data directory.
- Workload: 32 clients, 10,000 prepopulated keys, 128-byte values, uniform key distribution, 2,000 warm-up and 20,000 measured operations per run. Three seeds per GET/SET mix and topology produced 36 rows. All rows report 20,000 successful operations, zero failures, zero skipped operations, zero read misses, and caught-up replicas at the post-run check.

The table reports the median of three rows per cell. Throughput parentheses give the minimum and maximum; latency columns are medians of each row's successful-operation percentile, in microseconds.

| Topology | GET/SET | ops/s median (min–max) | p50 µs | p95 µs | p99 µs | Server CPU % |
|---|---|---:|---:|---:|---:|---:|
| Primary, `fsync` | 90/10 | 44,146 (43,136–44,527) | 523 | 2,198 | 2,981 | 114 |
| Primary, `fsync` | 50/50 | 33,977 (33,625–34,157) | 941 | 1,906 | 2,185 | 113 |
| Primary, `fsync` | 10/90 | 27,848 (27,722–28,178) | 1,187 | 1,564 | 1,768 | 110 |
| Primary, `os` | 90/10 | 53,611 (53,381–54,903) | 487 | 1,467 | 2,515 | 118 |
| Primary, `os` | 50/50 | 45,114 (44,393–45,139) | 586 | 1,513 | 1,900 | 129 |
| Primary, `os` | 10/90 | 38,787 (38,262–38,965) | 824 | 1,313 | 1,931 | 136 |
| Primary + 1 replica, `fsync` | 90/10 | 41,431 (40,682–42,170) | 519 | 2,502 | 3,491 | 122 |
| Primary + 1 replica, `fsync` | 50/50 | 31,252 (31,232–31,368) | 1,015 | 2,074 | 2,412 | 159 |
| Primary + 1 replica, `fsync` | 10/90 | 25,256 (24,906–25,275) | 1,315 | 1,786 | 2,049 | 177 |
| Primary + 2 replicas, `fsync` | 90/10 | 38,732 (38,713–39,657) | 534 | 2,640 | 3,694 | 130 |
| Primary + 2 replicas, `fsync` | 50/50 | 29,008 (28,838–29,198) | 1,063 | 2,247 | 2,617 | 196 |
| Primary + 2 replicas, `fsync` | 10/90 | 23,350 (22,774–24,068) | 1,402 | 1,946 | 2,240 | 238 |

Each measured interval lasted less than a second. Configuration order was fixed, and each data directory accumulated WAL through its nine runs. Server CPU above 100% means more than one core was busy. Peak RSS, data bytes, and WAL bytes are in the individual CSV rows. These short CI-runner measurements identify a repeatable comparison procedure, not a general throughput or durability guarantee; `os` mode has no restart-durability promise.
