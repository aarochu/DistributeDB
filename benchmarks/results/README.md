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
