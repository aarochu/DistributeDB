# Performance engineering

This document records Phase 7's lock-contention analysis and the measured effect of the one optimization it justified. The method follows SOW Phase 7: measure first, change one thing, then compare base and head on the same host.

## Method

`benchmarks/compare_revisions.sh BASE HEAD` builds both revisions in temporary worktrees on one host. It runs each GET/SET mix against a fresh single `fsync` primary per run, five trials per mix, alternating which revision goes first. Each run uses 32 clients, 10,000 prepopulated keys, 128-byte values, 2,000 warm-up and 20,000 measured operations. `ddb_bench` records client-observed throughput and latency, and copies the server's own `STATS` percentiles into the same CSV row (see [observability](observability.md)). The **Performance comparison** workflow runs the script on pull requests that change the server, WAL, or storage code.

## Lock contention before the change

The server keeps the map and WAL behind one reader/writer lock. Reads take it shared; the single write sequencer takes it exclusively to append a group, `fsync` it, and apply it. Before the change the sequencer held the write lock **across the `fsync`**.

In the base runs, every read's server-side p99 equalled its read-lock wait p99. All of the read tail was time spent waiting for the lock, between 1,049 and 4,988 µs, while the mean WAL sync took 206 to 552 µs. A read that arrives during a group commit waits for the append, the sync, and the apply. With a writer-preferring lock, it can also wait for the next queued group.

The lock-hold metric agreed: the median write-lock hold p99 was 742 to 1,049 µs per group, almost all of it the sync.

## The change

The group commit now has three steps (`Db::begin_group`, `PendingGroup::sync`, `Db::finish_group`):

1. Under the write lock, assign LSNs and append the group and its footer.
2. With no database lock held, `fsync` the segment.
3. Under the write lock, apply the group and release the responses, or fail closed if the sync failed.

Readers keep seeing only acknowledged state during the sync. WAL bytes and the order of file operations are unchanged, so recovery and the crash models are unaffected. Nothing else may append, rotate, or snapshot while a group is pending.

## Results

Comparison of `1d8fa17` (base) and `30bcfeb` (head), GitHub-hosted `ubuntu-24.04` runner, ext4, 2026-10-04. Raw rows: [`20261004T072003Z-compare-base.csv`](../benchmarks/results/20261004T072003Z-compare-base.csv) and [`20261004T072003Z-compare-head.csv`](../benchmarks/results/20261004T072003Z-compare-head.csv).

Server-side (µs; lock wait as the range over five trials, lock hold as the median):

| GET/SET | Read-lock wait p99, base | Read-lock wait p99, head | Write-lock hold p99, base | Write-lock hold p99, head |
|---|---|---|---|---|
| 90/10 | 1,049–4,988 | 111–186 | 1,049 | 47 |
| 50/50 | 1,049–1,247 | 93–156 | 882 | 93 |
| 10/90 | 1,049–2,098 | 78–156 | 742 | 93 |

The ranges do not overlap in any mix. Taking the sync out of the critical section cut the read tail inside the server by roughly a factor of ten, and cut write-lock hold time by about 90%.

Client-observed, median of five (range):

| GET/SET | ops/s, base | ops/s, head | client p99 µs, base | client p99 µs, head |
|---|---|---|---|---|
| 90/10 | 50,960 (32,300–52,600) | 56,657 (13,400–63,500) | 2,532 | 3,346 |
| 50/50 | 24,841 (22,000–36,800) | 36,021 (30,800–44,500) | 2,364 | 2,715 |
| 10/90 | 30,765 (23,400–32,300) | 25,309 (20,700–33,400) | 2,061 | 2,196 |

These differences are within run-to-run variation on this runner. The base and head throughput ranges overlap in every mix, and one head trial at 90/10 ran at a quarter of the others' throughput. The client p99 covers all operations, so it is dominated by writes, which still wait for their own sync. Client, server and benchmark share the runner's cores, and each measured window lasted under a second. This run does not show a client-visible throughput gain or loss. It does show that reads no longer queue behind disk flushes.

## Conclusions and next steps

- The read path no longer depends on `fsync` latency. On storage with slow or variable syncs, where the base server's read tail would grow with the disk's, this matters more than on a CI runner's disk.
- Read-lock wait p99 is now about 0.1 ms. Finer-grained locking such as lock striping, the next step in the SOW's optimization path, is not justified by these measurements.
- Longer measurement windows and dedicated hardware are needed before making client-visible throughput claims.
- The 100,000-write failure test showed that replica apply, at one sync per record with stop-and-wait acknowledgement, limits replication throughput. That is the next measured change.
