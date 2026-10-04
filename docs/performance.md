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

A second run on a fresh runner allocation ([workflow run 37185684419](https://github.com/aarochu/DistributeDB/actions/runs/37185684419), same method) reproduced the server-side result: read-lock wait p99 fell 82%, 85% and 89% at 90/10, 50/50 and 10/90. Median throughput changed by +18%, +7% and +1%. The first run's −18% at 10/90 did not recur, which is consistent with run-to-run noise.

## Conclusions and next steps

- The read path no longer depends on `fsync` latency. On storage with slow or variable syncs, where the base server's read tail would grow with the disk's, this matters more than on a CI runner's disk.
- Read-lock wait p99 is now about 0.1 ms. Finer-grained locking such as lock striping, the next step in the SOW's optimization path, is not justified by these measurements.
- Longer measurement windows and dedicated hardware are needed before making client-visible throughput claims.
- The 100,000-write failure test showed that replica apply, at one sync per record with stop-and-wait acknowledgement, limited replication throughput. The next section measures the change that addressed it.

## Replication: pipelined records and grouped replica syncs

### Bottleneck

The primary sent one record and waited for its ACK before sending the next, and the replica synced each record as its own WAL group. Replication therefore ran at one network round trip plus one replica `fsync` per record, while the primary commits up to 64 writes per sync. The 100,000-write failure test saw a replica about 10,000 records behind.

### Change

The primary now sends up to one group (64 records) back to back and accepts ACKs that cover part of the batch. The replica buffers its reads and applies the records already delivered as local groups, one sync each (`Db::apply_replicated_records`).

### Result

`replica_catch_up_ms` is the time from the end of a benchmark's measured window until the replica has applied every write. This comparison ran `d77a9ca` (base) against `a5c431b` (head) with one replica attached, three trials per mix, on the same runner ([`20261004T074644Z-compare-replica-base.csv`](../benchmarks/results/20261004T074644Z-compare-replica-base.csv), [`20261004T074644Z-compare-replica-head.csv`](../benchmarks/results/20261004T074644Z-compare-replica-head.csv)):

| GET/SET | Catch-up, base (ms, range) | Catch-up, head (ms, range) |
|---|---|---|
| 90/10 | 553–703 | 0.1–50 |
| 50/50 | 3,164–3,314 | 50–51 |
| 10/90 | 5,976 | 50–51 |

The benchmark polls for catch-up every 50 ms, so the head values mean the replica had already caught up at the first or second check: it kept pace with the primary instead of trailing it by seconds. Every run ended with the replica caught up. Primary throughput with and without the replica was unchanged within run-to-run variation (median changes of −2% to +1%).

## Phase 8: LSM engine compared with the in-memory engine

SOW Phase 8 asks for benchmarks comparing the advanced index with the initial storage approach. `benchmarks/compare_engines.sh` ran the three GET/SET mixes against a fresh in-memory primary and a fresh LSM primary built from the same revision, alternating the order, with three trials per mix. The run used 32 clients, 100,000 keys of 128-byte values (several times the 4 MiB memtable, so LSM reads go to tables on disk), and 20,000 measured operations, on a GitHub-hosted `ubuntu-24.04` runner on 2026-10-04. Raw rows: [`20261004T183724Z-engine-memory.csv`](../benchmarks/results/20261004T183724Z-engine-memory.csv) and [`20261004T183724Z-engine-lsm.csv`](../benchmarks/results/20261004T183724Z-engine-lsm.csv).

Ranges over three trials:

| GET/SET | Engine | ops/s | Server read p99 µs | Server peak RSS MiB |
|---|---|---|---|---|
| 90/10 | memory | 47,300–61,700 | 93–132 | 51.5–53.5 |
| 90/10 | LSM | 32,000–40,000 | 742–882 | 30.1–30.8 |
| 50/50 | memory | 26,600–43,000 | 93–132 | 53.8–55.0 |
| 50/50 | LSM | 8,900–32,700 | 371–441 | 30.8–30.8 |
| 10/90 | memory | 10,200–18,300 | 111–156 | 55.7–55.9 |
| 10/90 | LSM | 16,300–30,200 | 221–263 | 29.1–30.8 |

What the ranges support:

- **Memory:** the LSM server peaked at about 30 MiB against about 54 MiB for the in-memory server in every trial, because only the memtable and table indexes and bloom filters stay in memory. That gap grows with the dataset.
- **Read latency:** an LSM read that reaches a table costs a bloom check and a block read, so the server-side read p99 was 2 to 9 times the in-memory engine's in every trial. Read-heavy throughput was lower in every trial (32,000–40,000 against 47,300–61,700 ops/s).
- **Write-heavy throughput:** LSM ranged 16,300–30,200 ops/s and the in-memory engine 10,200–18,300. The medians favour LSM by 34%, but the ranges touch, so three trials do not establish it.
- **Stalls:** one 50/50 LSM trial ran at 8,900 ops/s with a 107 ms client p99. Flushes and compactions run on the sequencer thread and hold back new writes while a table is written ([limits](lsm.md#limits)). Moving maintenance to its own thread is the next measured change it suggests.
- **Disk:** the LSM directories were 11 to 13% smaller, mainly because the LSM engine deletes WAL covered by flushes while the in-memory engine keeps all WAL without a snapshot.

As with the other comparisons, this is one shared CI runner and sub-second windows. It shows the engines' relative behaviour on that runner, not absolute performance.
