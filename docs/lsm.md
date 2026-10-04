# LSM storage engine

DistributeDB has two storage engines for its key/value map. The in-memory engine is the initial approach: a hash map rebuilt from a snapshot and the WAL on every start, so the whole dataset lives in memory. The LSM engine (SOW §16 Option B, Phase 8) keeps recent writes in memory and older data in sorted tables on disk. The dataset no longer has to fit in memory, and the WAL and replication history stay bounded without snapshots.

Choose the engine when a data directory is created:

```sh
cargo run -- serve --data ./data/primary --storage lsm
cargo run -- replica --primary-addr 127.0.0.1:5556 --cluster-id CLUSTER_ID --data ./data/replica-1 --storage lsm --allow-snapshot-rebootstrap
```

`--memtable-bytes N` sets the flush threshold (default 4 MiB). The choice is persisted in `STORAGE_ENGINE`, and reopening a directory with the other engine is refused. Directories created before the LSM engine existed are in-memory directories. A primary and its replicas may use different engines.

## Structure

```text
client write ─► WAL (group commit, fsync) ─► memtable ─► immutable memtable ─► level-0 SSTables ─► level-1 SSTables
                                                           (flush)                     (compaction)
```

- **Memtable:** a sorted map of the newest version of each key since the last flush. Deletes are tombstones, so they can hide older versions on disk.
- **Immutable memtable:** the memtable being flushed. Reads still see it.
- **Level 0:** one table per flush. Level-0 tables may overlap, so a lookup checks all of them, newest first.
- **Level 1:** the bottom level, a run of disjoint tables in key order. A lookup checks at most one.

A lookup stops at the first component holding the key, newest first: memtable, immutable memtable, level 0, level 1. Tables whose key range excludes the key are skipped. Inside a table, a bloom filter (about 1% false positives) usually answers "absent" without reading the disk. Otherwise one 4 KiB block is read through `FileSystem::read_at`.

## Files

Each recovery generation has an `lsm/` directory:

- `MANIFEST` lists the live tables (id, level, length, entry count, key range) and the flush boundary: the WAL LSN and record hash the tables cover. It is replaced atomically by a temp write, sync, rename, and directory sync, and checked by a CRC32C. The layout is in `src/lsm/manifest.rs`.
- `<id>.sst` is an SSTable v1. It holds CRC-checked data blocks, a bloom filter, a block index with each block's last key, and a 64-byte checked footer. The layout is in `src/lsm/sstable.rs`.

Every table and manifest is validated when it is opened, and every block is validated when it is read. Corruption is an error, never a missing key.

## Flush and compaction

A flush runs once the memtable reaches its threshold:

1. Under the database write lock, the WAL is rotated so a segment starts right after the current LSN. The memtable is frozen with that LSN and record hash as its boundary.
2. Without the lock, the frozen memtable is written as a level-0 table, synced, renamed into place, and reopened to verify it.
3. Under the lock, a new manifest lists the table and records the boundary. Replication history up to the boundary is released (see below), and every WAL segment at or below the boundary is deleted.

Once level 0 holds four tables, a compaction merges all of them with the overlapping level-1 tables into new level-1 tables of about 8 MiB. The inputs include every older version of those keys, so tombstones and overwritten values are dropped. Compaction uses the same three steps, and a flush may run while it writes. The table ids it may use are reserved when it is planned, and input files are deleted only after the new manifest is durable.

The server runs flushes and compactions on two background threads, so a long compaction never delays a flush. When a flush is due, the sequencer freezes the memtable and rotates the WAL right after a group, while it still holds the write lock and no group is between append and apply, then hands the frozen memtable to the flush thread. The flush thread writes the table with no lock held and takes the write lock only to install it, so writes and reads continue while tables are written. If the memtable reaches four times its flush size while the previous one is still being written, new writes wait for that flush; this bounds memory on a slow disk. Library callers and replicas run maintenance synchronously after their writes.

## Recovery and crashes

On open, the manifest's tables are the base. Only the WAL after the flush boundary is replayed into the memtable, so recovery work is bounded by the memtable size rather than the write history (SOW Phase 4 goal). A missing WAL continuation is reported as corruption and is never silently skipped.

The atomic steps make every crash point recoverable:

| Crash point | State after reopen |
|---|---|
| While writing a table | The table is not in the manifest and is deleted; its data is replayed from the WAL. |
| After a table is written, before the manifest | Same: the table is unlisted and its records are still in the WAL. |
| After the manifest, before WAL deletion | The covered segments are skipped by replay and deleted at the next flush. |
| During compaction, before the manifest | Outputs are unlisted and deleted; the inputs are intact. |
| After compaction's manifest, before input deletion | The unlisted inputs are deleted. |

A failed flush or compaction makes the node fail closed, like a failed WAL sync: writes return `UNAVAILABLE` and reads continue. A read that cannot be completed, such as a damaged table block, returns `UNAVAILABLE` for that request.

## Replication

Replicas are served from in-memory replication history. A flush releases history up to its boundary, but no further than the slowest connected replica's acknowledged LSN, keeping at most one million extra records. A replica behind the released history is sent a snapshot image built from the tables alone. The tables hold exactly the state at their flush boundary, so the image is consistent without copying data. An LSM replica loads the image as its first table; an in-memory replica installs it as a snapshot. As for the in-memory engine, a replica accepts an image only if it was provisioned with `--allow-snapshot-rebootstrap`.

When a connected replica's records are released mid-stream, for example a disconnected replica that returns after many flushes, the primary closes the connection. The replica's reconnect handshake then offers the image.

## Observability

`STATS` reports `storage_engine` and, for the LSM engine, `lsm_memtable_bytes`, `lsm_tables`, `lsm_level0_tables`, `lsm_table_bytes`, `lsm_flushes_total`, and `lsm_compactions_total`. For the LSM engine, `keys` is an upper bound that counts versions and tombstones not yet compacted away; an exact count would read every table. `snapshot_lsn` reports the replication history base, which is the flush boundary or the pinned replica floor.

## Measured comparison

Against the in-memory engine on a CI runner with 100,000 keys, the LSM server used about 45% less peak memory and was slower on read-heavy work, because reads that reach a table read from disk. Write-heavy results favoured LSM but were within run-to-run variation. One trial showed a write stall during maintenance. See [performance engineering](performance.md#phase-8-lsm-engine-compared-with-the-in-memory-engine) for the ranges.

## Limits

- **Two levels:** level 1 is a single sorted run, and each compaction rewrites the level-1 tables overlapping level 0. For large datasets that is much more write amplification than a multi-level design.
- **One flush and one compaction at a time:** each runs on its own background thread; a burst that outruns a slow flush is held back at four memtables' worth of data.
- **Replica image cap:** a replica image is held in memory and capped at the 256 MiB snapshot-transfer limit. A larger dataset requires reprovisioning a replica that falls that far behind.
- **Scans merge every overlapping source:** `SCAN` merges the memtables with every table whose key range overlaps the scan, starting each table at its first block at or after the start key. Tombstones are skipped, so a range of mostly deleted keys costs reads that return nothing until compaction removes them.
- **Snapshots are in-memory only:** the offline `snapshot` command refuses LSM directories; flushes are the LSM engine's checkpoints.
