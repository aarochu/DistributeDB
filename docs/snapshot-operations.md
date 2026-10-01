# Local snapshot operation

The core library can publish a checked snapshot at the current applied LSN. The `snapshot` command makes this operation available for a **stopped primary**:

```sh
cargo run -- snapshot --data ./data/primary
```

Stop the `serve` process first. The command opens the same data directory with an exclusive lock, recovers its current snapshot and WAL, publishes a new snapshot, and prints its LSN. If the server still holds the lock, the command exits with an error before changing the snapshot. Restart `serve` with the same data directory afterward. The command uses `fsync` durability and refuses a replica data directory.

After publication, recovery loads the verified snapshot and replays the WAL tail after its LSN. The integration test in `tests/snapshot_cli.rs` writes 100 records, publishes a snapshot at LSN 100, verifies zero records replayed on restart, writes one more record, and verifies one record replayed on the next restart. It also checks that a concurrent snapshot command is rejected while the database is open.

This is a manual, offline operation. It pauses client availability for the duration of shutdown, recovery, snapshot writing, and restart. Automatic scheduling and nonblocking live snapshot publication are not implemented. A snapshot is part of this node's recovery chain; copying a snapshot file alone is not a complete backup procedure. See [recovery](recovery.md) for failure handling and [the design](Technical-Design.md) for publication ordering.
