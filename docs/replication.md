# Replication and replica recovery

DistributeDB uses one statically configured primary and asynchronous replicas. The primary returns `OK` after its own WAL group is synced and applied; it does not wait for a replica. A replica syncs each received record to its own WAL before acknowledging it. There is no election or automatic promotion.

## Provisioning

Start the primary with separate client and replication loopback ports. Its startup output includes the 32-character cluster ID. Provision each replica with that ID and a separate data directory. On a **new** replica directory, `--allow-snapshot-rebootstrap` persists a policy permitting snapshot replacement when its old WAL prefix can no longer be verified. Omitting the flag persists a deny policy. An existing replica's policy does not change when the flag is added later; the CLI rejects that attempt.

```sh
cargo run -- serve --addr 127.0.0.1:5555 --replication-addr 127.0.0.1:5556 --data ./data/primary
cargo run -- replica --primary-addr 127.0.0.1:5556 --cluster-id CLUSTER_ID --data ./data/replica-1 --allow-snapshot-rebootstrap
```

The demo listener is unauthenticated and bound to loopback. Use a different data directory for every node. The replica process currently has no client listener.

## Reconnect decisions

The replica sends its durable LSN and record hash. The primary checks cluster identity and the hash at that LSN when the prefix is retained. An ahead LSN or a known hash mismatch stops replication as divergence. A matching prefix resumes at the next LSN. A missing prefix requires a snapshot. An unprovisioned replica refuses that transfer and keeps its old data.

A provisioned replica verifies snapshot length, transfer CRC, embedded header and payload checksums, cluster ID, and LSN/hash boundary. It writes a new recovery generation containing the snapshot and an empty continuation WAL, syncs the files and directories, then switches the checked `CURRENT` pointer. It acknowledges the snapshot boundary only after publication. Subsequent records are copied from the primary's post-snapshot WAL. An interrupted transfer before publication leaves the old recovery generation selected.

Snapshot transfers are capped at 256 MiB and use chunks no larger than the 256 KiB frame limit. The current transfer receiver buffers that capped snapshot in memory before installing it. Old recovery generations are retained on disk; garbage collection and streaming to a temporary file remain to be implemented. If the primary publishes a newer snapshot and reclaims continuation WAL during a transfer, catch-up can stop and require a fresh connection. These limits mean snapshot catch-up is not yet a complete bounded-storage implementation.

`STATS` reports a connected replica's last acknowledged applied LSN and sampled lag. A disconnected replica's current lag is unknown. Neither a zero lag sample nor an ACK is a quorum commit or a failover guarantee.
