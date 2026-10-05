# Replication and replica recovery

DistributeDB uses one statically configured primary and asynchronous replicas. The primary returns `OK` after its own WAL group is synced and applied; it does not wait for a replica. A replica syncs each received record to its own WAL before acknowledging it. There is no election or automatic promotion.

## Provisioning

Start the primary with separate client and replication loopback ports. Its startup output includes the 32-character cluster ID. Provision each replica with that ID and a separate data directory. On a **new** replica directory, `--allow-snapshot-rebootstrap` persists a policy permitting snapshot replacement when its old WAL prefix can no longer be verified. Omitting the flag persists a deny policy. An existing replica's policy does not change when the flag is added later; the CLI rejects that attempt.

```sh
cargo run -- serve --addr 127.0.0.1:5555 --replication-addr 127.0.0.1:5556 --data ./data/primary
cargo run -- replica --primary-addr 127.0.0.1:5556 --cluster-id CLUSTER_ID --data ./data/replica-1 --allow-snapshot-rebootstrap
```

The demo listener is unauthenticated and bound to loopback. Use a different data directory for every node. A replica may add `--read-addr 127.0.0.1:5557` for a client read listener. Its `GET` and `EXISTS` responses reflect the replica's current applied prefix and may be stale; `SET` and `DELETE` return `NOT_PRIMARY`. Keep the read listener on loopback unless it is isolated on a trusted local container network.

## Reconnect decisions

The replica sends its durable LSN and record hash. The primary checks cluster identity and the hash at that LSN when the prefix is retained. An ahead LSN or a known hash mismatch stops replication as divergence. A matching prefix resumes at the next LSN. A missing prefix requires a snapshot. An unprovisioned replica refuses that transfer and keeps its old data.

A provisioned replica verifies snapshot length, transfer CRC, embedded header and payload checksums, cluster ID, and LSN/hash boundary. It writes a new recovery generation containing the snapshot and an empty continuation WAL, syncs the files and directories, then switches the checked `CURRENT` pointer. It acknowledges the snapshot boundary only after publication. Subsequent records are copied from the primary's post-snapshot WAL. An interrupted transfer before publication leaves the old recovery generation selected.

## Known divergence: explicit replacement

A known history-hash mismatch is a fatal reconnect error even when `--allow-snapshot-rebootstrap` was provisioned. The flag applies only when the primary cannot verify a reclaimed prefix. Do not repeatedly restart the divergent replica expecting its directory to be overwritten.

For the initial local deployment, replacement is the explicit reset procedure. Stop the replica and keep its data directory unchanged for investigation. Record the fatal diagnostic, configured cluster ID, node identity, and available `STATS` LSNs; the CLI does not currently expose the history hash. Decide which history is authoritative before serving that replica's reads again. If the primary is authoritative, provision a **new, empty** replica data directory with the primary's cluster ID and a distinct read-listener address if reads are enabled; use `--allow-snapshot-rebootstrap` on that new directory only if snapshot catch-up may be required. Verify its applied LSN and data against the primary before directing reads to it. Retain the old directory until its history is no longer needed for diagnosis. The implementation has no in-place reset or automatic promotion command.

Snapshot transfers are capped at 256 MiB and use chunks no larger than the 256 KiB frame limit. The replica appends incoming chunks to a temporary file and checks the offered checksum before installation. A normal interruption removes that file; after a process crash, the next transfer truncates any stale copy. The installer still loads the complete verified file into memory for snapshot decoding, and the primary also holds the outgoing image in memory, so peak transfer and install memory remain proportional to snapshot size. After the new `CURRENT` is durable, the replica deletes the generation it replaced. Startup also deletes any generation other than the one `CURRENT` selects, including a partial one left by an interrupted install. Deletion is best effort: an unexpected file in an old generation leaves that directory in place and does not stop the node. If the primary publishes a newer snapshot and reclaims continuation WAL during a transfer, catch-up can stop and require a fresh connection. These limits mean snapshot catch-up is not yet a complete bounded-storage implementation.

`STATS` reports a connected replica's last acknowledged applied LSN and sampled lag. A disconnected replica's current lag is unknown. Neither a zero lag sample nor an ACK is a quorum commit or a failover guarantee.
