# Docker local cluster

The Compose demo starts one primary and two asynchronous replicas, each with a persistent named volume. It is for local failure and replication exercises. The client port is published only on host loopback; the unauthenticated replication port stays on the Compose network. Do not use this configuration on an untrusted network.

Use Docker with the Compose plugin. From Git Bash, WSL, or another POSIX shell at the repository root:

```sh
sh scripts/start_cluster.sh
```

The script builds the image, starts the primary, reads its persisted cluster ID from startup output, and joins the two replicas. It does not create a second cluster identity on restart while the primary volume is retained. To send the SOW's sample writes:

```sh
printf 'SET user:1 Aaron\nSET user:2 Alice\nSET user:3 Bob\n' |
  docker compose exec -T primary distributedb client --addr 127.0.0.1:5555
docker compose exec -T primary distributedb client --addr 127.0.0.1:5555 --stats
```

The `STATS` response reports connected replicas and their last acknowledged applied LSN. A zero reported lag means the primary has received an ACK through its sampled durable LSN; it is not a quorum commit. Live replicas do not yet expose a client read port, so this demo cannot independently query their maps. Replication integration tests verify replica contents directly.

To exercise disconnect, catch-up, and primary recovery:

```sh
sh scripts/smoke_cluster.sh
```

The smoke script stops replica 2, writes while it is offline, restarts it, waits for two connected replicas with zero reported lag, then restarts the primary and verifies an acknowledged value is still readable. CI runs the same script. A container stop can terminate the process without an orderly application shutdown; this tests process restart behavior, not physical power-loss durability.

The smoke script also publishes an offline primary snapshot after the first three writes. To repeat that step manually, stop the primary first so the data-directory lock is released:

```sh
docker compose stop primary
docker compose run --rm --no-deps primary snapshot --data /data
docker compose start primary
```

The command prints the snapshot LSN. It will refuse to run while another primary process holds the data directory.

To inspect individual containers, use `docker compose --profile replicas ps` and `docker compose --profile replicas logs`. To stop the cluster while keeping all data volumes:

```sh
docker compose --profile replicas down
```

To reset the cluster and **delete all three named data volumes**:

```sh
docker compose --profile replicas down --volumes
```

The replica startup flag explicitly provisions snapshot rebootstrap on first use of each replica volume. Existing replica policy is not changed by a later flag. If a replica volume belongs to a different cluster, startup rejects its identity rather than joining it silently. The current snapshot transfer has a 256 MiB cap and buffers the transfer in memory; see [replication and recovery](replication.md).
