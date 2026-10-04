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

The `STATS` response reports connected replicas and their last acknowledged applied LSN. A zero reported lag means the primary has received an ACK through its sampled durable LSN; it is not a quorum commit. The Compose replicas expose read-only client listeners on their internal network. To query each replica independently:

```sh
printf 'GET user:1\n' | docker compose exec -T replica-1 distributedb client --addr 127.0.0.1:5555
printf 'GET user:1\n' | docker compose exec -T replica-2 distributedb client --addr 127.0.0.1:5555
```

Replica reads are eventually consistent while replication lags. Client writes sent to a replica return `NOT_PRIMARY`. The replica client ports are not published on the host.

To exercise disconnect, catch-up, and primary recovery:

```sh
sh scripts/smoke_cluster.sh
```

The smoke script verifies values on both replicas, stops replica 2, writes while it is offline, restarts it, waits for two connected replicas with zero reported lag, then restarts the primary and verifies an acknowledged value on all three nodes. CI runs the same script. Each service runs under Compose's `init` process, so a container stop delivers `SIGTERM` to the node and terminates it immediately, without an orderly application shutdown; this tests process restart behavior, not physical power-loss durability.

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

## SOW demonstration

`scripts/demo.sh` runs the SOW §26 scenario end to end and prints each step. It needs a fresh cluster, so reset first. This **deletes the three named data volumes**:

```sh
docker compose --profile replicas down --volumes
sh scripts/demo.sh
```

The script:

1. Starts the primary (Node A) and two replicas (Nodes B and C).
2. Writes `user:1` to `user:3` and reads them back from all three nodes.
3. Publishes an offline primary snapshot.
4. Kills Node B with `SIGKILL` and writes `user:4` and `user:5`. `STATS` then shows the primary ahead of Node B and its lag as `unknown`.
5. Restarts Node B and shows it caught up at zero lag.
6. Kills Node A with `SIGKILL` and restarts it. `STATS` shows the snapshot LSN, the two WAL records replayed after it, and the recovery time. All five users are still readable.
7. Runs the 90/10, 50/50 and 10/90 GET/SET benchmark mixes against the primary with both replicas attached.
8. Prints throughput, client-observed p50/p95/p99 latency, server-side latency percentiles, replication lag, and recovery time.

CI runs the same script on every pull request that touches the cluster. Client, primary and replicas share one host, so the benchmark figures describe that host only; see [the benchmark method](benchmarks.md).
