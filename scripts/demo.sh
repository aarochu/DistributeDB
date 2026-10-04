#!/usr/bin/env sh
# SOW §26 demonstration on the three-node Compose cluster: replicate, kill and
# catch up a replica, kill and recover the primary from its snapshot and WAL,
# then run the benchmark mixes and display throughput, latency, replication
# lag, and recovery time. Run from the repository root on a fresh cluster:
#   docker compose --profile replicas down --volumes && sh scripts/demo.sh
set -eu

step() {
    printf '\n== %s\n' "$*"
}

client() {
    docker compose exec -T "$1" distributedb client --addr 127.0.0.1:5555
}

stats() {
    docker compose exec -T primary distributedb client --addr 127.0.0.1:5555 --stats
}

show_stats() {
    stats | grep -E "^($1)=" | sed 's/^/   /'
}

# Wait until the primary reports `$1` connected replicas, all at zero lag.
wait_for_replicas() {
    attempt=0
    while [ "$attempt" -lt 60 ]; do
        sample=$(stats 2>/dev/null || true)
        connected=$(printf '%s\n' "$sample" | sed -n 's/^replicas_connected=//p')
        zero_lag=$(printf '%s\n' "$sample" | grep -Ec '^replica_[[:xdigit:]]{32}_lag=0$' || true)
        if [ "$connected" = "$1" ] && [ "$zero_lag" = "$1" ]; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    echo "replicas did not reach zero reported lag" >&2
    printf '%s\n' "$sample" >&2
    return 1
}

wait_for_primary() {
    attempt=0
    until printf 'EXISTS demo:probe\n' | client primary >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -eq 60 ]; then
            echo 'primary did not accept clients within 60 seconds' >&2
            return 1
        fi
        sleep 1
    done
}

# Print user:1..$2 as read from node $1, and fail if any value is wrong.
verify_users() {
    node=$1
    count=$2
    requests=''
    expected=''
    index=1
    for name in Aaron Alice Bob David Emma; do
        [ "$index" -le "$count" ] || break
        requests="${requests}GET user:$index
"
        expected="${expected}$name
"
        index=$((index + 1))
    done
    actual=$(printf '%s' "$requests" | client "$node")
    printf '   %-9s %s\n' "$node" "$(printf '%s' "$actual" | tr '\n' ' ')"
    if [ "$actual" != "$(printf '%s' "$expected")" ]; then
        echo "$node returned unexpected values" >&2
        return 1
    fi
}

step 'Launch Node A (primary) and Nodes B and C (replicas)'
sh scripts/start_cluster.sh

step 'Write three users through the primary'
printf 'SET user:1 Aaron\nSET user:2 Alice\nSET user:3 Bob\n' | client primary \
    | grep -c '^OK$' | grep -qx 3
wait_for_replicas 2
for node in primary replica-1 replica-2; do
    verify_users "$node" 3
done

step 'Publish a primary snapshot (offline: the snapshot command takes the data-directory lock)'
docker compose stop primary
docker compose run --rm --no-deps primary snapshot --data /data | sed 's/^/   /'
docker compose start primary
wait_for_primary
wait_for_replicas 2

step 'Kill Node B (replica-2) and keep writing'
docker compose kill replica-2
printf 'SET user:4 David\nSET user:5 Emma\n' | client primary \
    | grep -c '^OK$' | grep -qx 2
show_stats 'current_lsn|replicas_connected|replica_[[:xdigit:]]{32}_(applied_lsn|lag)'

step 'Restart Node B; it requests the entries after its durable LSN and catches up'
docker compose start replica-2
wait_for_replicas 2
show_stats 'current_lsn|replicas_connected|replica_[[:xdigit:]]{32}_(applied_lsn|lag)'
for node in replica-1 replica-2; do
    verify_users "$node" 5
done

step 'Kill Node A (primary) and restart it; it loads its snapshot and replays the WAL tail'
docker compose kill primary
docker compose start primary
wait_for_primary
show_stats 'snapshot_lsn|current_lsn|recovery_records_replayed|recovery_us'
verify_users primary 5
wait_for_replicas 2

step 'Run the benchmark mixes against the primary with two replicas'
for ratio in 0.9 0.5 0.1; do
    printf '   GET ratio %s: ' "$ratio"
    docker compose exec -T primary ddb_bench --addr 127.0.0.1:5555 \
        --clients 16 --operations 20000 --warmup 2000 --keys 1000 \
        --read-ratio "$ratio" --replicas 2 --topology demo-three-node \
        --output /tmp/demo-bench.csv 2>&1 >/dev/null \
        | grep 'successful ops'
done

step 'Server-side metrics after the benchmark'
show_stats 'current_lsn|read_latency_p(50|95|99)_us|write_latency_p(50|95|99)_us|wal_sync_avg_us|replicas_connected|replica_[[:xdigit:]]{32}_lag|recovery_us|recovery_records_replayed'

printf '\nDemo complete.\n'
