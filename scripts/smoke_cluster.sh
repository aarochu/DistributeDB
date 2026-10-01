#!/usr/bin/env sh
# Exercise the Compose cluster through its primary's client and STATS endpoint.
set -eu

client() {
    docker compose exec -T primary distributedb client --addr 127.0.0.1:5555
}

stats() {
    docker compose exec -T primary distributedb client --addr 127.0.0.1:5555 --stats
}

wait_for_replicas() {
    attempt=0
    while [ "$attempt" -lt 60 ]; do
        sample=$(stats 2>/dev/null || true)
        connected=$(printf '%s\n' "$sample" | sed -n 's/^replicas_connected=//p')
        zero_lag=$(printf '%s\n' "$sample" | grep -Ec '^replica_[[:xdigit:]]{32}_lag=0$' || true)
        if [ "$connected" = 2 ] && [ "$zero_lag" = 2 ]; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    echo 'two replicas did not reach zero reported lag' >&2
    printf '%s\n' "$sample" >&2
    return 1
}

printf 'SET user:1 Aaron\nSET user:2 Alice\nSET user:3 Bob\n' | client \
    | grep -c '^OK$' | grep -qx 3
wait_for_replicas

# The primary is stopped while the offline snapshot command holds its data
# directory lock. Restart it and require replicas to reconnect before testing
# an additional replica outage.
docker compose stop primary
docker compose run --rm --no-deps primary snapshot --data /data \
    | grep -Eq '^snapshot LSN: [1-9][0-9]*$'
docker compose start primary
wait_for_replicas

docker compose stop replica-2
printf 'SET user:4 David\nSET user:5 Emma\n' | client \
    | grep -c '^OK$' | grep -qx 2
docker compose start replica-2
wait_for_replicas

docker compose restart primary
attempt=0
until printf 'GET user:5\n' | client 2>/dev/null | grep -qx Emma; do
    attempt=$((attempt + 1))
    if [ "$attempt" -eq 60 ]; then
        echo 'primary did not recover the acknowledged value' >&2
        exit 1
    fi
    sleep 1
done
wait_for_replicas
echo 'three-node reconnect and primary recovery smoke test passed'
