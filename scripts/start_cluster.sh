#!/usr/bin/env sh
# Launch one primary, discover its persisted cluster ID, then join two replicas.
set -eu

docker compose up -d --build primary

attempt=0
while [ "$attempt" -lt 60 ]; do
    if printf 'EXISTS cluster:probe\n' | docker compose exec -T primary \
        distributedb client --addr 127.0.0.1:5555 >/dev/null 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "$attempt" -eq 60 ]; then
    echo 'primary did not become ready within 60 seconds' >&2
    exit 1
fi

DDB_CLUSTER_ID=$(docker compose logs --no-color primary \
    | sed -n 's/.*cluster ID: \([[:xdigit:]]\{32\}\).*/\1/p' \
    | tail -n 1)
if [ "${#DDB_CLUSTER_ID}" -ne 32 ]; then
    echo 'could not read primary cluster ID from logs' >&2
    exit 1
fi
export DDB_CLUSTER_ID
docker compose --profile replicas up -d --build replica-1 replica-2
echo "cluster ID: $DDB_CLUSTER_ID"
echo 'client: docker compose exec -T primary distributedb client --addr 127.0.0.1:5555'
