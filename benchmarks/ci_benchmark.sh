#!/usr/bin/env bash
set -euo pipefail

# Run the SOW mixes against four local configurations on one host: an fsync
# primary, a disposable os-durability primary, and an fsync primary with one
# or two asynchronous replicas. Rows are written to $DDB_RESULTS_DIR (default
# benchmarks/results). Intended for CI runners; see docs/benchmarks.md.
: "${DDB_RESULTS_DIR:=benchmarks/results}"
: "${DDB_OPERATIONS:=20000}"
: "${DDB_WARMUP:=2000}"
: "${DDB_FILESYSTEM:=$(df -T . | awk 'NR==2 {print $2}')}"
: "${DDB_STORAGE_MEDIUM:=ci-runner-disk}"

cargo build --offline --release --bins
bin=target/release/distributedb
work=$(mktemp -d)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
pids=()
cleanup() {
  for pid in "${pids[@]+"${pids[@]}"}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 200); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  echo "port $1 did not open" >&2
  return 1
}

# Starts a primary with stdin held open; sets SERVER_PID.
start_primary() {
  local durability=$1 dir=$2 port=$3
  sleep infinity | "$bin" serve --addr "127.0.0.1:$port" \
    --replication-addr "127.0.0.1:$((port + 1))" --data "$dir" \
    --durability "$durability" >"$dir.log" 2>&1 &
  SERVER_PID=$!
  pids+=("$SERVER_PID")
  wait_port "$port"
}

run_config() {
  local durability=$1 replicas=$2 topology=$3 port=$4
  local dir="$work/$topology-primary"
  mkdir -p "$dir"
  start_primary "$durability" "$dir" "$port"
  local primary_pid=$SERVER_PID
  if [[ "$replicas" -gt 0 ]]; then
    local cluster
    cluster=$(awk -F': ' '/^cluster ID:/ {print $2}' "$dir.log")
    for ((index = 1; index <= replicas; index++)); do
      sleep infinity | "$bin" replica --primary-addr "127.0.0.1:$((port + 1))" \
        --cluster-id "$cluster" --data "$work/$topology-replica-$index" \
        --allow-snapshot-rebootstrap >"$work/$topology-replica-$index.log" 2>&1 &
      pids+=("$!")
    done
  fi
  DDB_ADDR="127.0.0.1:$port" DDB_DURABILITY="$durability" \
    DDB_REPLICAS="$replicas" DDB_DATA_DIR="$dir" DDB_SERVER_PID="$primary_pid" \
    DDB_OPERATIONS="$DDB_OPERATIONS" DDB_WARMUP="$DDB_WARMUP" \
    DDB_FILESYSTEM="$DDB_FILESYSTEM" DDB_STORAGE_MEDIUM="$DDB_STORAGE_MEDIUM" \
    DDB_TOPOLOGY="$topology" \
    DDB_OUTPUT="$DDB_RESULTS_DIR/$stamp-ci.csv" \
    bash benchmarks/run_benchmarks.sh
}

run_config fsync 0 one-primary 6100
run_config os 0 one-primary-os 6200
run_config fsync 1 primary-one-replica 6300
run_config fsync 2 primary-two-replicas 6400
