#!/usr/bin/env bash
set -euo pipefail

# Compare the in-memory and LSM storage engines on one host with one build
# (SOW Phase 8). Every GET/SET mix runs against a fresh single fsync primary
# per run, alternating which engine goes first between trials. The default
# dataset is several times the LSM memtable, so LSM reads go to tables on
# disk. Each engine writes its own CSV under OUT_DIR, and a summary table is
# printed (and appended to $GITHUB_STEP_SUMMARY when set).
#
# Usage: benchmarks/compare_engines.sh [OUT_DIR]
out_dir=${1:-benchmarks/results}
: "${DDB_TRIALS:=3}"
: "${DDB_CLIENTS:=32}"
: "${DDB_OPERATIONS:=20000}"
: "${DDB_WARMUP:=2000}"
: "${DDB_KEYS:=100000}"
: "${DDB_VALUE_BYTES:=128}"
: "${DDB_FILESYSTEM:=$(df -T . | awk 'NR==2 {print $2}')}"
: "${DDB_STORAGE_MEDIUM:=unspecified}"

cargo build --offline --release --bins --quiet
build=target/release
work=$(mktemp -d)
server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]]; then kill "$server_pid" 2>/dev/null || true; fi
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

mkdir -p "$out_dir"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
port=7300
for trial in $(seq 1 "$DDB_TRIALS"); do
  order=(memory lsm)
  if (( trial % 2 == 0 )); then order=(lsm memory); fi
  for ratio in 0.9 0.5 0.1; do
    for engine in "${order[@]}"; do
      data="$work/data-$engine-$trial-$ratio"
      mkdir -p "$data"
      # Hold stdin open through a process substitution so $! is the server.
      "$build/distributedb" serve --addr "127.0.0.1:$port" \
        --replication-addr "127.0.0.1:$((port + 1))" --data "$data" \
        --storage "$engine" < <(sleep infinity) >"$data.log" 2>&1 &
      server_pid=$!
      wait_port "$port"
      printf '%s trial %s, GET ratio %s: ' "$engine" "$trial" "$ratio" >&2
      timeout 600 "$build/ddb_bench" --addr "127.0.0.1:$port" \
        --clients "$DDB_CLIENTS" --operations "$DDB_OPERATIONS" \
        --warmup "$DDB_WARMUP" --keys "$DDB_KEYS" \
        --value-bytes "$DDB_VALUE_BYTES" --read-ratio "$ratio" \
        --seed "$trial" --topology "engine-$engine" \
        --filesystem "$DDB_FILESYSTEM" --storage-medium "$DDB_STORAGE_MEDIUM" \
        --server-pid "$server_pid" --data-dir "$data" \
        --output "$out_dir/$stamp-engine-$engine.csv"
      kill "$server_pid"
      wait "$server_pid" 2>/dev/null || true
      server_pid=''
      port=$((port + 2))
    done
  done
done

python3 benchmarks/summarize_comparison.py \
  "$out_dir/$stamp-engine-memory.csv" "$out_dir/$stamp-engine-lsm.csv" memory lsm \
  | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
