#!/usr/bin/env bash
set -euo pipefail

# Compare two revisions on one host. Both are built in temporary worktrees,
# then every GET/SET mix runs against a fresh single fsync primary per run.
# Base and head alternate order between trials so slow drift on the host
# affects both. Each revision writes its own CSV under OUT_DIR, and a summary
# table is printed (and appended to $GITHUB_STEP_SUMMARY when set).
#
# Usage: benchmarks/compare_revisions.sh BASE_REF HEAD_REF [OUT_DIR]
base_ref=$1
head_ref=$2
out_dir=${3:-benchmarks/results}
: "${DDB_TRIALS:=5}"
: "${DDB_CLIENTS:=32}"
: "${DDB_OPERATIONS:=20000}"
: "${DDB_WARMUP:=2000}"
: "${DDB_KEYS:=10000}"
: "${DDB_VALUE_BYTES:=128}"
: "${DDB_FILESYSTEM:=$(df -T . | awk 'NR==2 {print $2}')}"
: "${DDB_STORAGE_MEDIUM:=unspecified}"
# 0 or 1: attach one asynchronous replica from the same build to each run.
: "${DDB_REPLICAS:=0}"
# memory or lsm. Only passed to `serve` for lsm, so older revisions without
# the flag still run with the default.
: "${DDB_STORAGE:=memory}"
storage_args=()
if [[ "$DDB_STORAGE" != memory ]]; then storage_args=(--storage "$DDB_STORAGE"); fi

repo=$(pwd)
work=$(mktemp -d)
server_pid=''
replica_pid=''
cleanup() {
  if [[ -n "$server_pid" ]]; then kill "$server_pid" 2>/dev/null || true; fi
  if [[ -n "$replica_pid" ]]; then kill "$replica_pid" 2>/dev/null || true; fi
  for label in base head; do
    git -C "$repo" worktree remove --force "$work/$label" 2>/dev/null || true
  done
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

for label in base head; do
  ref=$base_ref
  if [[ "$label" == head ]]; then ref=$head_ref; fi
  git worktree add --detach "$work/$label" "$ref" >/dev/null
  (cd "$work/$label" && cargo build --offline --release --bins --quiet)
done

mkdir -p "$out_dir"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
port=7100
for trial in $(seq 1 "$DDB_TRIALS"); do
  order=(base head)
  if (( trial % 2 == 0 )); then order=(head base); fi
  for ratio in 0.9 0.5 0.1; do
    for label in "${order[@]}"; do
      build="$work/$label/target/release"
      data="$work/data-$label-$trial-$ratio"
      mkdir -p "$data"
      # Hold stdin open through a process substitution rather than a pipeline,
      # so $! is the server alone and `wait` returns when it exits.
      "$build/distributedb" serve --addr "127.0.0.1:$port" \
        --replication-addr "127.0.0.1:$((port + 1))" --data "$data" \
        "${storage_args[@]+"${storage_args[@]}"}" < <(sleep infinity) >"$data.log" 2>&1 &
      server_pid=$!
      wait_port "$port"
      if [[ "$DDB_REPLICAS" -gt 0 ]]; then
        cluster=$(awk -F': ' '/^cluster ID:/ {print $2}' "$data.log")
        "$build/distributedb" replica --primary-addr "127.0.0.1:$((port + 1))" \
          --cluster-id "$cluster" --data "$data-replica" \
          < <(sleep infinity) >"$data-replica.log" 2>&1 &
        replica_pid=$!
      fi
      printf '%s trial %s, GET ratio %s: ' "$label" "$trial" "$ratio" >&2
      # Run from the worktree so the CSV records that revision.
      (cd "$work/$label" && timeout 300 "$build/ddb_bench" --addr "127.0.0.1:$port" \
        --clients "$DDB_CLIENTS" --operations "$DDB_OPERATIONS" \
        --warmup "$DDB_WARMUP" --keys "$DDB_KEYS" \
        --value-bytes "$DDB_VALUE_BYTES" --read-ratio "$ratio" \
        --seed "$trial" --topology "compare-$label" --replicas "$DDB_REPLICAS" \
        --filesystem "$DDB_FILESYSTEM" --storage-medium "$DDB_STORAGE_MEDIUM" \
        --server-pid "$server_pid" --data-dir "$data" \
        --output "$repo/$out_dir/$stamp-compare-$label.csv")
      if [[ -n "$replica_pid" ]]; then
        kill "$replica_pid"
        wait "$replica_pid" 2>/dev/null || true
        replica_pid=''
      fi
      kill "$server_pid"
      wait "$server_pid" 2>/dev/null || true
      server_pid=''
      port=$((port + 2))
    done
  done
done

python3 benchmarks/summarize_comparison.py \
  "$out_dir/$stamp-compare-base.csv" "$out_dir/$stamp-compare-head.csv" \
  | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
