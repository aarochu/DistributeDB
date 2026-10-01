#!/usr/bin/env bash
set -euo pipefail

# Start a server separately, then set DDB_ADDR and environment metadata before
# running this script. Each invocation records three repetitions per mix.
: "${DDB_ADDR:=127.0.0.1:5555}"
: "${DDB_DURABILITY:=fsync}"
: "${DDB_REPLICAS:=0}"
: "${DDB_CLIENTS:=32}"
: "${DDB_OPERATIONS:=100000}"
: "${DDB_WARMUP:=10000}"
: "${DDB_KEYS:=10000}"
: "${DDB_VALUE_BYTES:=128}"
: "${DDB_FILESYSTEM:=unspecified}"
: "${DDB_STORAGE_MEDIUM:=unspecified}"
: "${DDB_TOPOLOGY:=unspecified}"
: "${DDB_OUTPUT:=benchmarks/results/$(date -u +%Y%m%dT%H%M%SZ)-${DDB_DURABILITY}-replicas${DDB_REPLICAS}.csv}"

extra=()
if [[ -n "${DDB_DATA_DIR:-}" ]]; then
  extra+=(--data-dir "$DDB_DATA_DIR")
fi
if [[ -n "${DDB_SERVER_PID:-}" ]]; then
  extra+=(--server-pid "$DDB_SERVER_PID")
fi

for ratio in 0.9 0.5 0.1; do
  for seed in 1 2 3; do
    cargo run --offline --release --bin ddb_bench -- \
      --addr "$DDB_ADDR" --clients "$DDB_CLIENTS" \
      --operations "$DDB_OPERATIONS" --warmup "$DDB_WARMUP" \
      --read-ratio "$ratio" --keys "$DDB_KEYS" \
      --value-bytes "$DDB_VALUE_BYTES" --seed "$seed" \
      --durability "$DDB_DURABILITY" --replicas "$DDB_REPLICAS" \
      --filesystem "$DDB_FILESYSTEM" --storage-medium "$DDB_STORAGE_MEDIUM" \
      --topology "$DDB_TOPOLOGY" --output "$DDB_OUTPUT" "${extra[@]}"
  done
done

printf 'Wrote benchmark rows to %s\n' "$DDB_OUTPUT"
