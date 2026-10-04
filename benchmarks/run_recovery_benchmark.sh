#!/usr/bin/env bash
set -euo pipefail

# Compare restart work for identical final key/value state with a full WAL and
# with a snapshot plus a short WAL tail. Temporary data directories are removed
# by the benchmark binary. The CSV and context file are retained for review.
: "${DDB_RECOVERY_RECORDS:=10000}"
: "${DDB_RECOVERY_TAIL:=500}"
: "${DDB_RECOVERY_KEYS:=2000}"
: "${DDB_RECOVERY_VALUE_BYTES:=128}"
: "${DDB_RECOVERY_TRIALS:=7}"
: "${DDB_RECOVERY_OUTPUT:=benchmarks/results/$(date -u +%Y%m%dT%H%M%SZ)-recovery.csv}"

mkdir -p "$(dirname "$DDB_RECOVERY_OUTPUT")"
cargo build --offline --release --bin ddb_recovery_bench
target/release/ddb_recovery_bench \
  --records "$DDB_RECOVERY_RECORDS" \
  --tail "$DDB_RECOVERY_TAIL" \
  --keys "$DDB_RECOVERY_KEYS" \
  --value-bytes "$DDB_RECOVERY_VALUE_BYTES" \
  --trials "$DDB_RECOVERY_TRIALS" \
  --output "$DDB_RECOVERY_OUTPUT"

context="${DDB_RECOVERY_OUTPUT%.csv}.context.txt"
{
  printf 'revision=%s\n' "$(git rev-parse HEAD)"
  printf 'utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'rustc=%s\n' "$(rustc --version)"
  printf 'host=%s\n' "$(uname -srm)"
  printf 'filesystem=%s\n' "$(df -T . | awk 'NR==2 {print $2}')"
  printf 'runner=%s\n' "${RUNNER_NAME:-local}"
  printf 'storage_medium=%s\n' "${DDB_STORAGE_MEDIUM:-unspecified}"
} > "$context"
printf 'Wrote %s\n' "$context"
