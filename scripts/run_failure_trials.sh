#!/usr/bin/env bash
set -euo pipefail

# Run the process kill/restart scenario at 100 reproducible write boundaries.
# This takes substantially longer than the single-seed CI smoke test.
for seed in $(seq 0 99); do
  printf 'process failure seed %s/99\n' "$seed"
  DDB_FAILURE_SEED="$seed" cargo test --offline --test process_failure \
    replica_and_primary_process_kill_restart_converges -- --nocapture
done
