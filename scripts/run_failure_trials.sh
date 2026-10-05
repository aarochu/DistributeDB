#!/usr/bin/env bash
set -euo pipefail

# Run the two-node and three-node process failure paths at 100 reproducible
# write boundaries. This takes longer than the single-seed Rust test suite.
for seed in $(seq 0 99); do
  printf 'process failure seed %s/99\n' "$seed"
  DDB_FAILURE_SEED="$seed" cargo test --offline --test process_failure \
    replica_and_primary_process_kill_restart_converges -- --nocapture
  DDB_FAILURE_SEED="$seed" cargo test --offline --test process_failure \
    three_node_snapshot_and_process_restart_converges -- --nocapture
done
