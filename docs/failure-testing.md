# Process failure tests

`tests/process_failure.rs` runs the compiled server and replica as separate operating-system processes on loopback ports and distinct real data directories. It writes a known key sequence, kills the replica, writes while it is down, restarts it and waits for its durable applied LSN to catch up. It then kills and restarts the primary, writes again, and checks the replica's recovered on-disk state after both processes stop.

CI runs one seed. The [100-trial runner](../scripts/run_failure_trials.sh) changes the number of writes at each kill boundary using `DDB_FAILURE_SEED`. Its completion is a separate acceptance gate; the presence of the script alone does not establish that 100 trials have passed. Preserve the failing seed and logs if a trial fails.

The test models process termination. It does not prove that a storage device honors sync after loss of power. The simulated filesystem tests cover separately stated crash and torn-write models. A physical power-cut exercise on the [selected filesystem profile](ADR-001-Language-and-Filesystem.md) would be needed to make a stronger hardware durability claim.
