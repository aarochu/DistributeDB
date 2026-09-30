# Crash-Recovery Testing (Phase 2)

This note documents the automated durability and crash-recovery test suites
that form the Phase 2 acceptance evidence. It complements the authoritative
rules in [Technical-Design](Technical-Design.md) §6.3 (recovery), §6.4
(simulated power-loss model), §12 (testing table), and §13 (Phase 2 acceptance
gate), and the failure-testing requirements in [SOW](SOW.md) §8, §19, §20.

## What the suites cover

| Test file | Purpose |
|-----------|---------|
| `tests/wal_recovery.rs` | Durable write then real reopen over `RealFs`: the SOW §8 `SET A/B/C` example, delete-then-recover, empty value, binary key/value, fresh-empty-dir, and a many-groups sequence. |
| `tests/wal_cutpoints.rs` | Every deterministic §13 cut point plus the torn-write case, driven over `SimFs`. |
| `tests/wal_corruption.rs` | Corrupt-final-entry is ignored (`tail_truncated`); sealed / footer-closed corruption fails closed. |
| `tests/wal_crash_sim.rs` | The seeded simulated-power-loss gate: at least 1,000 seeds, "no acknowledged OK write is ever lost". |
| `tests/wal_kill_restart.rs` | Real spawn / kill / restart over `RealFs` using the `wal_kill_harness` binary. |

## Simulated power-loss model (SimFs)

`SimFs` (see `src/fileio/mod.rs`) keeps a **volatile** and a **stable** byte
image of every file plus a volatile / stable directory namespace:

- Appends, renames, and truncations mutate the **volatile** image.
- A successful `sync_file` promotes that file's bytes to **stable**.
- A successful `sync_dir` promotes namespace changes to **stable**.
- `crash()` discards all volatile state, leaving only the stable image, which
  is exactly what the next recovery sees.
- Before a group sync succeeds, whole pages may persist in arbitrary (seeded)
  order, so an intact later record can survive after a torn earlier record in
  the same unsynced group. Recovery must still discard the whole group after
  the last valid footer.
- The core model never alters already-stable bytes. A dedicated corruption
  test in `tests/wal_corruption.rs` deliberately overwrites stable bytes to
  exercise the narrower fail-closed guarantee of §6.3.

All fault injection (short writes, failed syncs, crash-at-step, page ordering)
is driven by an in-crate SplitMix64 PRNG seeded from an explicit seed, so every
run is fully reproducible.

## Cut points exercised

`tests/wal_cutpoints.rs` enumerates each deterministic cut point in a short
trace and asserts the §6.3 outcome:

- **before append** — prior synced state is intact;
- **before group sync** — the whole unsynced group is discarded;
- **during a short / partial write** — the torn record and its group vanish;
- **after group sync, before map apply** — the group is durable and replays
  (write-ahead order: durable then apply);
- **after apply, before response** — identical on-disk state, so it replays too;
- **torn earlier + intact later record** — the whole unclosed group after the
  last valid footer is discarded; the later record is NOT resurrected.

## Seed strategy and reproduction

`tests/wal_crash_sim.rs` runs seeds `1..=2000` (well over the §13 minimum of
1,000). For each seed:

1. A local LCG (seeded from the run seed) chooses the operation sequence and
   the number of operations before an explicit crash.
2. `SimFs` is constructed with the seed and a mix of short-write, sync-failure,
   and crash faults, so the fault decisions are reproducible from the seed.
3. A reference model records the last **acknowledged** outcome per key (an
   `Ok` return from `set`/`delete`). Keys whose write had an unknown outcome
   are *poisoned* and excluded from assertions.
4. After the crash, recovery reopens from the stable image (retrying on
   transient injected recovery faults, as a supervisor would) and asserts that
   every unambiguously acknowledged write survived.

On any violation the test prints the reproducing seed and a compact trace:

```text
SEED=<n> FAILED: <reason>
SEED=<n> TRACE: SET k0=v12 OK | DEL k3 OK | CRASH
```

Pin the failing seed as a regression case.

## Running the full acceptance gate

The default `cargo test` keeps the real spawn/kill suite small for CI speed
(a handful of runs). Run the full §13 gate of 100 kill/restart runs with:

```sh
DISTRIBUTEDB_KILL_RUNS=100 cargo test --test wal_kill_restart -- --nocapture
```

The seeded simulated-power-loss gate (>= 1,000 seeds) always runs in full
because it is in-memory and fast:

```sh
cargo test --test wal_crash_sim
```
