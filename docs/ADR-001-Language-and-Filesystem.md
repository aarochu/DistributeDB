# ADR-001: Initial language and filesystem profile

**Status:** Proposed for Phase 1; confirm against the actual development calendar before treating the estimate as a commitment.
**Date:** 2026-09-30
**Scope:** DistributeDB phases 1–7; file and wire bytes remain provisional until Phase 2 and protocol golden tests pass.

## Context

The SOW leaves the implementation language and operating environment open. The technical design needs one target so its sync, rename, directory, crash-simulation, and build/test instructions can be implemented and checked consistently. A portable implementation can follow later; broad portability at the outset would multiply filesystem cases before the core crash invariants are tested.

## Decision

Use **stable Rust** for the first implementation, with `cargo test` as the standard automated test command. Target **64-bit Linux on a local ext4 filesystem** for durability testing. The initial Docker cluster may run on that host using persistent local volumes, but Docker tests are integration tests, not proof of physical power-loss survival. Phase 1 contains the local map and typed `SET`, `GET`, `DELETE`, and `EXISTS` operations; it has no filesystem dependency yet. Phase 2 adds a small file-I/O adapter with a real Linux implementation and the deterministic simulated-power-loss implementation described in the design.

The real adapter must support full-write loops, file sync, directory sync, same-filesystem rename, file truncation, and explicit error propagation. The process holds an exclusive data-directory lock. A mutation `OK` in `fsync` mode follows successful synchronization of its complete group and footer. The exact use of Rust standard-library APIs versus a small system-call wrapper is an implementation detail; do not assume every platform gives the same semantics. Test the selected host filesystem and mount options and record them in benchmark and failure-test results. Treat networked filesystems, overlay filesystems, and Windows storage as unsupported durability profiles until tested separately.

## Reasons

- Rust's byte slices and checked conversions suit the fixed binary formats and size limits, while its ownership model helps keep concurrent map access and WAL ownership explicit. This is an engineering preference, not a claim that Rust removes protocol or crash-consistency bugs.
- Linux/ext4 gives one concrete filesystem profile for Phase 2 fault testing. Linux documents that syncing a file does not necessarily sync the directory entry created or renamed for it; the adapter therefore exposes both operations ([Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)).
- The SOW already allows `cargo test` and an equivalent in-memory map. No SOW requirement dictates C++ or Rust.

## Alternatives considered

**C++ with CMake/CTest:** compatible with the SOW's sample layout and viable for systems work. It adds more manual lifetime and synchronization review for this project; there is no demonstrated performance requirement that favors it at Phase 1.

**Cross-platform from day one:** broadens use but makes directory synchronization, rename, and crash guarantees harder to state and test. Defer until the Linux profile passes the Phase 2 failure gates.

**SQLite/RocksDB as the storage layer:** would shorten implementation time but replace the core WAL and crash-recovery exercise required by the SOW. Libraries may be used for test tooling, not as the v1 storage engine.

## Consequences and validation

The team needs a Linux/ext4 test host by Phase 2. Phase 1 can be developed on other platforms but cannot claim the target durability guarantee there. Record `rustc` version, kernel, filesystem, mount options, and storage device for failure tests. Golden fixtures must freeze the proposed WAL header, mutation record, group footer, snapshot, `CURRENT`, and TCP/replication formats before persistent test data is relied on. The simulated file layer tests call ordering; physical power-cut behavior remains a separate claim requiring separate evidence. Review this ADR if the available schedule differs materially from the design's 30 focused hours/week assumption.
