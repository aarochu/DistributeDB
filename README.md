# DistributeDB

DistributeDB is a planned fault-tolerant distributed key-value database. The project is intended to explore persistent storage, write-ahead logging, crash recovery, concurrent TCP clients, primary-replica replication, and measurable failure behavior.

## Overview

The initial implementation targets a networked key-value store with one primary node and one or more replicas. Clients will send operations to the primary; the primary will maintain an ordered mutation log, persist writes, and replicate them to followers. Snapshots and WAL replay will support restart recovery, while replica catch-up will handle temporary disconnection.

The project prioritizes correctness, explicit durability and consistency semantics, failure testing, and reproducible benchmarks over feature breadth. It is an engineering project for studying how storage, concurrency, networking, and replication interact in one system.

## Project status

**Status: Design and initial implementation planning.** The database is not implemented yet. There is no server, client, benchmark result, or runnable cluster in this repository at present.

The repository currently contains the [Statement of Work](docs/SOW.md), a [technical design and implementation plan](docs/Technical-Design.md), a [language and filesystem decision record](docs/ADR-001-Language-and-Filesystem.md), and [development setup notes](docs/Development-Setup.md). The design records proposed implementation choices; its byte formats and guarantees still require tests during implementation.

## Goals

The SOW defines these core goals:

- A minimal `SET`, `GET`, `DELETE`, and `EXISTS` key-value interface.
- Persistent state using a write-ahead log, crash recovery, and snapshots.
- A TCP client/server interface that handles concurrent clients, partial messages, malformed commands, disconnects, and shutdown.
- A single primary that orders mutations and replicates them to one or more followers.
- Replica catch-up from missing WAL entries, with snapshot transfer when required WAL has been compacted.
- Clear documentation of local durability, asynchronous replication, and potentially stale replica reads.
- Automated unit, integration, and failure-injection tests, plus observable replication lag and latency.
- Reproducible read-heavy, balanced, and write-heavy benchmark workloads.

## Non-goals

The initial scope excludes full SQL compatibility, distributed transactions, multi-primary consensus, a complete Raft implementation, automatic failover, sharding, a query optimizer, joins, secondary indexes, schema migrations, geographic replication, production-grade authentication, and encryption at rest. The SOW lists several of these as possible later extensions, not commitments for the core build.

## Planned architecture

Clients will initially connect to a manually configured primary. The primary will validate commands, append mutations to its WAL, update its local storage engine, and replicate the ordered log. Replicas will maintain their own state and track the last applied log sequence number (LSN). On restart, a node will load a snapshot and replay later WAL entries. A replica that falls behind will request missing entries or, if they are no longer available, a snapshot.

```mermaid
flowchart LR
    C[Clients] -->|TCP commands| P[Primary node]
    P --> V[Command validation]
    V --> W[Write-ahead log]
    W --> S[Local storage engine]
    W --> R[Replication stream]
    R --> F1[Replica 1: WAL and storage]
    R --> F2[Replica 2: WAL and storage]
    S --> N[Snapshots and recovery]
```

The first storage engine may use an in-memory hash map backed by persistent files. The SOW allows asynchronous replication initially; this means a primary acknowledgment and a replica acknowledgment are different events. The technical design specifies proposed boundaries for those events, but they are not implemented or verified yet. There is no automatic leader election or primary promotion in the core scope.

## Planned interface

| Command | Intended behavior |
|---|---|
| `SET key value` | Store or replace a value. |
| `GET key` | Retrieve a value. |
| `DELETE key` | Remove a key. |
| `EXISTS key` | Check whether a key is present. |

The SOW gives these command names but does not freeze a wire protocol, response encoding, or edge-case semantics. Those details are proposed in the technical design and will be validated during implementation. Optional commands and transactions are not required for the core system.

## Roadmap

| Phase | Planned outcome |
|---|---|
| 1 — Local key-value engine | In-memory operations, parser, and unit tests. |
| 2 — Persistent WAL | Durable writes, sequence numbers, and restart recovery. |
| 3 — Networking | TCP server/client and concurrent connections. |
| 4 — Snapshots | Snapshot creation, loading, and WAL rotation or truncation. |
| 5 — Replication | Primary and replicas with ordered log delivery and acknowledgments. |
| 6 — Failure recovery | Reconnect, WAL catch-up, snapshot-based recovery, and kill/restart tests. |
| 7 — Performance engineering | Benchmark harness, latency and throughput measurements, and profiling. |
| 8 — Optional advanced storage | One disk-aware index, B+ tree or LSM tree, only if the core is sound. |

All phases are planned. Progress will be recorded here as implementation and tests are added; this table does not imply any phase is complete.

## Testing and benchmarking plan

The SOW calls for tests of parsing, WAL serialization, snapshots, networking, concurrency, replication, and recovery. Failure tests will kill and restart nodes, interrupt replication, and examine incomplete WAL tails. The key durability acceptance condition is that every acknowledged durable write survives a supported process-crash recovery scenario.

Planned benchmark mixes are 90% reads / 10% writes, 50% reads / 50% writes, and 10% reads / 90% writes. Results will report throughput, latency percentiles, CPU, memory, and storage usage, with raw results under `benchmarks/results/` when a benchmark tool exists. **No benchmark results exist yet.**

## Getting started

There is nothing to build or run yet. Start with the [SOW](docs/SOW.md) for scope, then read the [technical design](docs/Technical-Design.md) for proposed interfaces and failure invariants. The [setup notes](docs/Development-Setup.md) describe the current documentation workflow and the proposed implementation environment.

The repository's CI currently checks documentation files and whitespace only. It does not represent a passing database test suite. Build, test, local-cluster, and benchmark commands will be added when those components exist.

## Contributing

Design review and corrections are useful at this stage, particularly for WAL and snapshot crash boundaries, replication catch-up, and testable acceptance criteria. New implementation work should preserve the SOW's scope, identify whether a behavior is source-derived or a new design choice, and add tests for its stated guarantees. Please avoid claiming a feature is complete until it is implemented and verified.

## License

MIT; see [LICENSE](LICENSE).
