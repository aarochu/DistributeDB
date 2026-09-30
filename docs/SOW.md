# DistributeDB — Statement of Work

## 1. Project Overview

DistributeDB is a fault-tolerant distributed key-value database built from scratch to explore the core systems concepts behind modern distributed storage platforms.

The system will support persistent storage, concurrent client access, write-ahead logging, crash recovery, replication, and basic failure handling across multiple database nodes.

The primary goal is not to recreate PostgreSQL, Cassandra, or Redis feature-for-feature. Instead, DistributeDB will implement a focused subset of database functionality deeply enough to demonstrate how storage engines, durability, replication, concurrency, and distributed coordination interact inside a real system.

The final system should allow multiple clients to send key-value operations to a primary database node, persist those operations safely to disk, replicate committed writes to one or more follower nodes, recover from process crashes, and measure performance under different workloads.

---

# 2. Project Goals

DistributeDB should demonstrate practical understanding of:

- Persistent data storage
- File and disk I/O
- Write-ahead logging
- Crash recovery
- Concurrent client handling
- Networked client/server architecture
- Replication
- Leader/follower database architecture
- Consistency and durability
- Failure recovery
- Performance benchmarking
- Systems observability
- Testing distributed systems

The project should prioritize correctness, measurable behavior, and clear system design over implementing a large number of features.

---

# 3. Core System Architecture

The initial system will use a primary-replica architecture.

```text
                 +------------------+
                 |      Client      |
                 +--------+---------+
                          |
                          | TCP
                          v
                 +------------------+
                 |   Primary Node   |
                 |                  |
                 | Command Parser   |
                 | Transaction Mgmt |
                 | Storage Engine   |
                 | WAL              |
                 | Replication      |
                 +--------+---------+
                          |
                Replicated Log
                          |
              +-----------+-----------+
              |                       |
              v                       v
      +---------------+       +---------------+
      |   Replica 1   |       |   Replica 2   |
      | Storage Engine|       | Storage Engine|
      | WAL           |       | WAL           |
      +---------------+       +---------------+
```

Clients will initially connect to the primary node.

The primary will:

1. Accept a request.
2. Validate the command.
3. Append the operation to the write-ahead log.
4. Apply the operation to the local storage engine.
5. Replicate the operation to follower nodes.
6. Return a response to the client.

Replica nodes will consume replicated operations and apply them to their own storage engines.

---

# 4. Functional Requirements

## 4.1 Key-Value Interface

The database will initially expose a minimal command interface.

Required operations:

```text
SET key value
GET key
DELETE key
EXISTS key
```

Example:

```text
SET user:123 Aaron
GET user:123
```

Response:

```text
Aaron
```

Optional later commands may include:

```text
SCAN
STATS
PING
BEGIN
COMMIT
ROLLBACK
```

The core implementation should remain functional even if optional commands are never completed.

---

# 5. Client-Server Networking

DistributeDB will operate as a networked database server.

Clients will communicate with database nodes over TCP.

Example:

```text
Client
   |
   | SET x 42
   v
Database Server
   |
   | OK
   v
Client
```

The server should support multiple simultaneous clients.

Initial implementation may use:

- one thread per client
- a thread pool

A later version may use:

- asynchronous networking
- event-driven I/O

The server must correctly handle:

- multiple concurrent connections
- malformed commands
- client disconnects
- partial messages
- server shutdown

---

# 6. Storage Engine

The storage engine is responsible for maintaining persistent key-value data.

The initial implementation may use an in-memory hash map backed by persistent storage.

Example:

```text
unordered_map<string, string>
```

or an equivalent structure.

The storage engine should expose an internal interface similar to:

```text
put(key, value)
get(key)
remove(key)
exists(key)
```

Persistence must not depend solely on memory.

---

# 7. Write-Ahead Logging

All mutations must be written to a Write-Ahead Log before they are considered durable.

Example WAL entries:

```text
1 SET user:1 Aaron
2 SET user:2 Alice
3 DELETE user:1
```

Each log entry should contain at minimum:

```text
Log Sequence Number
Operation
Key
Value
```

Potential binary format:

```text
| LSN | TYPE | KEY_LEN | VALUE_LEN | KEY | VALUE |
```

Initially, a human-readable format may be used to simplify development.

The WAL must ensure that database state can be reconstructed after a process crash.

---

# 8. Crash Recovery

On startup, the database will inspect its persistent state and WAL.

Recovery process:

```text
Database starts
      |
      v
Load snapshot
      |
      v
Read WAL
      |
      v
Replay operations after snapshot
      |
      v
Reconstructed database
```

The database should survive scenarios such as:

```text
SET A 1
SET B 2
SET C 3
<process killed>
```

After restart:

```text
GET A -> 1
GET B -> 2
GET C -> 3
```

The project must include automated tests that kill and restart the database to validate recovery behavior.

---

# 9. Snapshots

Replaying an indefinitely growing WAL will eventually become inefficient.

DistributeDB should therefore support snapshots.

A snapshot stores the current database state at a particular log sequence number.

Example:

```text
Snapshot LSN: 50000
```

On recovery:

```text
Load snapshot at LSN 50000
Replay WAL entries 50001+
```

This prevents the system from replaying every historical command during startup.

---

# 10. Replication

DistributeDB will implement primary-replica replication.

The primary node will maintain an ordered sequence of mutations.

Example:

```text
Primary

LSN 100 SET x 5
LSN 101 SET y 7
LSN 102 DELETE z
```

Replica:

```text
Receive LSN 100
Receive LSN 101
Receive LSN 102
```

Each replica will maintain its last applied log sequence number.

Example:

```text
Primary LSN: 5000
Replica LSN: 4988
Replication Lag: 12 entries
```

This metric should be visible through the server's statistics interface.

---

# 11. Replication Semantics

The first implementation may use asynchronous replication.

Flow:

```text
Client
   |
   v
Primary
   |
Append WAL
   |
Apply local write
   |
Send response
   |
Replicate asynchronously
   v
Replica
```

A later version may support synchronous replication.

Example:

```text
Client
   |
   v
Primary
   |
Write WAL
   |
Send to Replica
   |
Wait for ACK
   |
Commit
   |
Return OK
```

The README should document the durability tradeoff between synchronous and asynchronous replication.

---

# 12. Failure Handling

The system should tolerate at minimum:

- client disconnect
- primary process crash
- replica process crash
- replica restart
- temporary replica unavailability

When a replica restarts, it should determine what log entries it is missing.

Example:

```text
Primary LSN: 10,000

Replica LSN: 9,200
```

The primary should send:

```text
LSN 9201 -> 10000
```

instead of transferring the entire database.

If the gap is too large or the required WAL entries have already been compacted, the primary may instead transfer a snapshot.

---

# 13. Concurrency

The server must correctly handle multiple clients operating simultaneously.

Example:

```text
Client A:
SET balance 100

Client B:
GET balance

Client C:
SET user:1 Aaron
```

Shared database state must be protected using appropriate synchronization.

Potential approaches:

- mutexes
- read/write locks
- lock striping
- concurrent data structures

The first implementation may use a global mutex for correctness.

A later optimization can replace the global lock with finer-grained locking.

---

# 14. Consistency

Initial target:

**single-primary consistency**

All writes flow through the primary.

This creates a single global ordering of mutations.

Replica reads may initially be considered eventually consistent.

Example:

```text
SET x 10
```

Immediately reading from a replica may temporarily return an older value.

The system documentation must clearly state this behavior.

---

# 15. Optional Transactions

If core functionality is completed early, DistributeDB may support simple transactions.

Example:

```text
BEGIN

SET account:A 90
SET account:B 110

COMMIT
```

Minimum transaction properties:

- atomicity
- consistency of individual transactions
- basic isolation

A simple implementation may use a global transaction lock.

Full MVCC is explicitly outside the initial project scope.

---

# 16. Database Index

The first version may use an in-memory hash table.

A later version should implement a disk-aware index.

Two possible directions:

## Option A — B+ Tree

Implement:

- page-based nodes
- internal nodes
- leaf nodes
- node splits
- ordered range traversal

Example:

```text
            [20 | 50]
           /    |    \
        <20   20-50   >50
```

This direction emphasizes traditional relational database internals.

## Option B — LSM Tree

Implement:

```text
MemTable
   |
   v
Immutable MemTable
   |
   v
SSTable
   |
   v
Compaction
```

This direction aligns more closely with systems such as RocksDB and Cassandra.

Only one advanced index needs to be implemented.

---

# 17. Observability

The server should expose runtime statistics.

Example:

```text
STATS
```

Output:

```text
uptime_seconds: 3521
keys: 104923
requests_total: 834241
reads_total: 628443
writes_total: 205798
wal_entries: 205798
current_lsn: 205798
connected_clients: 14
replicas_connected: 2
replica_1_lag: 0
replica_2_lag: 34
```

Latency statistics should also be collected.

Possible metrics:

```text
p50 latency
p95 latency
p99 latency
throughput
replication lag
WAL flush latency
recovery time
```

---

# 18. Benchmarking

DistributeDB should contain a dedicated benchmarking tool.

Example:

```bash
./benchmark \
    --clients 32 \
    --operations 1000000 \
    --read-ratio 0.8
```

Workloads should include:

### Read-heavy

```text
90% GET
10% SET
```

### Balanced

```text
50% GET
50% SET
```

### Write-heavy

```text
10% GET
90% SET
```

Metrics:

```text
Operations/sec
Average latency
p50 latency
p95 latency
p99 latency
CPU usage
Memory usage
Database size
WAL size
```

Results should be published in:

```text
benchmarks/results/
```

---

# 19. Failure Testing

The project should deliberately test failure scenarios.

Example automated test:

```text
1. Launch primary.
2. Launch replica.
3. Write 100,000 records.
4. Kill primary during writes.
5. Restart primary.
6. Recover from WAL.
7. Compare recovered values with acknowledged writes.
```

Expected invariant:

```text
Every acknowledged durable write must survive restart.
```

Other failure experiments:

```text
Kill replica during replication
Restart replica
Verify catch-up

Corrupt final WAL entry
Restart database
Ignore incomplete record safely

Disconnect client during SET
Verify database remains consistent
```

---

# 20. Testing Strategy

Unit tests should cover:

```text
Command parsing
Storage engine operations
WAL serialization
WAL deserialization
Snapshot creation
Snapshot recovery
Replication message parsing
Index operations
Concurrency primitives
```

Integration tests should cover:

```text
Client/server communication
Database restart
Replica catch-up
Concurrent clients
Large workloads
Failure recovery
```

Tests should run through an automated test command.

Example:

```bash
ctest
```

or:

```bash
cargo test
```

depending on implementation language.

---

# 21. Repository Structure

Recommended layout:

```text
DistributeDB/
│
├── README.md
├── SOW.md
├── CMakeLists.txt
│
├── src/
│   ├── server/
│   ├── storage/
│   ├── wal/
│   ├── replication/
│   ├── networking/
│   ├── concurrency/
│   └── recovery/
│
├── include/
│
├── client/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── failure/
│
├── benchmarks/
│   ├── benchmark.cpp
│   ├── workloads/
│   └── results/
│
├── scripts/
│   ├── start_cluster.sh
│   ├── kill_primary.sh
│   └── run_benchmarks.sh
│
├── docs/
│   ├── architecture.md
│   ├── storage.md
│   ├── replication.md
│   ├── recovery.md
│   └── benchmarks.md
│
└── docker/
    └── docker-compose.yml
```

---

# 22. Development Phases

## Phase 1 — Local Key-Value Engine

Deliverables:

- SET
- GET
- DELETE
- in-memory storage
- command parser
- unit tests

Goal:

```text
Local database functionality works reliably.
```

---

## Phase 2 — Persistent WAL

Deliverables:

- WAL writer
- WAL reader
- sequence numbers
- durable writes
- restart recovery

Goal:

```text
Database survives process restart without losing acknowledged durable writes.
```

---

## Phase 3 — Networking

Deliverables:

- TCP server
- TCP client
- multiple client connections
- command protocol

Goal:

```text
Remote clients can interact with the database concurrently.
```

---

## Phase 4 — Snapshots

Deliverables:

- snapshot generation
- snapshot loading
- WAL truncation or rotation
- snapshot + WAL recovery

Goal:

```text
Recovery time no longer grows linearly with the entire command history.
```

---

## Phase 5 — Replication

Deliverables:

- primary node
- replica node
- replicated log
- replica acknowledgments
- replica catch-up

Goal:

```text
Multiple nodes maintain equivalent database state.
```

---

## Phase 6 — Failure Recovery

Deliverables:

- replica reconnect
- WAL replay
- missing-entry synchronization
- snapshot-based replica recovery
- kill/restart tests

Goal:

```text
The system continues operating correctly under controlled failures.
```

---

## Phase 7 — Performance Engineering

Deliverables:

- benchmark harness
- multi-client workload generator
- latency measurements
- throughput measurements
- profiling
- lock contention analysis

Potential optimization:

```text
global mutex
      ↓
reader/writer lock
      ↓
lock striping
```

Goal:

```text
Demonstrate measurable performance improvements rather than speculative optimization.
```

---

## Phase 8 — Advanced Storage

If time permits, implement either:

```text
B+ Tree
```

or:

```text
LSM Tree
```

The implementation should include tests and benchmarks comparing the advanced index with the initial storage approach.

---

# 23. Non-Goals

The following features are explicitly outside the initial scope:

- full SQL compatibility
- distributed transactions
- multi-primary consensus
- complete Raft implementation
- Kubernetes integration
- query optimizer
- joins
- secondary indexes
- schema migrations
- geographic replication
- production-grade authentication
- encryption-at-rest

These may be explored after the core system is complete.

The project should not sacrifice correctness of the fundamental storage and replication system in order to implement additional features.

---

# 24. Stretch Goals

Potential extensions include:

### Raft Consensus

Replace the manually configured primary with leader election using Raft.

```text
Follower
Follower
Follower
   |
Election
   |
Leader
```

---

### Automatic Failover

Detect primary failure and elect/promote a new primary.

---

### Sharding

Partition keys across nodes.

Example:

```text
hash(key) % node_count
```

---

### Consistent Hashing

Replace modulo partitioning with a consistent hash ring.

---

### MVCC

Implement multiple versions of records to support concurrent transactions.

---

### LSM Storage Engine

Create:

```text
MemTable
SSTables
Bloom Filters
Compaction
```

---

### Distributed Query Routing

Allow any node to receive a client request and forward it to the correct shard or primary.

---

# 25. Deliverables

The completed project should contain:

1. Functional distributed key-value database.
2. Persistent storage.
3. Write-ahead logging.
4. Crash recovery.
5. TCP client/server interface.
6. Concurrent client support.
7. Primary-replica replication.
8. Replica catch-up.
9. Snapshot support.
10. Automated unit tests.
11. Integration tests.
12. Failure-injection tests.
13. Benchmarking framework.
14. Published benchmark results.
15. Architecture documentation.
16. Docker-based local cluster setup.
17. README with reproducible commands.

---

# 26. Demonstration Scenario

The final demo should launch a three-node cluster.

```text
Node A — Primary
Node B — Replica
Node C — Replica
```

Run:

```text
SET user:1 Aaron
SET user:2 Alice
SET user:3 Bob
```

Verify values exist on all nodes.

Then:

```text
kill Node B
```

Continue writing:

```text
SET user:4 David
SET user:5 Emma
```

Restart Node B.

Node B should detect its missing log entries and synchronize automatically.

Next:

```text
kill Node A
```

Restart Node A.

Node A should reconstruct its state from its snapshot and WAL.

Finally, run the benchmark suite and display:

```text
Throughput
p50 latency
p95 latency
p99 latency
replication lag
recovery time
```

---

# 27. Success Criteria

The project will be considered complete when the following conditions are satisfied:

- Multiple clients can concurrently interact with the database.
- Data survives process crashes.
- WAL recovery reproduces acknowledged durable state.
- At least one replica maintains synchronized data.
- A disconnected replica can catch up after reconnecting.
- Snapshots reduce recovery work.
- Failure tests run automatically.
- Benchmarks produce reproducible throughput and latency measurements.
- System architecture and major design decisions are documented.
- The project can be reproduced locally from the README.

---

# 28. Final Project Positioning

DistributeDB should ultimately demonstrate the ability to design and implement a non-trivial software system involving:

```text
Networking
Concurrency
Storage
Operating-system I/O
Persistence
Distributed communication
Fault tolerance
Performance engineering
Testing
```

The project should emphasize measured engineering tradeoffs and correctness rather than simply maximizing feature count.

A finished DistributeDB should provide enough technical depth for detailed discussions around storage engines, replication, consistency, concurrency, crash recovery, networking, system performance, and distributed-system failure modes.