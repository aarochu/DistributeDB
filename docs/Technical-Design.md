# DistributeDB — Technical Design and Implementation Plan

**Status:** Revised design v3, not an implementation report
**Source baseline:** Attached *DistributeDB — Statement of Work* (SOW), sections 1–28, provided in the referenced “Candidate Evaluation” conversation
**Audience:** Software engineers, software architects, agentic architects, and future implementation agents
**Scope:** SOW phases 1–7 and their acceptance criteria; phase 8 and other stretch goals remain optional

## 1. Reading this document

`[SOW]` marks a requirement or option stated in the SOW. `[DECISION]` marks a proposed implementation choice added here. `[ASSUMPTION]` marks a condition on which a guarantee depends. `[OPEN]` marks a decision requiring project validation or a later choice. SOW examples are illustrative, not automatically protocol or file-format specifications.

This design specifies a manually configured primary with asynchronous replicas. “Fault tolerant” means recovery of an existing node and catch-up of replicas under the listed failures. It does **not** imply automatic primary promotion, zero data loss after primary disk loss, or availability while the sole primary is down. The SOW places automatic failover and consensus among stretch goals (§§23–24). The initial language/filesystem choice is recorded in [ADR-001](ADR-001-Language-and-Filesystem.md). The implemented WAL, snapshot, `CURRENT`, client-protocol, and replication-frame v1 layouts have byte-level tests. Format changes require versioning or an explicit incompatibility decision.

### 1.1 Requirement trace

| ID | Source | Requirement and disposition |
|---|---|---|
| R1 | SOW §§1, 4, 6 | `SET`, `GET`, `DELETE`, `EXISTS`; initial in-memory map backed by durable state. |
| R2 | SOW §§5, 13 | TCP, concurrent clients, partial input, malformed requests, disconnects, shutdown. |
| R3 | SOW §§7–9, 19 | WAL before durable acknowledgment; restart replay; snapshots; automated kill/restart tests. |
| R4 | SOW §§3, 10–14 | One ordered primary log, one or more replicas, lag reporting, catch-up, optional snapshot transfer, single-primary consistency. |
| R5 | SOW §§17–20 | Statistics, latency, benchmarks, unit/integration/failure tests. |
| R6 | SOW §§21–22, 25–27 | Phased delivery, local three-node demonstration, documentation, Docker setup, reproducible results. |
| R7 | SOW §§15–16, 23–24 | Transactions, advanced index, synchronous replication, Raft, failover, and sharding are outside the core plan; a semi-synchronous benchmark experiment is optional after core replication works. |

### 1.2 Proposed guarantees

| Operation / event | Core guarantee | Limit |
|---|---|---|
| Primary `SET` / `DELETE` returns `OK` in `fsync` mode | Its WAL record has been fully written, successfully synchronized with its group, and applied to the primary map before the response is sent. | Assumes the OS/storage honors synchronization and the data directory persists; a lost response leaves the outcome unknown to the client. |
| Primary `SET` / `DELETE` returns `OK_VOLATILE` in `os` mode | Its record was written to the OS and applied locally without an explicit sync. | Benchmark-only mode; it makes no restart-durability promise. |
| Primary `GET` / `EXISTS` | Reads the latest primary mutation whose response was sent before the read began; map application is the state boundary. | There is no multi-operation transaction or client-side exactly-once retry guarantee. |
| Replica read | Reads that replica's locally applied prefix. | It can lag the primary; it is not linearizable with primary writes. |
| Replica restart | Recovers a contiguous local prefix, then requests missing records or a snapshot. | It cannot serve as an automatically promoted primary. |
| Primary process crash | Acknowledged local writes reappear after restart. | A storage-device loss, lying flush, corruption outside the repairable tail, or simultaneous loss of all copies is outside the guarantee. |

## 2. Architecture and deployment

`[SOW]` Clients initially connect to a primary. The primary validates operations, logs mutations, updates local state, and replicates to followers. The final demonstration uses one primary and two replicas (§§3, 26).

`[DECISION]` The core node has six modules: framed TCP transport, command decoder, request coordinator, in-memory map, WAL/snapshot manager, and replication sender/receiver. A single mutation sequencer assigns LSNs. Replicas accept replication messages from the configured primary and may expose read-only client operations. The primary is selected in static configuration; startup must refuse a second primary against the same data directory. There is no election or automatic failover.

```text
clients -> framed TCP -> request coordinator -> map
                                |                 ^
                                v                 |
                         mutation sequencer -> WAL
                                |
                                +-> per-replica stream -> replica WAL -> replica map
                                                   \-> snapshot transfer when needed
```

`[DECISION]` Each node has its own data directory and stable `cluster_id` and `node_id`. Configuration includes role, client and replication listen addresses, primary address for replicas, limits, and storage path. The cluster identifier prevents accidental cross-cluster replication. A process takes an exclusive data-directory lock before recovery; if that lock is unavailable, startup fails. Replica data is never silently accepted from a different cluster or a divergent history.

### 2.1 Internal interfaces

The interfaces below are contracts, not a language commitment. Each method returns a typed result; errors are never converted to success responses.

```text
StorageEngine:
  get(key: bytes) -> Found(value: bytes) | NotFound
  exists(key: bytes) -> bool
  apply(record: Mutation) -> Applied | Error
  clone_at_boundary() -> (last_applied_lsn, immutable_state_copy)

Wal:
  append(record: Mutation) -> byte_offset | Error
  sync_through(lsn: u64) -> Durable(lsn) | Error
  scan(start_lsn: u64) -> ordered verified records | Corruption
  rotate_after(lsn: u64) -> Result

SnapshotStore:
  publish(lsn: u64, state_copy) -> SnapshotRef | Error
  load_latest_verified() -> (lsn, map) | None | Corruption

Replication:
  stream_from(last_durable_lsn, last_record_hash) -> Records | SnapshotRequired | Diverged
  install_snapshot(snapshot_ref, stream_cursor) -> Result

Node:
  execute(request) -> response
  recover() -> Ready | FailClosed
  shutdown(deadline) -> Result
```

`SET` replaces the entire value. `DELETE` always appends a tombstone and returns `OK`, including when the key is missing. It consumes an LSN even when it changes no value; this keeps a group's sequential result independent of the map state before that group. `GET` returns `NOT_FOUND` distinctly from a zero-length value. `EXISTS` returns a boolean. Keys and values are byte strings in storage; a CLI may accept UTF-8 text. No TTL, compare-and-swap, scan, or transaction semantics are implied.

## 3. Invariants and state machine

All implementers should preserve these invariants across modules and phases:

1. **Single writer:** At most one process writes a given node data directory. Only the configured primary accepts client mutations.
2. **Contiguous sequence:** Every stored mutation has a positive 64-bit LSN, and LSNs advance by exactly one within a history. A rejected command does not advance the LSN. Every valid `DELETE` does, even when the key is absent. Group footers do not have their own LSN.
3. **Write-ahead order:** Before a mutation changes the visible map, its complete WAL record is appended. Before `OK`, that record is synchronized successfully. On sync failure, the node stops accepting mutations and does not send `OK`.
4. **Prefix state:** A node's visible map equals application of a snapshot at LSN *S* and every verified mutation in the contiguous prefix *S+1…A*, where *A* is its applied LSN. It never applies an out-of-order entry.
5. **Acknowledgment:** A replica reports `durable_lsn = N` only after all records through *N* are locally synchronized; it reports `applied_lsn = N` only after they are reflected in its map. The primary does not interpret either as a quorum commit in asynchronous mode.
6. **Snapshot identity:** A published snapshot at *S* represents exactly the map after mutation *S*. The WAL needed for any restart remains available until that snapshot is verified and durably published.
7. **History identity:** Catch-up proceeds only when cluster ID and the record hash at the shared LSN match. A conflicting prefix is an error, never overwritten through ordinary WAL replay.
8. **Fail closed:** An LSN gap in a committed group, wrong version, damage in a sealed segment, unexpected middle-of-log corruption, or missing required file prevents serving requests. The last unclosed group in the active segment may be discarded under §6.3, including valid-looking records after its first damaged byte.
9. **Backpressure:** All inbound frames, queued requests, replica buffers, and snapshot transfers have explicit size bounds. Exhaustion produces a controlled error or disconnect rather than unbounded memory growth.

`[DECISION]` The normal local state transition for a mutation is `validated → assigned LSN → appended → group synchronized → applied → responded`. In benchmark-only `os` mode the synchronization step is omitted and the response is `OK_VOLATILE`. Replication is notified only for synchronized records in normal mode. The SOW's step list (§3) and asynchronous flow (§11) differ on response-versus-replication ordering, so this design uses the explicit asynchronous semantics from §11 and records the choice here.

## 4. Client TCP protocol

`[SOW]` The SOW shows textual commands, but gives no framing or encoding rule. `[DECISION]` Protocol v1 is a binary, length-prefixed request/response envelope with byte-string fields. A human-readable CLI translates `SET key value` and the other SOW commands into this wire format. This preserves command names while handling spaces, newlines, zero bytes, and partial TCP reads without quoting ambiguity.

### 4.1 Frame and payload

All integers are unsigned and little-endian, matching the WAL and replication protocols. A connection carries consecutive frames, with one request outstanding at a time. A reader first accumulates exactly 4 bytes, checks the length, then accumulates exactly that many bytes. It must not assume a `recv` corresponds to one request; TCP has no application message boundaries ([RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html)).

```text
frame := body_len:u32 | body[body_len]
body  := version:u8 | kind:u8 | payload
```

`body_len` is 2–1,048,576 bytes inclusive in v1. `version=1`. Unknown versions receive `UNSUPPORTED_VERSION` if the header is parseable, then the connection closes. The server rejects an oversized length before allocating a body buffer. Connection, read, write, and idle timeouts are configurable; defaults are proposed in §16.

Request `kind`: `1=SET`, `2=GET`, `3=DELETE`, `4=EXISTS`, `5=STATS`, `6=SCAN`, `7=PING`, `8=BEGIN`, `9=COMMIT`, `10=ROLLBACK`. `STATS` is an operational extension from the SOW optional list and is needed to expose replication lag. `SCAN` is a later extension that returns keys in order, and `PING` is a liveness check from the same SOW list. `BEGIN`, `COMMIT`, and `ROLLBACK` are the optional transactions of SOW §15, described below. The server reads one complete request, sends its response, then reads the next request on that connection. Different connections can interleave. There is no request ID or pipelining in v1.

`[DECISION]` A transaction belongs to one connection. After `BEGIN OK`, each `SET` and `DELETE` answers `QUEUED` and is held by the server, not written. `GET` and `EXISTS` on that connection see the transaction's own pending writes over committed state; `SCAN` is refused with `BAD_REQUEST`. `COMMIT` sends the queued mutations to the sequencer as one job, which is never split across groups. It answers like a single write: `OK` means every mutation is durable, and any other status means none was applied. Because the mutations share one footer-closed WAL group, recovery restores all or none of them (§6), and other connections see none or all of them, since a group is applied under one write lock. `ROLLBACK`, a disconnect, or a crash before `COMMIT` discards the queued writes. A transaction holds at most one group (64 mutations and 8 MiB). The write that would exceed that answers `RESOURCE_EXHAUSTED` and aborts the transaction: later writes answer `BAD_REQUEST`, and `COMMIT` writes nothing. Isolation is read committed with atomic visibility, with no conflict detection: of two transactions writing one key, the later `COMMIT` wins. Replicas apply the records in their own batches, so replica reads can briefly show part of a transaction before catching up. `BEGIN` on a replica answers `NOT_PRIMARY`.

```text
SET payload    := key_len:u32 | value_len:u32 | key | value
GET payload    := key_len:u32 | key
DELETE payload := key_len:u32 | key
EXISTS payload := key_len:u32 | key
STATS payload  := empty
SCAN payload   := start_len:u32 | end_len:u32 | limit:u32 | start | end
PING payload   := empty
BEGIN, COMMIT, ROLLBACK payload := empty

response payload := status:u16 | data_len:u32 | data[data_len]
```

In a response, `kind` echoes the request kind. Statuses: `0=OK`, `1=NOT_FOUND`, `2=BAD_REQUEST`, `3=NOT_PRIMARY`, `4=UNAVAILABLE`, `5=INTERNAL_ERROR`, `6=UNSUPPORTED_VERSION`, `7=RESOURCE_EXHAUSTED`, `8=OK_VOLATILE`, `9=QUEUED`. For `GET OK`, `data` is the value; for `EXISTS OK`, it is one byte (`0` or `1`); for `SET` and `DELETE`, success data is empty. Error data is bounded UTF-8 diagnostic text, never relied on for program behavior. `PING OK` carries the four bytes `PONG` and reads no database state. `STATS OK` carries bounded UTF-8 `name=value` lines with version as the first line; it is not JSON. `SCAN` returns pairs with `start <= key < end` in key order; `end_len=0` means no upper bound, bounds are at most 4,096 bytes, and `limit` is 1–10,000. `SCAN OK` data is `count:u32 | more:u8 | (key_len:u32 | value_len:u32 | key | value)*`. `more=1` means pairs remain past the last key: the limit was reached, or the page was cut to fit the frame. Each page is read under one read lock, so it is consistent; consecutive pages are not a snapshot. A response is never sent for a frame whose length prefix cannot be read; an invalid complete frame may receive `BAD_REQUEST` before closing.

`[DECISION]` Define one mutation limit: **the encoded WAL record including its four-byte length field is at most 1,000,000 bytes**. Since its fixed bytes total 33, accept `SET` only when `33 + key_len + value_len <= 1,000,000`, with `1 <= key_len <= 4,096`; `DELETE` uses the same formula with zero value bytes. Thus a 4,096-byte key permits at most 995,871 value bytes. The client frame and replica record frame each have a 1,048,576-byte body ceiling, so any accepted mutation fits both with envelope overhead. Apply the same key limit to reads. Reject trailing bytes, integer overflow, unknown kind, malformed field lengths, or invalid status payloads. The CLI's whitespace syntax is for demonstrations only; hex-encoded arguments in `client --hex` represent arbitrary bytes. Client and replication ports are separate so replica control messages cannot be parsed as client commands. Neither port is authenticated in the core SOW; local demo binds to loopback/private Docker networking only (§16).

### 4.2 Connection and retry behavior

Socket closure after a write request does not roll back a mutation. If the client disconnects or times out before receiving a success response, its outcome is **unknown**: the WAL may already contain the operation. A blind retry can duplicate a logical write and consume another LSN, although repeat `SET` of the same value is state-idempotent. Exactly-once request deduplication is outside v1; callers requiring safe retries need an application-level operation identifier or a future protocol extension. This limitation must appear in the README and tests.

## 5. Concurrency and ordering

`[SOW]` Concurrent clients are required; one thread per client is allowed initially (§§5, 13). `[DECISION]` Use one blocking thread per connection up to `max_connections=128`, plus a separate mutation sequencer thread. The acceptor rejects excess connections; each connection has one outstanding request. Thus 32 connected writers can all contribute to a batch rather than being capped by a smaller worker pool. The sequencer owns WAL append, group sync, map application, LSN assignment, and snapshot boundary selection. Its queue is bounded; overflow returns `RESOURCE_EXHAUSTED`. It drains whichever requests are already queued when it becomes free, up to **64 mutations and 8 MiB of encoded records plus footer**. The default intentional wait is **0 ms**; requests arriving while the previous sync is in progress naturally form the next group. A configurable wait may be used only after benchmarking. The sequencer appends the group and its footer, performs one WAL sync in `fsync` mode, applies mutations in order, then releases each response. A sync failure sends no success for that group and stops subsequent mutations. A full write to the OS without sync is enough only for `OK_VOLATILE` in benchmark mode.

Reads take the shared map lock directly; mutations take its exclusive lock only while applying. A read may run while a batch waits for WAL sync and will see the prior applied state. If a write response has already been sent before a read begins, the write was applied first, so the read sees it or a later mutation. A read concurrent with a write may see either side of that write's application boundary. Snapshot cloning runs at a sequencer boundary under the map lock, then writes the copied state without holding that lock. This is memory-expensive, but the initial map already keeps the full dataset in RAM. Cap snapshot memory and fail cleanly if a copy cannot be made.

The linearization point for `SET`/`DELETE` is map application after successful WAL sync in normal mode, or after OS write in benchmark-only mode. A primary `GET` linearizes while it holds the shared map lock. A write's response follows its linearization point. This yields linearizable single-operation behavior on the primary under a single process and a single configured primary, assuming the conditions in §1.2. There is no atomic read-modify-write or multi-key isolation. A replica's `GET` uses its own applied prefix and may be stale.

Deadlock rule: connection threads never hold socket or map locks while waiting for the sequencer; the sequencer never waits on network I/O; snapshot disk work occurs outside the sequencer; replica sender threads read immutable WAL ranges or snapshot files without holding map locks. A later event-loop conversion must preserve the same observable history and be justified by benchmarks.

## 6. Persistent layout and WAL

`[DECISION]` **Both primary and replica use the same generation directory layout.** Each node has an identity file, a `CURRENT` pointer, and an active generation containing immutable snapshots and append-only WAL segments. The primary creates generation 1 at initialization and never switches generations in the core design. Its ordinary local snapshots use sync plus rename *inside* that generation. A replica switches `CURRENT` only when installing a transferred snapshot and replacing its WAL continuation as one recoverable operation (§8.2). File names are data, not authority: validation uses embedded headers, checksums, LSNs, and cluster ID. One data directory cannot be shared by nodes. No storage engine page file is required in v1; the map is reconstructed from snapshot plus WAL.

```text
data/
  IDENTITY                  # version, cluster_id, node_id, role
  generations/
    0000000000000001/
      wal/                  # first-LSN-named segments
      snapshots/            # LSN-named snapshots
  CURRENT                   # active generation name + checksum
  tmp/                      # incomplete snapshots / incoming transfers
  LOCK                      # process exclusivity
```

### 6.1 WAL v1 byte format

All WAL fields are little-endian, independent of host architecture. The format is a proposed contract; golden byte fixtures must be checked before implementation. Every segment begins with a fixed 64-byte header: 8-byte magic `DDBWAL01`, `format_version:u16=1`, `header_len:u16=64`, `cluster_id:16 bytes`, `node_id:16 bytes`, `first_lsn:u64`, `reserved:8 zero bytes`, `header_crc32c:u32` over bytes 0–59. The `first_lsn` and filename must agree. IDs are binary UUIDs; byte order is treated as opaque.

Each mutation record is:

```text
record_len:u32             # bytes after this field, including final CRC; 29..999,996
lsn:u64
type:u8                   # 1 SET, 2 DELETE
key_len:u32
value_len:u32             # zero for DELETE
prev_hash:u64             # CRC64 of preceding mutation, zero at LSN 1
key[key_len]
value[value_len]
crc32c:u32                # over record_len through value (excludes this crc field)
```

The fixed bytes after `record_len` total 29 (`8+1+4+4+8+4`), including the final 4-byte CRC. Therefore `record_len = 29 + key_len + value_len`, and the complete encoded record is `4 + record_len = 33 + key_len + value_len <= 1,000,000`. The CRC input begins at the four-byte `record_len` field and ends at the final value byte; it excludes the stored CRC. CRC32C uses the Castagnoli polynomial with reflected representation `0x82F63B78`, initial register `0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. Define `record_hash = CRC64-ECMA-182(complete encoded record)`, including `record_len` and stored CRC. CRC64-ECMA-182 uses polynomial `0x42F0E1EBA9EA3693`, initial register zero, no input/output reflection, and final XOR zero ([CRC catalogue](https://reveng.sourceforge.io/crc-catalogue/17plus.htm)). Because the record contains `prev_hash`, this creates a chain of 64-bit checksums. It detects accidental divergence with a collision risk; it is not authentication or a proof of identical histories. A `DELETE` record has `value_len=0`. Reject unknown type, nonzero reserved bytes, invalid lengths, gaps, duplicate LSNs with different bytes, and checksum mismatch. Golden fixtures must state exact byte offsets and expected CRC/hash outputs.

Each group ends with a fixed 40-byte footer, distinct from a mutation record: `magic[8]=DDBGRP01 | first_lsn:u64 | last_lsn:u64 | count:u32 | last_record_hash:u64 | crc32c:u32`. The CRC covers the preceding 36 bytes. All integers are little-endian. A valid footer must match the contiguous mutations since the preceding footer or segment header, including count and final record hash. A group contains 1–64 mutation records; the records plus footer total at most 8 MiB. Rotation occurs **between** groups only, never inside one. Footers consume no LSN and are not sent as mutations to replicas. They are crash recovery boundaries, not independent client operations. The first group in a post-snapshot WAL segment starts at the segment's `first_lsn`.

`[DECISION]` Segment size target is 64 MiB; rotate only between complete groups. In `fsync` mode the current segment and its directory entry must be synchronized when created, and a segment is sealed by a successful sync before a new one becomes active. WAL writes handle partial OS writes and `EINTR`; append failures make the node read-only/fail-closed. Disk-full and sync errors are surfaced as `UNAVAILABLE` for the current request, and subsequent mutations are rejected until operator recovery.

### 6.2 Durability boundary

`[DECISION]` Default `fsync` mode uses bounded **group commit**: append a batch of complete records **and its footer**, successfully `fsync`/equivalent the WAL file once, apply those records in LSN order, then send `OK` for each. An `os` mode writes the group and footer to the OS, applies it, and responds `OK_VOLATILE` without explicit sync; it exists only to quantify the sync cost in benchmarks and cannot satisfy the durability acceptance gate. The selected policy is fixed at process start and reported by `STATS`, logs, and benchmark metadata; changing it on an existing data directory requires a clean stop and restart. When a new WAL file or renamed snapshot is created in `fsync` mode, its parent directory is synchronized as required by the target platform. Linux's `fsync` documentation explicitly distinguishes file content from directory entries ([Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)). The first implementation must choose and document a supported OS/filesystem profile; POSIX `fsync` leaves details dependent on the platform ([POSIX `fsync`](https://man7.org/linux/man-pages/man3/fsync.3p.html)). A process-kill test checks application recovery but does not prove survival of power loss or device failure.

`[ASSUMPTION]` The durability claim depends on a local filesystem and hardware honoring successful sync, with no undetected bit rot or administrative deletion. Docker bind mounts and virtualized storage require separate validation. Synchronous replica acknowledgment is not included; asynchronous replicas may lose acknowledged writes if the primary's durable storage is destroyed before replication catches up. `os` mode is restricted to a disposable single-primary data directory with replication disabled; it has no restart-recovery acceptance gate, and a crash may require resetting that directory before another benchmark.

An operation can be durable yet lack an `OK` because the process or connection failed after sync. Recovery replays it. This is required for consistency; the implementation must never remove such a valid record merely because no response was sent.

### 6.3 WAL recovery rules

Scan segments in LSN order and validate headers, record lengths, CRCs, the record-hash chain, contiguous mutation LSNs, and each group footer. **Only a footer-closed group is replayable.** In the last active segment, if parsing fails before the next valid footer, discard the entire group beginning after the preceding valid footer, even when valid-looking records follow the damaged record. A crash can persist later pages from an unsynced group while leaving an earlier page torn. If the bytes after that preceding footer exceed the 8 MiB group cap, or if damage occurs in a sealed segment, fail closed as interior corruption. If a footer and all its records validate, replay the group; it may have survived a crash even if no response reached the client. Truncate a discarded final group at the preceding footer, sync the truncation in `fsync` mode, and emit `tail_truncated`. This implements the SOW's corrupt-final-entry test (§19) without treating a later intact record in the same unsynced group as evidence of acknowledged data.

This rule still depends on the storage model: successful sync preserves the acknowledged group and its footer. A later device fault can corrupt an acknowledged group, and recovery cannot always distinguish that from a torn unsynced final group. The guarantee “every acknowledged write survives” applies to process crashes and the specified simulated-power-loss model, not arbitrary post-sync corruption. The simulator must verify that no acknowledged group is ever discarded. A deliberately corrupted synced group is tested separately and documented as outside that guarantee.

`[DECISION]` Recovery may replay a complete footer-closed group that was not synchronized before a process crash. This can make an unacknowledged request visible after restart, consistent with the unknown-outcome rule. The system must never infer from a local log record alone that a response reached a client. A WAL inspection tool reports last validated group and last applied LSN.

### 6.4 Simulated power-loss file layer

`[DECISION]` Phase 2 includes a small file-I/O abstraction around append, sync, truncate, create, rename, directory sync, and read. Its deterministic test implementation maintains **volatile** and **stable** file/directory images. Writes alter the volatile image; a successful file sync copies the eligible file bytes into the stable image; a successful directory sync makes name changes stable. A simulated crash discards volatile state and restarts recovery from stable state. Before a group sync succeeds, the layer may persist its pages in arbitrary order, including an intact later mutation after a torn earlier mutation. It can also inject short writes, failed syncs, and crashes at every step. It must **never** alter already synced bytes in the core model; a separate corruption test deliberately violates that assumption and verifies the narrower guarantee in §6.3. The model's rules and seed are published so its results are reproducible. This is a design-level test of call ordering, not a substitute for physical power-cut testing on the target filesystem.

## 7. Snapshots and reclamation

`[SOW]` Snapshots record state at an LSN and permit replay only after that point (§9). `[DECISION]` Snapshot v1 is an immutable sequence of key/value pairs in map iteration order; sorted output is unnecessary. Its 64-byte header has exact offsets: `magic[0:8] = DDBSNP01`, `version:u16[8:10] = 1`, `header_len:u16[10:12] = 64`, `cluster_id[12:28]`, `snapshot_lsn:u64[28:36]`, `record_hash_at_lsn:u64[36:44]`, `entry_count:u64[44:52]`, `payload_len:u64[52:60]`, `header_crc32c:u32[60:64]` over bytes 0–59. At LSN 0, `record_hash_at_lsn=0`. Each payload entry is `key_len:u32 | value_len:u32 | key | value`. A final `snapshot_crc64:u64` covers the entire header and payload using the same CRC64-ECMA-182 parameters as §6.1. All integers are little-endian. Duplicate keys, invalid lengths, wrong cluster, mismatched count/size, or bad checksum invalidate the snapshot. The record hash at the boundary lets a replica check catch-up history subject to CRC collision risk; golden fixtures must cover the header and a small snapshot.

**Publication sequence:** (1) at a completed-group sequencer boundary *S*, clone the map and record hash; (2) rotate WAL so later mutations are in a segment beginning *S+1*; (3) write a temporary snapshot in the active generation on the same filesystem; (4) fully write and sync it; (5) rename to its immutable final name; (6) sync the snapshot directory; (7) reload and verify the new snapshot from disk against its header, count, checksum, and a sampled or full reference-map comparison in tests; (8) only then mark snapshot *S* eligible for recovery and delete WAL segments wholly covered by *S*, subject to retention pins and the previous-chain rule below. Sync the WAL directory after deletion. On failure at any step, keep the old snapshot and WAL. Recovery selects the highest fully verified snapshot for which the remaining WAL is contiguous; if none qualifies, start from an empty state and LSN 0 only when a full log from LSN 1 exists. A corrupt latest snapshot must not cause silent fallback after older WAL was deleted; fail closed if no complete recovery chain remains.

`[DECISION]` Keep the previous verified snapshot and the WAL needed to reconstruct it until the newer snapshot has passed an actual reload verification; then the older recovery chain may be reclaimed. Reclamation must respect active catch-up readers; a replica that falls behind retained WAL receives a snapshot instead. The primary may reclaim records at or below its published snapshot LSN after any active stream has finished or pinned those files. Retention is bounded by disk budget; reaching the budget pauses new writes with `RESOURCE_EXHAUSTED`, rather than deleting data needed for recovery. Snapshot transfer pins a snapshot and the post-snapshot WAL range until the replica advances past it, or aborts and retries with a newer snapshot.

**Claim boundary:** A snapshot reduces replay from “all historical mutations” to “mutations since the chosen snapshot”; it does not guarantee a fixed restart time. Loading the full in-memory map is still proportional to live data size, and snapshot cadence plus post-snapshot WAL length determine recovery work. Acceptance measures bytes/records replayed and elapsed time rather than asserting a universal latency bound.

## 8. Replication and catch-up

`[SOW]` Replicas consume the primary's ordered mutations and report lag; asynchronous replication is allowed initially (§§10–12). `[DECISION]` One primary connection per replica carries ordered replication frames, with bounded in-flight bytes. The primary sends only locally durable mutation records. The replica validates them, appends bounded local groups with the same footer format, syncs each group, applies it in LSN order, then ACKs durable/applied LSNs. It may group records differently from the primary because footers are local recovery boundaries, not replicated mutations. This makes replica restart behavior unambiguous. The primary does not wait for this ACK before client `OK`.

### 8.1 Replication wire contract

Use the same four-byte **little-endian** length prefix as the client protocol but a distinct port and message namespace. Body: `version:u8 | kind:u8 | payload`. Frame cap 1,048,576 bytes for record frames and 262,144 bytes for snapshot chunks. Kinds: `1=HELLO`, `2=HELLO_ACK`, `3=RECORD`, `4=ACK`, `5=SNAPSHOT_OFFER`, `6=SNAPSHOT_CHUNK`, `7=SNAPSHOT_DONE`, `8=ERROR`, `9=HEARTBEAT`. Golden protocol fixtures must freeze field offsets, all payload layouts, and malformed-frame behavior before two-node integration.

`HELLO` contains cluster ID, replica node ID, last durable LSN and hash, and supported format versions. `HELLO_ACK` contains primary cluster ID, latest durable LSN/hash, earliest retained LSN, and selected protocol version. A record message contains the exact WAL mutation bytes plus current record hash. `ACK` contains highest contiguous durable LSN and highest applied LSN; never ACK a gap. Heartbeats report current durable LSN and monotonic send time for liveness/lag, but do not imply lease, election, or promotion. Unknown versions or invalid lengths close the connection with a metric increment. Replication messages are accepted only from the statically configured peer endpoint in the local demo; endpoint matching is not authentication.

Payloads, including embedded WAL records, use little-endian integers. The 16-byte IDs are opaque. `HELLO = cluster_id[16] | replica_id[16] | durable_lsn:u64 | record_hash:u64 | wal_version:u16 | snapshot_version:u16`; `HELLO_ACK = cluster_id[16] | primary_id[16] | durable_lsn:u64 | record_hash:u64 | earliest_retained_lsn:u64 | wal_version:u16 | snapshot_version:u16`; `RECORD = record_bytes_len:u32 | exact_wal_record | record_hash:u64`; `ACK = durable_lsn:u64 | applied_lsn:u64 | record_hash_at_durable_lsn:u64`; `HEARTBEAT = durable_lsn:u64 | send_monotonic_ns:u64`. The receiver requires `applied_lsn <= durable_lsn` and checks hashes at ACK boundaries. Connection closure discards in-flight messages; a new connection starts with `HELLO`, so a separate stream ID is unnecessary.

`SNAPSHOT_OFFER = snapshot_lsn:u64 | record_hash_at_lsn:u64 | snapshot_bytes:u64 | snapshot_crc64:u64`. `SNAPSHOT_CHUNK = snapshot_lsn:u64 | offset:u64 | chunk_len:u32 | bytes[chunk_len]`; chunks must be contiguous from offset zero, with no overlap or gap. `SNAPSHOT_DONE = snapshot_lsn:u64 | snapshot_crc64:u64`. A chunk is at most 262,144 bytes **including** its replication envelope and payload fields, so its data portion is smaller. The replica verifies the complete snapshot's embedded checksums and offered CRC64 before publication. `ERROR = code:u16 | diagnostic_len:u16 | bounded_utf8_diagnostic`; error code numbers and the diagnostic cap must be frozen in protocol fixtures. Reject messages that do not match the current connection state. The primary pins the offered snapshot and its continuation WAL until `SNAPSHOT_DONE` is acknowledged by an `ACK` at the snapshot LSN or the connection is abandoned.

### 8.2 Reconnect algorithm

1. Replica recovers its local snapshot/WAL and sends its last durable LSN plus hash.
2. Primary checks cluster ID and history at that LSN. If a retained WAL record or snapshot boundary provides the hash and it matches, stream `LSN+1` onward. If replica LSN is ahead of primary, or a **verifiable** shared LSN hash disagrees, stop with `DIVERGED`; do not rewrite history.
3. If the replica's LSN is below the earliest retained WAL and the primary has no hash checkpoint at that LSN, the primary **cannot verify the old prefix**. Snapshot installation is then a deliberate rebootstrap, not verified catch-up. It is allowed only when the replica's cluster ID matches and its persisted configuration explicitly sets `allow_snapshot_rebootstrap=true`; otherwise return `REBOOTSTRAP_REQUIRED` and stop. With authorization, offer a verified snapshot at *S* together with its hash and metadata, log the replacement of the old local history, download it to a temporary file, validate count/hash/checksum, and sync it. A known hash mismatch still stops as `DIVERGED` even when the flag is set.
4. Replica pauses serving reads, installs that snapshot atomically, resets its local WAL to an empty continuation from *S+1*, and reports *S* applied. The primary retains WAL from *S+1* while transfer is active. Replica then consumes the stream until caught up.
5. If transfer is interrupted, discard the incomplete temporary file and retry from the last valid local recovery point. If the retained continuation disappears before transfer finishes, renegotiate using a newer snapshot.

For step 4, the replica's snapshot and WAL reset are one recoverable generation change. Every node reads `CURRENT` to select a directory under `generations/`; each generation contains its own `wal/` and `snapshots/`. `CURRENT` v1 is 28 bytes: magic `DDBCUR01` (8), version `u16=1`, six reserved zero bytes, generation ID `u64`, and CRC32C `u32` over the preceding 24 bytes, all integers little-endian. The generation directory name is the ID as 16 lowercase hexadecimal digits. Write and sync all files in the new replica generation, sync its directories, write and sync a temporary `CURRENT`, atomically rename it over the old pointer, sync the data directory, then reclaim the old generation. On restart, validate the pointed-to generation; retain the previous generation until publication is confirmed. The primary's `CURRENT` stays at its initial generation; ordinary local snapshots are rename-published inside that generation (§7). Do **not** replace a replica snapshot and truncate its active WAL as independent operations.

The replica can serve reads after it installs a valid snapshot, but its reported lag remains nonzero until replay finishes. Replica lag is `primary_durable_lsn - replica_applied_lsn` when both are in the same history; report `unknown` when disconnected or divergent. A value of zero is a sampled state, not a promise the replica will stay current.

### 8.3 Failure handling and availability

| Failure | Required behavior |
|---|---|
| Client disconnect | Server finishes or aborts processing according to its current state; no partial map mutation. Client result may be unknown. |
| Replica disconnect / crash | Primary continues accepting writes; bounded sender queue is discarded, replica reconnects from its persisted prefix. |
| Primary process crash | Clients receive connection failure while it is down. Same primary recovers local state and resumes replication; no automatic promotion. |
| Disk full / sync error | Affected node stops acknowledging mutations, records an error metric, and requires recovery or operator action. |
| Network partition | Primary continues local durable writes; replicas can become stale. A partitioned replica cannot take writes. |
| Replica divergence | Stop replication and require explicit rebootstrap or operator investigation; do not mask it as ordinary lag. |
| Corrupt WAL/snapshot | Fail closed except for the final unclosed group in the active segment as defined in §6.3. |

The SOW's phrase “replicate committed writes” is interpreted as “send locally durable mutations.” There is no distributed commit point in asynchronous mode. The `ACK` above means replica durability/apply progress, not that the client write was committed by a quorum. Synchronous replication requires a separately specified commit protocol, failure policy, and fencing rules; merely waiting for one ACK would not by itself supply consensus or safe automatic failover.

## 9. Recovery algorithm

Startup is unavailable to clients until recovery completes. It must:

1. Acquire exclusive directory lock and validate identity/configuration/version.
2. Enumerate immutable generations, snapshots, and WAL segments; reject unexpected conflicting identities.
3. Choose a verified snapshot and contiguous WAL chain, preferring the newest fully recoverable state. Never skip a corrupt record in the middle.
4. Load snapshot map and hash, then replay each verified record after snapshot LSN in order. A replayed `SET` replaces the value; `DELETE` removes it. Application is deterministic and idempotent only with respect to the same exact record/LSN; a duplicate record at the same LSN is accepted only if byte-identical and is skipped.
5. Discard the final unclosed group in the last active segment only as allowed in §6.3, sync the truncation in `fsync` mode, set last durable/applied LSN, and publish readiness.
6. A replica connects to the primary and catches up; its readiness and sync status are separate. A primary starts replication senders after local recovery.

Persisted `applied_lsn` metadata is advisory: recovery derives the map from snapshot and WAL, avoiding a state where an advanced metadata counter hides unapplied records. A failed replay is a startup failure. Log and metric output should name the file, offset, LSN, and error class without printing user values.

## 10. Observability and diagnostics

`[SOW]` Expose runtime statistics and latency/lag (§17). `[DECISION]` `STATS` returns versioned `name=value` lines for role, durability policy, uptime, keys, connected clients, requests by command/status, accepted and rejected writes, current assigned/durable/applied LSN, WAL bytes, snapshot LSN, WAL sync count/errors/duration, replication connections, per-replica applied LSN and lag, and tail truncations. Add further counters only when they support a test or a stated operational question. Monotonic counters reset on process restart; LSNs and snapshot metadata persist. Use a monotonic clock for durations and a wall clock only for log timestamps.

Latency histograms cover end-to-end server request time and WAL sync separately. Publish count, p50, p95, p99, and maximum only for a stated time window with its sample count; percentile values from a tiny sample are still reported but should not be interpreted as stable. Logs include `event`, `node_id`, `role`, `lsn`, `peer`, and `error_code` where applicable and redact key/value bytes by default. Readiness is a boolean: a recovering node does not accept client requests; a caught-up replica's lag is shown separately rather than encoded into extra health states.

## 11. Benchmark plan

`[SOW]` Dedicated benchmark tool, read-heavy 90/10, balanced 50/50, write-heavy 10/90, throughput and latency plus resource usage, results under `benchmarks/results/` (§18). `[DECISION]` The client tool emits both a machine-readable result and a human summary. Every run records code revision, build mode, OS/kernel, filesystem, storage medium, CPU, RAM, node placement, dataset/key/value sizes, seed, clients, operations, warm-up, run duration, durability mode, snapshot policy, replication topology, and whether replicas were caught up. Use a fixed seed and a prepopulated dataset; specify whether reads are hits/misses and whether writes overwrite or insert. Random distributions are documented (uniform by default; skew is an additional workload).

Measure client-observed completion latency from send to full response and successful operations/sec after warm-up. Report failures/timeouts separately; excluding them from a throughput numerator without disclosure is misleading. Capture CPU, peak RSS, data directory bytes, WAL bytes, sync latency, recovery duration, replica lag, and snapshot pauses. Run at least three repetitions per workload/configuration and publish individual results, median, and variability. Compare **single-primary `os`**, **single-primary `fsync` with group commit**, and **`fsync` primary with one and two asynchronous replicas** on the same workload. The `os` run is a speed baseline without a durability guarantee, not an acceptance substitute. A local Docker cluster is not a network-isolated deployment. Profile before changing locks or indexing. No performance target is claimed before hardware and workload baselines exist.

## 12. Testing and adversarial verification

`[SOW]` Automated unit, integration, and failure tests, including process kill/restart and replica catch-up (§§19–20). `[DECISION]` A deterministic reference model applies the same successful mutation sequence to a simple map. Test harnesses record response boundaries and compare recovered state to the model, accounting for unknown-outcome writes separately. Assertions check prefixes and histories, not just final values.

| Layer | Required tests and falsification target |
|---|---|
| Parser/framing | Split every frame at every byte boundary; coalesce frames; zero-length/trailing fields; oversized length; integer overflow; invalid version/kind; disconnect mid-frame. No allocation above cap, crash, or frame confusion. |
| WAL format | Golden byte fixtures; CRC/hash changes; random truncation at every byte offset; sealed-segment corruption; LSN gap/duplicate/overflow; endian-independent decode. No corrupt record silently applied. |
| Durability | Use simulated power-loss layer (§6.4); inject failure before append, during short write, before/after group sync, before/after map apply, before response; kill process at each cut point. Every observed `OK` survives restart under assumptions; unknown outcomes may survive. `OK_VOLATILE` is excluded. |
| Snapshot | Kill after clone, temp write, file sync, rename, directory sync, WAL deletion; load oldest/newest valid local snapshot or replica generation; corrupt header/payload/trailer; concurrent writes during copy. State equals a valid prefix and no acknowledged data is lost. |
| Concurrency | Many clients interleave writes/reads; record completion intervals and check a linearizable sequential history for primary single-key operations; run under thread sanitizer where supported. No torn state, deadlock, or LSN duplicate. |
| Replication | Fragment/coalesce frames; drop/reorder/duplicate at test transport; replica crash before/after WAL sync and ACK; disconnect after snapshot chunk; reclaim WAL during transfer; mismatched cluster/hash. No gap applied or false ACK; rejoin converges or fails explicitly. |
| Failure demo | Three nodes; stop one replica, write, restart and catch up; stop/restart primary; verify values, hashes, lag, and recovery events. |
| Resource limits | Slow clients/replicas, disk full, queue saturation, huge keys/values, many connections. Bounded memory and controlled errors. |

The process-kill test and simulated layer are evidence for their stated crash models, not for physical power-loss durability on all devices. A physical power-cut or fault-injected filesystem test on the selected platform is needed before making that stronger claim. Tests must not assume an unacknowledged write is absent. An acknowledged write recovered on the primary but not yet sent to a replica is expected in asynchronous mode. A replica at zero sampled lag can still be lost later; catch-up is a convergence property only while the primary remains reachable and retains or can transfer the necessary state.

### 12.1 Adversarial claim audit

| Claim someone might infer | Counterexample / limit | Required wording or proof |
|---|---|---|
| “WAL append makes a write durable.” | Buffered data can disappear on crash; write can be partial. | `OK` only after full append and successful sync; test injected short writes and sync failure. |
| “`fsync` of snapshot file publishes its name.” | Directory entry may not be durable. | Sync containing directory after rename on the supported platform; validate crash cuts. ([Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)) |
| “One `recv` is one command.” | TCP is an octet stream. | Length framing and fragmented/coalesced tests. ([RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html)) |
| “CRC means a record is authentic.” | CRC can be forged and is not cryptographic authentication. | State it detects accidental damage; restrict demo network; authentication is outside SOW core. |
| “ACK means the client write is replicated.” | In async mode client `OK` precedes replica ACK. | Report local durable `OK` and per-replica progress separately. |
| “LSN equality means state equality.” | Different histories may reuse the same number. | Compare cluster ID and hash at common LSN; validate snapshot content. |
| “Replica lag zero means strong reads.” | Lag is sampled; next write may be pending. | Document eventual replica reads. |
| “Snapshot makes recovery constant time.” | Snapshot loading grows with live data and WAL tail grows between snapshots. | Measure load and replay separately. |
| “A later intact record after a torn record proves interior corruption.” | An unsynced group can persist out of order; later records can be intact while an earlier record is torn. | Replay only footer-closed groups; discard the final unclosed group within the 8 MiB bound and report it. Synced-byte corruption remains outside the crash-model guarantee. |
| “Killing the primary proves failover.” | No leader election or promotion is in core scope. | Demo restarts the same primary; describe downtime. |
| “Exactly-once writes follow from LSNs.” | Client can retry after a lost response. | State unknown outcomes; add request deduplication only as a future extension. |

## 13. Phased implementation and acceptance gates

Each phase produces runnable code, tests, and updated design/README notes. A future AI agent should read this document, the SOW, current code/tests, and unresolved decisions before changing a module; it must report which invariants it affects and add a falsifying test for a new guarantee. Do not infer completion from file presence or a green unit suite alone.

| Phase | Implementation order | Acceptance gate |
|---|---|---|
| 1 — Local engine | Implement typed commands and map; define `SET`/`DELETE` semantics; reference-model tests. | Required four operations behave correctly for empty/binary values and missing keys; no persistence claim yet. |
| 2 — Persistent WAL | Freeze v1 byte fixtures; append/group-footer/group-sync; scan/replay; identity and lock; simulated power-loss file layer. | Zero lost `OK` writes across at least 1,000 seeded randomized simulated crashes and 100 real process-kill/restart runs spanning append, footer, sync, apply, and response; test every deterministic cut point in a short trace, including later intact records after a torn record; discard the final unclosed group and fail closed for older corruption. Publish seeds and failure traces. |
| 3 — Networking | Freeze protocol fixtures; framed TCP client/server; one thread per connection up to the configured cap; graceful shutdown. | At least 32 concurrent clients complete 10,000 mixed operations; every byte split of representative frames and 1,000 seeded fragmented/coalesced streams parse correctly; malformed inputs do not crash the server. |
| 4 — Snapshots | Boundary copy, WAL rotation, durable publication, recovery selection, safe reclamation. | Zero lost `OK` writes across at least 1,000 seeded snapshot crash cuts; snapshot plus tail matches the reference map; a controlled 100,000-mutation history replays fewer records after snapshot than from LSN 1. |
| 5 — Replication | Static roles; stream/ACK; durable replica WAL; read-only replica interface; lag metrics. | One and two replicas each reach the primary's LSN/hash after 10,000 writes; primary continues to acknowledge writes during a 60-second replica outage. |
| 6 — Failure recovery | Reconnect, hash check, WAL catch-up, snapshot transfer with generation switch, kill/restart harness. | Run the three-node SOW demo at least 100 seeded times; stopped replica converges within a test timeout declared before running; primary restarts from snapshot/WAL; forced history mismatch fails explicitly. |
| 7 — Performance | Workload generator, repeatable results, profiling; optimize only with measured evidence. | All three SOW mixes run at least three times in each durability/topology configuration; publish throughput, p50/p95/p99, errors, resource use, group size, and environment metadata. |
| 8 — Optional storage | Choose B+ tree **or** LSM only after phases 1–7; specify its own crash invariants and migration. | Correctness and benchmark comparison to map/WAL baseline; core completion does not depend on this phase. |

### 13.1 Overall acceptance

The core project is complete when R1–R6 pass on a documented local environment: concurrent client operations; recovery of all acknowledged local durable writes after process kill; at least one replica synchronized and able to catch up after disconnection; snapshot recovery and measured replay reduction; automated failure tests; repeatable benchmark outputs; architecture and decision records; Docker three-node setup; and README commands that an engineer can execute from a clean checkout. Preserve the SOW's repository areas (`src`, `include` or equivalent, `client`, `tests`, `benchmarks/results`, `scripts`, `docs`, `docker`) while adapting build files to the selected language. `STATS` should expose lag even if other optional commands remain absent.

The numeric gates are minimum test coverage, not statistical proof of a zero failure rate. A failing seed is retained as a regression case; increasing trial count does not compensate for an untested failure class. Timeouts and hardware are fixed in the test manifest before a run, so tests cannot pass by silently extending deadlines.

### 13.2 Effort and sequence estimate

`[DECISION, planning estimate]` For one engineer familiar with the chosen language and able to work roughly 30 focused hours per week, allow 1 week for phase 1, 2–3 weeks for phase 2, 1 week for phase 3, 1–2 weeks for phase 4, 2 weeks for phase 5, 2 weeks for phase 6, and 1 week for phase 7: approximately **10–12 weeks** including integration and rework. These are estimates, not SOW commitments; compare them with the actual available calendar before scheduling. Phase 8 is additional work and should not displace failure testing. The dependency chain is local semantics → durable WAL → TCP → snapshot → replication → catch-up/failure → benchmarks. Freeze file-format fixtures in phase 2 and protocol fixtures in phases 3 and 5. If the available calendar is shorter, preserve phases 1–6 and trim optional commands, replica reads, and performance optimizations before relaxing correctness gates.

## 14. Risks and mitigations

| Risk | Impact | Mitigation / decision gate |
|---|---|---|
| Flush semantics differ by OS/filesystem/virtualization | Overstated durability | Specify support profile, sync files and directories, test power-loss separately, state assumptions. |
| Snapshot publication and WAL deletion race | Irrecoverable gap | Local rename publication; replica `CURRENT` generation switch; ordered sync, crash cut-point tests, retention pins. |
| Async replication loses writes on primary storage loss | Replica may lack acknowledged writes | Document local-durable scope; optional synchronous mode is a separate design. |
| Multiple manually configured primaries | Divergent histories | Static roles, directory locks, cluster/history checks; operational rule forbids promotion without fencing. |
| Slow or disconnected replica | Backlog/disk growth | Bounded queues and WAL retention; snapshot fallback; pause writes at disk limit. |
| Large in-memory map / snapshot copy | OOM or long pauses | Memory budget, admission limits, stream snapshot format later if measured; fail copy without data loss. |
| Wire/file format drift | Incompatible nodes or unreadable data | Version fields, golden fixtures, explicit migration policy, reject unknown versions. |
| Client retries | Duplicate logical operations | Document unknown outcomes; add dedup IDs only if required. |
| Local benchmark artifacts | Misleading performance claims | Full environment and workload metadata, repeated runs, no unstated extrapolation. |

## 15. Decision status and design review checklist

The SOW leaves these choices open. Their implementation status and remaining review work are:

1. **Implementation language and platform.** Resolved by [ADR-001](ADR-001-Language-and-Filesystem.md): stable Rust and an initial 64-bit Linux/local-ext4 durability profile. Physical power-loss behavior remains unverified.
2. **Binary format freeze.** WAL, snapshot, `CURRENT`, client-protocol, and replication-frame byte layouts have golden tests. A format change requires version migration or an explicit incompatibility decision.
3. **Generation verification.** Both roles use the same generation layout and checked `CURRENT` format. The primary remains in generation 1; a replica snapshot install switches generations. Simulated crash tests cover pointer replacement and cleanup; they do not prove hardware power-loss behavior.
4. **Operational defaults.** Connection count, queue depth, idle timeouts, WAL segment size, disk budget, and memory budget still need workload-specific tuning. Snapshot scheduling is manual and offline; no automatic interval is implemented.
5. **Replica reads.** An optional read-only replica listener is implemented and documented as eventually consistent. `STATS` exposes its applied LSN; individual read responses do not include it. The SOW requires clients to initially use the primary.
6. **Operational rebootstrap.** The replica provisioning flag permits snapshot replacement only when the old LSN cannot be verified and cluster IDs match. A known hash mismatch remains fatal even with that flag. The initial explicit reset procedure is to preserve the divergent directory and provision a new, empty replica directory after choosing the authoritative history; see [replication operations](replication.md#known-divergence-explicit-replacement). There is no in-place reset command or implicit reconnect side effect.
7. **Security boundary.** Core SOW excludes production authentication and encryption at rest. Confirm the demo is loopback/private-network only; any public deployment needs a separate threat model, authentication, transport encryption, and access controls.

Reviewers should trace each claimed guarantee to an invariant, a specific durable boundary, and an adversarial test. If one of those is absent, weaken the claim or add the missing design/test before implementation.

## 16. Defaults and implementation notes

`[DECISION, provisional]` Use 64 MiB WAL segments, 1,048,576-byte maximum frame body, 4,096-byte maximum key, **1,000,000-byte maximum encoded mutation record** (including its four-byte length field), 30-second idle client timeout, 5-second replica heartbeat interval, and 15-second replica reconnect detection. Use group-commit limits of 64 records and 8 MiB of records plus footer, with **0 ms intentional wait** by default. These are initial test defaults, not SOW requirements or throughput targets. The encoder enforces `33 + key_len + value_len <= 1,000,000` with checked arithmetic; there is no independent maximum-value constant that can contradict it. Snapshot publication is currently manual and offline; an automatic trigger such as 100,000 mutations or 1 GiB of WAL remains a candidate, not an implemented default.

`[OPTIONAL, phase 7]` A **semi-synchronous comparison** may wait for one replica's `durable_lsn` ACK before responding to a client after the primary's local sync and apply. It must define a timeout outcome (`UNAVAILABLE`, with the write possibly present), retain the primary's single-writer role, and report which replica acknowledged. It does not authorize replica promotion, prevent split brain, or provide consensus. Benchmark it against async only after its failure matrix covers replica disconnect before ACK, ACK loss, primary crash after replica ACK, and reconnect. If the project schedule is tight, omit this experiment without weakening core acceptance.

The README should state: required OS/filesystem profile; build/test commands; local three-node setup; data directory isolation; primary/replica ports; exact `OK` meaning; replica staleness; unknown client outcomes; backup/snapshot limitations; benchmark procedure; and recovery/fail-closed operator actions. A Docker Compose example should use volumes whose persistence semantics are documented and should not be presented as proof of power-loss durability.

## 17. External technical references used for claim validation

These references validate narrow platform/protocol facts; DistributeDB's format and semantics above are design proposals, not claims borrowed from these sources.

- [RFC 9293, Transmission Control Protocol](https://www.rfc-editor.org/rfc/rfc9293.html): TCP provides a byte stream without application message boundaries.
- [Linux `fsync(2)` manual](https://man7.org/linux/man-pages/man2/fsync.2.html): file synchronization and the separate need to synchronize directory entries on Linux.
- [POSIX `fsync` specification](https://man7.org/linux/man-pages/man3/fsync.3p.html): synchronization completion and platform-dependent guarantees.
- [Linux ext4 atomic-write documentation](https://docs.kernel.org/filesystems/ext4/atomic_writes.html): untorn writes require specific filesystem/device support; ordinary WAL records must therefore be framed and validated rather than assumed atomic.
- [CRC catalogue, CRC-64/ECMA-182](https://reveng.sourceforge.io/crc-catalogue/17plus.htm): algorithm parameters used for the proposed accidental-divergence checksum.

**Source boundary:** The attached SOW is the source of project requirements and phase scope. External references support technical constraints only. All numeric limits, binary layouts, concurrency structure, recovery rules, and protocol encodings in this document are added design decisions pending the review gates above.
