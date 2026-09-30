//! Persistent Write-Ahead Log: data-directory layout, writer with group
//! commit, and crash-recovery replay (Technical-Design §2.1, §3, §6, §9;
//! SOW §7, §8, §22).
//!
//! Phase 2 adds durable persistence on top of the Phase 1 in-memory engine.
//! This module ties together:
//!
//! * The pure byte format in [`format`] (Technical-Design §6.1).
//! * The [`FileSystem`](crate::fileio::FileSystem) abstraction from FEAT-001,
//!   so the writer/reader run against either [`RealFs`](crate::fileio::RealFs)
//!   or the deterministic [`SimFs`](crate::fileio::SimFs).
//!
//! # Data-directory layout (Technical-Design §6)
//!
//! ```text
//! data/
//!   IDENTITY                  # version, cluster_id, node_id, role
//!   generations/
//!     0000000000000001/
//!       wal/                  # first-LSN-named segments
//!       snapshots/            # (Phase 4; created empty here)
//!   CURRENT                   # minimal generation pointer (Phase 2)
//!   tmp/
//!   LOCK                      # process exclusivity
//! ```
//!
//! Snapshots and the full 28-byte `CURRENT` format are Phase 4 and out of
//! scope; Phase 2 creates the generation-1 directory structure and a minimal
//! `CURRENT`.
//!
//! # Durability (Technical-Design §6.2)
//!
//! In `fsync` mode a group commit appends 1..=64 complete records **and** the
//! group footer, performs one file `fsync`, then applies the mutations and
//! returns `OK`. In `os` mode the explicit sync is skipped (`OK_VOLATILE`
//! semantics, no restart-durability gate).
//!
//! # Recovery (Technical-Design §6.3, §9)
//!
//! Recovery scans segments in LSN order, validating headers, record CRCs, the
//! `prev_hash` chain, contiguous LSNs, and group footers. Only footer-closed
//! groups are replayable. In the last active segment an unclosed/torn final
//! group is discarded (and truncated + synced), emitting `tail_truncated`.
//! Interior/sealed-segment corruption or exceeding the 8 MiB group cap fails
//! closed.

pub mod format;

use crate::fileio::{FileSystem, FsError};
use crate::storage::{Mutation, StorageEngine};
use format::{
    DecodedRecord, FormatError, GroupFooter, MutationRecord, RecordType, SegmentHeader,
    GROUP_FOOTER_LEN, GROUP_MAGIC, MAX_RECORD_ENCODED_LEN, SEGMENT_HEADER_LEN,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum records per group (Technical-Design §6.1).
pub const MAX_GROUP_RECORDS: usize = 64;
/// Maximum bytes of records plus footer in a single group (8 MiB).
pub const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
/// Default segment rotation threshold. The spec targets 64 MiB but permits a
/// smaller documented threshold; we rotate once a segment reaches this many
/// bytes (checked only between groups).
pub const SEGMENT_ROTATE_BYTES: u64 = 64 * 1024 * 1024;
/// IDENTITY file format version.
pub const IDENTITY_VERSION: u32 = 1;

/// Durability mode for the WAL writer (Technical-Design §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Group commit with an explicit `fsync` before `OK` (default).
    Fsync,
    /// Write without an explicit sync (`OK_VOLATILE`; benchmark only).
    Os,
}

/// Errors produced by the WAL layer.
#[derive(Debug)]
pub enum WalError {
    /// A file-I/O failure from the [`FileSystem`] layer.
    Io(FsError),
    /// A byte-format decode failure.
    Format(FormatError),
    /// Recovery found corruption that fails closed (Technical-Design §6.3):
    /// interior corruption, sealed-segment damage, or a group exceeding the
    /// 8 MiB cap. Carries a human-readable description.
    Corruption(String),
    /// The IDENTITY file was missing, malformed, or inconsistent.
    Identity(String),
    /// The writer is in a fail-closed state after a prior append/sync failure
    /// (Technical-Design §3); it no longer accepts mutations.
    FailClosed,
    /// A mutation exceeded the encoded-size limit.
    MutationTooLarge {
        /// The encoded length that was rejected.
        encoded_len: usize,
    },
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "wal io error: {e}"),
            WalError::Format(e) => write!(f, "wal format error: {e}"),
            WalError::Corruption(m) => write!(f, "wal corruption (fail closed): {m}"),
            WalError::Identity(m) => write!(f, "identity error: {m}"),
            WalError::FailClosed => write!(f, "wal is fail-closed and rejecting mutations"),
            WalError::MutationTooLarge { encoded_len } => {
                write!(f, "mutation encoded length {encoded_len} exceeds limit")
            }
        }
    }
}

impl std::error::Error for WalError {}

impl From<FsError> for WalError {
    fn from(e: FsError) -> Self {
        WalError::Io(e)
    }
}

impl From<FormatError> for WalError {
    fn from(e: FormatError) -> Self {
        WalError::Format(e)
    }
}

/// Convenience result alias for WAL operations.
pub type WalResult<T> = Result<T, WalError>;

// ---------------------------------------------------------------------------
// Data-directory paths.
// ---------------------------------------------------------------------------

/// Resolved paths within a data directory.
#[derive(Debug, Clone)]
struct DataPaths {
    root: PathBuf,
}

impl DataPaths {
    fn new(root: &Path) -> Self {
        DataPaths {
            root: root.to_path_buf(),
        }
    }

    fn identity(&self) -> PathBuf {
        self.root.join("IDENTITY")
    }

    fn current(&self) -> PathBuf {
        self.root.join("CURRENT")
    }

    fn lock(&self) -> PathBuf {
        self.root.join("LOCK")
    }

    fn tmp(&self) -> PathBuf {
        self.root.join("tmp")
    }

    fn generation(&self) -> PathBuf {
        self.root.join("generations").join("0000000000000001")
    }

    fn wal_dir(&self) -> PathBuf {
        self.generation().join("wal")
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.generation().join("snapshots")
    }

    /// Segment file path for a segment whose first LSN is `first_lsn`
    /// (zero-padded to 20 digits so lexical order matches numeric order).
    fn segment(&self, first_lsn: u64) -> PathBuf {
        self.wal_dir().join(format!("{first_lsn:020}.wal"))
    }
}

// ---------------------------------------------------------------------------
// Node identity.
// ---------------------------------------------------------------------------

/// A node's persistent identity (Technical-Design §6, IDENTITY file).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// IDENTITY format version.
    pub version: u32,
    /// 16-byte binary cluster identifier.
    pub cluster_id: [u8; 16],
    /// 16-byte binary node identifier.
    pub node_id: [u8; 16],
    /// Node role string (e.g. "primary").
    pub role: String,
}

impl Identity {
    /// Encode IDENTITY to a small, self-describing binary layout:
    /// magic `DDBIDN01` | version:u32 | cluster_id[16] | node_id[16] |
    /// role_len:u32 | role | crc32c:u32 over all preceding bytes.
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"DDBIDN01");
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.cluster_id);
        buf.extend_from_slice(&self.node_id);
        let role_bytes = self.role.as_bytes();
        buf.extend_from_slice(&(role_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(role_bytes);
        let crc = crate::checksum::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode and validate an IDENTITY file.
    fn decode(buf: &[u8]) -> WalResult<Self> {
        if buf.len() < 8 + 4 + 16 + 16 + 4 + 4 {
            return Err(WalError::Identity("IDENTITY truncated".into()));
        }
        if &buf[0..8] != b"DDBIDN01" {
            return Err(WalError::Identity("bad IDENTITY magic".into()));
        }
        let stored_crc = u32::from_le_bytes([
            buf[buf.len() - 4],
            buf[buf.len() - 3],
            buf[buf.len() - 2],
            buf[buf.len() - 1],
        ]);
        let computed = crate::checksum::crc32c(&buf[..buf.len() - 4]);
        if stored_crc != computed {
            return Err(WalError::Identity("IDENTITY crc mismatch".into()));
        }
        let version = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let mut cluster_id = [0u8; 16];
        cluster_id.copy_from_slice(&buf[12..28]);
        let mut node_id = [0u8; 16];
        node_id.copy_from_slice(&buf[28..44]);
        let role_len = u32::from_le_bytes([buf[44], buf[45], buf[46], buf[47]]) as usize;
        let role_start = 48;
        let role_end = role_start + role_len;
        if role_end + 4 > buf.len() {
            return Err(WalError::Identity("IDENTITY role length invalid".into()));
        }
        let role = String::from_utf8_lossy(&buf[role_start..role_end]).into_owned();
        Ok(Identity {
            version,
            cluster_id,
            node_id,
            role,
        })
    }
}

/// Generate a 16-byte pseudo-unique identifier without an external RNG.
///
/// std has no UUID or RNG. We derive 16 bytes deterministically from the
/// current wall-clock time (nanoseconds), the process id, and a caller-supplied
/// `salt`, mixed through the in-crate SplitMix64 generator. This is not a real
/// UUID, but it is stable once persisted in IDENTITY (which is the requirement)
/// and unique enough for a local demo.
fn generate_id(salt: u64) -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let seed = nanos ^ pid.rotate_left(32) ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut rng = crate::fileio::SplitMix64::new(seed);
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&rng.next_u64().to_le_bytes());
    id[8..16].copy_from_slice(&rng.next_u64().to_le_bytes());
    id
}

// ---------------------------------------------------------------------------
// Recovery result.
// ---------------------------------------------------------------------------

/// Outcome of scanning and replaying the WAL during recovery.
#[derive(Debug, Clone)]
struct RecoveryOutcome {
    /// Verified, footer-closed mutations in LSN order.
    records: Vec<MutationRecord>,
    /// The `record_hash` of the last replayed record (0 if none).
    last_record_hash: u64,
    /// Whether a final unclosed/torn group was discarded.
    tail_truncated: bool,
    /// The active segment's first LSN (the newest segment).
    active_segment_first_lsn: u64,
    /// The byte offset in the active segment after the last valid footer
    /// (where the next group append should begin).
    active_segment_valid_len: u64,
}

// ---------------------------------------------------------------------------
// Segment scanning.
// ---------------------------------------------------------------------------

/// Result of scanning one segment's bytes.
struct SegmentScan {
    /// Verified, footer-closed records from this segment.
    records: Vec<MutationRecord>,
    /// The `record_hash` of the last footer-closed record (0 if none in this
    /// segment).
    last_record_hash: u64,
    /// Byte offset just past the last valid footer.
    valid_len: u64,
    /// True if a final unclosed/torn group was found (only meaningful for the
    /// active segment).
    truncated: bool,
}

/// Scan a single segment.
///
/// `expected_first_lsn` is the LSN the first record must carry (from the
/// filename/header). `prev_hash_in` is the record hash carried into this
/// segment (0 for the very first segment / LSN 1). `is_active` selects the
/// torn-tail rule (§6.3): only the active segment may discard a final unclosed
/// group; a sealed segment must be entirely footer-closed or it is interior
/// corruption.
fn scan_segment(
    bytes: &[u8],
    expected_first_lsn: u64,
    prev_hash_in: u64,
    is_active: bool,
) -> WalResult<SegmentScan> {
    let header = SegmentHeader::decode(bytes)?;
    if header.first_lsn != expected_first_lsn {
        return Err(WalError::Corruption(format!(
            "segment first_lsn {} does not match filename {}",
            header.first_lsn, expected_first_lsn
        )));
    }

    let mut committed_records: Vec<MutationRecord> = Vec::new();
    let mut committed_last_hash = prev_hash_in;
    let mut committed_next_lsn = expected_first_lsn;
    let mut committed_end: u64 = SEGMENT_HEADER_LEN as u64;

    // Pending (not yet footer-closed) group state.
    let mut pending: Vec<(MutationRecord, u64)> = Vec::new();
    let mut pending_prev_hash = committed_last_hash;
    let mut pending_next_lsn = committed_next_lsn;
    let mut pending_bytes: usize = 0;
    let mut group_first_lsn = committed_next_lsn;

    let mut off = SEGMENT_HEADER_LEN;

    loop {
        if off >= bytes.len() {
            // Reached end of segment. Any pending records form an unclosed
            // group.
            if pending.is_empty() {
                return Ok(SegmentScan {
                    records: committed_records,
                    last_record_hash: committed_last_hash,
                    valid_len: committed_end,
                    truncated: false,
                });
            }
            // Unclosed final group.
            if !is_active {
                return Err(WalError::Corruption(
                    "sealed segment ends without a closing footer".into(),
                ));
            }
            return Ok(SegmentScan {
                records: committed_records,
                last_record_hash: committed_last_hash,
                valid_len: committed_end,
                truncated: true,
            });
        }

        // Distinguish a footer from a record by magic.
        let is_footer = bytes.len() - off >= 8 && bytes[off..off + 8] == GROUP_MAGIC;
        if is_footer {
            match GroupFooter::decode(&bytes[off..]) {
                Ok(footer) => {
                    // Footer must match the pending group exactly.
                    let ok = !pending.is_empty()
                        && footer.count as usize == pending.len()
                        && footer.first_lsn == group_first_lsn
                        && footer.last_lsn == pending.last().unwrap().0.lsn
                        && footer.last_record_hash == pending.last().unwrap().1;
                    if !ok {
                        return corruption_or_tail(
                            is_active,
                            "group footer does not match its records",
                        );
                    }
                    // Commit the pending group.
                    for (rec, hash) in pending.drain(..) {
                        committed_records.push(rec);
                        committed_last_hash = hash;
                    }
                    committed_next_lsn = pending_next_lsn;
                    off += GROUP_FOOTER_LEN;
                    committed_end = off as u64;
                    // Reset pending group tracking for the next group.
                    pending_prev_hash = committed_last_hash;
                    pending_bytes = 0;
                    group_first_lsn = committed_next_lsn;
                }
                Err(_) => {
                    return corruption_or_tail(is_active, "damaged group footer");
                }
            }
            continue;
        }

        // Otherwise decode a mutation record.
        match MutationRecord::decode(&bytes[off..]) {
            Ok(DecodedRecord {
                record,
                consumed,
                record_hash,
            }) => {
                // Validate LSN contiguity and prev_hash chain.
                if record.lsn != pending_next_lsn {
                    return corruption_or_tail(is_active, "non-contiguous LSN");
                }
                if record.prev_hash != pending_prev_hash {
                    return corruption_or_tail(is_active, "broken prev_hash chain");
                }
                pending_bytes += consumed;
                // Enforce the 8 MiB group cap (records + footer).
                if pending.len() >= MAX_GROUP_RECORDS
                    || pending_bytes + GROUP_FOOTER_LEN > MAX_GROUP_BYTES
                {
                    // The bytes after the last footer exceed the legal group
                    // size before a footer appeared: interior corruption.
                    return Err(WalError::Corruption(
                        "group exceeds record/byte cap without a footer".into(),
                    ));
                }
                pending_prev_hash = record_hash;
                pending_next_lsn = record.lsn + 1;
                pending.push((record, record_hash));
                off += consumed;
            }
            Err(_) => {
                return corruption_or_tail(is_active, "damaged mutation record");
            }
        }
    }
}

/// Helper: in the active segment a parse failure after the last footer is a
/// torn tail (discard the group); in a sealed segment it is interior
/// corruption (fail closed).
fn corruption_or_tail(is_active: bool, detail: &str) -> WalResult<SegmentScan> {
    if is_active {
        // Signal a torn tail: the caller keeps only committed records and
        // truncates at the last footer. We encode this by returning a scan
        // marked truncated with the committed prefix captured by the caller.
        Err(WalError::Corruption(format!("__TAIL__{detail}")))
    } else {
        Err(WalError::Corruption(format!(
            "interior corruption: {detail}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Wal writer.
// ---------------------------------------------------------------------------

/// The persistent WAL writer bound to a data directory and a filesystem.
///
/// Owns LSN assignment, the active segment, the current pending group, and the
/// durability mode. Generic over [`FileSystem`] so tests use
/// [`SimFs`](crate::fileio::SimFs).
pub struct Wal<F: FileSystem> {
    fs: F,
    paths: DataPaths,
    identity: Identity,
    mode: DurabilityMode,
    /// Next LSN to assign.
    next_lsn: u64,
    /// The `record_hash` of the most recently appended (committed) record.
    last_record_hash: u64,
    /// Highest LSN durably synced.
    durable_lsn: u64,
    /// The active segment's first LSN.
    active_first_lsn: u64,
    /// Current byte length of the active segment file.
    active_len: u64,
    /// Fail-closed flag: once set, all mutations are rejected.
    failed: bool,
}

impl<F: FileSystem> Wal<F> {
    /// The durability mode this writer was opened with.
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// The next LSN that would be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// The highest LSN durably synced (Technical-Design §2.1 `sync_through`).
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }

    /// Append a group of one or more mutations, commit it (append records and
    /// footer, one `fsync` in `fsync` mode), and return the assigned LSNs.
    ///
    /// Enforces write-ahead order: the records and footer are appended and
    /// synced before the caller applies them and observes `OK`
    /// (Technical-Design §3, §6.2). On any I/O failure the writer transitions
    /// to a fail-closed state and returns an error without acknowledging.
    pub fn append_group(&mut self, mutations: &[Mutation]) -> WalResult<Vec<u64>> {
        if self.failed {
            return Err(WalError::FailClosed);
        }
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        if mutations.len() > MAX_GROUP_RECORDS {
            return Err(WalError::Corruption(format!(
                "group of {} exceeds {} records",
                mutations.len(),
                MAX_GROUP_RECORDS
            )));
        }

        // Rotate between groups if the active segment is large enough.
        if self.active_len >= SEGMENT_ROTATE_BYTES {
            self.rotate()?;
        }

        // Encode the whole group into a single buffer, chaining prev_hash.
        let mut buf: Vec<u8> = Vec::new();
        let mut assigned = Vec::with_capacity(mutations.len());
        let mut prev_hash = self.last_record_hash;
        let mut lsn = self.next_lsn;
        let first_lsn = lsn;
        let mut last_hash = prev_hash;
        for m in mutations {
            let rec = mutation_to_record(m, lsn, prev_hash);
            let encoded_len = rec.encoded_len();
            if encoded_len > MAX_RECORD_ENCODED_LEN {
                return Err(WalError::MutationTooLarge { encoded_len });
            }
            let encoded = rec.encode();
            let hash = crate::checksum::crc64_ecma(&encoded);
            // Enforce the 8 MiB group cap (records + footer).
            if buf.len() + encoded.len() + GROUP_FOOTER_LEN > MAX_GROUP_BYTES {
                return Err(WalError::Corruption(
                    "group would exceed the 8 MiB cap".into(),
                ));
            }
            buf.extend_from_slice(&encoded);
            assigned.push(lsn);
            prev_hash = hash;
            last_hash = hash;
            lsn += 1;
        }
        let last_lsn = lsn - 1;
        let footer = GroupFooter {
            first_lsn,
            last_lsn,
            count: mutations.len() as u32,
            last_record_hash: last_hash,
        };
        buf.extend_from_slice(&footer.encode());

        // Append the group + footer, then sync (fsync mode).
        let seg_path = self.paths.segment(self.active_first_lsn);
        let group_len = buf.len() as u64;
        if let Err(e) = self.fs.append(&seg_path, &buf) {
            self.failed = true;
            return Err(WalError::Io(e));
        }
        if self.mode == DurabilityMode::Fsync {
            if let Err(e) = self.fs.sync_file(&seg_path) {
                self.failed = true;
                return Err(WalError::Io(e));
            }
            self.durable_lsn = last_lsn;
        }

        // Commit writer bookkeeping only after the durable append succeeded.
        self.active_len += group_len;
        self.next_lsn = lsn;
        self.last_record_hash = last_hash;
        Ok(assigned)
    }

    /// Explicitly sync the WAL through `lsn` (Technical-Design §2.1
    /// `sync_through`). Group commit already syncs each committed group in
    /// `fsync` mode, so this syncs the active segment file and confirms the
    /// durability boundary.
    pub fn sync_through(&mut self, lsn: u64) -> WalResult<u64> {
        if self.failed {
            return Err(WalError::FailClosed);
        }
        if self.mode == DurabilityMode::Fsync {
            let seg_path = self.paths.segment(self.active_first_lsn);
            if let Err(e) = self.fs.sync_file(&seg_path) {
                self.failed = true;
                return Err(WalError::Io(e));
            }
        }
        if lsn < self.next_lsn {
            self.durable_lsn = self.durable_lsn.max(lsn);
        }
        Ok(self.durable_lsn)
    }

    /// Rotate to a new segment after the current one (Technical-Design §6.1:
    /// rotation happens between groups only). Seals the current segment with a
    /// sync, creates the new first-LSN-named segment with its header, and syncs
    /// the wal/ directory so the new file is durable.
    pub fn rotate_after(&mut self, _lsn: u64) -> WalResult<()> {
        self.rotate()
    }

    fn rotate(&mut self) -> WalResult<()> {
        let old_path = self.paths.segment(self.active_first_lsn);
        // Seal the prior segment with a successful sync first.
        if self.mode == DurabilityMode::Fsync {
            if let Err(e) = self.fs.sync_file(&old_path) {
                self.failed = true;
                return Err(WalError::Io(e));
            }
        }
        let new_first = self.next_lsn;
        let new_path = self.paths.segment(new_first);
        let header = SegmentHeader {
            cluster_id: self.identity.cluster_id,
            node_id: self.identity.node_id,
            first_lsn: new_first,
        };
        self.fs.create_file(&new_path)?;
        self.fs.append(&new_path, &header.encode())?;
        if self.mode == DurabilityMode::Fsync {
            self.fs.sync_file(&new_path)?;
            self.fs.sync_dir(&self.paths.wal_dir())?;
        }
        self.active_first_lsn = new_first;
        self.active_len = SEGMENT_HEADER_LEN as u64;
        Ok(())
    }

    /// Whether the writer has transitioned to a fail-closed state.
    pub fn is_failed(&self) -> bool {
        self.failed
    }
}

/// Map a [`Mutation`] to a [`MutationRecord`] at `lsn` with `prev_hash`.
fn mutation_to_record(m: &Mutation, lsn: u64, prev_hash: u64) -> MutationRecord {
    match m {
        Mutation::Set { key, value } => MutationRecord {
            lsn,
            rtype: RecordType::Set,
            key: key.clone(),
            value: value.clone(),
            prev_hash,
        },
        Mutation::Delete { key } => MutationRecord {
            lsn,
            rtype: RecordType::Delete,
            key: key.clone(),
            value: Vec::new(),
            prev_hash,
        },
    }
}

/// Convert a decoded [`MutationRecord`] back to a [`Mutation`] for replay.
fn record_to_mutation(rec: &MutationRecord) -> Mutation {
    match rec.rtype {
        RecordType::Set => Mutation::Set {
            key: rec.key.clone(),
            value: rec.value.clone(),
        },
        RecordType::Delete => Mutation::Delete {
            key: rec.key.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Db: the integration entry point.
// ---------------------------------------------------------------------------

/// A durable key-value database: a [`StorageEngine`] backed by the persistent
/// WAL (Technical-Design §2.1, §9; SOW §8).
///
/// [`Db::open`] acquires the data-directory lock, initializes a fresh data
/// directory or recovers an existing one by replaying every verified
/// footer-closed mutation through [`StorageEngine::apply`], then exposes a
/// durable write path ([`Db::set`], [`Db::delete`]) that appends to the WAL,
/// group-commits, applies, and only then returns.
pub struct Db<F: FileSystem> {
    engine: StorageEngine,
    wal: Wal<F>,
    last_applied_lsn: u64,
    tail_truncated: bool,
    _lock: Box<dyn crate::fileio::LockGuard>,
}

impl<F: FileSystem + Clone> Db<F> {
    /// Open (initializing if fresh) the data directory at `root` under `fs`
    /// with durability `mode`, returning a ready [`Db`].
    ///
    /// On a fresh directory this acquires the LOCK, creates the generation-1
    /// layout, generates + persists a fresh IDENTITY (stable cluster/node IDs),
    /// writes a minimal CURRENT, and creates the first segment. On an existing
    /// directory it acquires the LOCK, validates IDENTITY, then scans and
    /// replays the WAL.
    pub fn open(fs: F, root: &Path, mode: DurabilityMode) -> WalResult<Self> {
        let paths = DataPaths::new(root);
        // Acquire the exclusive data-directory lock before recovery (§3, §9).
        // The parent directory must exist to create the LOCK file.
        fs.create_dir_all(&paths.root)?;
        let lock = fs.acquire_lock(&paths.lock())?;

        let identity = if fs.exists(&paths.identity()) {
            let bytes = fs.read(&paths.identity())?;
            let id = Identity::decode(&bytes)?;
            if id.version != IDENTITY_VERSION {
                return Err(WalError::Identity(format!(
                    "unsupported IDENTITY version {}",
                    id.version
                )));
            }
            id
        } else {
            Self::init_fresh(&fs, &paths, mode)?
        };

        // Recover by scanning the WAL and replaying footer-closed groups.
        let outcome = Self::recover(&fs, &paths, &identity, mode)?;

        let mut engine = StorageEngine::new();
        let mut last_applied_lsn = 0u64;
        for rec in &outcome.records {
            engine.apply(record_to_mutation(rec));
            last_applied_lsn = rec.lsn;
        }

        let next_lsn = last_applied_lsn + 1;
        let durable_lsn = if mode == DurabilityMode::Fsync {
            last_applied_lsn
        } else {
            0
        };

        let wal = Wal {
            fs,
            paths,
            identity,
            mode,
            next_lsn,
            last_record_hash: outcome.last_record_hash,
            durable_lsn,
            active_first_lsn: outcome.active_segment_first_lsn,
            active_len: outcome.active_segment_valid_len,
            failed: false,
        };

        Ok(Db {
            engine,
            wal,
            last_applied_lsn,
            tail_truncated: outcome.tail_truncated,
            _lock: lock,
        })
    }

    /// Initialize a fresh data directory, returning the new IDENTITY.
    fn init_fresh(fs: &F, paths: &DataPaths, mode: DurabilityMode) -> WalResult<Identity> {
        fs.create_dir_all(&paths.tmp())?;
        fs.create_dir_all(&paths.wal_dir())?;
        fs.create_dir_all(&paths.snapshots_dir())?;

        let identity = Identity {
            version: IDENTITY_VERSION,
            cluster_id: generate_id(0xC1),
            node_id: generate_id(0x0D),
            role: "primary".to_string(),
        };
        // Write IDENTITY durably.
        fs.create_file(&paths.identity())?;
        fs.append(&paths.identity(), &identity.encode())?;

        // Minimal CURRENT pointing at generation 1 (full 28-byte format is
        // Phase 4). Store the generation directory name.
        fs.create_file(&paths.current())?;
        fs.append(&paths.current(), b"0000000000000001")?;

        // Create the first segment with its header at first_lsn = 1.
        let header = SegmentHeader {
            cluster_id: identity.cluster_id,
            node_id: identity.node_id,
            first_lsn: 1,
        };
        let seg = paths.segment(1);
        fs.create_file(&seg)?;
        fs.append(&seg, &header.encode())?;

        if mode == DurabilityMode::Fsync {
            fs.sync_file(&paths.identity())?;
            fs.sync_file(&paths.current())?;
            fs.sync_file(&seg)?;
            fs.sync_dir(&paths.wal_dir())?;
            fs.sync_dir(&paths.root)?;
        }
        Ok(identity)
    }

    /// Scan the WAL segments and replay footer-closed groups (§6.3, §9).
    ///
    /// Enumerates the segment files under `wal/`, ordered by their first-LSN
    /// filename. Every segment except the newest is treated as SEALED and must
    /// be entirely footer-closed (interior corruption fails closed). The newest
    /// segment is the ACTIVE one: a final unclosed/torn group is discarded and
    /// the segment truncated at the last valid footer (`tail_truncated`).
    fn recover(
        fs: &F,
        paths: &DataPaths,
        identity: &Identity,
        mode: DurabilityMode,
    ) -> WalResult<RecoveryOutcome> {
        // Enumerate segments and order them by their embedded first LSN.
        let mut segment_lsns: Vec<u64> = Vec::new();
        for name in fs.list_dir(&paths.wal_dir())? {
            if let Some(stem) = name.strip_suffix(".wal") {
                if let Ok(first_lsn) = stem.parse::<u64>() {
                    segment_lsns.push(first_lsn);
                }
            }
        }
        segment_lsns.sort_unstable();

        // A brand-new data directory always has segment 1 created at init.
        if segment_lsns.is_empty() {
            return Ok(RecoveryOutcome {
                records: Vec::new(),
                last_record_hash: 0,
                tail_truncated: false,
                active_segment_first_lsn: 1,
                active_segment_valid_len: SEGMENT_HEADER_LEN as u64,
            });
        }

        let mut records: Vec<MutationRecord> = Vec::new();
        let mut last_record_hash = 0u64;
        let mut prev_hash = 0u64;
        let mut tail_truncated = false;

        let mut active_segment_first_lsn = segment_lsns[0];
        let mut active_segment_valid_len = SEGMENT_HEADER_LEN as u64;

        let last_index = segment_lsns.len() - 1;
        for (i, &segment_first_lsn) in segment_lsns.iter().enumerate() {
            let seg_path = paths.segment(segment_first_lsn);
            let bytes = fs.read(&seg_path)?;
            let is_active = i == last_index;

            let scan = scan_as(&bytes, segment_first_lsn, prev_hash, is_active)?;

            records.extend(scan.records.iter().cloned());
            if !scan.records.is_empty() {
                last_record_hash = scan.last_record_hash;
            }
            prev_hash = scan.last_record_hash;

            if is_active {
                active_segment_first_lsn = segment_first_lsn;
                active_segment_valid_len = scan.valid_len;
                if scan.truncated {
                    tail_truncated = true;
                    // Truncate the segment at the last valid footer and sync.
                    fs.truncate(&seg_path, scan.valid_len)?;
                    if mode == DurabilityMode::Fsync {
                        fs.sync_file(&seg_path)?;
                    }
                }
            }
        }

        // `identity` is available for stricter cluster/node checks in a later
        // phase; the header first_lsn/filename agreement is already enforced.
        let _ = identity;

        Ok(RecoveryOutcome {
            records,
            last_record_hash,
            tail_truncated,
            active_segment_first_lsn,
            active_segment_valid_len,
        })
    }

    /// Durable `SET`: append + group-commit + apply, then return
    /// (Technical-Design §3, §6.2).
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> WalResult<u64> {
        self.apply_durable(Mutation::Set { key, value })
    }

    /// Durable `DELETE`: append + group-commit + apply, then return. Always
    /// advances the LSN, even when the key is absent (Technical-Design §3).
    pub fn delete(&mut self, key: Vec<u8>) -> WalResult<u64> {
        self.apply_durable(Mutation::Delete { key })
    }

    fn apply_durable(&mut self, m: Mutation) -> WalResult<u64> {
        let assigned = self.wal.append_group(std::slice::from_ref(&m))?;
        let lsn = assigned[0];
        // Write-ahead order: apply to the map only after the durable append.
        self.engine.apply(m);
        self.last_applied_lsn = lsn;
        Ok(lsn)
    }

    /// Look up `key` in the reconstructed engine.
    pub fn get(&self, key: &[u8]) -> crate::storage::GetResult {
        self.engine.get(key)
    }

    /// Whether `key` exists.
    pub fn exists(&self, key: &[u8]) -> bool {
        self.engine.exists(key)
    }

    /// The highest LSN applied to the engine.
    pub fn last_applied_lsn(&self) -> u64 {
        self.last_applied_lsn
    }

    /// The highest LSN durably synced by the WAL.
    pub fn last_durable_lsn(&self) -> u64 {
        self.wal.durable_lsn()
    }

    /// Whether recovery discarded a final unclosed/torn group (§6.3).
    pub fn tail_truncated(&self) -> bool {
        self.tail_truncated
    }

    /// Roll the WAL over to a new segment between groups (Technical-Design
    /// §6.1). Seals the current segment with a sync and starts a new
    /// first-LSN-named segment. Exposed so durability tests can produce a
    /// SEALED (non-final) segment and assert the §6.3 fail-closed boundary.
    pub fn rotate(&mut self) -> WalResult<()> {
        self.wal.rotate_after(self.wal.next_lsn())
    }

    /// Explicitly sync the WAL through `lsn` (Technical-Design §2.1
    /// `sync_through`); returns the highest durable LSN.
    pub fn sync_through(&mut self, lsn: u64) -> WalResult<u64> {
        self.wal.sync_through(lsn)
    }

    /// Number of keys currently stored.
    pub fn len(&self) -> usize {
        self.engine.len()
    }

    /// Whether the engine holds no keys.
    pub fn is_empty(&self) -> bool {
        self.engine.is_empty()
    }
}

/// Scan wrapper that converts the internal `__TAIL__` corruption marker (from
/// [`corruption_or_tail`]) into a truncated result for the active segment.
fn scan_as(
    bytes: &[u8],
    expected_first_lsn: u64,
    prev_hash_in: u64,
    is_active: bool,
) -> WalResult<SegmentScan> {
    match scan_segment(bytes, expected_first_lsn, prev_hash_in, is_active) {
        Ok(s) => Ok(s),
        Err(WalError::Corruption(m)) if is_active && m.starts_with("__TAIL__") => {
            // A torn tail in the active segment: re-run the scan but stop at
            // the last valid footer. We recover the committed prefix by
            // scanning again with a variant that returns committed state.
            scan_committed_prefix(bytes, expected_first_lsn, prev_hash_in)
        }
        Err(e) => Err(e),
    }
}

/// Scan only the committed (footer-closed) prefix of a segment, ignoring any
/// trailing unclosed/torn group. Used for the active segment's torn tail.
fn scan_committed_prefix(
    bytes: &[u8],
    expected_first_lsn: u64,
    prev_hash_in: u64,
) -> WalResult<SegmentScan> {
    let header = SegmentHeader::decode(bytes)?;
    if header.first_lsn != expected_first_lsn {
        return Err(WalError::Corruption("segment first_lsn mismatch".into()));
    }
    let mut committed_records: Vec<MutationRecord> = Vec::new();
    let mut committed_last_hash = prev_hash_in;
    let mut committed_next_lsn = expected_first_lsn;
    let mut committed_end: u64 = SEGMENT_HEADER_LEN as u64;

    let mut pending: Vec<(MutationRecord, u64)> = Vec::new();
    let mut pending_prev_hash = committed_last_hash;
    let mut pending_next_lsn = committed_next_lsn;
    let mut pending_bytes: usize = 0;
    let mut group_first_lsn = committed_next_lsn;

    let mut off = SEGMENT_HEADER_LEN;
    loop {
        if off >= bytes.len() {
            break;
        }
        let is_footer = bytes.len() - off >= 8 && bytes[off..off + 8] == GROUP_MAGIC;
        if is_footer {
            match GroupFooter::decode(&bytes[off..]) {
                Ok(footer) => {
                    let ok = !pending.is_empty()
                        && footer.count as usize == pending.len()
                        && footer.first_lsn == group_first_lsn
                        && footer.last_lsn == pending.last().unwrap().0.lsn
                        && footer.last_record_hash == pending.last().unwrap().1;
                    if !ok {
                        break;
                    }
                    for (rec, hash) in pending.drain(..) {
                        committed_records.push(rec);
                        committed_last_hash = hash;
                    }
                    committed_next_lsn = pending_next_lsn;
                    off += GROUP_FOOTER_LEN;
                    committed_end = off as u64;
                    pending_prev_hash = committed_last_hash;
                    pending_bytes = 0;
                    group_first_lsn = committed_next_lsn;
                }
                Err(_) => break,
            }
            continue;
        }
        match MutationRecord::decode(&bytes[off..]) {
            Ok(DecodedRecord {
                record,
                consumed,
                record_hash,
            }) => {
                if record.lsn != pending_next_lsn || record.prev_hash != pending_prev_hash {
                    break;
                }
                pending_bytes += consumed;
                if pending.len() >= MAX_GROUP_RECORDS
                    || pending_bytes + GROUP_FOOTER_LEN > MAX_GROUP_BYTES
                {
                    break;
                }
                pending_prev_hash = record_hash;
                pending_next_lsn = record.lsn + 1;
                pending.push((record, record_hash));
                off += consumed;
            }
            Err(_) => break,
        }
    }

    // A legitimate interrupted group can occupy at most MAX_GROUP_BYTES.
    // Damage beyond that bounded suffix cannot be explained by the writer's
    // final in-flight batch, even if a parser stopped at its first bad byte.
    if bytes.len().saturating_sub(committed_end as usize) > MAX_GROUP_BYTES {
        return Err(WalError::Corruption(
            "damaged active-segment suffix exceeds maximum group size".into(),
        ));
    }

    Ok(SegmentScan {
        records: committed_records,
        last_record_hash: committed_last_hash,
        valid_len: committed_end,
        truncated: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::{SimConfig, SimFs};
    use crate::storage::GetResult;

    fn sim() -> SimFs {
        SimFs::new(SimConfig::new(1))
    }

    fn root() -> PathBuf {
        PathBuf::from("/data")
    }

    #[test]
    fn open_fresh_then_write_and_read() {
        let fs = sim();
        let mut db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.last_applied_lsn(), 0);
        assert!(db.is_empty());
        let l1 = db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
        let l2 = db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
        let l3 = db.set(b"C".to_vec(), b"3".to_vec()).unwrap();
        assert_eq!((l1, l2, l3), (1, 2, 3));
        assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.last_applied_lsn(), 3);
        assert_eq!(db.last_durable_lsn(), 3);
    }

    #[test]
    fn lsn_advances_by_one_including_delete_of_absent_key() {
        let fs = sim();
        let mut db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.set(b"k".to_vec(), b"v".to_vec()).unwrap(), 1);
        // DELETE of an absent key still advances the LSN (§3).
        assert_eq!(db.delete(b"absent".to_vec()).unwrap(), 2);
        assert_eq!(db.delete(b"k".to_vec()).unwrap(), 3);
        assert_eq!(db.last_applied_lsn(), 3);
        assert_eq!(db.get(b"k"), GetResult::NotFound);
    }

    #[test]
    fn sow_set_a_b_c_reopen_reconstructs_state() {
        // SOW §8: SET A 1 / SET B 2 / SET C 3 / <killed> / restart / GET.
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
            db.set(b"C".to_vec(), b"3".to_vec()).unwrap();
        }
        // Simulate a crash: discard volatile, keep only synced (stable) bytes.
        fs.crash();
        // Reopen and confirm state reconstructs.
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.get(b"A"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.get(b"C"), GetResult::Found(b"3".to_vec()));
        assert_eq!(db.last_applied_lsn(), 3);
        assert!(!db.tail_truncated());
    }

    #[test]
    fn reopen_replays_mix_of_set_and_delete() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"x".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"y".to_vec(), b"2".to_vec()).unwrap();
            db.delete(b"x".to_vec()).unwrap();
        }
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.get(b"x"), GetResult::NotFound);
        assert_eq!(db.get(b"y"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.last_applied_lsn(), 3);
    }

    #[test]
    fn identity_is_stable_across_restart() {
        let fs = sim();
        let id1 = {
            let db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.wal.identity.clone()
        };
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.wal.identity, id1);
    }

    #[test]
    fn second_open_fails_with_lock_held() {
        let fs = sim();
        let _db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        // A second open while the first holds the lock must fail.
        let err = Db::open(fs, &root(), DurabilityMode::Fsync);
        assert!(matches!(err, Err(WalError::Io(FsError::Locked(_)))));
    }

    #[test]
    fn torn_final_group_is_discarded_as_tail_truncated() {
        // Write two committed groups, then append a partial (torn) third group
        // with NO footer to the active segment, then reopen.
        let fs = sim();
        let seg = {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
            db.wal.paths.segment(1)
        };
        // Manually append a valid-looking record with the correct chain but no
        // footer to simulate a torn/unsynced final group.
        let bytes = fs.read(&seg).unwrap();
        // Compute prev_hash = last record hash by decoding the committed data.
        let torn = MutationRecord {
            lsn: 3,
            rtype: RecordType::Set,
            key: b"c".to_vec(),
            value: b"3".to_vec(),
            // Deliberately correct chaining is not required for the discard;
            // even a valid-looking record after the last footer is dropped.
            prev_hash: last_hash_of(&bytes),
        };
        fs.append(&seg, &torn.encode()).unwrap();
        fs.sync_file(&seg).unwrap();
        fs.crash();

        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert!(db.tail_truncated());
        assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
        // The torn record after the last footer is discarded.
        assert_eq!(db.get(b"c"), GetResult::NotFound);
        assert_eq!(db.last_applied_lsn(), 2);
        // Writing continues from LSN 3 after truncation.
        let mut db = db;
        assert_eq!(db.set(b"d".to_vec(), b"4".to_vec()).unwrap(), 3);
    }

    #[test]
    fn oversized_damaged_active_suffix_fails_closed() {
        let fs = sim();
        let seg = {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
            db.wal.paths.segment(1)
        };
        fs.append(&seg, &vec![0xff; MAX_GROUP_BYTES + 1]).unwrap();
        fs.sync_file(&seg).unwrap();
        fs.crash();

        let err = Db::open(fs, &root(), DurabilityMode::Fsync);
        assert!(matches!(err, Err(WalError::Corruption(_))));
    }

    /// Decode the committed records of a segment to find the last record hash.
    fn last_hash_of(bytes: &[u8]) -> u64 {
        let s = scan_committed_prefix(bytes, 1, 0).unwrap();
        s.last_record_hash
    }

    #[test]
    fn interior_corruption_in_sealed_segment_fails_closed() {
        // Force a tiny rotate threshold indirectly: write one group, rotate,
        // then corrupt the sealed first segment and confirm recovery fails.
        let fs = sim();
        let seg1 = {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
            // Rotate manually so segment 1 becomes sealed and segment 2 active.
            db.wal.rotate_after(1).unwrap();
            db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
            db.wal.paths.segment(1)
        };
        // Corrupt a byte inside a committed record of the sealed segment 1.
        let mut bytes = fs.read(&seg1).unwrap();
        let idx = SEGMENT_HEADER_LEN + 30; // inside the first record's payload
        bytes[idx] ^= 0xff;
        // Overwrite the sealed segment's stable bytes to model post-sync
        // corruption (the deliberately-violating corruption test of §6.3).
        fs.truncate(&seg1, 0).unwrap();
        fs.append(&seg1, &bytes).unwrap();
        fs.sync_file(&seg1).unwrap();
        fs.crash();

        let err = Db::open(fs, &root(), DurabilityMode::Fsync);
        assert!(matches!(err, Err(WalError::Corruption(_))));
    }

    #[test]
    fn rotation_creates_new_segment_and_continues_lsns() {
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.wal.rotate_after(1).unwrap();
        assert_eq!(db.set(b"b".to_vec(), b"2".to_vec()).unwrap(), 2);
        // Segment 2 exists (named by its first LSN = 2).
        assert!(fs.exists(&db.wal.paths.segment(2)));
        drop(db);
        fs.crash();
        // Reopen replays across both segments.
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.last_applied_lsn(), 2);
    }

    #[test]
    fn os_mode_writes_without_sync() {
        let fs = sim();
        let mut db = Db::open(fs, &root(), DurabilityMode::Os).unwrap();
        assert_eq!(db.wal.mode(), DurabilityMode::Os);
        db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(db.get(b"k"), GetResult::Found(b"v".to_vec()));
        // os mode makes no durability promise; durable_lsn stays 0.
        assert_eq!(db.last_durable_lsn(), 0);
    }
}
