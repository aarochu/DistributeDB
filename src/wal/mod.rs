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

pub mod current;
pub mod format;
pub mod snapshot;

use crate::checksum::Crc64Ecma;
use crate::fileio::{FileSystem, FsError, FsResult};
use crate::lsm::tree::{
    CompactedTables, CompactionJob, FlushJob, LsmConfig, LsmTree, WrittenTable,
};
use crate::lsm::{LsmError, LsmResult};
use crate::storage::{GetResult, Mutation, ScanPage, StorageEngine};
use current::Current;
use format::{
    DecodedRecord, FormatError, GroupFooter, MutationRecord, RecordType, SegmentHeader,
    GROUP_FOOTER_LEN, GROUP_MAGIC, MAX_RECORD_ENCODED_LEN, SEGMENT_HEADER_LEN,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
/// Default bounded disk budget for retained recovery data (snapshots plus the
/// WAL segments still needed for recovery), in bytes (Technical-Design §7).
///
/// When a publish/retain would push retained recovery bytes over this budget,
/// new writes are paused with [`WalError::ResourceExhausted`] rather than
/// deleting data still needed for recovery. The default is generous (1 GiB) so
/// ordinary operation never trips it; tests configure a small budget via
/// [`Db::open_with_budget`] to exercise the pause.
pub const DEFAULT_RETENTION_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

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
    /// Client mutation attempted against a statically configured replica.
    ReadOnlyReplica,
    /// A WAL operation was attempted while a group appended by
    /// [`Wal::append_unsynced`] is still waiting for [`Wal::complete_group`].
    /// Only the single sequencer appends groups, so this indicates misuse.
    GroupInProgress,
    /// A mutation exceeded the encoded-size limit.
    MutationTooLarge {
        /// The encoded length that was rejected.
        encoded_len: usize,
    },
    /// Retained recovery data (snapshots + WAL needed for recovery) would
    /// exceed the configured disk budget (Technical-Design §7). New writes are
    /// paused instead of deleting data still needed for recovery; the server
    /// maps this to `RESOURCE_EXHAUSTED`.
    ResourceExhausted {
        /// The configured retention budget in bytes.
        budget_bytes: u64,
        /// The retained recovery bytes that would exceed it.
        needed_bytes: u64,
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
            WalError::ReadOnlyReplica => write!(f, "replica rejects client mutations"),
            WalError::GroupInProgress => write!(f, "a WAL group commit is already in progress"),
            WalError::MutationTooLarge { encoded_len } => {
                write!(f, "mutation encoded length {encoded_len} exceeds limit")
            }
            WalError::ResourceExhausted {
                budget_bytes,
                needed_bytes,
            } => write!(
                f,
                "retained recovery data {needed_bytes} bytes exceeds disk budget {budget_bytes} bytes"
            ),
        }
    }
}

impl std::error::Error for WalError {}

impl From<FsError> for WalError {
    fn from(e: FsError) -> Self {
        WalError::Io(e)
    }
}

impl From<LsmError> for WalError {
    fn from(e: LsmError) -> Self {
        match e {
            LsmError::Fs(e) => WalError::Io(e),
            LsmError::Corrupt(detail) => WalError::Corruption(format!("lsm: {detail}")),
            LsmError::Usage(detail) => WalError::Corruption(format!("lsm misuse: {detail}")),
        }
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
    generation_id: u64,
}

impl DataPaths {
    fn new(root: &Path) -> Self {
        DataPaths {
            root: root.to_path_buf(),
            generation_id: 1,
        }
    }

    fn with_generation(root: &Path, generation_id: u64) -> Self {
        DataPaths {
            root: root.to_path_buf(),
            generation_id,
        }
    }

    fn identity(&self) -> PathBuf {
        self.root.join("IDENTITY")
    }

    fn current(&self) -> PathBuf {
        self.root.join("CURRENT")
    }

    fn rebootstrap_policy(&self) -> PathBuf {
        self.root.join("REBOOTSTRAP_POLICY")
    }

    fn lock(&self) -> PathBuf {
        self.root.join("LOCK")
    }

    fn tmp(&self) -> PathBuf {
        self.root.join("tmp")
    }

    fn generation(&self) -> PathBuf {
        self.root
            .join("generations")
            .join(format!("{:016x}", self.generation_id))
    }

    fn wal_dir(&self) -> PathBuf {
        self.generation().join("wal")
    }

    fn lsm_dir(&self) -> PathBuf {
        self.generation().join("lsm")
    }

    fn storage_engine(&self) -> PathBuf {
        self.root.join("STORAGE_ENGINE")
    }

    fn snapshots_dir(&self) -> PathBuf {
        self.generation().join("snapshots")
    }

    /// Segment file path for a segment whose first LSN is `first_lsn`
    /// (zero-padded to 20 digits so lexical order matches numeric order).
    fn segment(&self, first_lsn: u64) -> PathBuf {
        self.wal_dir().join(format!("{first_lsn:020}.wal"))
    }

    /// Immutable snapshot file path for a snapshot taken at LSN `lsn`
    /// (zero-padded to 20 digits like segments, so lexical order matches
    /// numeric order). Snapshots live in the active generation's `snapshots/`
    /// directory (Technical-Design §6, §7).
    fn snapshot(&self, lsn: u64) -> PathBuf {
        self.snapshots_dir().join(format!("{lsn:020}.snap"))
    }

    /// Temporary path for an in-progress snapshot at LSN `lsn`, under `tmp/`
    /// on the same filesystem as the final snapshot so publication can use a
    /// sync + atomic rename (Technical-Design §7, ADR-001).
    fn snapshot_tmp(&self, lsn: u64) -> PathBuf {
        self.tmp().join(format!("{lsn:020}.snap.tmp"))
    }
}

/// The persisted engine choice. Directories created before it existed have
/// no `STORAGE_ENGINE` file and use the in-memory map.
const ENGINE_LSM: u8 = 1;

fn encode_storage_engine(kind: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(b"DDBENG01");
    bytes[8] = kind;
    let crc = crate::checksum::crc32c(&bytes[..12]);
    bytes[12..16].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_storage_engine(bytes: &[u8]) -> WalResult<u8> {
    if bytes.len() != 16
        || &bytes[..8] != b"DDBENG01"
        || bytes[9..12] != [0; 3]
        || bytes[8] != ENGINE_LSM
        || u32::from_le_bytes(bytes[12..16].try_into().unwrap())
            != crate::checksum::crc32c(&bytes[..12])
    {
        return Err(WalError::Identity("invalid STORAGE_ENGINE file".into()));
    }
    Ok(bytes[8])
}

fn encode_rebootstrap_policy(allowed: bool) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(b"DDBRBP01");
    bytes[8] = u8::from(allowed);
    let crc = crate::checksum::crc32c(&bytes[..12]);
    bytes[12..16].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_rebootstrap_policy(bytes: &[u8]) -> WalResult<bool> {
    if bytes.len() != 16 || &bytes[..8] != b"DDBRBP01" || bytes[9..12] != [0; 3] {
        return Err(WalError::Identity("invalid rebootstrap policy".into()));
    }
    if bytes[8] > 1
        || u32::from_le_bytes(bytes[12..16].try_into().unwrap())
            != crate::checksum::crc32c(&bytes[..12])
    {
        return Err(WalError::Identity(
            "invalid rebootstrap policy checksum or flag".into(),
        ));
    }
    Ok(bytes[8] == 1)
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
    /// The snapshot base LSN the map starts from (0 when no snapshot was
    /// used and the map was rebuilt from LSN 1) (Technical-Design §7).
    snapshot_lsn: u64,
    /// Key/value pairs loaded from the chosen snapshot (empty when none).
    snapshot_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    /// Verified, footer-closed mutations with lsn > `snapshot_lsn`, in order.
    records: Vec<MutationRecord>,
    /// The `record_hash` of the last replayed record (or the snapshot's
    /// boundary hash when no post-snapshot records were replayed).
    last_record_hash: u64,
    /// Whether a final unclosed/torn group was discarded.
    tail_truncated: bool,
    /// The active segment's first LSN (the newest segment).
    active_segment_first_lsn: u64,
    /// The byte offset in the active segment after the last valid footer
    /// (where the next group append should begin).
    active_segment_valid_len: u64,
}

/// The result of scanning the contiguous WAL tail after a recovery base
/// (a snapshot boundary or LSN 0). See [`Db::scan_wal_tail`].
#[derive(Debug, Clone)]
struct WalTail {
    /// Verified, footer-closed records with lsn > base, in LSN order.
    records: Vec<MutationRecord>,
    /// The `record_hash` of the last replayed record (base hash if none).
    last_record_hash: u64,
    /// Whether a final unclosed/torn group was discarded.
    tail_truncated: bool,
    /// The active (newest) segment's first LSN.
    active_segment_first_lsn: u64,
    /// Byte offset in the active segment after the last valid footer.
    active_segment_valid_len: u64,
}

/// A computed reclamation plan for a freshly verified snapshot: the WAL
/// segments and previous snapshot to delete, plus the retained recovery bytes
/// that would remain after executing it (Technical-Design §7). Planning is
/// separated from execution so the disk-budget check runs before any in-memory
/// recovery base advances.
#[derive(Debug, Clone)]
struct ReclamationPlan {
    /// First-LSNs of WAL segments wholly covered by the new snapshot.
    deletable_segments: Vec<u64>,
    /// The previous snapshot LSN whose recovery chain is now reclaimable
    /// (`None` when there was no previous snapshot).
    deletable_previous_snapshot: Option<u64>,
    /// Retained recovery bytes remaining after the plan is executed (the new
    /// snapshot plus retained WAL; excludes everything being reclaimed).
    retained_bytes: u64,
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
    /// Group-commit sync counters since this process opened the WAL.
    sync_stats: WalSyncStats,
    /// A group has been appended but not completed. While set, nothing else
    /// may append, rotate, or snapshot, because the active segment holds
    /// bytes the writer bookkeeping does not yet account for.
    group_pending: bool,
}

/// A group appended to the active segment but not yet synced, produced by
/// [`Wal::append_unsynced`]. Syncing it needs only this value, not the writer,
/// so a caller can run [`UnsyncedGroup::sync`] without holding the lock that
/// guards the [`Wal`]. [`Wal::complete_group`] then commits the bookkeeping.
pub struct UnsyncedGroup<F: FileSystem> {
    fs: F,
    segment: PathBuf,
    /// `false` in `os` mode and for an empty group: nothing to sync.
    needs_sync: bool,
    assigned: Vec<u64>,
    next_lsn: u64,
    last_hash: u64,
    group_len: u64,
}

impl<F: FileSystem> UnsyncedGroup<F> {
    /// `fsync` the segment holding this group and return how long it took.
    pub fn sync(&self) -> FsResult<Duration> {
        if !self.needs_sync {
            return Ok(Duration::ZERO);
        }
        let started = Instant::now();
        self.fs.sync_file(&self.segment)?;
        Ok(started.elapsed())
    }
}

/// Group-commit `fsync` counters since the WAL was opened (SOW §17 "WAL flush
/// latency"; Technical-Design §10). Counters reset on process restart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalSyncStats {
    /// Successful group syncs.
    pub syncs: u64,
    /// Failed group syncs. The first failure makes the WAL fail closed.
    pub errors: u64,
    /// Total time spent in successful group syncs.
    pub total: Duration,
    /// Longest successful group sync.
    pub max: Duration,
}

impl<F: FileSystem> Wal<F> {
    /// Group-commit sync counters since this WAL was opened.
    pub fn sync_stats(&self) -> WalSyncStats {
        self.sync_stats
    }

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
    pub fn append_group(&mut self, mutations: &[Mutation]) -> WalResult<Vec<u64>>
    where
        F: Clone,
    {
        let group = self.append_unsynced(mutations)?;
        let synced = group.sync();
        self.complete_group(group, synced)
    }

    /// First half of [`Wal::append_group`]: assign LSNs and append the group
    /// and its footer to the active segment without syncing. The writer's
    /// LSN, hash, and length bookkeeping is unchanged until
    /// [`Wal::complete_group`], and no other group may start before then.
    pub fn append_unsynced(&mut self, mutations: &[Mutation]) -> WalResult<UnsyncedGroup<F>>
    where
        F: Clone,
    {
        self.ensure_idle()?;
        if self.failed {
            return Err(WalError::FailClosed);
        }
        if mutations.is_empty() {
            self.group_pending = true;
            return Ok(UnsyncedGroup {
                fs: self.fs.clone(),
                segment: self.paths.segment(self.active_first_lsn),
                needs_sync: false,
                assigned: Vec::new(),
                next_lsn: self.next_lsn,
                last_hash: self.last_record_hash,
                group_len: 0,
            });
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
        let footer = GroupFooter {
            first_lsn,
            last_lsn: lsn - 1,
            count: mutations.len() as u32,
            last_record_hash: last_hash,
        };
        buf.extend_from_slice(&footer.encode());

        let seg_path = self.paths.segment(self.active_first_lsn);
        if let Err(e) = self.fs.append(&seg_path, &buf) {
            self.failed = true;
            return Err(WalError::Io(e));
        }
        self.group_pending = true;
        Ok(UnsyncedGroup {
            fs: self.fs.clone(),
            segment: seg_path,
            needs_sync: self.mode == DurabilityMode::Fsync,
            assigned,
            next_lsn: lsn,
            last_hash,
            group_len: buf.len() as u64,
        })
    }

    /// Second half of [`Wal::append_group`]: given the outcome of
    /// [`UnsyncedGroup::sync`], either fail closed or commit the group's
    /// bookkeeping and return its LSNs. Only after this returns `Ok` may the
    /// caller apply the group and acknowledge it.
    pub fn complete_group(
        &mut self,
        group: UnsyncedGroup<F>,
        synced: FsResult<Duration>,
    ) -> WalResult<Vec<u64>> {
        self.group_pending = false;
        match synced {
            Err(e) => {
                self.sync_stats.errors += 1;
                self.failed = true;
                return Err(WalError::Io(e));
            }
            Ok(elapsed) if group.needs_sync => {
                self.sync_stats.syncs += 1;
                self.sync_stats.total += elapsed;
                self.sync_stats.max = self.sync_stats.max.max(elapsed);
                self.durable_lsn = group.next_lsn - 1;
            }
            Ok(_) => {}
        }
        self.active_len += group.group_len;
        self.next_lsn = group.next_lsn;
        self.last_record_hash = group.last_hash;
        Ok(group.assigned)
    }

    fn ensure_idle(&self) -> WalResult<()> {
        if self.group_pending {
            return Err(WalError::GroupInProgress);
        }
        Ok(())
    }

    /// Explicitly sync the WAL through `lsn` (Technical-Design §2.1
    /// `sync_through`). Group commit already syncs each committed group in
    /// `fsync` mode, so this syncs the active segment file and confirms the
    /// durability boundary.
    pub fn sync_through(&mut self, lsn: u64) -> WalResult<u64> {
        self.ensure_idle()?;
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
        self.ensure_idle()?;
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

/// Whether two key/value pair lists are equal as sets (order-independent).
///
/// Snapshot v1 stores pairs in map iteration order, which is unspecified, so a
/// full reference-map comparison during publish verification (§7) compares by
/// content, not order. Keys are unique in both (the snapshot decoder rejects
/// duplicates), so equal length plus a key/value lookup match is sufficient.
fn pairs_equal_as_set(a: &[(Vec<u8>, Vec<u8>)], b: &[(Vec<u8>, Vec<u8>)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let map: std::collections::HashMap<&[u8], &[u8]> = a
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    b.iter()
        .all(|(k, v)| map.get(k.as_slice()) == Some(&v.as_slice()))
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

/// A node's statically configured role. Replicas never accept client writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
}

impl NodeRole {
    fn as_str(self) -> &'static str {
        match self {
            NodeRole::Primary => "primary",
            NodeRole::Replica => "replica",
        }
    }
}

/// Options fixed when a data directory is first created or reopened.
#[derive(Debug, Clone, Copy)]
pub struct OpenConfig {
    pub role: NodeRole,
    /// A replica must be given its primary's cluster ID. On an existing data
    /// directory, a supplied ID must match the persisted IDENTITY.
    pub cluster_id: Option<[u8; 16]>,
    pub retention_budget_bytes: u64,
    /// Provision a new replica to permit deliberate snapshot replacement when
    /// its old prefix is no longer verifiable. Persisted on first open.
    pub allow_snapshot_rebootstrap: bool,
    /// Storage engine for the key/value map. Persisted when a directory is
    /// created; reopening with a different engine is refused.
    pub storage: StorageKind,
}

impl Default for OpenConfig {
    fn default() -> Self {
        Self {
            role: NodeRole::Primary,
            cluster_id: None,
            retention_budget_bytes: DEFAULT_RETENTION_BUDGET_BYTES,
            allow_snapshot_rebootstrap: false,
            storage: StorageKind::Memory,
        }
    }
}

/// Storage engine for the key/value map (SOW §16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageKind {
    /// The in-memory hash map, rebuilt from snapshot and WAL on open.
    #[default]
    Memory,
    /// The LSM tree. Its tables are the recovery base; the WAL after the last
    /// flush is replayed into the memtable on open, and older WAL is deleted.
    /// `durable` is taken from the database's durability mode.
    Lsm(LsmConfig),
}

/// The map behind a [`Db`].
#[derive(Debug)]
enum Engine<F: FileSystem + Clone> {
    Memory(StorageEngine),
    Lsm(LsmTree<F>),
}

impl<F: FileSystem + Clone> Engine<F> {
    fn apply(&mut self, mutation: Mutation) {
        match self {
            Engine::Memory(engine) => engine.apply(mutation),
            Engine::Lsm(tree) => tree.apply(mutation),
        }
    }
}

/// Reports the lowest LSN a connected replica has acknowledged, if any.
pub type ReplicationFloor = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Records the LSM engine keeps beyond its flush boundary for a slow
/// connected replica. A replica further behind is offered a table image when
/// it reconnects.
const MAX_PINNED_RECORDS: u64 = 1_000_000;

/// See [`Db::lsm_write_stall`].
pub const LSM_STALL_FACTOR: usize = 4;

/// LSM counters reported by `STATS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmStats {
    /// Approximate bytes in the mutable memtable.
    pub memtable_bytes: usize,
    /// Installed tables.
    pub tables: usize,
    /// Installed level-0 tables.
    pub level_zero_tables: usize,
    /// Bytes of installed table files.
    pub table_bytes: u64,
    /// Flushes installed by this process.
    pub flushes: u64,
    /// Compactions installed by this process.
    pub compactions: u64,
}

/// A memtable flush started by [`Db::begin_lsm_flush`]. Write it without
/// holding the database lock, then pass the result to
/// [`Db::finish_lsm_flush`].
pub struct LsmFlush<F: FileSystem> {
    job: FlushJob<F>,
}

impl<F: FileSystem> LsmFlush<F> {
    /// Encode, publish, and verify the table.
    pub fn write(&self) -> LsmResult<WrittenTable> {
        self.job.write()
    }
}

/// A compaction started by [`Db::begin_lsm_compaction`]. Write it without
/// holding the database lock, then pass the result to
/// [`Db::finish_lsm_compaction`].
pub struct LsmCompaction<F: FileSystem> {
    job: CompactionJob<F>,
}

impl<F: FileSystem> LsmCompaction<F> {
    /// Merge the inputs and publish the output tables.
    pub fn write(&self) -> LsmResult<CompactedTables> {
        self.job.write()
    }

    /// [`LsmCompaction::write`] at no more than `bytes_per_sec`.
    pub fn write_paced(&self, bytes_per_sec: u64) -> LsmResult<CompactedTables> {
        self.job.write_paced(Some(bytes_per_sec))
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
pub struct Db<F: FileSystem + Clone> {
    engine: Engine<F>,
    wal: Wal<F>,
    last_applied_lsn: u64,
    /// Footer-closed records since the active snapshot boundary. These are
    /// rebuilt from disk on restart and are the only history offered to a
    /// replica without a snapshot transfer.
    retained_records: Vec<MutationRecord>,
    snapshot_hash: u64,
    tail_truncated: bool,
    /// The LSN of the snapshot currently used as the recovery base (0 if the
    /// map was rebuilt from LSN 1 with no snapshot) (Technical-Design §7).
    snapshot_lsn: u64,
    /// The number of WAL records replayed after the recovery base during the
    /// last [`Db::open`] (records with `lsn > snapshot_lsn`). This is the work
    /// recovery had to do beyond loading the snapshot map, and is bounded by
    /// `last_applied_lsn - snapshot_lsn` (Technical-Design §7 claim boundary;
    /// SOW §22 recovery-work-reduction acceptance goal).
    records_replayed: u64,
    /// Wall time [`Db::open`] spent from acquiring the data-directory lock
    /// through rebuilding the map (SOW §17 "recovery time").
    recovery_duration: Duration,
    /// Stale recovery generations this process has removed (see
    /// [`Db::remove_stale_generations`]).
    generations_removed: u64,
    /// Lowest LSN connected replicas still need; set by the replication
    /// listener.
    replication_floor: Option<ReplicationFloor>,
    /// LSM flushes and compactions installed by this process.
    lsm_flushes: u64,
    lsm_compactions: u64,
    /// Bounded disk budget for retained recovery data (snapshots + retained
    /// WAL). Reaching it pauses new snapshots' reclamation with
    /// [`WalError::ResourceExhausted`] (Technical-Design §7).
    retention_budget_bytes: u64,
    allow_snapshot_rebootstrap: bool,
    _lock: Box<dyn crate::fileio::LockGuard>,
}

/// A client mutation group appended to the WAL but not yet synced or applied;
/// see [`Db::begin_group`].
pub struct PendingGroup<F: FileSystem> {
    group: UnsyncedGroup<F>,
    prev_hash: u64,
}

impl<F: FileSystem> PendingGroup<F> {
    /// `fsync` the group. Needs no access to the [`Db`], so the caller can
    /// release the database lock first.
    pub fn sync(&self) -> FsResult<Duration> {
        self.group.sync()
    }
}

/// Verified snapshot bytes and the history boundary offered to a replica.
pub struct ReplicationSnapshot {
    pub bytes: Vec<u8>,
    pub lsn: u64,
    pub record_hash: u64,
    pub crc64: u64,
}

/// A replica's incomplete snapshot transfer. Only one receiver uses a data
/// directory at a time; a retry truncates any file left by a process crash.
/// The completed image is still loaded for the existing snapshot decoder.
pub(crate) struct SnapshotDownload<F: FileSystem> {
    fs: F,
    path: PathBuf,
    expected_len: usize,
    received: usize,
    crc: Crc64Ecma,
    trailer: Vec<u8>,
}

impl<F: FileSystem> SnapshotDownload<F> {
    pub(crate) fn received(&self) -> usize {
        self.received
    }

    pub(crate) fn append(&mut self, offset: u64, chunk: &[u8]) -> WalResult<()> {
        if offset != self.received as u64
            || chunk.is_empty()
            || chunk.len() > self.expected_len - self.received
        {
            return Err(WalError::Corruption(
                "invalid snapshot chunk offset or length".into(),
            ));
        }
        self.fs.append(&self.path, chunk)?;
        let mut combined = Vec::with_capacity(self.trailer.len() + chunk.len());
        combined.extend_from_slice(&self.trailer);
        combined.extend_from_slice(chunk);
        let crc_len = combined.len().saturating_sub(8);
        self.crc.update(&combined[..crc_len]);
        self.trailer.clear();
        self.trailer.extend_from_slice(&combined[crc_len..]);
        self.received += chunk.len();
        Ok(())
    }

    pub(crate) fn finish(self, expected_crc64: u64) -> WalResult<Vec<u8>> {
        if self.received != self.expected_len || self.trailer.len() != 8 {
            return Err(WalError::Corruption("incomplete snapshot transfer".into()));
        }
        let trailer = u64::from_le_bytes(self.trailer[..].try_into().expect("eight bytes"));
        if trailer != expected_crc64 || self.crc.finalize() != expected_crc64 {
            return Err(WalError::Corruption("snapshot checksum mismatch".into()));
        }
        self.fs.sync_file(&self.path)?;
        Ok(self.fs.read(&self.path)?)
    }
}

impl<F: FileSystem> Drop for SnapshotDownload<F> {
    fn drop(&mut self) {
        let _ = self.fs.remove_file(&self.path);
    }
}

impl<F: FileSystem + Clone> Db<F> {
    /// Stage a replica snapshot in the data directory's temporary area so a
    /// stalled transfer does not reserve the advertised image size in RAM.
    pub(crate) fn begin_snapshot_download(
        &self,
        expected_len: usize,
    ) -> WalResult<SnapshotDownload<F>> {
        if self.wal.identity.role != "replica" || !self.allow_snapshot_rebootstrap {
            return Err(WalError::Identity(
                "snapshot rebootstrap is not provisioned".into(),
            ));
        }
        let fs = self.wal.fs.clone();
        let path = self.wal.paths.tmp().join("replica-snapshot-download.tmp");
        fs.create_dir_all(&self.wal.paths.tmp())?;
        // SimFs can resurrect a removed but not directory-synced temp file
        // when the same name is created again. Truncate after create in both
        // adapters so retries never append to an earlier transfer.
        fs.create_file(&path)?;
        fs.truncate(&path, 0)?;
        Ok(SnapshotDownload {
            fs,
            path,
            expected_len,
            received: 0,
            crc: Crc64Ecma::new(),
            trailer: Vec::with_capacity(8),
        })
    }

    /// Persisted node identity, including the cluster ID a replica must use
    /// when it is provisioned. This value is stable across restarts.
    pub fn identity(&self) -> &Identity {
        &self.wal.identity
    }

    /// Active recovery generation selected by the checked CURRENT pointer.
    pub fn generation_id(&self) -> u64 {
        self.wal.paths.generation_id
    }

    /// Active local durability policy. Replication requires `Fsync`.
    pub fn durability_mode(&self) -> DurabilityMode {
        self.wal.mode
    }

    pub fn allows_snapshot_rebootstrap(&self) -> bool {
        self.allow_snapshot_rebootstrap
    }

    /// Open (initializing if fresh) the data directory at `root` under `fs`
    /// with durability `mode`, returning a ready [`Db`].
    ///
    /// On a fresh directory this acquires the LOCK, creates the generation-1
    /// layout, generates + persists a fresh IDENTITY (stable cluster/node IDs),
    /// writes a checked CURRENT, and creates the first segment. On an existing
    /// directory it acquires the LOCK, validates IDENTITY, then scans and
    /// replays the WAL.
    pub fn open(fs: F, root: &Path, mode: DurabilityMode) -> WalResult<Self> {
        Self::open_configured(fs, root, mode, OpenConfig::default())
    }

    /// Like [`Db::open`] but with an explicit retention disk budget in bytes
    /// (Technical-Design §7). Used by tests to exercise the
    /// [`WalError::ResourceExhausted`] pause with a small budget.
    pub fn open_with_budget(
        fs: F,
        root: &Path,
        mode: DurabilityMode,
        retention_budget_bytes: u64,
    ) -> WalResult<Self> {
        Self::open_configured(
            fs,
            root,
            mode,
            OpenConfig {
                retention_budget_bytes,
                ..OpenConfig::default()
            },
        )
    }

    /// Open with an explicit persisted role and cluster membership. This is
    /// the entry point for statically provisioned replicas.
    pub fn open_configured(
        fs: F,
        root: &Path,
        mode: DurabilityMode,
        config: OpenConfig,
    ) -> WalResult<Self> {
        if config.role == NodeRole::Replica && config.cluster_id.is_none() {
            return Err(WalError::Identity(
                "replica requires an explicit cluster ID".into(),
            ));
        }
        if config.role == NodeRole::Replica && mode != DurabilityMode::Fsync {
            return Err(WalError::Identity(
                "replica requires fsync durability".into(),
            ));
        }
        let mut paths = DataPaths::new(root);
        // Acquire the exclusive data-directory lock before recovery (§3, §9).
        // The parent directory must exist to create the LOCK file.
        fs.create_dir_all(&paths.root)?;
        let lock = fs.acquire_lock(&paths.lock())?;
        let recovery_started = Instant::now();

        let identity = if fs.exists(&paths.identity()) {
            let bytes = fs.read(&paths.identity())?;
            let id = Identity::decode(&bytes)?;
            if id.version != IDENTITY_VERSION {
                return Err(WalError::Identity(format!(
                    "unsupported IDENTITY version {}",
                    id.version
                )));
            }
            if id.role != config.role.as_str() {
                return Err(WalError::Identity(
                    "configured role differs from IDENTITY".into(),
                ));
            }
            if config
                .cluster_id
                .is_some_and(|cluster| cluster != id.cluster_id)
            {
                return Err(WalError::Identity(
                    "configured cluster ID differs from IDENTITY".into(),
                ));
            }
            id
        } else {
            Self::init_fresh(&fs, &paths, mode, config)?
        };

        let stored_lsm = fs.exists(&paths.storage_engine())
            && decode_storage_engine(&fs.read(&paths.storage_engine())?)? == ENGINE_LSM;
        if stored_lsm != matches!(config.storage, StorageKind::Lsm(_)) {
            let stored = if stored_lsm { "lsm" } else { "memory" };
            return Err(WalError::Identity(format!(
                "data directory uses the {stored} storage engine"
            )));
        }

        let allow_snapshot_rebootstrap = if identity.role == "replica" {
            if fs.exists(&paths.rebootstrap_policy()) {
                decode_rebootstrap_policy(&fs.read(&paths.rebootstrap_policy())?)?
            } else {
                // Directories created before snapshot rebootstrap existed stay
                // fail-closed until explicitly reprovisioned.
                false
            }
        } else {
            false
        };

        let current_bytes = fs.read(&paths.current())?;
        let generation_id = if current_bytes == b"0000000000000001" {
            // Phase 2–4 directories used a provisional ASCII pointer. Replace
            // it by a synced v1 pointer through a same-filesystem rename. A
            // crash before the rename leaves the old pointer readable, so the
            // migration is safe to retry.
            Self::upgrade_legacy_current(&fs, &paths, mode)?;
            1
        } else {
            Current::decode(&current_bytes)
                .map_err(|e| WalError::Identity(e.into()))?
                .generation
        };
        paths = DataPaths::with_generation(root, generation_id);
        if !fs.exists(&paths.generation()) {
            return Err(WalError::Identity(
                "CURRENT points to a missing generation".into(),
            ));
        }

        // Recover the map's base, then replay the contiguous footer-closed WAL
        // tail after it. The in-memory map starts from the chosen snapshot
        // (empty when none qualified); the LSM tree starts from its installed
        // tables, which cover the WAL up to their flush boundary.
        let (outcome, mut engine) = match config.storage {
            StorageKind::Memory => {
                let outcome = Self::recover(&fs, &paths, &identity, mode)?;
                let mut engine = StorageEngine::new();
                for (key, value) in &outcome.snapshot_pairs {
                    engine.apply(Mutation::Set {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                (outcome, Engine::Memory(engine))
            }
            StorageKind::Lsm(lsm_config) => {
                let tree = LsmTree::open(
                    fs.clone(),
                    &paths.lsm_dir(),
                    &paths.tmp(),
                    LsmConfig {
                        durable: mode == DurabilityMode::Fsync,
                        ..lsm_config
                    },
                )?;
                let outcome = Self::recover_from_lsm_base(
                    &fs,
                    &paths,
                    &identity,
                    mode,
                    tree.flushed_lsn(),
                    tree.flushed_hash(),
                )?;
                (outcome, Engine::Lsm(tree))
            }
        };
        let mut last_applied_lsn = outcome.snapshot_lsn;
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

        // Recovery read only the generation CURRENT selects; every other
        // generation is garbage. Removal is best effort so a stray file in an
        // old generation cannot stop the node from opening; the next open
        // retries whatever remains.
        let generations_removed =
            Self::remove_stale_generations(&fs, &paths.root, generation_id, mode).unwrap_or(0);

        let snapshot_hash = outcome
            .records
            .first()
            .map(|record| record.prev_hash)
            .unwrap_or(outcome.last_record_hash);
        let retained_records = outcome.records.clone();
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
            sync_stats: WalSyncStats::default(),
            group_pending: false,
        };

        Ok(Db {
            engine,
            wal,
            last_applied_lsn,
            retained_records,
            snapshot_hash,
            tail_truncated: outcome.tail_truncated,
            snapshot_lsn: outcome.snapshot_lsn,
            records_replayed: outcome.records.len() as u64,
            recovery_duration: recovery_started.elapsed(),
            generations_removed,
            replication_floor: None,
            lsm_flushes: 0,
            lsm_compactions: 0,
            retention_budget_bytes: config.retention_budget_bytes,
            allow_snapshot_rebootstrap,
            _lock: lock,
        })
    }

    fn upgrade_legacy_current(fs: &F, paths: &DataPaths, mode: DurabilityMode) -> WalResult<()> {
        fs.create_dir_all(&paths.tmp())?;
        let temp = paths.tmp().join("CURRENT.v1.tmp");
        if fs.exists(&temp) {
            fs.truncate(&temp, 0)?;
        } else {
            fs.create_file(&temp)?;
        }
        fs.append(&temp, &Current { generation: 1 }.encode())?;
        if mode == DurabilityMode::Fsync {
            fs.sync_file(&temp)?;
        }
        fs.rename(&temp, &paths.current())?;
        if mode == DurabilityMode::Fsync {
            fs.sync_dir(&paths.tmp())?;
            fs.sync_dir(&paths.root)?;
        }
        Ok(())
    }

    /// Initialize a fresh data directory, returning the new IDENTITY.
    fn init_fresh(
        fs: &F,
        paths: &DataPaths,
        mode: DurabilityMode,
        config: OpenConfig,
    ) -> WalResult<Identity> {
        fs.create_dir_all(&paths.tmp())?;
        fs.create_dir_all(&paths.wal_dir())?;
        fs.create_dir_all(&paths.snapshots_dir())?;

        let identity = Identity {
            version: IDENTITY_VERSION,
            cluster_id: config.cluster_id.unwrap_or_else(|| generate_id(0xC1)),
            node_id: generate_id(0x0D),
            role: config.role.as_str().to_string(),
        };
        // Record the engine before IDENTITY, whose presence marks a
        // completed initialization.
        let lsm = matches!(config.storage, StorageKind::Lsm(_));
        if lsm {
            fs.create_file(&paths.storage_engine())?;
            fs.append(&paths.storage_engine(), &encode_storage_engine(ENGINE_LSM))?;
        }
        // Write IDENTITY durably.
        fs.create_file(&paths.identity())?;
        fs.append(&paths.identity(), &identity.encode())?;
        if config.role == NodeRole::Replica {
            fs.create_file(&paths.rebootstrap_policy())?;
            fs.append(
                &paths.rebootstrap_policy(),
                &encode_rebootstrap_policy(config.allow_snapshot_rebootstrap),
            )?;
        }

        // The checked CURRENT pointer selects the recovery generation.
        fs.create_file(&paths.current())?;
        fs.append(&paths.current(), &Current { generation: 1 }.encode())?;

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
            if lsm {
                fs.sync_file(&paths.storage_engine())?;
            }
            fs.sync_file(&paths.identity())?;
            if config.role == NodeRole::Replica {
                fs.sync_file(&paths.rebootstrap_policy())?;
            }
            fs.sync_file(&paths.current())?;
            fs.sync_file(&seg)?;
            fs.sync_dir(&paths.wal_dir())?;
            fs.sync_dir(&paths.snapshots_dir())?;
            fs.sync_dir(&paths.generation())?;
            fs.sync_dir(&paths.root.join("generations"))?;
            fs.sync_dir(&paths.tmp())?;
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
    /// First LSNs of the WAL segments, ascending.
    fn segment_first_lsns(fs: &F, paths: &DataPaths) -> WalResult<Vec<u64>> {
        let mut segment_lsns: Vec<u64> = Vec::new();
        for name in fs.list_dir(&paths.wal_dir())? {
            if let Some(stem) = name.strip_suffix(".wal") {
                if let Ok(first_lsn) = stem.parse::<u64>() {
                    segment_lsns.push(first_lsn);
                }
            }
        }
        segment_lsns.sort_unstable();
        Ok(segment_lsns)
    }

    /// Recovery for the LSM engine: the installed tables are the base, so
    /// replay the WAL that continues from their flush boundary. Each flush
    /// starts a WAL segment right after its boundary, so the chain must exist;
    /// its absence is corruption, not a reason to fall back.
    fn recover_from_lsm_base(
        fs: &F,
        paths: &DataPaths,
        identity: &Identity,
        mode: DurabilityMode,
        base_lsn: u64,
        base_hash: u64,
    ) -> WalResult<RecoveryOutcome> {
        let segment_lsns = Self::segment_first_lsns(fs, paths)?;
        let tail = Self::scan_wal_tail(
            fs,
            paths,
            identity,
            &segment_lsns,
            mode,
            base_lsn,
            base_hash,
        )?
        .ok_or_else(|| {
            WalError::Corruption(format!(
                "the WAL does not continue from the LSM flush boundary {base_lsn}"
            ))
        })?;
        let last_record_hash = if tail.records.is_empty() {
            base_hash
        } else {
            tail.last_record_hash
        };
        Ok(RecoveryOutcome {
            snapshot_lsn: base_lsn,
            snapshot_pairs: Vec::new(),
            records: tail.records,
            last_record_hash,
            tail_truncated: tail.tail_truncated,
            active_segment_first_lsn: tail.active_segment_first_lsn,
            active_segment_valid_len: tail.active_segment_valid_len,
        })
    }

    fn recover(
        fs: &F,
        paths: &DataPaths,
        identity: &Identity,
        mode: DurabilityMode,
    ) -> WalResult<RecoveryOutcome> {
        // Enumerate segments and order them by their embedded first LSN.
        let segment_lsns = Self::segment_first_lsns(fs, paths)?;

        // Enumerate verified-candidate snapshots, ordered by LSN descending so
        // we prefer the newest recoverable state (Technical-Design §7, §9.3).
        let mut snapshot_lsns: Vec<u64> = Vec::new();
        for name in fs.list_dir(&paths.snapshots_dir())? {
            if let Some(stem) = name.strip_suffix(".snap") {
                if let Ok(lsn) = stem.parse::<u64>() {
                    snapshot_lsns.push(lsn);
                }
            }
        }
        snapshot_lsns.sort_unstable();
        snapshot_lsns.reverse();

        // Whether the newest snapshot on disk failed verification. This gates
        // the fail-closed rule: a corrupt LATEST snapshot must never silently
        // fall back to an older/empty state if no complete chain remains
        // (Technical-Design §7).
        let mut latest_snapshot_corrupt = false;

        // Try each snapshot, newest first: it must decode+verify AND its
        // post-snapshot WAL tail must be a contiguous, chaining, footer-closed
        // log from snapshot_lsn+1 to the end.
        for (idx, &snap_lsn) in snapshot_lsns.iter().enumerate() {
            match Self::load_verified_snapshot(fs, paths, identity, snap_lsn) {
                Ok(decoded) => {
                    match Self::scan_wal_tail(
                        fs,
                        paths,
                        identity,
                        &segment_lsns,
                        mode,
                        snap_lsn,
                        decoded.header.record_hash_at_lsn,
                    )? {
                        Some(tail) => {
                            let last_record_hash = if tail.records.is_empty() {
                                decoded.header.record_hash_at_lsn
                            } else {
                                tail.last_record_hash
                            };
                            return Ok(RecoveryOutcome {
                                snapshot_lsn: snap_lsn,
                                snapshot_pairs: decoded.pairs,
                                records: tail.records,
                                last_record_hash,
                                tail_truncated: tail.tail_truncated,
                                active_segment_first_lsn: tail.active_segment_first_lsn,
                                active_segment_valid_len: tail.active_segment_valid_len,
                            });
                        }
                        None => {
                            // Verified snapshot but no contiguous WAL tail; try
                            // the next-lower snapshot.
                            continue;
                        }
                    }
                }
                Err(_) => {
                    if idx == 0 {
                        latest_snapshot_corrupt = true;
                    }
                    // Try the next-lower snapshot.
                    continue;
                }
            }
        }

        // No snapshot yielded a complete chain. Fall back to a full log from
        // LSN 1 ONLY when a complete contiguous log from LSN 1 exists.
        match Self::scan_wal_tail(fs, paths, identity, &segment_lsns, mode, 0, 0)? {
            Some(tail) => {
                let last_record_hash = if tail.records.is_empty() {
                    0
                } else {
                    tail.last_record_hash
                };
                Ok(RecoveryOutcome {
                    snapshot_lsn: 0,
                    snapshot_pairs: Vec::new(),
                    records: tail.records,
                    last_record_hash,
                    tail_truncated: tail.tail_truncated,
                    active_segment_first_lsn: tail.active_segment_first_lsn,
                    active_segment_valid_len: tail.active_segment_valid_len,
                })
            }
            None => {
                // No complete recovery chain remains. If the latest snapshot
                // was corrupt (and we already deleted older WAL), we must fail
                // closed rather than silently start empty (Technical-Design
                // §7).
                if latest_snapshot_corrupt {
                    Err(WalError::Corruption(
                        "latest snapshot is corrupt and no complete recovery chain remains".into(),
                    ))
                } else {
                    Err(WalError::Corruption(
                        "no complete WAL chain from LSN 1 and no usable snapshot".into(),
                    ))
                }
            }
        }
    }

    /// Load and fully verify the snapshot at `snap_lsn` from disk against its
    /// header, cluster id, checksums, `entry_count`, and `payload_len`
    /// (Technical-Design §7). Returns the decoded snapshot on success.
    fn load_verified_snapshot(
        fs: &F,
        paths: &DataPaths,
        identity: &Identity,
        snap_lsn: u64,
    ) -> WalResult<crate::wal::snapshot::DecodedSnapshot> {
        let bytes = fs.read(&paths.snapshot(snap_lsn))?;
        let decoded = snapshot::decode(&bytes, Some(identity.cluster_id))
            .map_err(|e| WalError::Corruption(format!("snapshot {snap_lsn} invalid: {e}")))?;
        // The filename LSN must match the header's snapshot_lsn: names are
        // data, not authority (Technical-Design §6).
        if decoded.header.snapshot_lsn != snap_lsn {
            return Err(WalError::Corruption(format!(
                "snapshot header lsn {} does not match filename {snap_lsn}",
                decoded.header.snapshot_lsn
            )));
        }
        Ok(decoded)
    }

    /// Scan the WAL segments for the contiguous, footer-closed log that begins
    /// at `base_lsn + 1` and chains from `base_hash`, replaying every verified
    /// record with `lsn > base_lsn` (Technical-Design §6.3, §7, §9).
    ///
    /// `base_lsn == 0` / `base_hash == 0` means "from LSN 1 with no snapshot".
    /// The chain must be contiguous: the first relevant segment must begin at
    /// `base_lsn + 1`, later segments must begin exactly where the previous one
    /// ended, and the `prev_hash` chain must continue from `base_hash`. Sealed
    /// segments must be entirely footer-closed (interior corruption fails
    /// closed); the newest segment may discard a torn final group and is
    /// truncated at the last valid footer.
    ///
    /// Returns `Ok(Some(tail))` when a complete contiguous chain exists,
    /// `Ok(None)` when the WAL does not form such a chain from this base (so
    /// the caller can try a lower snapshot or fail closed), and `Err(...)` for
    /// interior/sealed corruption that must fail closed regardless of base.
    fn scan_wal_tail(
        fs: &F,
        paths: &DataPaths,
        identity: &Identity,
        segment_lsns: &[u64],
        mode: DurabilityMode,
        base_lsn: u64,
        base_hash: u64,
    ) -> WalResult<Option<WalTail>> {
        // A brand-new data directory (no snapshot, base 0) always has segment 1
        // created at init; an empty enumeration only happens on a fresh dir.
        if segment_lsns.is_empty() {
            if base_lsn == 0 {
                return Ok(Some(WalTail {
                    records: Vec::new(),
                    last_record_hash: 0,
                    tail_truncated: false,
                    active_segment_first_lsn: 1,
                    active_segment_valid_len: SEGMENT_HEADER_LEN as u64,
                }));
            }
            return Ok(None);
        }

        // The relevant segments are those whose records can carry lsn >
        // base_lsn. A segment with first_lsn <= base_lsn holds only records at
        // or below the snapshot boundary (already captured by the snapshot) or
        // straddles the boundary. For a clean chain the segment beginning at
        // exactly base_lsn+1 must exist; segments strictly below that are
        // reclaimable/covered and are skipped only if a segment at base_lsn+1
        // is present.
        let expected_first = base_lsn + 1;
        // Find the index of the segment that begins at expected_first.
        let start_idx = match segment_lsns.iter().position(|&s| s == expected_first) {
            Some(i) => i,
            None => {
                // No segment starts exactly at base_lsn+1: the contiguous chain
                // from this base is not present.
                return Ok(None);
            }
        };

        let relevant = &segment_lsns[start_idx..];

        let mut records: Vec<MutationRecord> = Vec::new();
        let mut last_record_hash = base_hash;
        let mut prev_hash = base_hash;
        let mut next_expected_first = expected_first;
        let mut tail_truncated = false;

        let mut active_segment_first_lsn = relevant[0];
        let mut active_segment_valid_len = SEGMENT_HEADER_LEN as u64;

        let last_index = relevant.len() - 1;
        for (i, &segment_first_lsn) in relevant.iter().enumerate() {
            // Segment continuity: this segment must begin exactly where the
            // previous one ended.
            if segment_first_lsn != next_expected_first {
                return Ok(None);
            }
            let seg_path = paths.segment(segment_first_lsn);
            let bytes = fs.read(&seg_path)?;
            let is_active = i == last_index;

            // An ACTIVE (newest) segment too short to even hold a header is an
            // incomplete/torn segment creation: a crash interrupted a rotate
            // (e.g. snapshot-publication step 2) after the file appeared but
            // before its header was durably synced. This is the active-segment
            // analogue of a torn tail (§6.3): the earlier SEALED segments hold
            // all committed records, so we repair the empty segment by writing
            // a fresh header and treat it as an empty active segment. A SEALED
            // segment (not the newest) that is this short is genuine
            // corruption and still fails closed. (Technical-Design §6.3, §7.)
            if is_active && bytes.len() < SEGMENT_HEADER_LEN {
                let header = SegmentHeader {
                    cluster_id: identity.cluster_id,
                    node_id: identity.node_id,
                    first_lsn: segment_first_lsn,
                };
                fs.truncate(&seg_path, 0)?;
                fs.append(&seg_path, &header.encode())?;
                if mode == DurabilityMode::Fsync {
                    fs.sync_file(&seg_path)?;
                    fs.sync_dir(&paths.wal_dir())?;
                }
                tail_truncated = true;
                active_segment_first_lsn = segment_first_lsn;
                active_segment_valid_len = SEGMENT_HEADER_LEN as u64;
                // No records in this segment; the chain ends here.
                break;
            }

            let scan = scan_as(&bytes, segment_first_lsn, prev_hash, is_active)?;

            records.extend(scan.records.iter().cloned());
            if !scan.records.is_empty() {
                last_record_hash = scan.last_record_hash;
            }
            prev_hash = scan.last_record_hash;
            // The next segment must begin one past the last committed record
            // in this segment.
            next_expected_first = records
                .last()
                .map(|r| r.lsn + 1)
                .unwrap_or(segment_first_lsn);

            if is_active {
                active_segment_first_lsn = segment_first_lsn;
                active_segment_valid_len = scan.valid_len;
                if scan.truncated {
                    tail_truncated = true;
                    fs.truncate(&seg_path, scan.valid_len)?;
                    if mode == DurabilityMode::Fsync {
                        fs.sync_file(&seg_path)?;
                    }
                }
            }
        }

        Ok(Some(WalTail {
            records,
            last_record_hash,
            tail_truncated,
            active_segment_first_lsn,
            active_segment_valid_len,
        }))
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
        if self.wal.identity.role != "primary" {
            return Err(WalError::ReadOnlyReplica);
        }
        let prev_hash = self.wal.last_record_hash;
        let assigned = self.wal.append_group(std::slice::from_ref(&m))?;
        let lsn = assigned[0];
        let rec = mutation_to_record(&m, lsn, prev_hash);
        // Write-ahead order: apply to the map only after the durable append.
        self.engine.apply(m);
        self.last_applied_lsn = lsn;
        self.retained_records.push(rec);
        // The write is durable and applied. A maintenance failure makes the
        // node fail closed, so later writes report it.
        let _ = self.maintain_lsm();
        Ok(lsn)
    }

    /// Durably apply a group of up to [`MAX_GROUP_RECORDS`] mutations in a
    /// single group commit (one `fsync` in `fsync` mode), then apply them to
    /// the engine in order and return the assigned LSNs (Technical-Design §3,
    /// §5, §6.2).
    ///
    /// This is the group-commit entry point used by the Phase 3 networking
    /// sequencer (Technical-Design §5): a batch of client `SET`/`DELETE`
    /// mutations is appended, footer-closed, and synced once, so the fixed
    /// `fsync` cost is amortized across the whole batch. Write-ahead order is
    /// preserved: the mutations are applied to the map only after the durable
    /// append succeeds. On any WAL error the engine is left untouched and the
    /// error is returned; the caller (sequencer) surfaces it to each waiting
    /// request. An empty slice is a no-op returning an empty `Vec`.
    ///
    /// The batching bounds (`<= MAX_GROUP_RECORDS` records and
    /// `<= MAX_GROUP_BYTES` encoded) are enforced by
    /// [`Wal::append_group`]; callers should pre-drain within those limits.
    pub fn apply_group(&mut self, muts: &[Mutation]) -> WalResult<Vec<u64>> {
        if self.wal.identity.role != "primary" {
            return Err(WalError::ReadOnlyReplica);
        }
        if muts.is_empty() {
            return Ok(Vec::new());
        }
        let pending = self.begin_group(muts)?;
        let synced = pending.sync();
        let assigned = self.finish_group(pending, muts, synced)?;
        // As in `apply_durable`, a maintenance failure fails closed.
        let _ = self.maintain_lsm();
        Ok(assigned)
    }

    /// First half of [`Db::apply_group`]: append `muts` to the WAL without
    /// syncing or applying them. The map, LSNs, and replication history are
    /// unchanged, so readers keep seeing only acknowledged state.
    ///
    /// The server's sequencer calls this under the database write lock,
    /// releases the lock for [`PendingGroup::sync`], then retakes it for
    /// [`Db::finish_group`]. Reads therefore do not wait for the `fsync`
    /// (Phase 7 lock-scope change). No other mutation, snapshot, or rotation
    /// is accepted until the group finishes.
    pub fn begin_group(&mut self, muts: &[Mutation]) -> WalResult<PendingGroup<F>> {
        if self.wal.identity.role != "primary" {
            return Err(WalError::ReadOnlyReplica);
        }
        let prev_hash = self.wal.last_record_hash;
        let group = self.wal.append_unsynced(muts)?;
        Ok(PendingGroup { group, prev_hash })
    }

    /// Second half of [`Db::apply_group`]: given the sync outcome, fail closed
    /// or apply `muts` (the same slice passed to [`Db::begin_group`]) to the
    /// map in LSN order and return their LSNs.
    pub fn finish_group(
        &mut self,
        pending: PendingGroup<F>,
        muts: &[Mutation],
        synced: FsResult<Duration>,
    ) -> WalResult<Vec<u64>> {
        let mut prev_hash = pending.prev_hash;
        let assigned = self.wal.complete_group(pending.group, synced)?;
        debug_assert_eq!(assigned.len(), muts.len());
        // Write-ahead order: apply to the map only after the durable append.
        for (m, &lsn) in muts.iter().zip(assigned.iter()) {
            self.engine.apply(m.clone());
            let rec = mutation_to_record(m, lsn, prev_hash);
            prev_hash = rec.record_hash();
            self.retained_records.push(rec);
        }
        if let Some(&last) = assigned.last() {
            self.last_applied_lsn = last;
        }
        Ok(assigned)
    }

    /// Return a known hash at `lsn`, or `None` when that prefix was reclaimed
    /// or the requested LSN is ahead of this node. Hashes detect accidental
    /// divergence; they are not authentication.
    pub fn record_hash_at(&self, lsn: u64) -> Option<u64> {
        if lsn == self.snapshot_lsn {
            return Some(self.snapshot_hash);
        }
        if lsn < self.snapshot_lsn || lsn > self.last_applied_lsn {
            return None;
        }
        let index = usize::try_from(lsn - self.snapshot_lsn - 1).ok()?;
        self.retained_records
            .get(index)
            .map(MutationRecord::record_hash)
    }

    /// Copy at most `limit` durable records following a verified LSN. `None`
    /// means the requested prefix is outside the retained history and requires
    /// an explicit snapshot rebootstrap or divergence decision.
    pub fn durable_records_after(&self, lsn: u64, limit: usize) -> Option<Vec<MutationRecord>> {
        if self.wal.mode != DurabilityMode::Fsync
            || lsn < self.snapshot_lsn
            || lsn > self.wal.durable_lsn
        {
            return None;
        }
        let start = usize::try_from(lsn - self.snapshot_lsn).ok()?;
        Some(
            self.retained_records
                .iter()
                .skip(start)
                .take(limit)
                .cloned()
                .collect(),
        )
    }

    /// Clone the verified active snapshot for a replica that cannot verify a
    /// reclaimed WAL prefix. The transfer layer bounds the offered size.
    pub fn replication_snapshot(&self) -> WalResult<Option<ReplicationSnapshot>> {
        if let Engine::Lsm(tree) = &self.engine {
            // The installed tables hold exactly the state at their flush
            // boundary, which is at or after the history base, so the records
            // after it are retained. Encode them as a snapshot image.
            let (lsn, record_hash) = (tree.flushed_lsn(), tree.flushed_hash());
            if lsn == 0 {
                return Ok(None);
            }
            let pairs = tree.table_pairs()?;
            let bytes = snapshot::encode(self.wal.identity.cluster_id, lsn, record_hash, &pairs);
            let crc64 = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().unwrap());
            return Ok(Some(ReplicationSnapshot {
                bytes,
                lsn,
                record_hash,
                crc64,
            }));
        }
        if self.snapshot_lsn == 0 {
            return Ok(None);
        }
        let bytes = self
            .wal
            .fs
            .read(&self.wal.paths.snapshot(self.snapshot_lsn))?;
        let decoded = snapshot::decode(&bytes, Some(self.wal.identity.cluster_id))
            .map_err(|error| WalError::Corruption(format!("replication snapshot: {error}")))?;
        if decoded.header.snapshot_lsn != self.snapshot_lsn
            || decoded.header.record_hash_at_lsn != self.snapshot_hash
        {
            return Err(WalError::Corruption(
                "replication snapshot boundary differs from active WAL".into(),
            ));
        }
        let crc = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().unwrap());
        Ok(Some(ReplicationSnapshot {
            bytes,
            lsn: self.snapshot_lsn,
            record_hash: self.snapshot_hash,
            crc64: crc,
        }))
    }

    /// Atomically install a received snapshot as a new replica recovery
    /// generation. A crash before CURRENT publication keeps the old recovery
    /// point; a crash after it selects the fully synced new snapshot and empty
    /// continuation WAL. An in-process I/O failure requires restart.
    pub fn install_replica_snapshot(
        &mut self,
        bytes: &[u8],
        expected_lsn: u64,
        expected_hash: u64,
    ) -> WalResult<u64> {
        if self.wal.identity.role != "replica" || !self.allow_snapshot_rebootstrap {
            return Err(WalError::Identity(
                "snapshot rebootstrap is not provisioned for this replica".into(),
            ));
        }
        if self.wal.failed {
            return Err(WalError::FailClosed);
        }
        let decoded = snapshot::decode(bytes, Some(self.wal.identity.cluster_id))
            .map_err(|error| WalError::Corruption(format!("received snapshot: {error}")))?;
        if decoded.header.snapshot_lsn != expected_lsn
            || decoded.header.record_hash_at_lsn != expected_hash
            || expected_lsn <= self.last_applied_lsn
        {
            return Err(WalError::Corruption(
                "received snapshot boundary is invalid or stale".into(),
            ));
        }
        let next_lsn = expected_lsn
            .checked_add(1)
            .ok_or_else(|| WalError::Corruption("snapshot LSN exhausted".into()))?;
        let mut generation = self.wal.paths.generation_id;
        let paths = loop {
            generation = generation
                .checked_add(1)
                .ok_or_else(|| WalError::Corruption("generation ID exhausted".into()))?;
            let candidate = DataPaths::with_generation(&self.wal.paths.root, generation);
            if !self.wal.fs.exists(&candidate.generation()) {
                break candidate;
            }
        };
        let fs = &self.wal.fs;
        fs.create_dir_all(&paths.wal_dir())?;
        fs.create_dir_all(&paths.snapshots_dir())?;
        // The new generation's base: a snapshot file for the in-memory map,
        // or a tree holding the image as its first table for the LSM engine.
        let engine = match &self.engine {
            Engine::Memory(_) => {
                let snapshot_path = paths.snapshot(expected_lsn);
                fs.create_file(&snapshot_path)?;
                fs.append(&snapshot_path, bytes)?;
                fs.sync_file(&snapshot_path)?;
                let mut engine = StorageEngine::new();
                for (key, value) in decoded.pairs {
                    engine.apply(Mutation::Set { key, value });
                }
                Engine::Memory(engine)
            }
            Engine::Lsm(old) => {
                let mut tree =
                    LsmTree::open(fs.clone(), &paths.lsm_dir(), &paths.tmp(), old.config())?;
                tree.ingest(decoded.pairs, expected_lsn, expected_hash)?;
                Engine::Lsm(tree)
            }
        };
        let segment_path = paths.segment(next_lsn);
        let header = SegmentHeader {
            cluster_id: self.wal.identity.cluster_id,
            node_id: self.wal.identity.node_id,
            first_lsn: next_lsn,
        };
        fs.create_file(&segment_path)?;
        fs.append(&segment_path, &header.encode())?;
        fs.sync_file(&segment_path)?;
        fs.sync_dir(&paths.snapshots_dir())?;
        fs.sync_dir(&paths.wal_dir())?;
        fs.sync_dir(&paths.generation())?;
        fs.sync_dir(&paths.root.join("generations"))?;

        let pointer_tmp = paths.tmp().join("CURRENT.rebootstrap.tmp");
        if fs.exists(&pointer_tmp) {
            fs.truncate(&pointer_tmp, 0)?;
        } else {
            fs.create_file(&pointer_tmp)?;
        }
        fs.append(&pointer_tmp, &Current { generation }.encode())?;
        fs.sync_file(&pointer_tmp)?;
        fs.rename(&pointer_tmp, &paths.current())?;
        // A failure after rename may leave the running replica with an
        // uncertain pointer. It must stop and recover rather than ACK.
        if let Err(error) = fs
            .sync_dir(&paths.tmp())
            .and_then(|_| fs.sync_dir(&paths.root))
        {
            self.wal.failed = true;
            return Err(error.into());
        }

        self.engine = engine;
        self.wal.paths = paths;
        self.wal.next_lsn = next_lsn;
        self.wal.last_record_hash = expected_hash;
        self.wal.durable_lsn = expected_lsn;
        self.wal.active_first_lsn = next_lsn;
        self.wal.active_len = SEGMENT_HEADER_LEN as u64;
        self.last_applied_lsn = expected_lsn;
        self.snapshot_lsn = expected_lsn;
        self.snapshot_hash = expected_hash;
        self.retained_records.clear();
        self.records_replayed = 0;
        // CURRENT now durably selects the new generation, so the one it
        // replaced (and any earlier leftovers) are garbage. Best effort: a
        // failure here leaves old files that the next open removes.
        let root = self.wal.paths.root.clone();
        let mode = self.wal.mode;
        if let Ok(removed) = Self::remove_stale_generations(&self.wal.fs, &root, generation, mode) {
            self.generations_removed += removed;
        }
        Ok(expected_lsn)
    }

    /// Remove every recovery generation except `current`: older generations
    /// superseded by a published CURRENT, and newer ones left by a snapshot
    /// install that stopped before publishing CURRENT. Recovery reads only the
    /// generation CURRENT selects, so a crash part-way through leaves a partial
    /// directory that is never read and that a later call finishes removing.
    ///
    /// Generation IDs are assigned upward from 1, and an install takes the
    /// first free ID above the current one, so leftovers above `current` are
    /// contiguous. This probes each ID below `current`, one existence check
    /// per past generation. Returns the number of generations removed.
    fn remove_stale_generations(
        fs: &F,
        root: &Path,
        current: u64,
        mode: DurabilityMode,
    ) -> WalResult<u64> {
        let mut stale: Vec<DataPaths> = (1..current)
            .map(|id| DataPaths::with_generation(root, id))
            .filter(|paths| fs.exists(&paths.generation()))
            .collect();
        let mut above = current;
        while let Some(id) = above.checked_add(1) {
            let paths = DataPaths::with_generation(root, id);
            if !fs.exists(&paths.generation()) {
                break;
            }
            stale.push(paths);
            above = id;
        }
        for paths in &stale {
            for dir in [paths.wal_dir(), paths.snapshots_dir(), paths.lsm_dir()] {
                for name in fs.list_dir(&dir)? {
                    fs.remove_file(&dir.join(name))?;
                }
                if fs.exists(&dir) {
                    fs.remove_dir(&dir)?;
                }
            }
            fs.remove_dir(&paths.generation())?;
        }
        if !stale.is_empty() && mode == DurabilityMode::Fsync {
            fs.sync_dir(&root.join("generations"))?;
        }
        Ok(stale.len() as u64)
    }

    /// Stale recovery generations removed by this process, at open and after
    /// snapshot installs.
    pub fn generations_removed(&self) -> u64 {
        self.generations_removed
    }

    /// Accept one validated primary record on a replica. The local WAL may
    /// have a different group footer, but its mutation bytes and hash chain
    /// must be identical. ACK only after this method returns successfully.
    pub fn apply_replicated_record(&mut self, record: &MutationRecord) -> WalResult<u64> {
        self.apply_replicated_records(std::slice::from_ref(record))?;
        Ok(record.lsn)
    }

    /// Accept a run of validated primary records on a replica, appending them
    /// as local groups of up to [`MAX_GROUP_RECORDS`] records and
    /// [`MAX_GROUP_BYTES`], each with one sync, instead of one sync per record.
    /// Every record is checked for contiguity and identical bytes before any
    /// is written. Records already applied must match local history and are
    /// skipped. Returns the last applied LSN; ACK only after it returns.
    pub fn apply_replicated_records(&mut self, records: &[MutationRecord]) -> WalResult<u64> {
        if self.wal.identity.role != "replica" {
            return Err(WalError::Identity(
                "replicated records require a replica data directory".into(),
            ));
        }
        let mut next_lsn = self.wal.next_lsn;
        let mut prev_hash = self.wal.last_record_hash;
        let mut fresh: Vec<(Mutation, &MutationRecord)> = Vec::with_capacity(records.len());
        for record in records {
            if record.lsn <= self.last_applied_lsn {
                match self.record_hash_at(record.lsn) {
                    Some(hash) if hash == record.record_hash() => continue,
                    _ => return Err(WalError::Corruption("replicated history diverged".into())),
                }
            }
            if record.lsn != next_lsn || record.prev_hash != prev_hash {
                return Err(WalError::Corruption(
                    "replicated LSN or previous hash is not contiguous".into(),
                ));
            }
            let mutation = record_to_mutation(record);
            let expected = mutation_to_record(&mutation, next_lsn, prev_hash);
            if expected.encode() != record.encode() {
                return Err(WalError::Corruption(
                    "replicated record bytes differ".into(),
                ));
            }
            next_lsn += 1;
            prev_hash = record.record_hash();
            fresh.push((mutation, record));
        }
        let mut start = 0;
        while start < fresh.len() {
            let mut end = start;
            let mut bytes = GROUP_FOOTER_LEN;
            while end < fresh.len() && end - start < MAX_GROUP_RECORDS {
                let len = fresh[end].1.encoded_len();
                if end > start && bytes + len > MAX_GROUP_BYTES {
                    break;
                }
                bytes += len;
                end += 1;
            }
            let group: Vec<Mutation> = fresh[start..end].iter().map(|(m, _)| m.clone()).collect();
            self.wal.append_group(&group)?;
            for (mutation, record) in &fresh[start..end] {
                self.engine.apply(mutation.clone());
                self.last_applied_lsn = record.lsn;
                self.retained_records.push(MutationRecord::clone(record));
            }
            start = end;
        }
        // As in `apply_durable`, a maintenance failure fails closed.
        let _ = self.maintain_lsm();
        Ok(self.last_applied_lsn)
    }

    /// Look up `key`. The LSM engine reads tables from disk, so a lookup can
    /// fail; servers use this rather than [`Db::get`].
    pub fn try_get(&self, key: &[u8]) -> WalResult<GetResult> {
        match &self.engine {
            Engine::Memory(engine) => Ok(engine.get(key)),
            Engine::Lsm(tree) => Ok(match tree.get(key)? {
                Some(value) => GetResult::Found(value),
                None => GetResult::NotFound,
            }),
        }
    }

    /// Up to `limit` pairs with `start <= key < end` in key order, and
    /// whether more remain. Sorted merge for the LSM engine; a sort of the
    /// keys in range for the in-memory map.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> WalResult<ScanPage> {
        match &self.engine {
            Engine::Memory(engine) => Ok(engine.scan(start, end, limit)),
            Engine::Lsm(tree) => Ok(tree.scan(start, end, limit)?),
        }
    }

    /// Whether `key` exists; fallible like [`Db::try_get`].
    pub fn try_exists(&self, key: &[u8]) -> WalResult<bool> {
        match &self.engine {
            Engine::Memory(engine) => Ok(engine.exists(key)),
            Engine::Lsm(tree) => Ok(tree.get(key)?.is_some()),
        }
    }

    /// Look up `key`. Panics if the LSM engine cannot read a table; use
    /// [`Db::try_get`] where that must be handled.
    pub fn get(&self, key: &[u8]) -> GetResult {
        self.try_get(key)
            .unwrap_or_else(|error| panic!("storage read failed: {error}"))
    }

    /// Whether `key` exists. Panics like [`Db::get`].
    pub fn exists(&self, key: &[u8]) -> bool {
        self.try_exists(key)
            .unwrap_or_else(|error| panic!("storage read failed: {error}"))
    }

    /// `"memory"` or `"lsm"`.
    pub fn storage_engine(&self) -> &'static str {
        match self.engine {
            Engine::Memory(_) => "memory",
            Engine::Lsm(_) => "lsm",
        }
    }

    /// LSM counters, or `None` for the in-memory engine.
    pub fn lsm_stats(&self) -> Option<LsmStats> {
        let Engine::Lsm(tree) = &self.engine else {
            return None;
        };
        Some(LsmStats {
            memtable_bytes: tree.memtable_bytes(),
            tables: tree.tables().count(),
            level_zero_tables: tree.level_zero_tables(),
            table_bytes: tree.table_bytes(),
            flushes: self.lsm_flushes,
            compactions: self.lsm_compactions,
        })
    }

    /// Start a memtable flush if the LSM memtable is full: rotate the WAL so
    /// a segment begins after the boundary, then freeze the memtable. Call
    /// between groups; the returned job is written without the database lock.
    /// Any error makes the node fail closed.
    pub fn begin_lsm_flush(&mut self) -> WalResult<Option<LsmFlush<F>>> {
        let Engine::Lsm(tree) = &mut self.engine else {
            return Ok(None);
        };
        // A group between append and apply holds bytes the WAL bookkeeping
        // does not yet cover, so the WAL cannot rotate now; try again later.
        if self.wal.failed || self.wal.group_pending || !tree.should_flush() {
            return Ok(None);
        }
        if self.wal.next_lsn != self.wal.active_first_lsn {
            if let Err(error) = self.wal.rotate() {
                self.wal.failed = true;
                return Err(error);
            }
        }
        match tree.freeze(self.last_applied_lsn, self.wal.last_record_hash) {
            Ok(job) => Ok(job.map(|job| LsmFlush { job })),
            Err(error) => {
                self.wal.failed = true;
                Err(error.into())
            }
        }
    }

    /// Install a written flush: list the table, advance the replication
    /// history base to its boundary, and delete WAL segments it covers. A
    /// replica behind the new boundary receives a table image instead of
    /// records. Any error makes the node fail closed; the WAL still holds
    /// every flushed mutation.
    pub fn finish_lsm_flush(&mut self, written: LsmResult<WrittenTable>) -> WalResult<()> {
        let Engine::Lsm(tree) = &mut self.engine else {
            return Err(WalError::Corruption("no LSM flush is in progress".into()));
        };
        if let Err(error) = written.and_then(|table| tree.install(table)) {
            self.wal.failed = true;
            return Err(error.into());
        }
        let (base, hash) = (tree.flushed_lsn(), tree.flushed_hash());
        self.lsm_flushes += 1;
        // Replication history follows the flush boundary, but stays at the
        // slowest connected replica within a bounded distance. The WAL files
        // are not needed for that: history is served from memory.
        let floor = self.replication_floor.as_ref().and_then(|floor| floor());
        let history_base = match floor {
            Some(floor) if floor < base && base - floor <= MAX_PINNED_RECORDS => {
                floor.max(self.snapshot_lsn)
            }
            _ => base,
        };
        let history_hash = self.record_hash_at(history_base).unwrap_or(hash);
        self.advance_history_base(history_base, history_hash);
        let reclaimed = self.reclaim_wal_through(base);
        if reclaimed.is_err() {
            self.wal.failed = true;
        }
        reclaimed
    }

    /// Let a primary's replication listener report the lowest LSN connected
    /// replicas still need, so LSM flushes keep that history.
    pub fn set_replication_floor(&mut self, floor: ReplicationFloor) {
        self.replication_floor = Some(floor);
    }

    /// Whether an LSM flush is due and the node is not fail-closed.
    pub fn lsm_flush_due(&self) -> bool {
        matches!(&self.engine, Engine::Lsm(tree) if tree.should_flush()) && !self.wal.failed
    }

    /// Whether an LSM compaction is due and the node is not fail-closed.
    pub fn lsm_compaction_due(&self) -> bool {
        matches!(&self.engine, Engine::Lsm(tree) if tree.should_compact()) && !self.wal.failed
    }

    /// Whether new writes should wait for the flush in progress: the
    /// memtable has grown to [`LSM_STALL_FACTOR`] times its flush size while
    /// the previous memtable is still being written. Without this, a slow
    /// disk would let memory grow without bound.
    pub fn lsm_write_stall(&self) -> bool {
        let Engine::Lsm(tree) = &self.engine else {
            return false;
        };
        !self.wal.failed
            && tree.flush_pending()
            && tree.memtable_bytes()
                >= tree
                    .config()
                    .memtable_bytes
                    .saturating_mul(LSM_STALL_FACTOR)
    }

    /// Start a compaction if level 0 has reached its trigger.
    pub fn begin_lsm_compaction(&mut self) -> Option<LsmCompaction<F>> {
        let Engine::Lsm(tree) = &mut self.engine else {
            return None;
        };
        if self.wal.failed {
            return None;
        }
        tree.plan_compaction().map(|job| LsmCompaction { job })
    }

    /// Install a written compaction. Any error makes the node fail closed.
    pub fn finish_lsm_compaction(
        &mut self,
        compacted: LsmResult<CompactedTables>,
    ) -> WalResult<()> {
        let Engine::Lsm(tree) = &mut self.engine else {
            return Err(WalError::Corruption(
                "no LSM compaction is in progress".into(),
            ));
        };
        match compacted.and_then(|tables| tree.install_compaction(tables)) {
            Ok(()) => {
                self.lsm_compactions += 1;
                Ok(())
            }
            Err(error) => {
                tree.abort_compaction();
                self.wal.failed = true;
                Err(error.into())
            }
        }
    }

    /// Run any due LSM flush and compaction synchronously. Library callers
    /// and replicas use this; the server runs the slow steps without the
    /// database lock instead.
    pub fn maintain_lsm(&mut self) -> WalResult<()> {
        if let Some(flush) = self.begin_lsm_flush()? {
            let written = flush.write();
            self.finish_lsm_flush(written)?;
        }
        if let Some(compaction) = self.begin_lsm_compaction() {
            let compacted = compaction.write();
            self.finish_lsm_compaction(compacted)?;
        }
        Ok(())
    }

    /// Drop replication history at or below `base`, which is now covered by
    /// the LSM tables.
    fn advance_history_base(&mut self, base: u64, hash: u64) {
        let covered = usize::try_from(base.saturating_sub(self.snapshot_lsn))
            .unwrap_or(usize::MAX)
            .min(self.retained_records.len());
        self.retained_records.drain(..covered);
        self.snapshot_lsn = base;
        self.snapshot_hash = hash;
    }

    /// Delete WAL segments that hold only records at or below `base`. The
    /// flush rotated the WAL at the boundary, so every segment starting at or
    /// below it ends at or below it.
    fn reclaim_wal_through(&mut self, base: u64) -> WalResult<()> {
        let mut removed = false;
        for first_lsn in Self::segment_first_lsns(&self.wal.fs, &self.wal.paths)? {
            if first_lsn <= base && first_lsn != self.wal.active_first_lsn {
                self.wal
                    .fs
                    .remove_file(&self.wal.paths.segment(first_lsn))?;
                removed = true;
            }
        }
        if removed && self.wal.mode == DurabilityMode::Fsync {
            self.wal.fs.sync_dir(&self.wal.paths.wal_dir())?;
        }
        Ok(())
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

    /// The LSN of the snapshot currently used as the recovery base, or 0 when
    /// the map was rebuilt from LSN 1 with no snapshot (Technical-Design §7,
    /// §17).
    pub fn snapshot_lsn(&self) -> u64 {
        self.snapshot_lsn
    }

    /// The number of WAL records replayed after the recovery base during the
    /// most recent [`Db::open`] (records with `lsn > snapshot_lsn`)
    /// (Technical-Design §7; SOW §22).
    ///
    /// After a snapshot at LSN *S* this is bounded by
    /// `last_applied_lsn - snapshot_lsn`: recovery loads the snapshot map and
    /// replays only the post-snapshot WAL tail, so its work no longer scales
    /// with the entire command history. Segments wholly covered by *S* are
    /// reclaimed and never read during recovery.
    pub fn records_replayed(&self) -> u64 {
        self.records_replayed
    }

    /// Time the most recent [`Db::open`] spent recovering: from acquiring the
    /// data-directory lock through loading the snapshot and replaying the WAL
    /// tail (SOW §17, §26 "recovery time").
    pub fn recovery_duration(&self) -> Duration {
        self.recovery_duration
    }

    /// Group-commit sync counters since this process opened the database.
    pub fn wal_sync_stats(&self) -> WalSyncStats {
        self.wal.sync_stats()
    }

    /// The configured bounded disk budget for retained recovery data in bytes
    /// (Technical-Design §7).
    pub fn retention_budget_bytes(&self) -> u64 {
        self.retention_budget_bytes
    }

    /// Publish a snapshot at the current completed-group boundary, following
    /// the exact §7 publication sequence, and return the snapshot's LSN *S*
    /// (Technical-Design §7).
    ///
    /// The 8 steps, in order:
    ///
    /// 1. At the completed-group boundary *S* = [`last_applied_lsn`], clone the
    ///    map pairs and capture the boundary `record_hash`.
    /// 2. Rotate the WAL so subsequent mutations begin a segment at *S+1*.
    /// 3. Encode and write the snapshot bytes to a temp file in `tmp/` on the
    ///    same filesystem.
    /// 4. Fully write and `sync_file` the temp snapshot.
    /// 5. Rename it to its immutable final `{S:020}.snap` name in `snapshots/`.
    /// 6. `sync_dir` the `snapshots/` directory.
    /// 7. Reload and VERIFY the new snapshot from disk (decode + header /
    ///    cluster / `entry_count` / `payload_len` / checksums, plus a full
    ///    reference-map comparison) BEFORE trusting it.
    /// 8. Only then mark *S* eligible and delete WAL segments wholly covered by
    ///    *S* (subject to the previous-chain retention rule and the disk
    ///    budget), then `sync_dir` the `wal/` directory.
    ///
    /// On failure at ANY step the old snapshot and old WAL are left untouched
    /// and an error is returned (fail-closed, never a gap). If retaining the
    /// new snapshot plus the WAL still needed for recovery would exceed the
    /// configured disk budget, no recovery data is deleted and
    /// [`WalError::ResourceExhausted`] is returned so the server pauses writes.
    ///
    /// [`last_applied_lsn`]: Db::last_applied_lsn
    pub fn publish_snapshot(&mut self) -> WalResult<u64> {
        let Engine::Memory(engine) = &self.engine else {
            return Err(WalError::Identity(
                "the LSM engine checkpoints by flushing tables, not by snapshots".into(),
            ));
        };
        let pairs = engine.snapshot_pairs();
        self.wal.ensure_idle()?;
        if self.wal.is_failed() {
            return Err(WalError::FailClosed);
        }

        // (1) Clone the map pairs and capture the boundary hash at S.
        let s = self.last_applied_lsn;
        let record_hash_at_lsn = self.wal.last_record_hash;
        let cluster_id = self.wal.identity.cluster_id;
        let mode = self.wal.mode;

        // The previous verified snapshot (if any) is retained until the new one
        // passes reload verification (previous-chain rule, §7).
        let previous_snapshot_lsn = self.snapshot_lsn;

        // (2) Rotate the WAL so later mutations begin a segment at S+1.
        // Rotation happens between groups only; this is a completed-group
        // boundary. If a segment beginning at S+1 already exists (no writes
        // since the last rotate), skip the rotate to avoid a duplicate.
        if self.wal.next_lsn != self.wal.active_first_lsn {
            self.wal.rotate_after(s)?;
        }

        // (3)+(4) Encode and durably write the snapshot to a temp file.
        let bytes = snapshot::encode(cluster_id, s, record_hash_at_lsn, &pairs);
        let tmp = self.wal.paths.snapshot_tmp(s);
        let final_path = self.wal.paths.snapshot(s);
        // Start from a clean temp file (a stale temp from a prior aborted
        // publish must not corrupt this one).
        if self.wal.fs.exists(&tmp) {
            self.wal.fs.remove_file(&tmp)?;
            if mode == DurabilityMode::Fsync {
                self.wal.fs.sync_dir(&self.wal.paths.tmp())?;
            }
        }
        self.wal.fs.create_file(&tmp)?;
        self.wal.fs.append(&tmp, &bytes)?;
        if mode == DurabilityMode::Fsync {
            self.wal.fs.sync_file(&tmp)?;
        }

        // (5)+(6) Atomic rename into snapshots/ then sync the directory.
        self.wal.fs.rename(&tmp, &final_path)?;
        if mode == DurabilityMode::Fsync {
            self.wal.fs.sync_dir(&self.wal.paths.snapshots_dir())?;
        }

        // (7) Reload and verify the new snapshot from disk before trusting it.
        let reloaded = self.wal.fs.read(&final_path)?;
        let decoded = snapshot::decode(&reloaded, Some(cluster_id)).map_err(|e| {
            WalError::Corruption(format!("snapshot reload verification failed: {e}"))
        })?;
        if decoded.header.snapshot_lsn != s
            || decoded.header.record_hash_at_lsn != record_hash_at_lsn
            || decoded.header.entry_count != pairs.len() as u64
        {
            return Err(WalError::Corruption(
                "snapshot reload verification: header does not match published boundary".into(),
            ));
        }
        // Full reference-map comparison (§7): the reloaded pairs must equal the
        // cloned pairs as a set (iteration order is unspecified).
        if !pairs_equal_as_set(&decoded.pairs, &pairs) {
            return Err(WalError::Corruption(
                "snapshot reload verification: reloaded pairs differ from reference map".into(),
            ));
        }

        // (8) Plan reclamation of the WAL segments wholly covered by S and of
        // the previous snapshot's now-reclaimable recovery chain, then enforce
        // the disk budget. The budget check happens BEFORE the in-memory base
        // moves so a `ResourceExhausted` return leaves `snapshot_lsn` unchanged
        // (the return value and observable state agree, §7).
        let plan = self.plan_reclamation(s, previous_snapshot_lsn)?;
        if plan.retained_bytes > self.retention_budget_bytes {
            return Err(WalError::ResourceExhausted {
                budget_bytes: self.retention_budget_bytes,
                needed_bytes: plan.retained_bytes,
            });
        }

        // The budget allows the new snapshot: it is now the verified recovery
        // base. Advance the in-memory base only after the check passes.
        self.snapshot_lsn = s;
        self.snapshot_hash = record_hash_at_lsn;
        self.retained_records.clear();

        // Execute reclamation: delete the covered WAL and the previous
        // snapshot, then sync the affected directories (delete-then-sync keeps
        // it crash-safe; a crash before the dir sync restores the files and the
        // next publish re-derives the same set, so no recovery gap opens, §7).
        self.execute_reclamation(&plan)?;

        Ok(s)
    }

    /// Plan the reclamation for a freshly verified snapshot at `s`: which WAL
    /// segments are wholly covered by `s`, whether the previous snapshot's
    /// recovery chain is now reclaimable, and the retained recovery bytes that
    /// would remain AFTER executing the plan (Technical-Design §7).
    ///
    /// A segment is "wholly covered" when the next segment's `first_lsn` is
    /// `<= s+1` (so every record in it is `<= s`). The current chain begins at
    /// the segment starting at `s+1`; it and all later segments are never
    /// touched.
    ///
    /// The previous snapshot at `previous_snapshot_lsn` (if any) was the prior
    /// recovery base; §7 permits reclaiming the older recovery chain once the
    /// new snapshot has passed reload verification, which has happened by the
    /// time this runs. It is therefore scheduled for deletion and is NOT
    /// counted in the retained-bytes total.
    fn plan_reclamation(&self, s: u64, previous_snapshot_lsn: u64) -> WalResult<ReclamationPlan> {
        // Enumerate current segments by first LSN.
        let mut segment_lsns: Vec<u64> = Vec::new();
        for name in self.wal.fs.list_dir(&self.wal.paths.wal_dir())? {
            if let Some(stem) = name.strip_suffix(".wal") {
                if let Ok(first_lsn) = stem.parse::<u64>() {
                    segment_lsns.push(first_lsn);
                }
            }
        }
        segment_lsns.sort_unstable();

        // A segment at index i is wholly covered by S when the NEXT segment
        // begins at or before S+1 (so this segment's records are all <= S).
        let mut deletable_segments: Vec<u64> = Vec::new();
        for i in 0..segment_lsns.len() {
            let first = segment_lsns[i];
            if let Some(next_first) = segment_lsns.get(i + 1).copied() {
                if next_first <= s + 1 {
                    deletable_segments.push(first);
                }
            }
        }

        // The previous snapshot's file is reclaimable now (the new snapshot
        // verified). Guard against reclaiming the current snapshot (S) or a
        // zero "no previous snapshot" sentinel.
        let deletable_previous_snapshot =
            if previous_snapshot_lsn != 0 && previous_snapshot_lsn != s {
                Some(previous_snapshot_lsn)
            } else {
                None
            };

        // Retained recovery bytes AFTER executing the plan: the new snapshot
        // file plus every WAL segment we keep. The previous snapshot and the
        // covered WAL segments are being reclaimed, so they are excluded.
        let mut retained_bytes: u64 = 0;
        if let Ok(bytes) = self.wal.fs.read(&self.wal.paths.snapshot(s)) {
            retained_bytes += bytes.len() as u64;
        }
        for &first in &segment_lsns {
            if deletable_segments.contains(&first) {
                continue;
            }
            if let Ok(bytes) = self.wal.fs.read(&self.wal.paths.segment(first)) {
                retained_bytes += bytes.len() as u64;
            }
        }

        Ok(ReclamationPlan {
            deletable_segments,
            deletable_previous_snapshot,
            retained_bytes,
        })
    }

    /// Execute a [`ReclamationPlan`]: delete the covered WAL segments and the
    /// previous snapshot file, then `sync_dir` each affected directory in
    /// `fsync` mode (Technical-Design §7).
    ///
    /// Delete-then-sync keeps reclamation crash-safe: a crash after the deletes
    /// but before the directory sync leaves the removals volatile (see the
    /// `SimFs` delete-durability model), so recovery restores the files and the
    /// next publish re-derives the same reclaimable set. Deleting never opens a
    /// recovery gap because the retained chain from `s+1` is untouched, and the
    /// previous snapshot is removed only after the new snapshot has both
    /// verified and been recorded as the recovery base.
    fn execute_reclamation(&mut self, plan: &ReclamationPlan) -> WalResult<()> {
        let mode = self.wal.mode;

        for first in &plan.deletable_segments {
            self.wal.fs.remove_file(&self.wal.paths.segment(*first))?;
        }
        if mode == DurabilityMode::Fsync && !plan.deletable_segments.is_empty() {
            self.wal.fs.sync_dir(&self.wal.paths.wal_dir())?;
        }

        if let Some(prev) = plan.deletable_previous_snapshot {
            let prev_path = self.wal.paths.snapshot(prev);
            if self.wal.fs.exists(&prev_path) {
                self.wal.fs.remove_file(&prev_path)?;
                if mode == DurabilityMode::Fsync {
                    self.wal.fs.sync_dir(&self.wal.paths.snapshots_dir())?;
                }
            }
        }
        Ok(())
    }

    /// Number of keys currently stored. For the LSM engine this merges every
    /// table, so it panics on a read failure and costs a full scan; `STATS`
    /// uses [`Db::approximate_len`].
    pub fn len(&self) -> usize {
        match &self.engine {
            Engine::Memory(engine) => engine.len(),
            Engine::Lsm(tree) => tree
                .live_pairs()
                .unwrap_or_else(|error| panic!("storage read failed: {error}"))
                .len(),
        }
    }

    /// Whether the engine holds no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The exact key count for the in-memory map; for the LSM engine an upper
    /// bound that counts versions and tombstones not yet compacted away.
    pub fn approximate_len(&self) -> u64 {
        match &self.engine {
            Engine::Memory(engine) => engine.len() as u64,
            Engine::Lsm(tree) => tree.approx_key_count(),
        }
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

    fn generation_dir(id: u64) -> PathBuf {
        root().join("generations").join(format!("{id:016x}"))
    }

    #[test]
    fn open_removes_generations_left_by_interrupted_install() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        }
        // An install that stopped before publishing CURRENT leaves a complete
        // generation 2 and a partial generation 3 that recovery never reads.
        let segment = generation_dir(2)
            .join("wal")
            .join("00000000000000000002.wal");
        fs.create_dir_all(segment.parent().unwrap()).unwrap();
        fs.create_file(&segment).unwrap();
        fs.append(&segment, b"unpublished").unwrap();
        fs.create_dir_all(&generation_dir(3).join("snapshots"))
            .unwrap();

        let db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.generation_id(), 1);
        assert_eq!(db.generations_removed(), 2);
        assert!(!fs.exists(&generation_dir(2)));
        assert!(!fs.exists(&generation_dir(3)));
        assert_eq!(db.get(b"k"), GetResult::Found(b"v".to_vec()));
        drop(db);

        // The removal was synced: a crash does not bring the directories back.
        fs.crash();
        let db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.generations_removed(), 0);
        assert!(!fs.exists(&generation_dir(2)));
        assert_eq!(db.get(b"k"), GetResult::Found(b"v".to_vec()));
    }

    #[test]
    fn unexpected_content_in_stale_generation_does_not_block_open() {
        let fs = sim();
        drop(Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap());
        let stray = generation_dir(2).join("operator-notes").join("keep.txt");
        fs.create_dir_all(stray.parent().unwrap()).unwrap();
        fs.create_file(&stray).unwrap();

        let db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.generation_id(), 1);
        assert_eq!(db.generations_removed(), 0);
        assert!(fs.exists(&stray));
    }

    fn set_mutation(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn pending_group_is_invisible_and_blocks_other_writers() {
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"k".to_vec(), b"old".to_vec()).unwrap();
        let muts = [set_mutation(b"k", b"new"), set_mutation(b"j", b"1")];
        let pending = db.begin_group(&muts).unwrap();

        // Appended but not synced or applied: readers see acknowledged state.
        assert_eq!(db.get(b"k"), GetResult::Found(b"old".to_vec()));
        assert_eq!(db.last_applied_lsn(), 1);
        assert_eq!(db.last_durable_lsn(), 1);
        // Nothing else may touch the WAL until the group finishes.
        assert!(matches!(
            db.begin_group(&muts),
            Err(WalError::GroupInProgress)
        ));
        assert!(matches!(
            db.set(b"x".to_vec(), b"y".to_vec()),
            Err(WalError::GroupInProgress)
        ));
        assert!(matches!(
            db.publish_snapshot(),
            Err(WalError::GroupInProgress)
        ));

        let synced = pending.sync();
        assert_eq!(db.finish_group(pending, &muts, synced).unwrap(), vec![2, 3]);
        assert_eq!(db.get(b"k"), GetResult::Found(b"new".to_vec()));
        assert_eq!(db.last_durable_lsn(), 3);
        assert_eq!(db.wal_sync_stats().syncs, 2);
        // The writer is idle again, and the group recovers after a crash.
        db.set(b"x".to_vec(), b"y".to_vec()).unwrap();
        drop(db);
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.last_applied_lsn(), 4);
        assert_eq!(db.get(b"j"), GetResult::Found(b"1".to_vec()));
    }

    #[test]
    fn failed_group_sync_fails_closed_without_applying() {
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        let muts = [set_mutation(b"k", b"v")];
        let pending = db.begin_group(&muts).unwrap();
        let failed = Err(FsError::InjectedFault("sync failed".into()));
        assert!(matches!(
            db.finish_group(pending, &muts, failed),
            Err(WalError::Io(_))
        ));
        assert_eq!(db.get(b"k"), GetResult::NotFound);
        assert_eq!(db.last_applied_lsn(), 0);
        assert_eq!(db.wal_sync_stats().errors, 1);
        assert!(matches!(
            db.set(b"k".to_vec(), b"v".to_vec()),
            Err(WalError::FailClosed)
        ));
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
    fn current_pointer_is_checked_before_recovery() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
            assert_eq!(db.generation_id(), 1);
        }
        let current = DataPaths::new(&root()).current();
        let mut bytes = fs.read(&current).unwrap();
        assert_eq!(bytes.len(), current::CURRENT_LEN);
        bytes[16] ^= 1;
        fs.truncate(&current, 0).unwrap();
        fs.append(&current, &bytes).unwrap();
        fs.sync_file(&current).unwrap();
        fs.crash();
        assert!(matches!(
            Db::open(fs, &root(), DurabilityMode::Fsync),
            Err(WalError::Identity(_))
        ));
    }

    #[test]
    fn legacy_pointer_upgrades_without_losing_data() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        }
        let current = DataPaths::new(&root()).current();
        fs.truncate(&current, 0).unwrap();
        fs.append(&current, b"0000000000000001").unwrap();
        fs.sync_file(&current).unwrap();
        fs.crash();
        let db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(
            db.get(b"k"),
            crate::storage::GetResult::Found(b"v".to_vec())
        );
        assert_eq!(
            Current::decode(&fs.read(&current).unwrap()),
            Ok(Current { generation: 1 })
        );
    }

    #[test]
    fn replica_identity_is_persisted_and_validated() {
        let fs = sim();
        let cluster = [7u8; 16];
        let config = OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        };
        {
            let mut db =
                Db::open_configured(fs.clone(), &root(), DurabilityMode::Fsync, config).unwrap();
            assert_eq!(db.identity().cluster_id, cluster);
            assert_eq!(db.identity().role, "replica");
            assert!(matches!(
                db.set(b"k".to_vec(), b"v".to_vec()),
                Err(WalError::ReadOnlyReplica)
            ));
        }
        assert!(Db::open_configured(fs.clone(), &root(), DurabilityMode::Fsync, config).is_ok());
        assert!(matches!(
            Db::open(fs.clone(), &root(), DurabilityMode::Fsync),
            Err(WalError::Identity(_))
        ));
        let wrong_cluster = OpenConfig {
            cluster_id: Some([8u8; 16]),
            ..config
        };
        assert!(matches!(
            Db::open_configured(fs, &root(), DurabilityMode::Fsync, wrong_cluster),
            Err(WalError::Identity(_))
        ));
    }

    #[test]
    fn snapshot_download_spools_chunks_and_discards_interrupted_transfer() {
        let fs = sim();
        let cluster = [7u8; 16];
        let db = Db::open_configured(
            fs.clone(),
            &root(),
            DurabilityMode::Fsync,
            OpenConfig {
                role: NodeRole::Replica,
                cluster_id: Some(cluster),
                allow_snapshot_rebootstrap: true,
                ..OpenConfig::default()
            },
        )
        .unwrap();
        let bytes = snapshot::encode(cluster, 1, 42, &[(b"k".to_vec(), b"v".to_vec())]);
        let crc = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().unwrap());
        let path = root().join("tmp/replica-snapshot-download.tmp");

        {
            let mut download = db.begin_snapshot_download(bytes.len()).unwrap();
            download.append(0, &bytes[..5]).unwrap();
            assert_eq!(download.received(), 5);
            assert!(fs.exists(&path));
            assert!(download.append(3, &bytes[5..6]).is_err());
        }
        assert!(!fs.exists(&path));

        let mut download = db.begin_snapshot_download(bytes.len()).unwrap();
        for chunk in bytes.chunks(7) {
            download.append(download.received() as u64, chunk).unwrap();
        }
        assert_eq!(download.finish(crc).unwrap(), bytes);
        assert!(!fs.exists(&path));

        let mut download = db.begin_snapshot_download(bytes.len()).unwrap();
        download.append(0, &bytes).unwrap();
        assert!(matches!(
            download.finish(crc ^ 1),
            Err(WalError::Corruption(_))
        ));
        assert!(!fs.exists(&path));
    }

    #[test]
    fn replica_applies_a_run_of_records_as_local_groups() {
        let cluster = [7u8; 16];
        let mut primary = Db::open_configured(
            sim(),
            Path::new("/primary"),
            DurabilityMode::Fsync,
            OpenConfig {
                cluster_id: Some(cluster),
                ..OpenConfig::default()
            },
        )
        .unwrap();
        for index in 0..100u32 {
            primary
                .set(index.to_le_bytes().to_vec(), vec![index as u8])
                .unwrap();
        }
        let records = primary.durable_records_after(0, 200).unwrap();
        assert_eq!(records.len(), 100);
        let replica_config = OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        };

        // A gap anywhere in the run rejects the whole run before any write.
        let mut gapped = Db::open_configured(
            sim(),
            Path::new("/gapped"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap();
        let gap = [records[0].clone(), records[2].clone()];
        assert!(matches!(
            gapped.apply_replicated_records(&gap),
            Err(WalError::Corruption(_))
        ));
        assert_eq!(gapped.last_applied_lsn(), 0);
        assert_eq!(gapped.wal_sync_stats().syncs, 0);

        let replica_fs = sim();
        let mut replica = Db::open_configured(
            replica_fs.clone(),
            Path::new("/replica"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap();
        assert_eq!(replica.apply_replicated_records(&records[..1]).unwrap(), 1);
        // The already-applied first record is skipped; the other 99 become
        // groups of 64 and 35 records, one sync each.
        assert_eq!(replica.apply_replicated_records(&records).unwrap(), 100);
        assert_eq!(replica.wal_sync_stats().syncs, 3);
        assert_eq!(replica.record_hash_at(100), primary.record_hash_at(100));
        assert_eq!(
            replica.get(&99u32.to_le_bytes()),
            crate::storage::GetResult::Found(vec![99])
        );
        drop(replica);
        replica_fs.crash();
        let reopened = Db::open_configured(
            replica_fs,
            Path::new("/replica"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap();
        assert_eq!(reopened.last_applied_lsn(), 100);
        assert_eq!(reopened.record_hash_at(100), primary.record_hash_at(100));
    }

    #[test]
    fn replica_applies_only_contiguous_primary_history() {
        let fs = sim();
        let replica_fs = sim();
        let cluster = [9u8; 16];
        let primary_config = OpenConfig {
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        };
        let mut primary = Db::open_configured(
            fs.clone(),
            Path::new("/primary"),
            DurabilityMode::Fsync,
            primary_config,
        )
        .unwrap();
        primary.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        primary.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        let records = primary.durable_records_after(0, 64).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(primary.record_hash_at(2), Some(records[1].record_hash()));

        let replica_config = OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster),
            ..OpenConfig::default()
        };
        let mut replica = Db::open_configured(
            replica_fs.clone(),
            Path::new("/replica"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap();
        assert!(replica.apply_replicated_record(&records[1]).is_err());
        assert_eq!(replica.apply_replicated_record(&records[0]).unwrap(), 1);
        assert_eq!(replica.apply_replicated_record(&records[0]).unwrap(), 1);
        assert_eq!(replica.apply_replicated_record(&records[1]).unwrap(), 2);
        assert_eq!(
            replica.get(b"b"),
            crate::storage::GetResult::Found(b"2".to_vec())
        );
        assert_eq!(replica.record_hash_at(2), primary.record_hash_at(2));
        drop(replica);
        drop(primary);
        replica_fs.crash();
        let replica = Db::open_configured(
            replica_fs.clone(),
            Path::new("/replica"),
            DurabilityMode::Fsync,
            replica_config,
        )
        .unwrap();
        assert_eq!(replica.last_applied_lsn(), 2);
        assert_eq!(
            replica.get(b"a"),
            crate::storage::GetResult::Found(b"1".to_vec())
        );
    }

    #[test]
    fn snapshot_moves_replication_history_boundary() {
        let fs = sim();
        let mut db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        let hash_at_one = db.record_hash_at(1);
        db.publish_snapshot().unwrap();
        assert!(db.durable_records_after(0, 64).is_none());
        assert_eq!(db.record_hash_at(1), hash_at_one);
        assert_eq!(db.durable_records_after(1, 64).unwrap().len(), 0);
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        assert_eq!(db.durable_records_after(1, 64).unwrap().len(), 1);
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

    // ---- Snapshot publication + snapshot-aware recovery (§7) -------------

    #[test]
    fn publish_snapshot_rotates_wal_to_s_plus_one() {
        let fs = sim();
        let mut db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        // Snapshot at S = 2.
        let s = db.publish_snapshot().unwrap();
        assert_eq!(s, 2);
        assert_eq!(db.snapshot_lsn(), 2);
        // A new segment beginning at S+1 = 3 exists; later writes land there.
        assert!(db.wal.fs.exists(&db.wal.paths.segment(3)));
        assert_eq!(db.set(b"c".to_vec(), b"3".to_vec()).unwrap(), 3);
    }

    #[test]
    fn reopen_reconstructs_from_snapshot_plus_post_snapshot_wal() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"A".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"B".to_vec(), b"2".to_vec()).unwrap();
            db.publish_snapshot().unwrap(); // S = 2
                                            // Post-snapshot mutations continue at LSN 3+.
            db.set(b"C".to_vec(), b"3".to_vec()).unwrap();
            db.delete(b"A".to_vec()).unwrap();
        }
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        // Recovery base is the snapshot at S = 2; only lsn > 2 was replayed.
        assert_eq!(db.snapshot_lsn(), 2);
        assert_eq!(db.get(b"A"), GetResult::NotFound); // deleted post-snapshot
        assert_eq!(db.get(b"B"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.get(b"C"), GetResult::Found(b"3".to_vec()));
        assert_eq!(db.last_applied_lsn(), 4);
        // Writing continues from LSN 5, chaining from the recovered hash.
        let mut db = db;
        assert_eq!(db.set(b"D".to_vec(), b"4".to_vec()).unwrap(), 5);
    }

    #[test]
    fn snapshot_with_no_post_snapshot_writes_reopens_identically() {
        let fs = sim();
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"x".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"y".to_vec(), b"2".to_vec()).unwrap();
            db.publish_snapshot().unwrap(); // S = 2, no writes after
        }
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.snapshot_lsn(), 2);
        assert_eq!(db.last_applied_lsn(), 2);
        assert_eq!(db.get(b"x"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"y"), GetResult::Found(b"2".to_vec()));
    }

    #[test]
    fn second_snapshot_reclaims_wal_covered_by_first() {
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        db.publish_snapshot().unwrap(); // S1 = 2; segments 1 (<=2) and 3 (active)
        let seg1 = db.wal.paths.segment(1);
        // Segment 1 is wholly covered by S1 but retained (it is the base's
        // covered log); reclamation only happens when a newer snapshot's chain
        // no longer needs it. After S1, the current chain begins at seg 3.
        db.set(b"c".to_vec(), b"3".to_vec()).unwrap(); // lsn 3 in seg 3
        let snap1 = db.wal.paths.snapshot(2);
        db.publish_snapshot().unwrap(); // S2 = 3; rotates to seg 4
        let seg3 = db.wal.paths.segment(3);
        // After S2, segments wholly covered by S2 (segs 1 and 3) are deleted;
        // the current chain begins at seg 4.
        assert!(!db.wal.fs.exists(&seg1), "seg1 should be reclaimed");
        assert!(!db.wal.fs.exists(&seg3), "seg3 should be reclaimed");
        assert!(db.wal.fs.exists(&db.wal.paths.segment(4)));
        // The previous snapshot's recovery chain is reclaimed once the newer
        // snapshot has verified (§7 previous-chain rule): S1's file is gone,
        // only S2's remains.
        assert!(
            !db.wal.fs.exists(&snap1),
            "previous snapshot S1 should be reclaimed after S2 verifies"
        );
        assert!(db.wal.fs.exists(&db.wal.paths.snapshot(3)));
        // The snapshot at S2 remains, and recovery still works after a crash.
        drop(db);
        fs.crash();
        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        assert_eq!(db.snapshot_lsn(), 3);
        assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.get(b"c"), GetResult::Found(b"3".to_vec()));
        assert_eq!(db.last_applied_lsn(), 3);
    }

    #[test]
    fn disk_budget_exhaustion_returns_resource_exhausted_and_keeps_data() {
        // A tiny budget: publishing would require retaining more than the
        // budget, so publication must NOT delete recovery data and must return
        // the RESOURCE_EXHAUSTED-mapped error.
        let fs = sim();
        let mut db = Db::open_with_budget(fs.clone(), &root(), DurabilityMode::Fsync, 8).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        let seg1 = db.wal.paths.segment(1);
        let err = db.publish_snapshot();
        assert!(
            matches!(err, Err(WalError::ResourceExhausted { .. })),
            "expected ResourceExhausted, got {err:?}"
        );
        // Recovery data was NOT deleted: segment 1 still present.
        assert!(db.wal.fs.exists(&seg1));
        // The snapshot file itself was written and verified before reclamation
        // (the snapshot is durable; only the reclamation step paused).
        assert!(db.wal.fs.exists(&db.wal.paths.snapshot(2)));
        // The in-memory recovery base did NOT advance on the ResourceExhausted
        // path: the return value and observable state agree (§7). snapshot_lsn
        // stays 0 because nothing was reclaimed.
        assert_eq!(db.snapshot_lsn(), 0);
    }

    #[test]
    fn previous_snapshot_reclamation_frees_budget_accounting() {
        // After a second verified snapshot the previous snapshot file is gone
        // and the retained-bytes accounting reflects it: the retained total is
        // the new snapshot plus the retained WAL only, not the old snapshot.
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        db.publish_snapshot().unwrap(); // S1 = 2
        db.set(b"c".to_vec(), b"3".to_vec()).unwrap();
        db.publish_snapshot().unwrap(); // S2 = 3; reclaims S1's chain

        // The old snapshot file is gone.
        assert!(!db.wal.fs.exists(&db.wal.paths.snapshot(2)));

        // Recompute the reclamation plan from the current on-disk state (a
        // no-op replan against S2 with no previous snapshot): its retained
        // bytes must exclude the reclaimed old snapshot and equal the sum of
        // the new snapshot plus the retained WAL segments actually on disk.
        let plan = db.plan_reclamation(db.snapshot_lsn(), 0).unwrap();
        let snap_bytes = db.wal.fs.read(&db.wal.paths.snapshot(3)).unwrap().len() as u64;
        let mut expected = snap_bytes;
        for name in db.wal.fs.list_dir(&db.wal.paths.wal_dir()).unwrap() {
            if let Some(stem) = name.strip_suffix(".wal") {
                if let Ok(first) = stem.parse::<u64>() {
                    if !plan.deletable_segments.contains(&first) {
                        expected +=
                            db.wal.fs.read(&db.wal.paths.segment(first)).unwrap().len() as u64;
                    }
                }
            }
        }
        assert_eq!(plan.retained_bytes, expected);
    }

    #[test]
    fn corrupt_latest_snapshot_without_chain_fails_closed() {
        let fs = sim();
        let snap_path = {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
            db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
            db.publish_snapshot().unwrap(); // S = 2; seg 1 (<=2) retained, seg 3 active
                                            // Reclaim seg 1 by a second snapshot so no LSN-1 chain remains.
            db.set(b"c".to_vec(), b"3".to_vec()).unwrap();
            db.publish_snapshot().unwrap(); // S = 3; deletes segs 1 and 3
            db.wal.paths.snapshot(3)
        };
        // Corrupt the LATEST snapshot's stable bytes (post-sync corruption,
        // the deliberately-violating §6.3-style test).
        let mut bytes = fs.read(&snap_path).unwrap();
        bytes[70] ^= 0xFF; // flip a payload byte
        fs.truncate(&snap_path, 0).unwrap();
        fs.append(&snap_path, &bytes).unwrap();
        fs.sync_file(&snap_path).unwrap();
        fs.crash();
        // The latest snapshot is corrupt and older WAL was deleted, so no
        // complete chain remains: recovery must fail closed, not start empty.
        let err = Db::open(fs, &root(), DurabilityMode::Fsync);
        assert!(
            matches!(err, Err(WalError::Corruption(_))),
            "expected Corruption on corrupt latest snapshot with no chain"
        );
    }

    #[test]
    fn corrupt_higher_snapshot_falls_back_to_lower_verified_chain() {
        // Recovery selects the HIGHEST verified snapshot with a contiguous WAL
        // tail. If a higher snapshot exists on disk but is corrupt, and a lower
        // verified snapshot plus its contiguous WAL tail still forms a complete
        // chain, recovery uses the lower snapshot (§7, §9.3). Here S1 is a real
        // verified snapshot whose WAL tail (seg 2, LSN 2) is intact; a corrupt
        // higher snapshot file at LSN 3 is placed on disk and must be skipped.
        let fs = sim();
        let (snap1_bytes, cluster) = {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
            db.publish_snapshot().unwrap(); // S1 = 1; reclaims seg 1, seg 2 active
            db.set(b"b".to_vec(), b"2".to_vec()).unwrap(); // lsn 2 in seg 2
            let snap1 = db.wal.paths.snapshot(1);
            (fs.read(&snap1).unwrap(), db.wal.identity.cluster_id)
        };
        assert!(!snap1_bytes.is_empty());
        // Fabricate a corrupt HIGHER snapshot at LSN 3 (valid header framing so
        // it is enumerated, but a flipped payload/crc byte so decode fails).
        let mut higher =
            crate::wal::snapshot::encode(cluster, 3, 12345, &[(b"z".to_vec(), b"9".to_vec())]);
        let last = higher.len() - 1;
        higher[last] ^= 0xFF; // break the trailing snapshot_crc64
        let snap3 = DataPaths::new(&root()).snapshot(3);
        fs.create_file(&snap3).unwrap();
        fs.append(&snap3, &higher).unwrap();
        fs.sync_file(&snap3).unwrap();
        fs.sync_dir(&DataPaths::new(&root()).snapshots_dir())
            .unwrap();
        fs.crash();

        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        // The corrupt LSN-3 snapshot is skipped; recovery falls back to the
        // verified S1 = 1 plus its contiguous WAL tail (seg 2, LSN 2).
        assert_eq!(db.snapshot_lsn(), 1);
        assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.last_applied_lsn(), 2);
    }

    #[test]
    fn recovery_repairs_empty_trailing_segment_from_failed_rotate() {
        // A crash during a rotate (e.g. snapshot-publication step 2) can leave
        // the NEW segment file present but empty on the stable image because
        // its header sync failed. The earlier SEALED segment holds all
        // committed records. Recovery must treat the empty trailing segment as
        // a torn active segment (§6.3), repair its header, and recover the
        // acknowledged writes rather than failing closed. (Regression guard for
        // the FEAT-004 crash-safety suite.)
        let fs = sim();
        let seg2;
        {
            let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
            db.set(b"a".to_vec(), b"1".to_vec()).unwrap(); // lsn 1, seg 1
            db.set(b"b".to_vec(), b"2".to_vec()).unwrap(); // lsn 2, seg 1
            db.rotate().unwrap(); // seal seg 1, open seg 3 (first_lsn = 3)
            seg2 = db.wal.paths.segment(3);
        }
        // Simulate the failed-rotate remnant: the trailing segment exists but
        // its header never became durable (a 0-byte file on the stable image).
        fs.truncate(&seg2, 0).unwrap();
        fs.sync_file(&seg2).unwrap();
        fs.crash();

        let db = Db::open(fs, &root(), DurabilityMode::Fsync).unwrap();
        // All acknowledged writes are recovered from the sealed segment 1.
        assert_eq!(db.get(b"a"), GetResult::Found(b"1".to_vec()));
        assert_eq!(db.get(b"b"), GetResult::Found(b"2".to_vec()));
        assert_eq!(db.last_applied_lsn(), 2);
        assert!(db.tail_truncated());
        // The repaired active segment accepts a new write that continues the
        // LSN sequence.
        let mut db = db;
        let lsn = db.set(b"c".to_vec(), b"3".to_vec()).unwrap();
        assert_eq!(lsn, 3);
        assert_eq!(db.get(b"c"), GetResult::Found(b"3".to_vec()));
    }

    #[test]
    fn publish_snapshot_failure_leaves_old_state_intact() {
        // Arm sync failures so the temp-snapshot sync (step 4) fails; the old
        // WAL and (absent) snapshot must be left intact and an error returned.
        let fs = sim();
        let mut db = Db::open(fs.clone(), &root(), DurabilityMode::Fsync).unwrap();
        db.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        fs.arm_sync_failures(1000);
        let err = db.publish_snapshot();
        assert!(err.is_err(), "publish should fail when a sync fails");
        fs.arm_sync_failures(0);
        // No snapshot was published; snapshot_lsn stays 0 and segment 1 (the
        // pre-publish WAL) is intact.
        assert_eq!(db.snapshot_lsn(), 0);
        assert!(db.wal.fs.exists(&db.wal.paths.segment(1)));
    }
}
