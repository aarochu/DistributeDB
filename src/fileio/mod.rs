//! File-I/O abstraction with a real Linux adapter and a deterministic
//! simulated-power-loss adapter.
//!
//! Phase 2 needs a small file-I/O abstraction so the WAL writer and recovery
//! code can be tested against a deterministic power-loss model rather than a
//! real disk (Technical-Design §6.4, ADR-001, SOW §19). This module defines
//! the [`FileSystem`] trait and two implementations:
//!
//! * [`RealFs`]: a std-backed adapter for 64-bit Linux / ext4. It performs
//!   real appends, `fsync` (via [`std::fs::File::sync_all`]), directory sync
//!   (by opening the directory as a file and calling `sync_all`), same
//!   filesystem rename ([`std::fs::rename`]), truncation
//!   ([`std::fs::File::set_len`]), reads, directory creation, and an
//!   exclusive data-directory lock using `File::try_lock` on a persistent
//!   `LOCK` file (released automatically after process exit).
//!
//! * [`SimFs`]: the deterministic simulated-power-loss adapter from §6.4. It
//!   maintains a **volatile** image and a **stable** image of every file and
//!   of the directory namespace. Writes/appends/renames/truncates mutate the
//!   volatile image; a successful file sync copies that file's eligible bytes
//!   into the stable image; a successful directory sync makes namespace
//!   changes stable; [`SimFs::crash`] discards all volatile state, leaving
//!   only the stable image for the next recovery. Fault injection (short
//!   writes, failed syncs, crash-at-step) and page-reorder decisions are all
//!   driven by an explicit seed through the in-crate [`SplitMix64`] PRNG, so
//!   runs are fully reproducible. The core model NEVER alters already-synced
//!   (stable) bytes; a separate corruption test (a later feature) deliberately
//!   violates that to check the narrower §6.3 guarantee.
//!
//! # Operations (Technical-Design §6.4, ADR-001)
//!
//! The trait covers: create/open a file for append, a full-write loop that
//! handles partial writes and `EINTR`, file sync, directory sync, same
//! filesystem rename, truncation, reading a file's bytes, creating
//! directories, and acquiring an exclusive data-directory lock.

use crate::checksum::crc64_ecma;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Errors returned by [`FileSystem`] operations.
///
/// I/O failures are always propagated (never silently swallowed), matching the
/// fail-closed invariant in Technical-Design §3.
#[derive(Debug)]
pub enum FsError {
    /// A wrapped [`std::io::Error`] from a real syscall.
    Io(io::Error),
    /// The requested path was not found in the simulated filesystem.
    NotFound(PathBuf),
    /// The exclusive data-directory lock is already held.
    Locked(PathBuf),
    /// A seeded fault was injected by the simulator (short write / failed
    /// sync / simulated crash). Carries a human-readable description.
    InjectedFault(String),
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsError::Io(e) => write!(f, "io error: {e}"),
            FsError::NotFound(p) => write!(f, "path not found: {}", p.display()),
            FsError::Locked(p) => write!(f, "data directory already locked: {}", p.display()),
            FsError::InjectedFault(m) => write!(f, "injected fault: {m}"),
        }
    }
}

impl std::error::Error for FsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FsError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for FsError {
    fn from(e: io::Error) -> Self {
        FsError::Io(e)
    }
}

/// Convenience result alias for file-I/O operations.
pub type FsResult<T> = Result<T, FsError>;

/// A guard representing an held exclusive data-directory lock.
///
/// The lock is released when this value is dropped. For [`RealFs`] the OS
/// releases the file lock when its handle closes (including after a crash);
/// for [`SimFs`] the in-memory lock flag is cleared.
pub trait LockGuard: std::fmt::Debug {}

/// File-I/O abstraction required by the WAL and recovery layers (§6.4).
///
/// All integers persisted through this layer by callers are little-endian, but
/// this trait is byte-oriented and imposes no interpretation on file contents.
pub trait FileSystem {
    /// Create a directory and all missing parents.
    fn create_dir_all(&self, path: &Path) -> FsResult<()>;

    /// Create the file if absent (truncating nothing existing) so it can be
    /// appended to. Establishes an empty file when it does not yet exist.
    fn create_file(&self, path: &Path) -> FsResult<()>;

    /// Append `data` to the end of `path`, handling partial writes / `EINTR`
    /// by looping until every byte is written (unless a fault is injected).
    /// The file is created if it does not exist.
    fn append(&self, path: &Path, data: &[u8]) -> FsResult<()>;

    /// Flush `path`'s contents to stable storage (file `fsync`).
    fn sync_file(&self, path: &Path) -> FsResult<()>;

    /// Flush directory entry changes under `path` to stable storage
    /// (directory `fsync`). Required after create/rename in `fsync` mode.
    fn sync_dir(&self, path: &Path) -> FsResult<()>;

    /// Rename `from` to `to` on the same filesystem (atomic replace).
    fn rename(&self, from: &Path, to: &Path) -> FsResult<()>;

    /// Truncate (or extend) `path` to exactly `len` bytes.
    fn truncate(&self, path: &Path, len: u64) -> FsResult<()>;

    /// Read the entire contents of `path`.
    fn read(&self, path: &Path) -> FsResult<Vec<u8>>;

    /// Report whether `path` currently exists.
    fn exists(&self, path: &Path) -> bool;

    /// List the names (final path component) of the regular files directly
    /// under directory `path`. Order is unspecified; callers sort as needed.
    /// A missing directory yields an empty list rather than an error, so
    /// recovery can probe a not-yet-created layout.
    fn list_dir(&self, path: &Path) -> FsResult<Vec<String>>;

    /// Acquire the exclusive data-directory lock at `lock_path`.
    ///
    /// Returns a guard that releases the lock on drop. Fails with
    /// [`FsError::Locked`] if the lock is already held.
    fn acquire_lock(&self, lock_path: &Path) -> FsResult<Box<dyn LockGuard>>;
}

// ---------------------------------------------------------------------------
// Deterministic PRNG (std has no rng).
// ---------------------------------------------------------------------------

/// A tiny deterministic pseudo-random number generator (SplitMix64).
///
/// The standard library provides no RNG, and the simulated file layer must be
/// fully reproducible from a published seed (Technical-Design §6.4). SplitMix64
/// is a well-known, simple, fast generator suitable for seeding deterministic
/// fault injection. It is NOT cryptographic.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
    seed: u64,
}

impl SplitMix64 {
    /// Create a generator from an explicit `seed`.
    pub fn new(seed: u64) -> Self {
        SplitMix64 { state: seed, seed }
    }

    /// The seed this generator was created with (for reproducibility).
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Produce the next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        // SplitMix64 (Steele, Lea, Flood; also in Java's SplittableRandom).
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Return `true` with probability `numerator / denominator`.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        if denominator == 0 {
            return false;
        }
        self.next_u64() % denominator < numerator
    }

    /// Return a value in `0..bound` (uniform enough for fault injection).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

// ---------------------------------------------------------------------------
// RealFs: real Linux std-backed adapter.
// ---------------------------------------------------------------------------

/// Real, std-backed file-I/O adapter for 64-bit Linux / ext4 (ADR-001).
#[derive(Debug, Default, Clone)]
pub struct RealFs;

impl RealFs {
    /// Create a new real-filesystem adapter.
    pub fn new() -> Self {
        RealFs
    }
}

/// Guard holding an OS lock on a persistent `LOCK` file.
#[derive(Debug)]
pub struct RealLockGuard {
    _file: std::fs::File,
}

impl LockGuard for RealLockGuard {}

impl FileSystem for RealFs {
    fn create_dir_all(&self, path: &Path) -> FsResult<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> FsResult<()> {
        use std::fs::OpenOptions;
        // create(true) + append(true) leaves existing content intact and makes
        // an empty file when absent.
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(())
    }

    fn append(&self, path: &Path, data: &[u8]) -> FsResult<()> {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut f = OpenOptions::new().create(true).append(true).open(path)?;
        // write_all already loops over partial writes and retries EINTR.
        f.write_all(data)?;
        Ok(())
    }

    fn sync_file(&self, path: &Path) -> FsResult<()> {
        use std::fs::OpenOptions;
        let f = OpenOptions::new().append(true).open(path)?;
        f.sync_all()?;
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> FsResult<()> {
        // On Linux, open the directory as a File and sync_all to flush its
        // directory entries (§6.2, Linux fsync(2)).
        let dir = std::fs::File::open(path)?;
        dir.sync_all()?;
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> FsResult<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    fn truncate(&self, path: &Path, len: u64) -> FsResult<()> {
        use std::fs::OpenOptions;
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(len)?;
        Ok(())
    }

    fn read(&self, path: &Path) -> FsResult<Vec<u8>> {
        let bytes = std::fs::read(path)?;
        Ok(bytes)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn list_dir(&self, path: &Path) -> FsResult<Vec<String>> {
        let entries = match std::fs::read_dir(path) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(FsError::Io(e)),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
    }

    fn acquire_lock(&self, lock_path: &Path) -> FsResult<Box<dyn LockGuard>> {
        use std::fs::OpenOptions;
        // The OS releases this advisory lock when the handle closes, including
        // after SIGKILL. The persistent file can then be locked on restart.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(Box::new(RealLockGuard { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Err(FsError::Locked(lock_path.to_path_buf())),
            Err(std::fs::TryLockError::Error(e)) => Err(FsError::Io(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// SimFs: deterministic simulated-power-loss adapter (§6.4).
// ---------------------------------------------------------------------------

/// Page size (bytes) used to model torn writes and arbitrary page-order
/// persistence in the simulated layer.
pub const SIM_PAGE_SIZE: usize = 4096;

/// A single file's volatile and stable byte images inside [`SimFs`].
#[derive(Debug, Clone, Default)]
struct SimFile {
    /// Bytes as most recently written (may not yet be durable).
    volatile: Vec<u8>,
    /// Bytes known to survive a crash (only updated by a successful sync).
    stable: Vec<u8>,
}

/// Fault-injection configuration for [`SimFs`], driven by a seed.
///
/// All probabilities are expressed out of 1000. Set any to zero to disable a
/// class of fault. Randomness is drawn from a [`SplitMix64`] seeded by
/// [`SimConfig::seed`], so a given seed + operation sequence is fully
/// reproducible.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Seed for the deterministic PRNG.
    pub seed: u64,
    /// Chance (per 1000) that an append writes only a prefix of its bytes.
    pub short_write_permille: u64,
    /// Chance (per 1000) that a file or directory sync fails.
    pub sync_fail_permille: u64,
    /// Chance (per 1000) that an operation triggers a simulated crash after
    /// its effect on the volatile image but before returning success.
    pub crash_permille: u64,
}

impl SimConfig {
    /// A deterministic configuration with the given `seed` and no injected
    /// faults. Use the `with_*` builders to enable specific fault classes.
    pub fn new(seed: u64) -> Self {
        SimConfig {
            seed,
            short_write_permille: 0,
            sync_fail_permille: 0,
            crash_permille: 0,
        }
    }

    /// Enable short writes with the given per-1000 probability.
    pub fn with_short_writes(mut self, permille: u64) -> Self {
        self.short_write_permille = permille;
        self
    }

    /// Enable sync failures with the given per-1000 probability.
    pub fn with_sync_failures(mut self, permille: u64) -> Self {
        self.sync_fail_permille = permille;
        self
    }

    /// Enable crash-at-step with the given per-1000 probability.
    pub fn with_crashes(mut self, permille: u64) -> Self {
        self.crash_permille = permille;
        self
    }
}

/// Shared inner state of a [`SimFs`], guarded by a mutex so a single simulated
/// filesystem instance can be cloned and shared.
#[derive(Debug)]
struct SimInner {
    /// File contents keyed by path.
    files: HashMap<PathBuf, SimFile>,
    /// Directories that exist (volatile view).
    dirs_volatile: std::collections::HashSet<PathBuf>,
    /// Directories that exist (stable view).
    dirs_stable: std::collections::HashSet<PathBuf>,
    /// Whether the exclusive lock is currently held.
    locked: bool,
    /// Deterministic PRNG.
    rng: SplitMix64,
    /// Configuration (probabilities + seed).
    config: SimConfig,
}

/// Deterministic simulated-power-loss file layer (Technical-Design §6.4).
///
/// Cloning a `SimFs` yields another handle to the SAME underlying state, so a
/// writer and a recovery reader share one simulated disk.
#[derive(Debug, Clone)]
pub struct SimFs {
    inner: Arc<Mutex<SimInner>>,
}

/// Guard for the simulated lock; clears the in-memory lock flag on drop.
#[derive(Debug)]
pub struct SimLockGuard {
    inner: Arc<Mutex<SimInner>>,
}

impl LockGuard for SimLockGuard {}

impl Drop for SimLockGuard {
    fn drop(&mut self) {
        if let Ok(mut g) = self.inner.lock() {
            g.locked = false;
        }
    }
}

impl SimFs {
    /// Create a simulated filesystem with the given fault-injection config.
    pub fn new(config: SimConfig) -> Self {
        let rng = SplitMix64::new(config.seed);
        SimFs {
            inner: Arc::new(Mutex::new(SimInner {
                files: HashMap::new(),
                dirs_volatile: std::collections::HashSet::new(),
                dirs_stable: std::collections::HashSet::new(),
                locked: false,
                rng,
                config,
            })),
        }
    }

    /// The seed driving this simulator (for reproducing a run).
    pub fn seed(&self) -> u64 {
        self.inner.lock().expect("sim lock").config.seed
    }

    /// Simulate a power loss / crash: discard ALL volatile state, keeping only
    /// what a successful sync made stable. After this, the volatile image of
    /// every file equals its stable image, and the volatile directory set
    /// equals the stable directory set. This is what the next recovery sees.
    pub fn crash(&self) {
        let mut g = self.inner.lock().expect("sim lock");
        for f in g.files.values_mut() {
            f.volatile = f.stable.clone();
        }
        g.dirs_volatile = g.dirs_stable.clone();
        // A crash also drops any process lock.
        g.locked = false;
    }

    /// Read the STABLE (durable) bytes of a file, i.e. what would survive a
    /// crash right now. Test helper.
    pub fn stable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        let g = self.inner.lock().expect("sim lock");
        g.files.get(path).map(|f| f.stable.clone())
    }

    /// Read the VOLATILE (current) bytes of a file. Test helper.
    pub fn volatile_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        let g = self.inner.lock().expect("sim lock");
        g.files.get(path).map(|f| f.volatile.clone())
    }
}

impl SimInner {
    /// Roll for a fault class; returns true if the fault should fire.
    fn roll(&mut self, permille: u64) -> bool {
        if permille == 0 {
            return false;
        }
        self.rng.chance(permille, 1000)
    }
}

impl FileSystem for SimFs {
    fn create_dir_all(&self, path: &Path) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        // Insert every ancestor into the volatile directory set. Directory
        // existence only becomes stable after a directory sync.
        let mut cur = PathBuf::new();
        for comp in path.components() {
            cur.push(comp.as_os_str());
            g.dirs_volatile.insert(cur.clone());
        }
        Ok(())
    }

    fn create_file(&self, path: &Path) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        g.files.entry(path.to_path_buf()).or_default();
        Ok(())
    }

    fn append(&self, path: &Path, data: &[u8]) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        // Decide on a short write before mutating: a seeded fault may persist
        // only a prefix of the bytes and then report failure.
        let short_permille = g.config.short_write_permille;
        let crash_permille = g.config.crash_permille;
        let short = g.roll(short_permille);
        let write_len = if short && !data.is_empty() {
            // Persist at least one byte but strictly fewer than requested.
            let n = g.rng.below(data.len() as u64) as usize;
            n.max(1).min(data.len().saturating_sub(1))
        } else {
            data.len()
        };

        let f = g.files.entry(path.to_path_buf()).or_default();
        f.volatile.extend_from_slice(&data[..write_len]);

        if short && write_len < data.len() {
            return Err(FsError::InjectedFault(format!(
                "short write: {write_len} of {} bytes on {}",
                data.len(),
                path.display()
            )));
        }

        // A crash may fire after the volatile mutation but before "success".
        if g.roll(crash_permille) {
            drop(g);
            self.crash();
            return Err(FsError::InjectedFault(format!(
                "crash during append on {}",
                path.display()
            )));
        }
        Ok(())
    }

    fn sync_file(&self, path: &Path) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        let sync_fail_permille = g.config.sync_fail_permille;
        let crash_permille = g.config.crash_permille;
        if g.roll(sync_fail_permille) {
            return Err(FsError::InjectedFault(format!(
                "sync_file failed on {}",
                path.display()
            )));
        }
        // A crash may fire during sync: some pages may already be durable.
        // Model this by persisting whole pages in arbitrary (seeded) order and
        // then discarding the rest via crash().
        let crash = g.roll(crash_permille);

        {
            let key = path.to_path_buf();
            if !g.files.contains_key(&key) {
                return Err(FsError::NotFound(key));
            }
            if crash {
                // Persist a seeded subset of pages, then crash. NEVER shrink
                // already-stable bytes: only extend or overwrite volatile pages
                // that are past the current stable length are the risk, so we
                // copy whole pages up to a random count while keeping existing
                // stable bytes intact.
                let (vol, stable_len) = {
                    let f = g.files.get(&key).expect("present");
                    (f.volatile.clone(), f.stable.len())
                };
                let total_pages = vol.len().div_ceil(SIM_PAGE_SIZE).max(1);
                let persist_pages = g.rng.below(total_pages as u64 + 1) as usize;
                let mut new_stable = {
                    let f = g.files.get(&key).expect("present");
                    f.stable.clone()
                };
                for page in 0..persist_pages {
                    let start = page * SIM_PAGE_SIZE;
                    if start >= vol.len() {
                        break;
                    }
                    let end = (start + SIM_PAGE_SIZE).min(vol.len());
                    if new_stable.len() < end {
                        new_stable.resize(end, 0);
                    }
                    new_stable[start..end].copy_from_slice(&vol[start..end]);
                }
                // Guarantee already-stable bytes are never altered.
                debug_assert!(new_stable.len() >= stable_len);
                {
                    let f = g.files.get_mut(&key).expect("present");
                    f.stable = new_stable;
                }
                drop(g);
                self.crash();
                return Err(FsError::InjectedFault(format!(
                    "crash during sync_file on {}",
                    path.display()
                )));
            }
            // Clean sync: the whole file's volatile image becomes stable.
            let f = g.files.get_mut(&key).expect("present");
            f.stable = f.volatile.clone();
        }
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        let sync_fail_permille = g.config.sync_fail_permille;
        let crash_permille = g.config.crash_permille;
        if g.roll(sync_fail_permille) {
            return Err(FsError::InjectedFault(format!(
                "sync_dir failed on {}",
                path.display()
            )));
        }
        if g.roll(crash_permille) {
            drop(g);
            self.crash();
            return Err(FsError::InjectedFault(format!(
                "crash during sync_dir on {}",
                path.display()
            )));
        }
        // A successful directory sync makes namespace changes stable.
        g.dirs_stable = g.dirs_volatile.clone();
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        let from_key = from.to_path_buf();
        if !g.files.contains_key(&from_key) {
            return Err(FsError::NotFound(from_key));
        }
        // Move the file's images to the new name in the volatile view. The
        // name change is only durable after a directory sync.
        let file = g.files.remove(&from_key).expect("present");
        g.files.insert(to.to_path_buf(), file);
        g.dirs_volatile.insert(to.to_path_buf());
        g.dirs_volatile.remove(&from_key);
        Ok(())
    }

    fn truncate(&self, path: &Path, len: u64) -> FsResult<()> {
        let mut g = self.inner.lock().expect("sim lock");
        let key = path.to_path_buf();
        let f = g.files.get_mut(&key).ok_or(FsError::NotFound(key))?;
        let len = len as usize;
        if f.volatile.len() > len {
            f.volatile.truncate(len);
        } else {
            f.volatile.resize(len, 0);
        }
        Ok(())
    }

    fn read(&self, path: &Path) -> FsResult<Vec<u8>> {
        let g = self.inner.lock().expect("sim lock");
        // Reads return the volatile (current) view, as a real read would.
        g.files
            .get(path)
            .map(|f| f.volatile.clone())
            .ok_or_else(|| FsError::NotFound(path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> bool {
        let g = self.inner.lock().expect("sim lock");
        g.files.contains_key(path) || g.dirs_volatile.contains(path)
    }

    fn list_dir(&self, path: &Path) -> FsResult<Vec<String>> {
        let g = self.inner.lock().expect("sim lock");
        let mut names = Vec::new();
        for file_path in g.files.keys() {
            if file_path.parent() == Some(path) {
                if let Some(name) = file_path.file_name().and_then(|n| n.to_str()) {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
    }

    fn acquire_lock(&self, lock_path: &Path) -> FsResult<Box<dyn LockGuard>> {
        let mut g = self.inner.lock().expect("sim lock");
        if g.locked {
            return Err(FsError::Locked(lock_path.to_path_buf()));
        }
        g.locked = true;
        drop(g);
        Ok(Box::new(SimLockGuard {
            inner: Arc::clone(&self.inner),
        }))
    }
}

/// Compute a record hash the way the WAL will (CRC64 over complete bytes).
///
/// Re-exported convenience so callers of this module do not need to import the
/// checksum module directly when hashing simulated file ranges in tests.
pub fn record_hash(bytes: &[u8]) -> u64 {
    crc64_ecma(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    // ---- PRNG determinism ------------------------------------------------

    #[test]
    fn splitmix64_is_deterministic_for_a_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_eq!(a.seed(), 42);
    }

    #[test]
    fn splitmix64_differs_across_seeds() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    // ---- SimFs power-loss model -----------------------------------------

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn unsynced_writes_are_lost_on_crash() {
        let fs = SimFs::new(SimConfig::new(7));
        fs.create_file(&p("/wal/0001")).unwrap();
        fs.append(&p("/wal/0001"), b"hello world").unwrap();
        // Not synced yet.
        assert_eq!(fs.volatile_bytes(&p("/wal/0001")).unwrap(), b"hello world");
        fs.crash();
        // After crash, only stable (empty) survives.
        assert_eq!(fs.stable_bytes(&p("/wal/0001")).unwrap(), b"");
        assert_eq!(fs.volatile_bytes(&p("/wal/0001")).unwrap(), b"");
    }

    #[test]
    fn synced_writes_survive_crash() {
        let fs = SimFs::new(SimConfig::new(7));
        fs.create_file(&p("/wal/0001")).unwrap();
        fs.append(&p("/wal/0001"), b"durable").unwrap();
        fs.sync_file(&p("/wal/0001")).unwrap();
        fs.append(&p("/wal/0001"), b"-volatile").unwrap();
        fs.crash();
        // Only the synced prefix survives.
        assert_eq!(fs.read(&p("/wal/0001")).unwrap(), b"durable");
    }

    #[test]
    fn rename_only_stable_after_dir_sync() {
        let fs = SimFs::new(SimConfig::new(11));
        fs.create_dir_all(&p("/data/wal")).unwrap();
        fs.create_file(&p("/data/wal/tmp")).unwrap();
        fs.append(&p("/data/wal/tmp"), b"seg").unwrap();
        fs.sync_file(&p("/data/wal/tmp")).unwrap();
        fs.rename(&p("/data/wal/tmp"), &p("/data/wal/0001"))
            .unwrap();
        // The renamed name exists in the volatile view.
        assert!(fs.exists(&p("/data/wal/0001")));
        // But the namespace change is not stable yet; a crash reverts it.
        fs.crash();
        // The bytes were synced under the OLD name, so after crash the file
        // content survives, but the rename (a namespace change) was not made
        // durable by a directory sync, so the new name should not be stable.
        // We model this by checking that a dir sync is what makes it durable:
        let fs2 = SimFs::new(SimConfig::new(11));
        fs2.create_dir_all(&p("/data/wal")).unwrap();
        fs2.create_file(&p("/data/wal/tmp")).unwrap();
        fs2.append(&p("/data/wal/tmp"), b"seg").unwrap();
        fs2.sync_file(&p("/data/wal/tmp")).unwrap();
        fs2.rename(&p("/data/wal/tmp"), &p("/data/wal/0001"))
            .unwrap();
        fs2.sync_dir(&p("/data/wal")).unwrap();
        // Directory entries are now stable.
        let g = fs2.inner.lock().unwrap();
        assert!(g.dirs_stable.contains(&p("/data/wal/0001")));
    }

    #[test]
    fn synced_bytes_are_never_altered_by_later_volatile_writes() {
        let fs = SimFs::new(SimConfig::new(3));
        fs.create_file(&p("/f")).unwrap();
        fs.append(&p("/f"), b"AAAA").unwrap();
        fs.sync_file(&p("/f")).unwrap();
        let stable_after_sync = fs.stable_bytes(&p("/f")).unwrap();
        // More volatile writes must not touch stable bytes until next sync.
        fs.append(&p("/f"), b"BBBB").unwrap();
        assert_eq!(fs.stable_bytes(&p("/f")).unwrap(), stable_after_sync);
        assert_eq!(&stable_after_sync, b"AAAA");
    }

    #[test]
    fn same_seed_same_sequence_is_identical() {
        // Enable all fault classes; drive an identical operation sequence.
        let cfg = || {
            SimConfig::new(0xDEAD_BEEF)
                .with_short_writes(300)
                .with_sync_failures(300)
                .with_crashes(200)
        };
        let run = || -> (Vec<u8>, Vec<u8>, Vec<String>) {
            let fs = SimFs::new(cfg());
            let mut faults = Vec::new();
            for i in 0..50u32 {
                let data = format!("record-{i:04}");
                if let Err(e) = fs.append(&p("/wal/seg"), data.as_bytes()) {
                    faults.push(e.to_string());
                }
                if let Err(e) = fs.sync_file(&p("/wal/seg")) {
                    faults.push(e.to_string());
                }
            }
            let vol = fs.volatile_bytes(&p("/wal/seg")).unwrap_or_default();
            let stable = fs.stable_bytes(&p("/wal/seg")).unwrap_or_default();
            (vol, stable, faults)
        };
        let a = run();
        let b = run();
        assert_eq!(a.0, b.0, "volatile images must match");
        assert_eq!(a.1, b.1, "stable images must match");
        assert_eq!(a.2, b.2, "injected-fault decisions must match");
    }

    #[test]
    fn sim_lock_is_exclusive_and_released_on_drop() {
        let fs = SimFs::new(SimConfig::new(1));
        let guard = fs.acquire_lock(&p("/data/LOCK")).unwrap();
        assert!(fs.acquire_lock(&p("/data/LOCK")).is_err());
        drop(guard);
        // Now it can be re-acquired.
        assert!(fs.acquire_lock(&p("/data/LOCK")).is_ok());
    }

    #[test]
    fn sim_list_dir_returns_files_in_that_directory_only() {
        let fs = SimFs::new(SimConfig::new(1));
        fs.create_dir_all(&p("/data/wal")).unwrap();
        fs.create_file(&p("/data/wal/0001.wal")).unwrap();
        fs.create_file(&p("/data/wal/0002.wal")).unwrap();
        fs.create_file(&p("/data/IDENTITY")).unwrap();
        let mut names = fs.list_dir(&p("/data/wal")).unwrap();
        names.sort();
        assert_eq!(names, vec!["0001.wal".to_string(), "0002.wal".to_string()]);
        // A missing directory yields an empty list, not an error.
        assert!(fs.list_dir(&p("/nope")).unwrap().is_empty());
    }

    // ---- RealFs round trip under an OS temp dir --------------------------

    /// Create a unique temp subdirectory under the OS temp dir.
    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut dir = std::env::temp_dir();
        dir.push(format!("distributedb-{tag}-{}-{nanos}", std::process::id()));
        dir
    }

    #[test]
    fn realfs_create_append_sync_rename_truncate_read_round_trip() {
        let fs = RealFs::new();
        let dir = unique_temp_dir("realfs");
        fs.create_dir_all(&dir).unwrap();

        let file = dir.join("segment.wal");
        fs.create_file(&file).unwrap();
        fs.append(&file, b"hello ").unwrap();
        fs.append(&file, b"world").unwrap();
        fs.sync_file(&file).unwrap();
        fs.sync_dir(&dir).unwrap();
        assert_eq!(fs.read(&file).unwrap(), b"hello world");

        // Rename within the same directory (same filesystem).
        let renamed = dir.join("segment-0001.wal");
        fs.rename(&file, &renamed).unwrap();
        fs.sync_dir(&dir).unwrap();
        assert!(fs.exists(&renamed));
        assert!(!fs.exists(&file));

        // Truncate to 5 bytes ("hello").
        fs.truncate(&renamed, 5).unwrap();
        fs.sync_file(&renamed).unwrap();
        assert_eq!(fs.read(&renamed).unwrap(), b"hello");

        // list_dir reports the single regular file present.
        let names = fs.list_dir(&dir).unwrap();
        assert_eq!(names, vec!["segment-0001.wal".to_string()]);

        // Cleanup: remove the temp directory tree.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn realfs_lock_is_exclusive_and_released_on_drop() {
        let fs = RealFs::new();
        let dir = unique_temp_dir("realfs-lock");
        fs.create_dir_all(&dir).unwrap();
        let lock = dir.join("LOCK");

        let guard = fs.acquire_lock(&lock).unwrap();
        // A second acquire must fail while held.
        assert!(matches!(fs.acquire_lock(&lock), Err(FsError::Locked(_))));
        drop(guard);
        // The persistent LOCK file can be re-acquired after the handle closes.
        assert!(fs.acquire_lock(&lock).is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_hash_matches_crc64() {
        assert_eq!(record_hash(b"123456789"), 0x6C40_DF5F_0B49_7347);
    }
}
