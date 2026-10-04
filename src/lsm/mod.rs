//! Log-structured merge tree storage (SOW §16 Option B, Phase 8).
//!
//! The LSM path keeps recent writes in a sorted in-memory [`MemTable`] and
//! moves older data into immutable, sorted, checksummed [`sstable`] files, so
//! the live dataset no longer has to fit in memory. Lookups consult the
//! newest component first; a tombstone in a newer component hides older
//! versions of the key. [`merge::MergeIter`] combines sorted components for
//! compaction and full scans.
//!
//! [`tree::LsmTree`] combines them: it flushes the memtable to SSTables and
//! records the live tables in a [`manifest`].
//!
//! [`MemTable`]: memtable::MemTable

pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod sstable;
pub mod tree;

use crate::fileio::FsError;

/// Errors from LSM components. Corruption is fail closed: it is never turned
/// into a partial result or reported as a missing key.
#[derive(Debug)]
pub enum LsmError {
    /// The filesystem failed.
    Fs(FsError),
    /// Stored bytes failed validation. Carries a description.
    Corrupt(String),
    /// A caller broke a component's contract, such as adding unsorted keys.
    Usage(&'static str),
}

impl std::fmt::Display for LsmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LsmError::Fs(e) => write!(f, "lsm filesystem error: {e}"),
            LsmError::Corrupt(m) => write!(f, "lsm corruption: {m}"),
            LsmError::Usage(m) => write!(f, "lsm usage error: {m}"),
        }
    }
}

impl std::error::Error for LsmError {}

impl From<FsError> for LsmError {
    fn from(e: FsError) -> Self {
        LsmError::Fs(e)
    }
}

/// Result alias for LSM operations.
pub type LsmResult<T> = Result<T, LsmError>;

/// One version of a key: `Some(value)`, or `None` for a tombstone.
pub type Entry = Option<Vec<u8>>;

/// The outcome of looking up a key in one LSM component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// The component holds this value for the key.
    Found(Vec<u8>),
    /// The component holds a tombstone: the key is deleted, and older
    /// components must not be consulted.
    Deleted,
    /// The component has no version of the key; consult older components.
    Absent,
}
