//! Shared helpers for the Phase 2 durability / crash-recovery integration
//! tests (FEAT-003; SOW §8, §19, §20; Technical-Design §6.3, §6.4, §13).
//!
//! These helpers are compiled into each integration-test crate that declares
//! `mod common;`. They provide unique OS-temp data directories with automatic
//! cleanup so no runtime data is ever committed (context.json snapshot rule).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A unique temporary directory under [`std::env::temp_dir`] that removes
/// itself (recursively) when dropped.
///
/// Every test that touches the real filesystem uses one of these so the data
/// directory lives under the OS temp dir with cleanup, never inside the repo.
pub struct TempDir {
    path: PathBuf,
}

// A process-local counter guarantees uniqueness even when two dirs are created
// within the same nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

impl TempDir {
    /// Create a fresh, empty, uniquely named temporary directory.
    pub fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name = format!("distributedb-{tag}-{pid}-{nanos}-{n}");
        let path = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }

    /// The path of the temporary directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Best-effort recursive cleanup; ignore errors during drop.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
