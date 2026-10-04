//! Phase 7 lock-scope change: the sequencer syncs a WAL group without holding
//! the database lock, so reads are not queued behind an `fsync`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, FileSystem, FsResult, LockGuard, Server, ServerConfig, SimConfig,
    SimFs, Status,
};

const SLOW_SYNC: Duration = Duration::from_millis(600);

/// A [`SimFs`] whose file syncs take [`SLOW_SYNC`] once `slow` is set,
/// standing in for a slow disk.
#[derive(Clone)]
struct SlowSyncFs {
    inner: SimFs,
    slow: Arc<AtomicBool>,
}

impl FileSystem for SlowSyncFs {
    fn create_dir_all(&self, path: &Path) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn create_file(&self, path: &Path) -> FsResult<()> {
        self.inner.create_file(path)
    }
    fn append(&self, path: &Path, data: &[u8]) -> FsResult<()> {
        self.inner.append(path, data)
    }
    fn sync_file(&self, path: &Path) -> FsResult<()> {
        if self.slow.load(Ordering::SeqCst) {
            thread::sleep(SLOW_SYNC);
        }
        self.inner.sync_file(path)
    }
    fn sync_dir(&self, path: &Path) -> FsResult<()> {
        self.inner.sync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> FsResult<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir(&self, path: &Path) -> FsResult<()> {
        self.inner.remove_dir(path)
    }
    fn truncate(&self, path: &Path, len: u64) -> FsResult<()> {
        self.inner.truncate(path, len)
    }
    fn read(&self, path: &Path) -> FsResult<Vec<u8>> {
        self.inner.read(path)
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }
    fn list_dir(&self, path: &Path) -> FsResult<Vec<String>> {
        self.inner.list_dir(path)
    }
    fn acquire_lock(&self, lock_path: &Path) -> FsResult<Box<dyn LockGuard>> {
        self.inner.acquire_lock(lock_path)
    }
}

#[test]
fn reads_do_not_wait_for_a_group_sync() {
    let fs = SlowSyncFs {
        inner: SimFs::new(SimConfig::new(7)),
        slow: Arc::new(AtomicBool::new(false)),
    };
    let mut db = Db::open(fs.clone(), Path::new("/slow"), DurabilityMode::Fsync).unwrap();
    db.set(b"existing".to_vec(), b"old".to_vec()).unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let addr = server.local_addr();
    let mut reader = Client::connect(addr).unwrap();
    fs.slow.store(true, Ordering::SeqCst);

    let writer = thread::spawn(move || {
        let mut client = Client::connect(addr).unwrap();
        let started = Instant::now();
        let status = client.set(b"existing".to_vec(), b"new".to_vec()).unwrap();
        (status, started.elapsed())
    });
    // Let the sequencer reach the slow sync.
    thread::sleep(SLOW_SYNC / 4);

    let started = Instant::now();
    let value = reader.get(b"existing".to_vec()).unwrap();
    let read_time = started.elapsed();
    // The unacknowledged write is not visible, and the read did not queue
    // behind the remaining sync time.
    assert_eq!(value, Some(b"old".to_vec()));
    assert!(
        read_time < SLOW_SYNC / 3,
        "GET took {read_time:?} during a {SLOW_SYNC:?} sync"
    );

    let (status, write_time) = writer.join().unwrap();
    assert_eq!(status, Status::Ok);
    assert!(write_time >= SLOW_SYNC, "SET returned before its sync");
    assert_eq!(
        reader.get(b"existing".to_vec()).unwrap(),
        Some(b"new".to_vec())
    );
    let stats = reader.stats().unwrap();
    assert!(stats.lines().any(|line| line == "current_lsn=2"), "{stats}");
    drop(reader);
    server.shutdown();
}
