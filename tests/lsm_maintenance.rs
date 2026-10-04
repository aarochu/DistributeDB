//! The server flushes LSM memtables on a background thread, so writes keep
//! committing while a table is written.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use distributedb::{
    Client, Db, DurabilityMode, FileSystem, FsResult, LockGuard, LsmConfig, OpenConfig, Server,
    ServerConfig, SimConfig, SimFs, Status, StorageKind,
};

const SLOW_TABLE_SYNC: Duration = Duration::from_millis(800);

/// A [`SimFs`] whose SSTable syncs are slow, standing in for writing a large
/// table to a slow disk. WAL syncs stay fast.
#[derive(Clone)]
struct SlowTableFs {
    inner: SimFs,
    writing_table: Arc<AtomicBool>,
}

impl FileSystem for SlowTableFs {
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
        if !path.to_string_lossy().contains(".sst") {
            return self.inner.sync_file(path);
        }
        self.writing_table.store(true, Ordering::SeqCst);
        thread::sleep(SLOW_TABLE_SYNC);
        let result = self.inner.sync_file(path);
        self.writing_table.store(false, Ordering::SeqCst);
        result
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
    fn read_at(&self, path: &Path, offset: u64, len: usize) -> FsResult<Vec<u8>> {
        self.inner.read_at(path, offset, len)
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

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

#[test]
fn writes_commit_while_a_flush_is_written() {
    let fs = SlowTableFs {
        inner: SimFs::new(SimConfig::new(21)),
        writing_table: Arc::new(AtomicBool::new(false)),
    };
    let config = OpenConfig {
        storage: StorageKind::Lsm(LsmConfig {
            memtable_bytes: 4096,
            l0_compaction_trigger: 1000,
            ..LsmConfig::default()
        }),
        ..OpenConfig::default()
    };
    let db =
        Db::open_configured(fs.clone(), Path::new("/lsm"), DurabilityMode::Fsync, config).unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();

    // Fill the memtable until the background flush starts writing a table.
    let mut written = 0u32;
    while !fs.writing_table.load(Ordering::SeqCst) {
        assert!(written < 5000, "no flush started");
        assert_eq!(client.set(key(written), b"v".to_vec()).unwrap(), Status::Ok);
        written += 1;
    }

    // While the table is still being written, writes keep committing.
    let mut slowest = Duration::ZERO;
    for _ in 0..20 {
        let started = Instant::now();
        assert_eq!(client.set(key(written), b"v".to_vec()).unwrap(), Status::Ok);
        slowest = slowest.max(started.elapsed());
        written += 1;
    }
    assert!(
        fs.writing_table.load(Ordering::SeqCst),
        "the flush finished before the writes; the test did not overlap them"
    );
    assert!(
        slowest < SLOW_TABLE_SYNC / 4,
        "a write took {slowest:?} during a {SLOW_TABLE_SYNC:?} table sync"
    );

    // The flush completes and every key is readable.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let stats = client.stats().unwrap();
        if stats.lines().any(|line| {
            line.strip_prefix("lsm_flushes_total=")
                .and_then(|count| count.parse::<u64>().ok())
                .is_some_and(|count| count >= 1)
        }) {
            break;
        }
        assert!(Instant::now() < deadline, "flush did not finish: {stats}");
        thread::sleep(Duration::from_millis(20));
    }
    for i in 0..written {
        assert_eq!(client.get(key(i)).unwrap(), Some(b"v".to_vec()), "key {i}");
    }
    drop(client);
    server.shutdown();
}
