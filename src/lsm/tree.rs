//! The LSM tree: a mutable memtable, an immutable memtable being flushed, and
//! SSTables listed by the manifest.
//!
//! Lookups consult the memtable, then the immutable memtable, then tables in
//! manifest order (newest first), skipping tables whose key range excludes the
//! key. The first component holding the key decides: a value, or a tombstone
//! that hides every older version.
//!
//! A flush has three steps so the slow one can run without the caller's lock:
//!
//! 1. [`LsmTree::freeze`] turns the memtable into the immutable memtable and
//!    returns a [`FlushJob`]. Reads still see the frozen entries.
//! 2. [`FlushJob::write`] encodes and publishes the SSTable, then reopens it
//!    to verify it. It needs only the job, not the tree.
//! 3. [`LsmTree::install`] lists the table in a new manifest, together with
//!    the WAL boundary it covers, and drops the immutable memtable.
//!
//! A crash before step 3 leaves a table file the manifest does not list; the
//! next [`LsmTree::open`] deletes it. The tree does not write a WAL: the
//! caller logs mutations first and, after a restart, replays records above
//! [`LsmTree::flushed_lsn`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::manifest::{Manifest, TableRecord};
use super::memtable::MemTable;
use super::merge::{MergeIter, Source};
use super::sstable::{self, SsTable, SsTableBuilder};
use super::{Lookup, LsmError, LsmResult};
use crate::fileio::FileSystem;
use crate::storage::Mutation;

/// Name of the manifest file inside the LSM directory.
pub const MANIFEST_FILE: &str = "MANIFEST";

/// Tuning and durability settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmConfig {
    /// Flush once the memtable holds about this many bytes.
    pub memtable_bytes: usize,
    /// Sync files and directories. `false` only for disposable data.
    pub durable: bool,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_bytes: 4 * 1024 * 1024,
            durable: true,
        }
    }
}

fn table_name(id: u64) -> String {
    format!("{id:020}.sst")
}

/// A table listed by the manifest, opened for reads.
#[derive(Debug, Clone)]
struct LiveTable {
    record: TableRecord,
    table: SsTable,
}

/// The table id and WAL boundary of the memtable being flushed.
#[derive(Debug, Clone, Copy)]
struct Frozen {
    id: u64,
    lsn: u64,
    hash: u64,
}

/// An LSM tree stored in one directory.
#[derive(Debug)]
pub struct LsmTree<F: FileSystem + Clone> {
    fs: F,
    dir: PathBuf,
    tmp_dir: PathBuf,
    config: LsmConfig,
    memtable: MemTable,
    immutable: Option<Arc<MemTable>>,
    frozen: Option<Frozen>,
    manifest: Manifest,
    tables: Vec<LiveTable>,
}

/// Work to write one flushed SSTable, independent of the tree.
#[derive(Debug)]
pub struct FlushJob<F: FileSystem> {
    fs: F,
    memtable: Arc<MemTable>,
    path: PathBuf,
    tmp_path: PathBuf,
    frozen: Frozen,
    durable: bool,
}

/// A published, verified SSTable waiting for [`LsmTree::install`].
#[derive(Debug)]
pub struct WrittenTable {
    record: TableRecord,
    table: SsTable,
    frozen: Frozen,
}

fn write_manifest<F: FileSystem>(
    fs: &F,
    dir: &Path,
    tmp_dir: &Path,
    manifest: &Manifest,
    durable: bool,
) -> LsmResult<()> {
    sstable::publish(
        fs,
        &tmp_dir.join("lsm-MANIFEST.tmp"),
        &dir.join(MANIFEST_FILE),
        &manifest.encode(),
        durable,
    )?;
    Ok(())
}

impl<F: FileSystem + Clone> LsmTree<F> {
    /// Open the tree in `dir`, creating an empty one if it has no manifest.
    /// `tmp_dir` must be on the same filesystem. Every listed table is
    /// opened and checked against the manifest; unlisted table files are
    /// deleted.
    pub fn open(fs: F, dir: &Path, tmp_dir: &Path, config: LsmConfig) -> LsmResult<Self> {
        let fresh = !fs.exists(dir);
        fs.create_dir_all(dir)?;
        fs.create_dir_all(tmp_dir)?;
        if fresh && config.durable {
            if let Some(parent) = dir.parent() {
                fs.sync_dir(parent)?;
            }
        }
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = if fs.exists(&manifest_path) {
            Manifest::decode(&fs.read(&manifest_path)?).map_err(|detail| {
                LsmError::Corrupt(format!("{}: {detail}", manifest_path.display()))
            })?
        } else {
            let manifest = Manifest::default();
            write_manifest(&fs, dir, tmp_dir, &manifest, config.durable)?;
            manifest
        };

        let mut tables = Vec::with_capacity(manifest.tables.len());
        for record in &manifest.tables {
            let path = dir.join(table_name(record.id));
            if !fs.exists(&path) {
                return Err(LsmError::Corrupt(format!(
                    "manifest lists missing table {}",
                    path.display()
                )));
            }
            let table = SsTable::open(&fs, &path, record.file_len)?;
            if table.entry_count() != record.entry_count
                || table.max_key() != Some(record.max_key.as_slice())
            {
                return Err(LsmError::Corrupt(format!(
                    "{} disagrees with its manifest entry",
                    path.display()
                )));
            }
            tables.push(LiveTable {
                record: record.clone(),
                table,
            });
        }

        let live: HashSet<String> = manifest.tables.iter().map(|r| table_name(r.id)).collect();
        let mut removed = false;
        for name in fs.list_dir(dir)? {
            if name.ends_with(".sst") && !live.contains(&name) {
                fs.remove_file(&dir.join(&name))?;
                removed = true;
            }
        }
        if removed && config.durable {
            fs.sync_dir(dir)?;
        }

        Ok(Self {
            fs,
            dir: dir.to_path_buf(),
            tmp_dir: tmp_dir.to_path_buf(),
            config,
            memtable: MemTable::new(),
            immutable: None,
            frozen: None,
            manifest,
            tables,
        })
    }

    /// Record a mutation in the memtable. The caller has already made it
    /// durable in its WAL.
    pub fn apply(&mut self, mutation: Mutation) {
        self.memtable.apply(mutation);
    }

    /// Whether the memtable has reached its flush size and no flush is
    /// already in progress.
    pub fn should_flush(&self) -> bool {
        self.immutable.is_none() && self.memtable.approx_bytes() >= self.config.memtable_bytes
    }

    /// Look up `key` across all components.
    pub fn get(&self, key: &[u8]) -> LsmResult<Option<Vec<u8>>> {
        let frozen = self
            .immutable
            .as_ref()
            .map_or(Lookup::Absent, |memtable| memtable.get(key));
        for lookup in [self.memtable.get(key), frozen] {
            match lookup {
                Lookup::Found(value) => return Ok(Some(value)),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        for live in &self.tables {
            if key < live.record.min_key.as_slice() || key > live.record.max_key.as_slice() {
                continue;
            }
            match live.table.get(&self.fs, key)? {
                Lookup::Found(value) => return Ok(Some(value)),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        Ok(None)
    }

    /// Every key's newest version in ascending order, tombstones included.
    pub fn iter(&self) -> LsmResult<MergeIter<'_>> {
        let mut sources: Vec<Source<'_>> = Vec::new();
        sources.push(Box::new(
            self.memtable
                .iter()
                .map(|(key, entry)| Ok::<_, LsmError>((key.clone(), entry.clone()))),
        ));
        if let Some(frozen) = &self.immutable {
            sources.push(Box::new(
                frozen
                    .iter()
                    .map(|(key, entry)| Ok::<_, LsmError>((key.clone(), entry.clone()))),
            ));
        }
        for live in &self.tables {
            sources.push(Box::new(live.table.iter(&self.fs)));
        }
        MergeIter::new(sources)
    }

    /// Every live key and value in ascending key order.
    pub fn live_pairs(&self) -> LsmResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut pairs = Vec::new();
        for item in self.iter()? {
            if let (key, Some(value)) = item? {
                pairs.push((key, value));
            }
        }
        Ok(pairs)
    }

    /// Freeze the memtable for flushing. `lsn` and `hash` identify the last
    /// WAL record applied to it; the installed table will cover exactly the
    /// WAL up to `lsn`. Returns `None` when the memtable is empty.
    pub fn freeze(&mut self, lsn: u64, hash: u64) -> LsmResult<Option<FlushJob<F>>> {
        if self.immutable.is_some() {
            return Err(LsmError::Usage("a flush is already in progress"));
        }
        if self.memtable.is_empty() {
            return Ok(None);
        }
        let frozen = Frozen {
            id: self.manifest.next_table_id,
            lsn,
            hash,
        };
        self.manifest.next_table_id += 1;
        self.immutable = Some(Arc::new(std::mem::take(&mut self.memtable)));
        self.frozen = Some(frozen);
        Ok(self.flush_job())
    }

    /// A job for the memtable already frozen, to retry a failed write.
    pub fn flush_job(&self) -> Option<FlushJob<F>> {
        let memtable = Arc::clone(self.immutable.as_ref()?);
        let frozen = self.frozen?;
        let name = table_name(frozen.id);
        Some(FlushJob {
            fs: self.fs.clone(),
            memtable,
            path: self.dir.join(&name),
            tmp_path: self.tmp_dir.join(format!("lsm-{name}.tmp")),
            frozen,
            durable: self.config.durable,
        })
    }

    /// List a written table as the newest and record its WAL boundary in a
    /// new manifest, then drop the immutable memtable it came from.
    pub fn install(&mut self, written: WrittenTable) -> LsmResult<()> {
        match self.frozen {
            Some(frozen) if frozen.id == written.frozen.id => {}
            _ => return Err(LsmError::Usage("table does not match the frozen memtable")),
        }
        let mut manifest = self.manifest.clone();
        manifest.tables.insert(0, written.record.clone());
        manifest.flushed_lsn = written.frozen.lsn;
        manifest.flushed_hash = written.frozen.hash;
        write_manifest(
            &self.fs,
            &self.dir,
            &self.tmp_dir,
            &manifest,
            self.config.durable,
        )?;
        self.manifest = manifest;
        self.tables.insert(
            0,
            LiveTable {
                record: written.record,
                table: written.table,
            },
        );
        self.immutable = None;
        self.frozen = None;
        Ok(())
    }

    /// Freeze, write, and install in one call. Returns whether a table was
    /// written.
    pub fn flush(&mut self, lsn: u64, hash: u64) -> LsmResult<bool> {
        let Some(job) = self.freeze(lsn, hash)? else {
            return Ok(false);
        };
        let written = job.write()?;
        self.install(written)?;
        Ok(true)
    }

    /// Highest WAL LSN contained in the installed tables.
    pub fn flushed_lsn(&self) -> u64 {
        self.manifest.flushed_lsn
    }

    /// Record hash at [`LsmTree::flushed_lsn`].
    pub fn flushed_hash(&self) -> u64 {
        self.manifest.flushed_hash
    }

    /// Approximate bytes held by the mutable memtable.
    pub fn memtable_bytes(&self) -> usize {
        self.memtable.approx_bytes()
    }

    /// Whether a frozen memtable is waiting to be installed.
    pub fn flush_pending(&self) -> bool {
        self.immutable.is_some()
    }

    /// Installed tables, in search order.
    pub fn tables(&self) -> impl Iterator<Item = &TableRecord> + '_ {
        self.tables.iter().map(|live| &live.record)
    }

    /// Total bytes of installed table files.
    pub fn table_bytes(&self) -> u64 {
        self.tables.iter().map(|live| live.record.file_len).sum()
    }
}

impl<F: FileSystem> FlushJob<F> {
    /// Encode and publish the frozen memtable as an SSTable, then reopen it
    /// from disk to verify it. Safe to run without the tree's lock.
    pub fn write(&self) -> LsmResult<WrittenTable> {
        let mut builder = SsTableBuilder::new();
        for (key, entry) in self.memtable.iter() {
            builder.add(key, entry.as_deref())?;
        }
        let min_key = builder
            .first_key()
            .expect("a frozen memtable is not empty")
            .to_vec();
        let max_key = builder
            .last_key()
            .expect("a frozen memtable is not empty")
            .to_vec();
        let entry_count = builder.entry_count();
        let bytes = builder.finish();
        let file_len =
            sstable::publish(&self.fs, &self.tmp_path, &self.path, &bytes, self.durable)?;
        let table = SsTable::open(&self.fs, &self.path, file_len)?;
        if table.entry_count() != entry_count {
            return Err(LsmError::Corrupt(format!(
                "{} reloaded with the wrong entry count",
                self.path.display()
            )));
        }
        Ok(WrittenTable {
            record: TableRecord {
                id: self.frozen.id,
                level: 0,
                file_len,
                entry_count,
                min_key,
                max_key,
            },
            table,
            frozen: self.frozen,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::{SimConfig, SimFs};

    fn set(key: &str, value: &str) -> Mutation {
        Mutation::Set {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn delete(key: &str) -> Mutation {
        Mutation::Delete {
            key: key.as_bytes().to_vec(),
        }
    }

    fn open(fs: &SimFs) -> LsmTree<SimFs> {
        LsmTree::open(
            fs.clone(),
            Path::new("/db/lsm"),
            Path::new("/db/tmp"),
            LsmConfig::default(),
        )
        .unwrap()
    }

    fn get(tree: &LsmTree<SimFs>, key: &str) -> Option<String> {
        tree.get(key.as_bytes())
            .unwrap()
            .map(|value| String::from_utf8(value).unwrap())
    }

    #[test]
    fn reads_see_memtable_frozen_memtable_and_tables_in_order() {
        let fs = SimFs::new(SimConfig::new(1));
        let mut tree = open(&fs);
        tree.apply(set("a", "1"));
        tree.apply(set("b", "2"));
        assert!(tree.flush(2, 22).unwrap());

        tree.apply(delete("b"));
        tree.apply(set("c", "3"));
        let job = tree.freeze(4, 44).unwrap().unwrap();
        tree.apply(set("d", "4"));
        tree.apply(set("a", "5"));
        assert_eq!(get(&tree, "a").as_deref(), Some("5"));
        assert_eq!(get(&tree, "b"), None, "the frozen tombstone hides the table");
        assert_eq!(get(&tree, "c").as_deref(), Some("3"));
        assert_eq!(get(&tree, "d").as_deref(), Some("4"));
        assert_eq!(get(&tree, "e"), None);
        assert!(tree.freeze(5, 55).is_err(), "one flush at a time");

        tree.install(job.write().unwrap()).unwrap();
        assert_eq!(tree.flushed_lsn(), 4);
        assert_eq!(tree.flushed_hash(), 44);
        assert_eq!(tree.tables().count(), 2);
        assert_eq!(get(&tree, "b"), None);
        assert_eq!(get(&tree, "c").as_deref(), Some("3"));
        let pairs = tree.live_pairs().unwrap();
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        let expected: [&[u8]; 3] = [b"a", b"c", b"d"];
        assert_eq!(keys, expected);
    }

    #[test]
    fn installed_tables_and_boundary_survive_reopen_and_crash() {
        let fs = SimFs::new(SimConfig::new(2));
        let mut tree = open(&fs);
        for index in 0..500 {
            tree.apply(set(&format!("key-{index:04}"), &index.to_string()));
        }
        tree.flush(500, 5000).unwrap();
        tree.apply(set("unflushed", "lost"));
        drop(tree);
        fs.crash();

        let tree = open(&fs);
        assert_eq!(tree.flushed_lsn(), 500);
        assert_eq!(tree.flushed_hash(), 5000);
        assert_eq!(get(&tree, "key-0123").as_deref(), Some("123"));
        assert_eq!(get(&tree, "unflushed"), None, "the caller's WAL replays this");
    }

    #[test]
    fn table_written_but_not_installed_is_removed_on_open() {
        let fs = SimFs::new(SimConfig::new(3));
        let mut tree = open(&fs);
        tree.apply(set("a", "1"));
        let job = tree.freeze(1, 11).unwrap().unwrap();
        let written = job.write().unwrap();
        let orphan = written.table.path().to_path_buf();
        assert!(fs.exists(&orphan));
        drop(tree);
        fs.crash();

        let tree = open(&fs);
        assert!(!fs.exists(&orphan));
        assert_eq!(tree.tables().count(), 0);
        assert_eq!(tree.flushed_lsn(), 0);
    }

    #[test]
    fn failed_write_can_be_retried_from_the_frozen_memtable() {
        let fs = SimFs::new(SimConfig::new(4));
        let mut tree = open(&fs);
        tree.apply(set("a", "1"));
        let job = tree.freeze(1, 11).unwrap().unwrap();
        drop(job);
        assert!(tree.flush_pending());
        assert_eq!(get(&tree, "a").as_deref(), Some("1"));
        let retry = tree.flush_job().unwrap();
        tree.install(retry.write().unwrap()).unwrap();
        assert!(!tree.flush_pending());
        assert_eq!(get(&tree, "a").as_deref(), Some("1"));
    }

    #[test]
    fn missing_table_fails_open() {
        let fs = SimFs::new(SimConfig::new(5));
        let mut tree = open(&fs);
        tree.apply(set("a", "1"));
        tree.flush(1, 11).unwrap();
        drop(tree);
        fs.remove_file(&Path::new("/db/lsm").join(table_name(1)))
            .unwrap();
        let reopened = LsmTree::open(
            fs.clone(),
            Path::new("/db/lsm"),
            Path::new("/db/tmp"),
            LsmConfig::default(),
        );
        assert!(matches!(reopened, Err(LsmError::Corrupt(_))));
    }

    #[test]
    fn flush_threshold_and_empty_flush() {
        let fs = SimFs::new(SimConfig::new(6));
        let mut tree = LsmTree::open(
            fs,
            Path::new("/db/lsm"),
            Path::new("/db/tmp"),
            LsmConfig {
                memtable_bytes: 200,
                durable: true,
            },
        )
        .unwrap();
        assert!(!tree.flush(0, 0).unwrap());
        assert!(!tree.should_flush());
        for index in 0..10 {
            tree.apply(set(&format!("k{index}"), "value"));
        }
        assert!(tree.should_flush());
        tree.flush(10, 100).unwrap();
        assert!(!tree.should_flush());
        assert_eq!(tree.memtable_bytes(), 0);
        assert!(tree.table_bytes() > 0);
    }
}
