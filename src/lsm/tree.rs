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
//!
//! # Levels and compaction
//!
//! Flushed tables form level 0. They may overlap, so all are searched, newest
//! first. Level 1 is the bottom level: a run of disjoint tables in key order.
//! Once level 0 holds [`LsmConfig::l0_compaction_trigger`] tables, compaction
//! merges all of them with the level-1 tables their key range overlaps into
//! new level-1 tables of about [`LsmConfig::target_table_bytes`] each.
//! Because every older version of those keys is among the inputs, tombstones
//! and shadowed versions are dropped. Compaction uses the same three steps as
//! a flush ([`LsmTree::plan_compaction`], [`CompactionJob::write`],
//! [`LsmTree::install_compaction`]); a flush may run while it writes, and the
//! table ids it may use are reserved when it is planned. Input files are
//! deleted only after the new manifest is durable.

use std::cmp::Ordering;
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
    /// Compact once level 0 holds this many tables.
    pub l0_compaction_trigger: usize,
    /// Split compaction output into tables of about this many bytes.
    pub target_table_bytes: usize,
    /// Sync files and directories. `false` only for disposable data.
    pub durable: bool,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_bytes: 4 * 1024 * 1024,
            l0_compaction_trigger: 4,
            target_table_bytes: 8 * 1024 * 1024,
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
    compacting: bool,
    manifest: Manifest,
    tables: Vec<LiveTable>,
}

/// Work to merge level 0 into level 1, independent of the tree.
#[derive(Debug)]
pub struct CompactionJob<F: FileSystem> {
    fs: F,
    dir: PathBuf,
    tmp_dir: PathBuf,
    durable: bool,
    target_table_bytes: usize,
    /// Inputs in search order: level 0 newest first, then level 1.
    inputs: Vec<LiveTable>,
    first_id: u64,
    reserved_ids: u64,
}

/// Published, verified compaction output waiting for
/// [`LsmTree::install_compaction`].
#[derive(Debug)]
pub struct CompactedTables {
    input_ids: Vec<u64>,
    outputs: Vec<LiveTable>,
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

        // Level 0 precedes level 1, whose tables are disjoint and ascending.
        let mut previous_max: Option<&[u8]> = None;
        for record in &manifest.tables {
            match record.level {
                0 if previous_max.is_none() => {}
                1 if previous_max.is_none_or(|max| max < record.min_key.as_slice()) => {
                    previous_max = Some(record.max_key.as_slice());
                }
                _ => {
                    return Err(LsmError::Corrupt(format!(
                        "{}: table levels or level-1 ranges are out of order",
                        manifest_path.display()
                    )));
                }
            }
        }

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
            compacting: false,
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

    fn level_zero_count(&self) -> usize {
        self.tables
            .iter()
            .filter(|live| live.record.level == 0)
            .count()
    }

    /// Whether level 0 has reached the compaction trigger and no compaction
    /// is already in progress.
    pub fn should_compact(&self) -> bool {
        !self.compacting && self.level_zero_count() >= self.config.l0_compaction_trigger
    }

    /// Plan a compaction of all level-0 tables and the level-1 tables their
    /// key range overlaps, reserving table ids for the output. Returns `None`
    /// below the trigger or while another compaction is in progress.
    pub fn plan_compaction(&mut self) -> Option<CompactionJob<F>> {
        if !self.should_compact() {
            return None;
        }
        let level_zero = self.tables.iter().filter(|live| live.record.level == 0);
        let min = level_zero.clone().map(|live| &live.record.min_key).min()?;
        let max = level_zero.map(|live| &live.record.max_key).max()?;
        let inputs: Vec<LiveTable> = self
            .tables
            .iter()
            .filter(|live| {
                live.record.level == 0
                    || (live.record.max_key >= *min && live.record.min_key <= *max)
            })
            .cloned()
            .collect();
        let input_bytes: u64 = inputs.iter().map(|live| live.record.file_len).sum();
        let target = self.config.target_table_bytes.max(1) as u64;
        let reserved_ids = input_bytes / target + inputs.len() as u64 + 2;
        let first_id = self.manifest.next_table_id;
        self.manifest.next_table_id += reserved_ids;
        self.compacting = true;
        Some(CompactionJob {
            fs: self.fs.clone(),
            dir: self.dir.clone(),
            tmp_dir: self.tmp_dir.clone(),
            durable: self.config.durable,
            target_table_bytes: self.config.target_table_bytes,
            inputs,
            first_id,
            reserved_ids,
        })
    }

    /// Give up a planned compaction, for example after its write failed.
    /// Any outputs it published are unlisted and are deleted on the next
    /// open.
    pub fn abort_compaction(&mut self) {
        self.compacting = false;
    }

    /// Replace a compaction's inputs with its outputs in a new manifest, then
    /// delete the input files. Tables flushed while the compaction ran stay
    /// ahead of its output in search order.
    pub fn install_compaction(&mut self, compacted: CompactedTables) -> LsmResult<()> {
        if !self.compacting {
            return Err(LsmError::Usage("no compaction is in progress"));
        }
        let inputs: HashSet<u64> = compacted.input_ids.iter().copied().collect();
        let mut tables: Vec<LiveTable> = self
            .tables
            .iter()
            .filter(|live| !inputs.contains(&live.record.id))
            .cloned()
            .collect();
        tables.extend(compacted.outputs);
        // Stable: level 0 keeps its newest-first order ahead of level 1,
        // and level 1 is ordered by key.
        tables.sort_by(|a, b| match (a.record.level, b.record.level) {
            (0, 0) => Ordering::Equal,
            (0, _) => Ordering::Less,
            (_, 0) => Ordering::Greater,
            _ => a.record.min_key.cmp(&b.record.min_key),
        });
        let mut manifest = self.manifest.clone();
        manifest.tables = tables.iter().map(|live| live.record.clone()).collect();
        write_manifest(
            &self.fs,
            &self.dir,
            &self.tmp_dir,
            &manifest,
            self.config.durable,
        )?;
        self.manifest = manifest;
        self.tables = tables;
        self.compacting = false;
        // The inputs are no longer listed. A crash before these deletions
        // leaves unlisted files that the next open removes.
        for id in inputs {
            let path = self.dir.join(table_name(id));
            if self.fs.exists(&path) {
                self.fs.remove_file(&path)?;
            }
        }
        if self.config.durable {
            self.fs.sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Plan, write, and install a compaction in one call. Returns whether one
    /// ran.
    pub fn compact(&mut self) -> LsmResult<bool> {
        let Some(job) = self.plan_compaction() else {
            return Ok(false);
        };
        let installed = job
            .write()
            .and_then(|compacted| self.install_compaction(compacted));
        if let Err(error) = installed {
            self.abort_compaction();
            return Err(error);
        }
        Ok(true)
    }
}

impl<F: FileSystem> CompactionJob<F> {
    /// Merge the inputs, dropping tombstones and shadowed versions, and
    /// publish level-1 tables of about the target size. Safe to run without
    /// the tree's lock.
    pub fn write(&self) -> LsmResult<CompactedTables> {
        let sources: Vec<Source<'_>> = self
            .inputs
            .iter()
            .map(|live| Box::new(live.table.iter(&self.fs)) as Source<'_>)
            .collect();
        let mut outputs = Vec::new();
        let mut builder = SsTableBuilder::new();
        for item in MergeIter::new(sources)? {
            let (key, entry) = item?;
            // Level 1 is the bottom level: nothing older can be hidden.
            let Some(value) = entry else {
                continue;
            };
            builder.add(&key, Some(&value))?;
            if builder.approx_len() >= self.target_table_bytes {
                let full = std::mem::take(&mut builder);
                outputs.push(self.write_table(full, outputs.len())?);
            }
        }
        if !builder.is_empty() {
            outputs.push(self.write_table(builder, outputs.len())?);
        }
        Ok(CompactedTables {
            input_ids: self.inputs.iter().map(|live| live.record.id).collect(),
            outputs,
        })
    }

    fn write_table(&self, builder: SsTableBuilder, index: usize) -> LsmResult<LiveTable> {
        if index as u64 >= self.reserved_ids {
            return Err(LsmError::Usage(
                "compaction produced more tables than it reserved",
            ));
        }
        let id = self.first_id + index as u64;
        let min_key = builder.first_key().expect("output is not empty").to_vec();
        let max_key = builder.last_key().expect("output is not empty").to_vec();
        let entry_count = builder.entry_count();
        let bytes = builder.finish();
        let name = table_name(id);
        let path = self.dir.join(&name);
        let tmp_path = self.tmp_dir.join(format!("lsm-{name}.tmp"));
        let file_len = sstable::publish(&self.fs, &tmp_path, &path, &bytes, self.durable)?;
        let table = SsTable::open(&self.fs, &path, file_len)?;
        if table.entry_count() != entry_count {
            return Err(LsmError::Corrupt(format!(
                "{} reloaded with the wrong entry count",
                path.display()
            )));
        }
        Ok(LiveTable {
            record: TableRecord {
                id,
                level: 1,
                file_len,
                entry_count,
                min_key,
                max_key,
            },
            table,
        })
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
        assert_eq!(
            get(&tree, "b"),
            None,
            "the frozen tombstone hides the table"
        );
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
        assert_eq!(
            get(&tree, "unflushed"),
            None,
            "the caller's WAL replays this"
        );
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
                ..LsmConfig::default()
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

    fn small() -> LsmConfig {
        LsmConfig {
            memtable_bytes: usize::MAX,
            l0_compaction_trigger: 3,
            target_table_bytes: 1024,
            durable: true,
        }
    }

    fn open_small(fs: &SimFs) -> LsmTree<SimFs> {
        LsmTree::open(
            fs.clone(),
            Path::new("/db/lsm"),
            Path::new("/db/tmp"),
            small(),
        )
        .unwrap()
    }

    #[test]
    fn compaction_merges_level_zero_into_sorted_level_one() {
        let fs = SimFs::new(SimConfig::new(7));
        let mut tree = open_small(&fs);
        let mut lsn = 0u64;
        // Three overlapping flushes: values, overwrites, then deletes.
        for round in 0..3u32 {
            for index in 0..200u32 {
                let key = format!("key-{index:04}");
                if round == 2 && index % 3 == 0 {
                    tree.apply(Mutation::Delete {
                        key: key.into_bytes(),
                    });
                } else {
                    tree.apply(set(&key, &format!("{round}-{index}")));
                }
                lsn += 1;
            }
            tree.flush(lsn, lsn * 10).unwrap();
        }
        assert!(tree.should_compact());
        let inputs: Vec<u64> = tree.tables().map(|table| table.id).collect();
        assert!(tree.compact().unwrap());

        assert!(tree.tables().all(|table| table.level == 1));
        let records: Vec<TableRecord> = tree.tables().cloned().collect();
        assert!(records.len() > 1, "output is split at the target size");
        for pair in records.windows(2) {
            assert!(pair[0].max_key < pair[1].min_key);
        }
        for id in inputs {
            assert!(!fs.exists(&Path::new("/db/lsm").join(table_name(id))));
        }
        let check = |tree: &LsmTree<SimFs>| {
            for index in 0..200u32 {
                let expected = (index % 3 != 0).then(|| format!("2-{index}"));
                assert_eq!(get(tree, &format!("key-{index:04}")), expected);
            }
            let entries: u64 = tree.tables().map(|table| table.entry_count).sum();
            assert_eq!(entries, 133, "tombstones and shadowed versions are gone");
        };
        check(&tree);
        assert_eq!(tree.flushed_lsn(), 600);
        drop(tree);
        fs.crash();
        check(&open_small(&fs));
    }

    #[test]
    fn flush_during_compaction_stays_newer() {
        let fs = SimFs::new(SimConfig::new(8));
        let mut tree = open_small(&fs);
        for round in 0..3u64 {
            tree.apply(set("k", &format!("old-{round}")));
            tree.apply(set(&format!("only-{round}"), "x"));
            tree.flush(round + 1, 0).unwrap();
        }
        let job = tree.plan_compaction().unwrap();
        assert!(tree.plan_compaction().is_none(), "one compaction at a time");
        tree.apply(set("k", "newest"));
        tree.flush(4, 0).unwrap();
        tree.install_compaction(job.write().unwrap()).unwrap();

        assert_eq!(get(&tree, "k").as_deref(), Some("newest"));
        assert_eq!(get(&tree, "only-1").as_deref(), Some("x"));
        assert_eq!(tree.tables().next().unwrap().level, 0);
        drop(tree);
        fs.crash();
        assert_eq!(get(&open_small(&fs), "k").as_deref(), Some("newest"));
    }

    #[test]
    fn crash_before_compaction_install_keeps_the_inputs() {
        let fs = SimFs::new(SimConfig::new(9));
        let mut tree = open_small(&fs);
        for round in 0..3u64 {
            tree.apply(set(&format!("k{round}"), "v"));
            tree.flush(round + 1, 0).unwrap();
        }
        let written = tree.plan_compaction().unwrap().write().unwrap();
        let outputs: Vec<PathBuf> = written
            .outputs
            .iter()
            .map(|output| output.table.path().to_path_buf())
            .collect();
        assert!(!outputs.is_empty());
        drop(written);
        drop(tree);
        fs.crash();

        let tree = open_small(&fs);
        assert!(outputs.iter().all(|path| !fs.exists(path)));
        assert_eq!(tree.tables().count(), 3);
        for round in 0..3 {
            assert_eq!(get(&tree, &format!("k{round}")).as_deref(), Some("v"));
        }
    }

    #[test]
    fn later_compaction_rewrites_overlapping_level_one() {
        let fs = SimFs::new(SimConfig::new(10));
        let mut tree = open_small(&fs);
        let mut lsn = 0u64;
        for round in 0..3 {
            tree.apply(set("k", &format!("v{round}")));
            lsn += 1;
            tree.flush(lsn, 0).unwrap();
        }
        tree.compact().unwrap();
        assert_eq!(get(&tree, "k").as_deref(), Some("v2"));

        tree.apply(delete("k"));
        for round in 0..3 {
            tree.apply(set(&format!("z{round}"), "z"));
            lsn += 1;
            tree.flush(lsn, 0).unwrap();
        }
        tree.compact().unwrap();
        assert_eq!(get(&tree, "k"), None);
        let entries: u64 = tree.tables().map(|table| table.entry_count).sum();
        assert_eq!(entries, 3, "the tombstone and the old value are both gone");
    }

    #[test]
    fn aborted_compaction_can_be_planned_again() {
        let fs = SimFs::new(SimConfig::new(11));
        let mut tree = open_small(&fs);
        for round in 0..3u64 {
            tree.apply(set(&format!("k{round}"), "v"));
            tree.flush(round + 1, 0).unwrap();
        }
        let first = tree.plan_compaction().unwrap();
        drop(first);
        tree.abort_compaction();
        assert!(tree.compact().unwrap());
        assert_eq!(tree.tables().count(), 1);
    }
}
