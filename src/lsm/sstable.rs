//! SSTable v1: an immutable, sorted, checksummed run of key versions.
//!
//! An SSTable stores each key at most once, in strictly ascending order,
//! either with a value or as a tombstone. Encoding is I/O free
//! ([`SsTableBuilder`]); [`publish`] writes a table atomically, and
//! [`SsTable`] reads one block per lookup through
//! [`FileSystem::read_at`].
//!
//! # Layout
//!
//! ```text
//! file        := data_block* || bloom || index || footer(64)
//! data_block  := entry+ || block_crc32c:u32          (CRC over the entries)
//! entry       := kind:u8 | key_len:u32 | value_len:u32 | key | value
//!                kind 0 = value, 1 = tombstone (value_len 0)
//! bloom       := filter bits; bit i is bit (i % 8) of byte (i / 8)
//! index       := block_count:u32 || (offset:u64 | len:u32 | last_key_len:u32 | last_key)*
//!                len excludes the block CRC
//! footer      := magic "DDBSST01" [0:8] | version:u16 [8:10] = 1
//!                | footer_len:u16 [10:12] = 64 | entry_count:u64 [12:20]
//!                | index_offset:u64 [20:28] | index_len:u32 [28:32]
//!                | index_crc32c:u32 [32:36] | bloom_offset:u64 [36:44]
//!                | bloom_len:u32 [44:48] | bloom_crc32c:u32 [48:52]
//!                | bloom_hashes:u8 [52] | reserved zero [53:60]
//!                | footer_crc32c:u32 [60:64] over bytes 0..60
//! ```
//!
//! All integers are little-endian. Blocks hold about [`BLOCK_TARGET_BYTES`]
//! of entries. The bloom filter uses ten bits per key and seven probes
//! derived from the key's CRC64. Opening a table validates the footer, the
//! section offsets against the file length, the bloom and index checksums,
//! contiguous block handles, and ascending index keys; each block's checksum
//! and key order are checked when it is read.

use std::path::{Path, PathBuf};

use super::{Entry, Lookup, LsmError, LsmResult};
use crate::checksum::{crc32c, crc64_ecma};
use crate::fileio::FileSystem;

/// Magic bytes at the start of every SSTable footer.
pub const SSTABLE_MAGIC: [u8; 8] = *b"DDBSST01";
/// SSTable format version.
pub const SSTABLE_VERSION: u16 = 1;
/// Fixed footer length in bytes.
pub const FOOTER_LEN: usize = 64;
/// A data block is closed once its entries reach this many bytes.
pub const BLOCK_TARGET_BYTES: usize = 4096;
/// Largest key plus value accepted in one entry.
pub const MAX_ENTRY_BYTES: usize = 64 * 1024 * 1024;

const KIND_VALUE: u8 = 0;
const KIND_TOMBSTONE: u8 = 1;
const ENTRY_HEADER_LEN: usize = 9;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 7;
const MIN_BLOOM_BITS: usize = 64;

/// Probabilistic membership filter: no false negatives, about 1% false
/// positives at ten bits per key.
#[derive(Debug, Clone)]
struct Bloom {
    bits: Vec<u8>,
    hashes: u8,
}

impl Bloom {
    fn build(key_hashes: &[u64]) -> Self {
        let nbits = (key_hashes.len() * BLOOM_BITS_PER_KEY)
            .max(MIN_BLOOM_BITS)
            .next_multiple_of(8);
        let mut bits = vec![0u8; nbits / 8];
        for &hash in key_hashes {
            for bit in bit_positions(hash, nbits as u64, BLOOM_HASHES) {
                bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
            }
        }
        Self {
            bits,
            hashes: BLOOM_HASHES,
        }
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = self.bits.len() as u64 * 8;
        bit_positions(crc64_ecma(key), nbits, self.hashes)
            .all(|bit| self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) != 0)
    }
}

/// Double hashing: probe `i` is `h1 + i * h2` modulo the filter size.
fn bit_positions(hash: u64, nbits: u64, count: u8) -> impl Iterator<Item = u64> {
    let h1 = hash & 0xffff_ffff;
    let h2 = (hash >> 32) | 1;
    (0..u64::from(count)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % nbits)
}

/// Location and last key of one data block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockHandle {
    offset: u64,
    len: u32,
    last_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    entry_count: u64,
    index_offset: u64,
    index_len: u32,
    index_crc: u32,
    bloom_offset: u64,
    bloom_len: u32,
    bloom_crc: u32,
    bloom_hashes: u8,
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().expect("two bytes"))
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

impl Footer {
    fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut b = [0u8; FOOTER_LEN];
        b[0..8].copy_from_slice(&SSTABLE_MAGIC);
        b[8..10].copy_from_slice(&SSTABLE_VERSION.to_le_bytes());
        b[10..12].copy_from_slice(&(FOOTER_LEN as u16).to_le_bytes());
        b[12..20].copy_from_slice(&self.entry_count.to_le_bytes());
        b[20..28].copy_from_slice(&self.index_offset.to_le_bytes());
        b[28..32].copy_from_slice(&self.index_len.to_le_bytes());
        b[32..36].copy_from_slice(&self.index_crc.to_le_bytes());
        b[36..44].copy_from_slice(&self.bloom_offset.to_le_bytes());
        b[44..48].copy_from_slice(&self.bloom_len.to_le_bytes());
        b[48..52].copy_from_slice(&self.bloom_crc.to_le_bytes());
        b[52] = self.bloom_hashes;
        let crc = crc32c(&b[..60]);
        b[60..64].copy_from_slice(&crc.to_le_bytes());
        b
    }

    fn decode(b: &[u8]) -> Result<Self, String> {
        if b.len() != FOOTER_LEN {
            return Err("footer has the wrong length".into());
        }
        if b[0..8] != SSTABLE_MAGIC {
            return Err("bad SSTable magic".into());
        }
        if u16_at(b, 8) != SSTABLE_VERSION {
            return Err(format!("unsupported SSTable version {}", u16_at(b, 8)));
        }
        if usize::from(u16_at(b, 10)) != FOOTER_LEN {
            return Err("unexpected footer length".into());
        }
        if crc32c(&b[..60]) != u32_at(b, 60) {
            return Err("footer checksum mismatch".into());
        }
        if b[53..60].iter().any(|&byte| byte != 0) {
            return Err("reserved footer bytes are not zero".into());
        }
        if b[52] == 0 {
            return Err("bloom filter has no hash functions".into());
        }
        Ok(Self {
            entry_count: u64_at(b, 12),
            index_offset: u64_at(b, 20),
            index_len: u32_at(b, 28),
            index_crc: u32_at(b, 32),
            bloom_offset: u64_at(b, 36),
            bloom_len: u32_at(b, 44),
            bloom_crc: u32_at(b, 48),
            bloom_hashes: b[52],
        })
    }
}

/// Encodes a table in memory from keys added in strictly ascending order.
#[derive(Debug, Default)]
pub struct SsTableBuilder {
    out: Vec<u8>,
    block: Vec<u8>,
    index: Vec<BlockHandle>,
    key_hashes: Vec<u64>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    entries: u64,
}

impl SsTableBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `key` with `value`, or a tombstone when `value` is `None`.
    /// Keys must be strictly ascending.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> LsmResult<()> {
        if let Some(last) = &self.last_key {
            if key <= last.as_slice() {
                return Err(LsmError::Usage("SSTable keys must be strictly ascending"));
            }
        }
        let value_bytes = value.unwrap_or(&[]);
        if key.len() + value_bytes.len() > MAX_ENTRY_BYTES {
            return Err(LsmError::Usage("SSTable entry exceeds the size limit"));
        }
        self.block.push(if value.is_some() {
            KIND_VALUE
        } else {
            KIND_TOMBSTONE
        });
        self.block
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.block
            .extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        self.block.extend_from_slice(key);
        self.block.extend_from_slice(value_bytes);
        self.key_hashes.push(crc64_ecma(key));
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
        self.entries += 1;
        if self.block.len() >= BLOCK_TARGET_BYTES {
            self.finish_block();
        }
        Ok(())
    }

    fn finish_block(&mut self) {
        if self.block.is_empty() {
            return;
        }
        let last_key = self
            .last_key
            .clone()
            .expect("a non-empty block has a last key");
        self.index.push(BlockHandle {
            offset: self.out.len() as u64,
            len: self.block.len() as u32,
            last_key,
        });
        let crc = crc32c(&self.block);
        self.out.extend_from_slice(&self.block);
        self.out.extend_from_slice(&crc.to_le_bytes());
        self.block.clear();
    }

    /// Entries added so far.
    pub fn entry_count(&self) -> u64 {
        self.entries
    }

    /// Whether no entry has been added.
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Approximate encoded size so far, for splitting large outputs.
    pub fn approx_len(&self) -> usize {
        self.out.len() + self.block.len()
    }

    /// Smallest key added.
    pub fn first_key(&self) -> Option<&[u8]> {
        self.first_key.as_deref()
    }

    /// Largest key added.
    pub fn last_key(&self) -> Option<&[u8]> {
        self.last_key.as_deref()
    }

    /// Close the last block and append the bloom filter, index, and footer.
    pub fn finish(mut self) -> Vec<u8> {
        self.finish_block();
        let bloom = Bloom::build(&self.key_hashes);
        let bloom_offset = self.out.len() as u64;
        let bloom_crc = crc32c(&bloom.bits);
        self.out.extend_from_slice(&bloom.bits);

        let mut index = Vec::new();
        index.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for handle in &self.index {
            index.extend_from_slice(&handle.offset.to_le_bytes());
            index.extend_from_slice(&handle.len.to_le_bytes());
            index.extend_from_slice(&(handle.last_key.len() as u32).to_le_bytes());
            index.extend_from_slice(&handle.last_key);
        }
        let index_offset = self.out.len() as u64;
        let index_crc = crc32c(&index);
        self.out.extend_from_slice(&index);

        let footer = Footer {
            entry_count: self.entries,
            index_offset,
            index_len: index.len() as u32,
            index_crc,
            bloom_offset,
            bloom_len: bloom.bits.len() as u32,
            bloom_crc,
            bloom_hashes: bloom.hashes,
        };
        self.out.extend_from_slice(&footer.encode());
        self.out
    }
}

/// Bounds-checked sequential reader over a byte slice.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .at
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| "length overruns its section".to_string())?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32_at(self.take(4)?, 0))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64_at(self.take(8)?, 0))
    }

    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }
}

/// Decode and validate the index against the length of the data section.
fn decode_index(bytes: &[u8], data_len: u64) -> Result<Vec<BlockHandle>, String> {
    let mut reader = Reader { bytes, at: 0 };
    let count = reader.u32()?;
    let mut handles: Vec<BlockHandle> = Vec::new();
    let mut expected_offset = 0u64;
    for _ in 0..count {
        let offset = reader.u64()?;
        let len = reader.u32()?;
        let key_len = reader.u32()? as usize;
        let last_key = reader.take(key_len)?.to_vec();
        if offset != expected_offset || len == 0 {
            return Err("data blocks are not contiguous".into());
        }
        if let Some(previous) = handles.last() {
            if last_key <= previous.last_key {
                return Err("index keys are not ascending".into());
            }
        }
        expected_offset = offset
            .checked_add(u64::from(len) + 4)
            .ok_or_else(|| "block offset overflows".to_string())?;
        handles.push(BlockHandle {
            offset,
            len,
            last_key,
        });
    }
    if !reader.done() {
        return Err("trailing bytes after the index".into());
    }
    if expected_offset != data_len {
        return Err("data blocks do not fill the data section".into());
    }
    Ok(handles)
}

/// Decode one block's entries, requiring ascending keys and well-formed
/// tombstones.
fn decode_block(bytes: &[u8]) -> Result<Vec<(Vec<u8>, Entry)>, String> {
    let mut reader = Reader { bytes, at: 0 };
    let mut entries: Vec<(Vec<u8>, Entry)> = Vec::new();
    while !reader.done() {
        let header = reader.take(ENTRY_HEADER_LEN)?;
        let kind = header[0];
        let key_len = u32_at(header, 1) as usize;
        let value_len = u32_at(header, 5) as usize;
        let key = reader.take(key_len)?.to_vec();
        let value = reader.take(value_len)?;
        let entry = match kind {
            KIND_VALUE => Some(value.to_vec()),
            KIND_TOMBSTONE if value_len == 0 => None,
            KIND_TOMBSTONE => return Err("tombstone carries a value".into()),
            other => return Err(format!("unknown entry kind {other}")),
        };
        if let Some((previous, _)) = entries.last() {
            if key <= *previous {
                return Err("block keys are not ascending".into());
            }
        }
        entries.push((key, entry));
    }
    if entries.is_empty() {
        return Err("empty data block".into());
    }
    Ok(entries)
}

/// Atomically publish an encoded table at `final_path`: write it to
/// `tmp_path` on the same filesystem, sync it, rename it into place, then
/// sync both directories. A crash leaves either no table at `final_path` or
/// the complete table. Returns the table's length in bytes.
pub fn publish<F: FileSystem>(
    fs: &F,
    tmp_path: &Path,
    final_path: &Path,
    bytes: &[u8],
    durable: bool,
) -> LsmResult<u64> {
    if fs.exists(tmp_path) {
        fs.truncate(tmp_path, 0)?;
    } else {
        fs.create_file(tmp_path)?;
    }
    fs.append(tmp_path, bytes)?;
    if durable {
        fs.sync_file(tmp_path)?;
    }
    fs.rename(tmp_path, final_path)?;
    if durable {
        if let Some(dir) = final_path.parent() {
            fs.sync_dir(dir)?;
        }
        if let Some(dir) = tmp_path.parent() {
            fs.sync_dir(dir)?;
        }
    }
    Ok(bytes.len() as u64)
}

/// An open, validated SSTable. Lookups read one block from disk; the index
/// and bloom filter stay in memory.
#[derive(Debug, Clone)]
pub struct SsTable {
    path: PathBuf,
    file_len: u64,
    entry_count: u64,
    index: Vec<BlockHandle>,
    bloom: Bloom,
}

impl SsTable {
    /// Open the table at `path`, whose length is `file_len` bytes (recorded
    /// by the caller when the table was published).
    pub fn open<F: FileSystem>(fs: &F, path: &Path, file_len: u64) -> LsmResult<Self> {
        let corrupt = |detail: &str| LsmError::Corrupt(format!("{}: {detail}", path.display()));
        if file_len < FOOTER_LEN as u64 {
            return Err(corrupt("shorter than its footer"));
        }
        let footer_offset = file_len - FOOTER_LEN as u64;
        let footer = Footer::decode(&fs.read_at(path, footer_offset, FOOTER_LEN)?)
            .map_err(|detail| corrupt(&detail))?;
        let sections_fit = footer.index_offset.checked_add(u64::from(footer.index_len))
            == Some(footer_offset)
            && footer.bloom_offset.checked_add(u64::from(footer.bloom_len))
                == Some(footer.index_offset);
        if !sections_fit || footer.bloom_len == 0 {
            return Err(corrupt("section offsets do not match the file length"));
        }
        let bits = fs.read_at(path, footer.bloom_offset, footer.bloom_len as usize)?;
        if crc32c(&bits) != footer.bloom_crc {
            return Err(corrupt("bloom filter checksum mismatch"));
        }
        let index_bytes = fs.read_at(path, footer.index_offset, footer.index_len as usize)?;
        if crc32c(&index_bytes) != footer.index_crc {
            return Err(corrupt("index checksum mismatch"));
        }
        let index =
            decode_index(&index_bytes, footer.bloom_offset).map_err(|detail| corrupt(&detail))?;
        if index.is_empty() != (footer.entry_count == 0) {
            return Err(corrupt("entry count disagrees with the index"));
        }
        Ok(Self {
            path: path.to_path_buf(),
            file_len,
            entry_count: footer.entry_count,
            index,
            bloom: Bloom {
                bits,
                hashes: footer.bloom_hashes,
            },
        })
    }

    /// The table's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The table's length in bytes.
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Number of entries, including tombstones.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Largest key in the table.
    pub fn max_key(&self) -> Option<&[u8]> {
        self.index.last().map(|handle| handle.last_key.as_slice())
    }

    /// Look up `key` in this table only.
    pub fn get<F: FileSystem>(&self, fs: &F, key: &[u8]) -> LsmResult<Lookup> {
        if !self.bloom.may_contain(key) {
            return Ok(Lookup::Absent);
        }
        let block = self
            .index
            .partition_point(|handle| handle.last_key.as_slice() < key);
        if block == self.index.len() {
            return Ok(Lookup::Absent);
        }
        for (entry_key, entry) in self.read_block(fs, block)? {
            if entry_key.as_slice() == key {
                return Ok(match entry {
                    Some(value) => Lookup::Found(value),
                    None => Lookup::Deleted,
                });
            }
            if entry_key.as_slice() > key {
                break;
            }
        }
        Ok(Lookup::Absent)
    }

    /// Every entry in ascending key order, read one block at a time.
    pub fn iter<'a, F: FileSystem>(&'a self, fs: &'a F) -> TableIter<'a, F> {
        TableIter {
            table: self,
            fs,
            next_block: 0,
            current: Vec::new().into_iter(),
            failed: false,
        }
    }

    fn read_block<F: FileSystem>(&self, fs: &F, block: usize) -> LsmResult<Vec<(Vec<u8>, Entry)>> {
        let handle = &self.index[block];
        let corrupt = |detail: &str| {
            LsmError::Corrupt(format!("{} block {block}: {detail}", self.path.display()))
        };
        let len = handle.len as usize;
        let bytes = fs.read_at(&self.path, handle.offset, len + 4)?;
        let (data, crc) = bytes.split_at(len);
        if crc32c(data) != u32_at(crc, 0) {
            return Err(corrupt("checksum mismatch"));
        }
        let entries = decode_block(data).map_err(|detail| corrupt(&detail))?;
        let last = &entries[entries.len() - 1].0;
        if *last != handle.last_key {
            return Err(corrupt("last key disagrees with the index"));
        }
        if block > 0 && entries[0].0 <= self.index[block - 1].last_key {
            return Err(corrupt("keys overlap the previous block"));
        }
        Ok(entries)
    }
}

/// Iterator over an [`SsTable`]. A read or validation error is yielded once
/// and ends the iteration.
pub struct TableIter<'a, F: FileSystem> {
    table: &'a SsTable,
    fs: &'a F,
    next_block: usize,
    current: std::vec::IntoIter<(Vec<u8>, Entry)>,
    failed: bool,
}

impl<F: FileSystem> Iterator for TableIter<'_, F> {
    type Item = LsmResult<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(item) = self.current.next() {
                return Some(Ok(item));
            }
            if self.next_block >= self.table.index.len() {
                return None;
            }
            match self.table.read_block(self.fs, self.next_block) {
                Ok(entries) => {
                    self.next_block += 1;
                    self.current = entries.into_iter();
                }
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::{SimConfig, SimFs};

    fn key(i: u32) -> Vec<u8> {
        format!("key-{i:06}").into_bytes()
    }

    /// Values vary in length, including empty values.
    fn value(i: u32) -> Vec<u8> {
        vec![(i % 251) as u8; (i % 40) as usize]
    }

    fn is_tombstone(i: u32) -> bool {
        i % 10 == 3
    }

    fn build(n: u32) -> Vec<u8> {
        let mut builder = SsTableBuilder::new();
        for i in 0..n {
            let value = value(i);
            let entry = (!is_tombstone(i)).then_some(value.as_slice());
            builder.add(&key(i), entry).unwrap();
        }
        builder.finish()
    }

    fn publish_sim(fs: &SimFs, bytes: &[u8]) -> (PathBuf, u64) {
        fs.create_dir_all(Path::new("/t/tmp")).unwrap();
        fs.create_dir_all(Path::new("/t/lsm")).unwrap();
        let path = PathBuf::from("/t/lsm/00000000000000000001.sst");
        let len = publish(fs, Path::new("/t/tmp/table.tmp"), &path, bytes, true).unwrap();
        (path, len)
    }

    fn open_sim(fs: &SimFs, bytes: &[u8]) -> SsTable {
        let (path, len) = publish_sim(fs, bytes);
        SsTable::open(fs, &path, len).unwrap()
    }

    #[test]
    fn point_lookups_cover_values_tombstones_and_misses() {
        let fs = SimFs::new(SimConfig::new(1));
        let table = open_sim(&fs, &build(2000));
        assert!(table.index.len() > 1, "test needs several blocks");
        assert_eq!(table.entry_count(), 2000);
        for i in 0..2000 {
            let expected = if is_tombstone(i) {
                Lookup::Deleted
            } else {
                Lookup::Found(value(i))
            };
            assert_eq!(table.get(&fs, &key(i)).unwrap(), expected, "key {i}");
        }
        for absent in [&b""[..], b"key-", b"key-000000x", b"zzz"] {
            assert_eq!(table.get(&fs, absent).unwrap(), Lookup::Absent);
        }
        assert_eq!(table.max_key(), Some(key(1999).as_slice()));
    }

    #[test]
    fn iteration_returns_every_entry_in_order() {
        let fs = SimFs::new(SimConfig::new(2));
        let table = open_sim(&fs, &build(2000));
        let entries: Vec<(Vec<u8>, Entry)> = table.iter(&fs).collect::<LsmResult<_>>().unwrap();
        assert_eq!(entries.len(), 2000);
        for (i, (k, entry)) in entries.iter().enumerate() {
            let i = i as u32;
            assert_eq!(k, &key(i));
            let expected = (!is_tombstone(i)).then(|| value(i));
            assert_eq!(entry, &expected);
        }
    }

    #[test]
    fn empty_table_round_trips() {
        let fs = SimFs::new(SimConfig::new(3));
        let table = open_sim(&fs, &SsTableBuilder::new().finish());
        assert_eq!(table.entry_count(), 0);
        assert_eq!(table.get(&fs, b"anything").unwrap(), Lookup::Absent);
        assert_eq!(table.iter(&fs).count(), 0);
        assert_eq!(table.max_key(), None);
    }

    #[test]
    fn bloom_filter_has_no_false_negatives_and_few_false_positives() {
        let fs = SimFs::new(SimConfig::new(4));
        let table = open_sim(&fs, &build(1000));
        assert!((0..1000).all(|i| table.bloom.may_contain(&key(i))));
        let false_positives = (0..10_000)
            .filter(|i| table.bloom.may_contain(format!("absent-{i}").as_bytes()))
            .count();
        assert!(false_positives < 300, "{false_positives} false positives");
    }

    #[test]
    fn builder_rejects_unsorted_and_duplicate_keys() {
        let mut builder = SsTableBuilder::new();
        builder.add(b"b", Some(b"1")).unwrap();
        assert!(matches!(
            builder.add(b"b", Some(b"2")),
            Err(LsmError::Usage(_))
        ));
        assert!(matches!(builder.add(b"a", None), Err(LsmError::Usage(_))));
        assert_eq!(builder.entry_count(), 1);
        assert_eq!(builder.first_key(), Some(&b"b"[..]));
    }

    #[test]
    fn corrupted_block_is_reported_not_hidden() {
        let fs = SimFs::new(SimConfig::new(5));
        let mut bytes = build(2000);
        bytes[20] ^= 0xff;
        let table = open_sim(&fs, &bytes);
        assert!(matches!(table.get(&fs, &key(0)), Err(LsmError::Corrupt(_))));
        let first = table.iter(&fs).next().unwrap();
        assert!(matches!(first, Err(LsmError::Corrupt(_))));
        assert!(table.iter(&fs).nth(1).is_none());
    }

    #[test]
    fn damaged_footer_or_wrong_length_fails_open() {
        let bytes = build(100);
        let mut damaged = bytes.clone();
        let footer_byte = damaged.len() - 10;
        damaged[footer_byte] ^= 1;
        let fs = SimFs::new(SimConfig::new(6));
        let (path, len) = publish_sim(&fs, &damaged);
        assert!(matches!(
            SsTable::open(&fs, &path, len),
            Err(LsmError::Corrupt(_))
        ));

        let fs = SimFs::new(SimConfig::new(7));
        let (path, len) = publish_sim(&fs, &bytes);
        assert!(matches!(
            SsTable::open(&fs, &path, len - 1),
            Err(LsmError::Corrupt(_))
        ));
        assert!(SsTable::open(&fs, &path, len).is_ok());
    }

    #[test]
    fn published_table_survives_a_crash() {
        let fs = SimFs::new(SimConfig::new(8));
        let (path, len) = publish_sim(&fs, &build(500));
        fs.crash();
        let table = SsTable::open(&fs, &path, len).unwrap();
        assert_eq!(table.entry_count(), 500);
        assert_eq!(table.get(&fs, &key(42)).unwrap(), Lookup::Found(value(42)));
    }
}
