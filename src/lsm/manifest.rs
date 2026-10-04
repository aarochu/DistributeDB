//! The LSM manifest: which SSTables are live and what they cover.
//!
//! The manifest is the LSM tree's single source of truth. A table file that
//! the manifest does not list is garbage, and a listed table that is missing
//! or damaged is an error. The manifest is replaced atomically (temp write,
//! sync, rename, directory sync), so a crash leaves either the old or the new
//! version.
//!
//! `flushed_lsn` is the highest WAL LSN whose effect is contained in the
//! listed tables. After a restart the caller replays only WAL records above
//! it, and may reclaim WAL wholly at or below it. `flushed_hash` is the
//! record hash at that LSN, the boundary a replica's history is checked
//! against.
//!
//! # Layout
//!
//! ```text
//! manifest := magic "DDBLSM01" | version:u16 = 1 | reserved:u16 = 0
//!             | next_table_id:u64 | flushed_lsn:u64 | flushed_hash:u64
//!             | table_count:u32 | table* | crc32c:u32 (over everything before it)
//! table    := id:u64 | level:u8 | file_len:u64 | entry_count:u64
//!             | min_key_len:u32 | min_key | max_key_len:u32 | max_key
//! ```
//!
//! All integers are little-endian. Tables are listed in the order the tree
//! searches them.

use std::collections::HashSet;

use super::sstable::{u32_at, Reader};
use crate::checksum::crc32c;

/// Magic bytes at the start of a manifest.
pub const MANIFEST_MAGIC: [u8; 8] = *b"DDBLSM01";
/// Manifest format version.
pub const MANIFEST_VERSION: u16 = 1;

/// One live SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRecord {
    /// File name stem; unique and increasing in creation order.
    pub id: u64,
    /// 0 for flushed tables, which may overlap; higher levels are produced
    /// by compaction.
    pub level: u8,
    /// Length of the table file in bytes.
    pub file_len: u64,
    /// Entries in the table, including tombstones.
    pub entry_count: u64,
    /// Smallest key in the table.
    pub min_key: Vec<u8>,
    /// Largest key in the table.
    pub max_key: Vec<u8>,
}

/// The decoded manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The id the next table will receive.
    pub next_table_id: u64,
    /// Highest LSN contained in the tables.
    pub flushed_lsn: u64,
    /// Record hash at `flushed_lsn` (0 at LSN 0).
    pub flushed_hash: u64,
    /// Live tables in search order.
    pub tables: Vec<TableRecord>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            next_table_id: 1,
            flushed_lsn: 0,
            flushed_hash: 0,
            tables: Vec::new(),
        }
    }
}

impl Manifest {
    /// Encode the manifest, including its trailing checksum.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MANIFEST_MAGIC);
        out.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.next_table_id.to_le_bytes());
        out.extend_from_slice(&self.flushed_lsn.to_le_bytes());
        out.extend_from_slice(&self.flushed_hash.to_le_bytes());
        out.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());
        for table in &self.tables {
            out.extend_from_slice(&table.id.to_le_bytes());
            out.push(table.level);
            out.extend_from_slice(&table.file_len.to_le_bytes());
            out.extend_from_slice(&table.entry_count.to_le_bytes());
            out.extend_from_slice(&(table.min_key.len() as u32).to_le_bytes());
            out.extend_from_slice(&table.min_key);
            out.extend_from_slice(&(table.max_key.len() as u32).to_le_bytes());
            out.extend_from_slice(&table.max_key);
        }
        let crc = crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode and validate a manifest. Every failure is fail closed.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("manifest is truncated".into());
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32c(body) != u32_at(crc, 0) {
            return Err("manifest checksum mismatch".into());
        }
        let mut reader = Reader {
            bytes: body,
            at: 0,
        };
        if reader.take(8)? != MANIFEST_MAGIC {
            return Err("bad manifest magic".into());
        }
        let header = reader.take(4)?;
        let version = u16::from_le_bytes([header[0], header[1]]);
        if version != MANIFEST_VERSION {
            return Err(format!("unsupported manifest version {version}"));
        }
        if header[2..4] != [0, 0] {
            return Err("reserved manifest bytes are not zero".into());
        }
        let next_table_id = reader.u64()?;
        let flushed_lsn = reader.u64()?;
        let flushed_hash = reader.u64()?;
        let count = reader.u32()?;
        let mut tables = Vec::new();
        let mut ids = HashSet::new();
        for _ in 0..count {
            let id = reader.u64()?;
            let level = reader.take(1)?[0];
            let file_len = reader.u64()?;
            let entry_count = reader.u64()?;
            let min_len = reader.u32()? as usize;
            let min_key = reader.take(min_len)?.to_vec();
            let max_len = reader.u32()? as usize;
            let max_key = reader.take(max_len)?.to_vec();
            if id == 0 || id >= next_table_id || !ids.insert(id) {
                return Err(format!("invalid or duplicate table id {id}"));
            }
            if min_key > max_key || entry_count == 0 {
                return Err(format!("table {id} has an invalid key range or count"));
            }
            tables.push(TableRecord {
                id,
                level,
                file_len,
                entry_count,
                min_key,
                max_key,
            });
        }
        if !reader.done() {
            return Err("trailing bytes in the manifest".into());
        }
        Ok(Self {
            next_table_id,
            flushed_lsn,
            flushed_hash,
            tables,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            next_table_id: 4,
            flushed_lsn: 1234,
            flushed_hash: 0xdead_beef,
            tables: vec![
                TableRecord {
                    id: 3,
                    level: 0,
                    file_len: 4096,
                    entry_count: 10,
                    min_key: b"a".to_vec(),
                    max_key: b"m".to_vec(),
                },
                TableRecord {
                    id: 1,
                    level: 1,
                    file_len: 8192,
                    entry_count: 20,
                    min_key: b"b".to_vec(),
                    max_key: b"b".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn round_trips() {
        let manifest = sample();
        assert_eq!(Manifest::decode(&manifest.encode()).unwrap(), manifest);
        let empty = Manifest::default();
        assert_eq!(Manifest::decode(&empty.encode()).unwrap(), empty);
    }

    #[test]
    fn any_damaged_byte_is_rejected() {
        let bytes = sample().encode();
        for index in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[index] ^= 0x40;
            assert!(Manifest::decode(&damaged).is_err(), "byte {index}");
        }
        assert!(Manifest::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn rejects_inconsistent_tables() {
        let mut duplicate = sample();
        duplicate.tables[1].id = 3;
        assert!(Manifest::decode(&duplicate.encode()).is_err());

        let mut unallocated = sample();
        unallocated.tables[0].id = 4;
        assert!(Manifest::decode(&unallocated.encode()).is_err());

        let mut inverted = sample();
        inverted.tables[0].min_key = b"z".to_vec();
        assert!(Manifest::decode(&inverted.encode()).is_err());
    }
}
