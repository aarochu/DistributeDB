//! The in-memory, sorted write buffer of the LSM tree.
//!
//! A [`MemTable`] holds the newest version of each key written since the last
//! flush, including tombstones for deletes, in key order so it can be written
//! out as an SSTable without sorting. Its approximate size decides when the
//! engine flushes it.

use std::collections::BTreeMap;

use super::{Entry, Lookup};
use crate::storage::Mutation;

/// Accounting overhead per entry, approximating the map node and allocation
/// cost on top of the key and value bytes.
const ENTRY_OVERHEAD: usize = 32;

/// Sorted map from key to its newest version since the last flush.
#[derive(Debug, Default, Clone)]
pub struct MemTable {
    entries: BTreeMap<Vec<u8>, Entry>,
    approx_bytes: usize,
}

impl MemTable {
    /// Create an empty memtable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a mutation. A delete is stored as a tombstone so it can hide
    /// older versions of the key in SSTables.
    pub fn apply(&mut self, mutation: Mutation) {
        let (key, entry) = match mutation {
            Mutation::Set { key, value } => (key, Some(value)),
            Mutation::Delete { key } => (key, None),
        };
        let key_len = key.len();
        let value_len = entry.as_ref().map_or(0, Vec::len);
        if let Some(old) = self.entries.insert(key, entry) {
            self.approx_bytes -= ENTRY_OVERHEAD + key_len + old.map_or(0, |v| v.len());
        }
        self.approx_bytes += ENTRY_OVERHEAD + key_len + value_len;
    }

    /// Look up `key` in this memtable only.
    pub fn get(&self, key: &[u8]) -> Lookup {
        match self.entries.get(key) {
            Some(Some(value)) => Lookup::Found(value.clone()),
            Some(None) => Lookup::Deleted,
            None => Lookup::Absent,
        }
    }

    /// Approximate memory held by the entries, in bytes.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Number of keys, including tombstones.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the memtable holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Entries in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Entry)> + '_ {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn newest_version_wins_and_deletes_become_tombstones() {
        let mut table = MemTable::new();
        table.apply(set(b"a", b"1"));
        table.apply(set(b"a", b"2"));
        table.apply(Mutation::Delete { key: b"b".to_vec() });
        assert_eq!(table.get(b"a"), Lookup::Found(b"2".to_vec()));
        assert_eq!(table.get(b"b"), Lookup::Deleted);
        assert_eq!(table.get(b"c"), Lookup::Absent);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn size_accounting_tracks_replacements() {
        let mut table = MemTable::new();
        table.apply(set(b"key", &[0; 100]));
        assert_eq!(table.approx_bytes(), ENTRY_OVERHEAD + 3 + 100);
        table.apply(set(b"key", &[0; 10]));
        assert_eq!(table.approx_bytes(), ENTRY_OVERHEAD + 3 + 10);
        table.apply(Mutation::Delete {
            key: b"key".to_vec(),
        });
        assert_eq!(table.approx_bytes(), ENTRY_OVERHEAD + 3);
    }

    #[test]
    fn iteration_is_sorted() {
        let mut table = MemTable::new();
        for key in [b"c", b"a", b"b"] {
            table.apply(set(key, b"v"));
        }
        let keys: Vec<&[u8]> = table.iter().map(|(k, _)| k.as_slice()).collect();
        let expected: [&[u8]; 3] = [b"a", b"b", b"c"];
        assert_eq!(keys, expected);
    }
}
