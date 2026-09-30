//! Storage engine module.
//!
//! Implements the in-memory `StorageEngine` contract from Technical-Design
//! §2.1 (SOW R1). Phase 1 is single-threaded and in-memory; no WAL, LSN, or
//! networking yet (those arrive in later phases).
//!
//! Contract (Technical-Design §2.1):
//! * `SET` replaces the entire value.
//! * `DELETE` always succeeds and returns OK, including when the key is
//!   missing (a future WAL will still record a tombstone).
//! * `GET` returns [`GetResult::NotFound`] distinctly from a zero-length
//!   (empty) value.
//! * `EXISTS` returns a boolean.
//!
//! Keys and values are byte strings ([`Vec<u8>`]); a CLI may accept UTF-8 text
//! and convert it to bytes before calling into the engine.

use std::collections::HashMap;

/// Result of a [`StorageEngine::get`] lookup.
///
/// Per Technical-Design §2.1, a missing key ([`GetResult::NotFound`]) is
/// distinct from a key that maps to a zero-length value
/// ([`GetResult::Found`] with an empty `Vec`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetResult {
    /// The key exists and maps to the contained value (which may be empty).
    Found(Vec<u8>),
    /// The key does not exist.
    NotFound,
}

/// A state-changing operation applied to the [`StorageEngine`].
///
/// Modeled after the `Mutation` record in Technical-Design §2.1. In later
/// phases these records are what the WAL persists and replicates; in Phase 1
/// they are applied directly to the in-memory map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Set `key` to `value`, replacing any existing value entirely.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Delete `key`. Always succeeds, even if the key is absent.
    Delete { key: Vec<u8> },
}

/// In-memory key-value storage engine (Phase 1).
///
/// Backed by a [`HashMap<Vec<u8>, Vec<u8>>`]. Single-threaded and in-memory;
/// durability, ordering (LSN/WAL), and networking are added in later phases.
#[derive(Debug, Default, Clone)]
pub struct StorageEngine {
    map: HashMap<Vec<u8>, Vec<u8>>,
}

impl StorageEngine {
    /// Create an empty storage engine.
    pub fn new() -> Self {
        StorageEngine {
            map: HashMap::new(),
        }
    }

    /// Look up `key`.
    ///
    /// Returns [`GetResult::Found`] with a clone of the stored value (which may
    /// be empty) or [`GetResult::NotFound`] when the key is absent. An empty
    /// stored value is reported as `Found`, never `NotFound`.
    pub fn get(&self, key: &[u8]) -> GetResult {
        match self.map.get(key) {
            Some(value) => GetResult::Found(value.clone()),
            None => GetResult::NotFound,
        }
    }

    /// Return whether `key` exists.
    pub fn exists(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    /// Apply a [`Mutation`], mutating the engine state.
    ///
    /// `Set` replaces the entire value for the key. `Delete` removes the key
    /// and always succeeds, whether or not the key was present.
    pub fn apply(&mut self, mutation: Mutation) {
        match mutation {
            Mutation::Set { key, value } => {
                self.map.insert(key, value);
            }
            Mutation::Delete { key } => {
                self.map.remove(&key);
            }
        }
    }

    /// Convenience: set `key` to `value`, replacing any existing value.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.apply(Mutation::Set { key, value });
    }

    /// Convenience: delete `key`. Always succeeds (Technical-Design §2.1).
    pub fn delete(&mut self, key: Vec<u8>) {
        self.apply(Mutation::Delete { key });
    }

    /// Number of keys currently stored.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the engine holds no keys.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_get_returns_value() {
        let mut engine = StorageEngine::new();
        engine.set(b"k".to_vec(), b"v".to_vec());
        assert_eq!(engine.get(b"k"), GetResult::Found(b"v".to_vec()));
    }

    #[test]
    fn get_missing_key_returns_not_found() {
        let engine = StorageEngine::new();
        assert_eq!(engine.get(b"absent"), GetResult::NotFound);
    }

    #[test]
    fn set_replaces_existing_value_entirely() {
        let mut engine = StorageEngine::new();
        engine.set(b"k".to_vec(), b"first".to_vec());
        engine.set(b"k".to_vec(), b"second".to_vec());
        assert_eq!(engine.get(b"k"), GetResult::Found(b"second".to_vec()));
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn empty_value_is_found_not_not_found() {
        let mut engine = StorageEngine::new();
        engine.set(b"k".to_vec(), Vec::new());
        // Empty value must be distinct from a missing key.
        assert_eq!(engine.get(b"k"), GetResult::Found(Vec::new()));
        assert_ne!(engine.get(b"k"), GetResult::NotFound);
        assert!(engine.exists(b"k"));
    }

    #[test]
    fn delete_removes_existing_key() {
        let mut engine = StorageEngine::new();
        engine.set(b"k".to_vec(), b"v".to_vec());
        engine.delete(b"k".to_vec());
        assert_eq!(engine.get(b"k"), GetResult::NotFound);
        assert!(!engine.exists(b"k"));
    }

    #[test]
    fn delete_missing_key_succeeds() {
        let mut engine = StorageEngine::new();
        // Must not panic and must leave the engine in a valid empty state.
        engine.delete(b"absent".to_vec());
        assert_eq!(engine.get(b"absent"), GetResult::NotFound);
        assert!(engine.is_empty());
    }

    #[test]
    fn exists_true_and_false_cases() {
        let mut engine = StorageEngine::new();
        assert!(!engine.exists(b"k"));
        engine.set(b"k".to_vec(), b"v".to_vec());
        assert!(engine.exists(b"k"));
    }

    #[test]
    fn apply_matches_convenience_methods() {
        let mut via_apply = StorageEngine::new();
        via_apply.apply(Mutation::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        });
        via_apply.apply(Mutation::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        });
        via_apply.apply(Mutation::Delete { key: b"a".to_vec() });

        let mut via_convenience = StorageEngine::new();
        via_convenience.set(b"a".to_vec(), b"1".to_vec());
        via_convenience.set(b"b".to_vec(), b"2".to_vec());
        via_convenience.delete(b"a".to_vec());

        assert_eq!(via_apply.get(b"a"), GetResult::NotFound);
        assert_eq!(via_apply.get(b"b"), GetResult::Found(b"2".to_vec()));
        assert_eq!(via_apply.get(b"a"), via_convenience.get(b"a"));
        assert_eq!(via_apply.get(b"b"), via_convenience.get(b"b"));
        assert_eq!(via_apply.len(), via_convenience.len());
    }

    #[test]
    fn binary_non_utf8_keys_and_values_round_trip() {
        let mut engine = StorageEngine::new();
        let key = vec![0x00, 0xff, 0xfe, 0x01];
        let value = vec![0x80, 0x00, 0xc0, 0xff];
        assert!(std::str::from_utf8(&key).is_err());
        assert!(std::str::from_utf8(&value).is_err());
        engine.set(key.clone(), value.clone());
        assert_eq!(engine.get(&key), GetResult::Found(value));
        assert!(engine.exists(&key));
    }
}
