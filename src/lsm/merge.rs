//! K-way merge of sorted key-version streams.
//!
//! Compaction and full scans combine a memtable and several SSTables. Each
//! source yields strictly ascending keys; sources are ordered newest first.
//! [`MergeIter`] yields every key once, with the version from the newest
//! source that holds it. Tombstones are yielded too: only the caller knows
//! whether an older component could still hold a value the tombstone hides.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::{Entry, LsmError, LsmResult};

/// One sorted input to a merge.
pub type Source<'a> = Box<dyn Iterator<Item = LsmResult<(Vec<u8>, Entry)>> + 'a>;

/// Merges sorted sources, newest first. A source error is yielded once,
/// after any key already taken from the merge, and ends the iteration.
pub struct MergeIter<'a> {
    sources: Vec<Source<'a>>,
    /// The entry for each source's current key; the key itself is in `heap`.
    heads: Vec<Option<Entry>>,
    /// Current keys, smallest first; ties go to the lower (newer) source.
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    pending_error: Option<LsmError>,
    done: bool,
}

impl<'a> MergeIter<'a> {
    /// Start a merge over `sources`, ordered newest first.
    pub fn new(sources: Vec<Source<'a>>) -> LsmResult<Self> {
        let mut merge = Self {
            heads: vec![None; sources.len()],
            sources,
            heap: BinaryHeap::new(),
            pending_error: None,
            done: false,
        };
        for index in 0..merge.sources.len() {
            merge.advance(index)?;
        }
        Ok(merge)
    }

    fn advance(&mut self, index: usize) -> LsmResult<()> {
        if let Some(item) = self.sources[index].next() {
            let (key, entry) = item?;
            self.heads[index] = Some(entry);
            self.heap.push(Reverse((key, index)));
        }
        Ok(())
    }
}

impl Iterator for MergeIter<'_> {
    type Item = LsmResult<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(error) = self.pending_error.take() {
            self.done = true;
            return Some(Err(error));
        }
        let Reverse((key, index)) = self.heap.pop()?;
        let entry = self.heads[index]
            .take()
            .expect("every queued key has an entry");
        if let Err(error) = self.advance(index) {
            self.pending_error = Some(error);
            return Some(Ok((key, entry)));
        }
        // Drop older versions of the same key from other sources.
        while let Some(Reverse((next_key, _))) = self.heap.peek() {
            if *next_key != key {
                break;
            }
            let Reverse((_, older)) = self.heap.pop().expect("peeked");
            self.heads[older] = None;
            if let Err(error) = self.advance(older) {
                self.pending_error = Some(error);
                break;
            }
        }
        Some(Ok((key, entry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Pair = (String, Option<String>);

    fn source(items: &[(&str, Option<&str>)]) -> Source<'static> {
        let items: Vec<LsmResult<(Vec<u8>, Entry)>> = items
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.as_bytes().to_vec(),
                    value.map(|v| v.as_bytes().to_vec()),
                ))
            })
            .collect();
        Box::new(items.into_iter())
    }

    fn collect(merge: MergeIter<'_>) -> Vec<Pair> {
        merge
            .map(|item| {
                let (key, entry) = item.unwrap();
                (
                    String::from_utf8(key).unwrap(),
                    entry.map(|v| String::from_utf8(v).unwrap()),
                )
            })
            .collect()
    }

    fn pairs(items: &[(&str, Option<&str>)]) -> Vec<Pair> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn newest_version_of_each_key_wins_including_tombstones() {
        let newest = source(&[("b", Some("new")), ("d", None)]);
        let middle = source(&[("a", Some("1")), ("b", Some("old")), ("c", Some("3"))]);
        let oldest = source(&[("b", Some("oldest")), ("d", Some("4")), ("e", Some("5"))]);
        let merged = collect(MergeIter::new(vec![newest, middle, oldest]).unwrap());
        let expected = pairs(&[
            ("a", Some("1")),
            ("b", Some("new")),
            ("c", Some("3")),
            ("d", None),
            ("e", Some("5")),
        ]);
        assert_eq!(merged, expected);
    }

    #[test]
    fn empty_and_single_sources() {
        assert!(collect(MergeIter::new(Vec::new()).unwrap()).is_empty());
        let merged = collect(MergeIter::new(vec![source(&[]), source(&[("x", None)])]).unwrap());
        assert_eq!(merged, pairs(&[("x", None)]));
    }

    #[test]
    fn source_error_follows_the_key_already_read() {
        let items: Vec<LsmResult<(Vec<u8>, Entry)>> = vec![
            Ok((b"a".to_vec(), Some(b"1".to_vec()))),
            Err(LsmError::Corrupt("bad block".into())),
            Ok((b"z".to_vec(), None)),
        ];
        let failing: Source<'static> = Box::new(items.into_iter());
        let mut merge = MergeIter::new(vec![failing]).unwrap();
        assert_eq!(
            merge.next().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert!(matches!(merge.next(), Some(Err(LsmError::Corrupt(_)))));
        assert!(merge.next().is_none());
    }
}
