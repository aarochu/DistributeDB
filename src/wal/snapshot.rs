//! Snapshot v1 on-disk byte format (Technical-Design §7).
//!
//! A snapshot records the full key/value state of the map at a completed-group
//! sequencer boundary (an LSN *S*). Snapshot v1 is an immutable sequence of
//! key/value pairs in map iteration order; sorted output is unnecessary
//! (Technical-Design §7).
//!
//! This module is a pure, **I/O-free** encode/decode layer mirroring
//! [`crate::wal::format`]: it never touches the filesystem, so callers build
//! snapshot bytes in memory and the golden-fixture tests can pin exact bytes.
//! Publication (temp write + sync + rename), recovery wiring, and reclamation
//! live in later features; this module only owns the byte format and its
//! strict, fail-closed decoder.
//!
//! # Layout
//!
//! A complete snapshot file is `header(64) || payload(payload_len) ||
//! snapshot_crc64(8)`:
//!
//! * The fixed 64-byte [`SnapshotHeader`] (offsets pinned below).
//! * The payload: each entry is `key_len:u32 | value_len:u32 | key | value`,
//!   concatenated in map iteration order.
//! * A trailing `snapshot_crc64:u64` (little-endian) covering the entire
//!   header and payload (everything before the trailing CRC itself).
//!
//! All integers are little-endian. `header_crc32c` uses CRC32C and
//! `snapshot_crc64` uses CRC64-ECMA-182 with the same parameters as
//! Technical-Design §6.1 (reused from [`crate::checksum`]).
//!
//! # Validation
//!
//! The decoder returns a typed [`SnapshotError`] and never converts corruption
//! into success (Technical-Design §3, §7). It rejects bad magic, wrong
//! version, wrong `header_len`, an unexpected `cluster_id` (when the caller
//! supplies one to check against), a `header_crc32c` mismatch, an
//! `entry_count`/`payload_len` that disagrees with the decoded payload, any
//! entry whose lengths overrun the payload, duplicate keys, and a
//! `snapshot_crc64` mismatch over the whole header and payload.

use std::collections::HashSet;

use crate::checksum::{crc32c, crc64_ecma};

/// Magic bytes at the start of every snapshot header (Technical-Design §7).
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"DDBSNP01";
/// Snapshot format version encoded in the header.
pub const SNAPSHOT_VERSION: u16 = 1;
/// Fixed snapshot-header length in bytes.
pub const SNAPSHOT_HEADER_LEN: usize = 64;
/// Fixed per-entry overhead in the payload (`key_len:u32 + value_len:u32`).
pub const ENTRY_FIXED_OVERHEAD: usize = 8;
/// Length in bytes of the trailing `snapshot_crc64` field.
pub const SNAPSHOT_CRC64_LEN: usize = 8;

/// Errors produced when decoding a malformed snapshot.
///
/// Every variant is a fail-closed condition (Technical-Design §3, §7): none is
/// recoverable in place, and none is ever converted into a partial success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    /// The buffer was too short to hold the structure being decoded.
    Truncated {
        /// Bytes needed at minimum.
        needed: usize,
        /// Bytes actually available.
        got: usize,
    },
    /// Magic bytes did not match [`SNAPSHOT_MAGIC`].
    BadMagic {
        /// The expected magic.
        expected: [u8; 8],
        /// The magic actually found.
        found: [u8; 8],
    },
    /// The `version` field was not [`SNAPSHOT_VERSION`].
    BadVersion(u16),
    /// The `header_len` field was not [`SNAPSHOT_HEADER_LEN`].
    BadHeaderLen(u16),
    /// The `cluster_id` did not equal a caller-supplied expected value.
    ClusterMismatch {
        /// The cluster the caller expected.
        expected: [u8; 16],
        /// The cluster found in the header.
        found: [u8; 16],
    },
    /// The stored `header_crc32c` did not match the computed value.
    HeaderCrcMismatch {
        /// The CRC stored in the header.
        stored: u32,
        /// The CRC computed over bytes `0..60`.
        computed: u32,
    },
    /// The `payload_len` field did not equal the actual payload byte length.
    PayloadLenMismatch {
        /// The `payload_len` field value.
        header: u64,
        /// The payload bytes actually present.
        actual: u64,
    },
    /// The `entry_count` field did not equal the number of decoded entries.
    EntryCountMismatch {
        /// The `entry_count` field value.
        header: u64,
        /// The entries actually decoded from the payload.
        actual: u64,
    },
    /// An entry's `key_len`/`value_len` overran the remaining payload bytes.
    EntryOverrun {
        /// Byte offset within the payload where the entry began.
        offset: usize,
        /// Bytes the entry claimed to need.
        needed: usize,
        /// Payload bytes remaining from `offset`.
        remaining: usize,
    },
    /// Two entries carried the same key.
    DuplicateKey(Vec<u8>),
    /// The stored `snapshot_crc64` did not match the computed value.
    SnapshotCrcMismatch {
        /// The CRC stored at the end of the file.
        stored: u64,
        /// The CRC computed over the whole header and payload.
        computed: u64,
    },
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Truncated { needed, got } => {
                write!(f, "truncated: needed {needed} bytes, got {got}")
            }
            SnapshotError::BadMagic { expected, found } => {
                write!(f, "bad magic: expected {expected:?}, found {found:?}")
            }
            SnapshotError::BadVersion(v) => write!(f, "unsupported snapshot version {v}"),
            SnapshotError::BadHeaderLen(l) => write!(f, "bad header_len {l}"),
            SnapshotError::ClusterMismatch { expected, found } => {
                write!(
                    f,
                    "cluster mismatch: expected {expected:?}, found {found:?}"
                )
            }
            SnapshotError::HeaderCrcMismatch { stored, computed } => write!(
                f,
                "header_crc32c mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            SnapshotError::PayloadLenMismatch { header, actual } => {
                write!(f, "payload_len mismatch: header {header}, actual {actual}")
            }
            SnapshotError::EntryCountMismatch { header, actual } => {
                write!(f, "entry_count mismatch: header {header}, actual {actual}")
            }
            SnapshotError::EntryOverrun {
                offset,
                needed,
                remaining,
            } => write!(
                f,
                "entry at offset {offset} needs {needed} bytes, {remaining} remaining"
            ),
            SnapshotError::DuplicateKey(key) => {
                write!(f, "duplicate key in payload: {key:?}")
            }
            SnapshotError::SnapshotCrcMismatch { stored, computed } => write!(
                f,
                "snapshot_crc64 mismatch: stored {stored:#018x}, computed {computed:#018x}"
            ),
        }
    }
}

impl std::error::Error for SnapshotError {}

// ---------------------------------------------------------------------------
// Little-endian read helpers.
// ---------------------------------------------------------------------------

fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn read_u64(buf: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[off..off + 8]);
    u64::from_le_bytes(b)
}

// ---------------------------------------------------------------------------
// Snapshot header.
// ---------------------------------------------------------------------------

/// A decoded snapshot header (Technical-Design §7).
///
/// Layout (offsets): `magic[0:8]`, `version[8:10]`, `header_len[10:12]`,
/// `cluster_id[12:28]`, `snapshot_lsn[28:36]`, `record_hash_at_lsn[36:44]`,
/// `entry_count[44:52]`, `payload_len[52:60]`, `header_crc32c[60:64]` over
/// bytes `0..60`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// 16-byte binary cluster identifier.
    pub cluster_id: [u8; 16],
    /// The LSN *S* whose post-application map this snapshot captures.
    pub snapshot_lsn: u64,
    /// The `record_hash` at LSN *S* (zero when `snapshot_lsn == 0`, §7).
    pub record_hash_at_lsn: u64,
    /// Number of key/value entries in the payload.
    pub entry_count: u64,
    /// Total encoded payload length in bytes.
    pub payload_len: u64,
}

impl SnapshotHeader {
    /// Encode this header to its fixed 64 bytes, computing `header_crc32c`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SNAPSHOT_HEADER_LEN];
        buf[0..8].copy_from_slice(&SNAPSHOT_MAGIC);
        buf[8..10].copy_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        buf[10..12].copy_from_slice(&(SNAPSHOT_HEADER_LEN as u16).to_le_bytes());
        buf[12..28].copy_from_slice(&self.cluster_id);
        buf[28..36].copy_from_slice(&self.snapshot_lsn.to_le_bytes());
        buf[36..44].copy_from_slice(&self.record_hash_at_lsn.to_le_bytes());
        buf[44..52].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[52..60].copy_from_slice(&self.payload_len.to_le_bytes());
        let crc = crc32c(&buf[0..60]);
        buf[60..64].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a snapshot header from the first 64 bytes of `buf`.
    ///
    /// Validates magic, version, `header_len`, and the `header_crc32c`. When
    /// `expected_cluster` is `Some`, the `cluster_id` must match it. Does not
    /// validate `entry_count`/`payload_len` against a payload; that is the
    /// job of [`decode`], which decodes the full file.
    pub fn decode(buf: &[u8], expected_cluster: Option<[u8; 16]>) -> Result<Self, SnapshotError> {
        if buf.len() < SNAPSHOT_HEADER_LEN {
            return Err(SnapshotError::Truncated {
                needed: SNAPSHOT_HEADER_LEN,
                got: buf.len(),
            });
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::BadMagic {
                expected: SNAPSHOT_MAGIC,
                found: magic,
            });
        }
        let version = read_u16(buf, 8);
        if version != SNAPSHOT_VERSION {
            return Err(SnapshotError::BadVersion(version));
        }
        let header_len = read_u16(buf, 10);
        if header_len as usize != SNAPSHOT_HEADER_LEN {
            return Err(SnapshotError::BadHeaderLen(header_len));
        }
        let mut cluster_id = [0u8; 16];
        cluster_id.copy_from_slice(&buf[12..28]);
        if let Some(expected) = expected_cluster {
            if cluster_id != expected {
                return Err(SnapshotError::ClusterMismatch {
                    expected,
                    found: cluster_id,
                });
            }
        }
        let stored_crc = read_u32(buf, 60);
        let computed = crc32c(&buf[0..60]);
        if stored_crc != computed {
            return Err(SnapshotError::HeaderCrcMismatch {
                stored: stored_crc,
                computed,
            });
        }
        let snapshot_lsn = read_u64(buf, 28);
        let record_hash_at_lsn = read_u64(buf, 36);
        let entry_count = read_u64(buf, 44);
        let payload_len = read_u64(buf, 52);
        Ok(SnapshotHeader {
            cluster_id,
            snapshot_lsn,
            record_hash_at_lsn,
            entry_count,
            payload_len,
        })
    }
}

// ---------------------------------------------------------------------------
// Payload encoding.
// ---------------------------------------------------------------------------

/// Encode the payload for `pairs`: each entry is `key_len:u32 | value_len:u32 |
/// key | value`, concatenated in the given order (Technical-Design §7).
fn encode_payload(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (key, value) in pairs {
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
    }
    buf
}

/// Encode a complete snapshot file for the given boundary and key/value pairs.
///
/// Produces `header(64) || payload || snapshot_crc64(8)` with a correct
/// `header_crc32c`, `entry_count`, `payload_len`, and trailing
/// `snapshot_crc64` (Technical-Design §7). The `pairs` are written in the
/// order given (map iteration order at the sequencer boundary); sorting is not
/// required. At `snapshot_lsn == 0`, callers pass `record_hash_at_lsn == 0`.
pub fn encode(
    cluster_id: [u8; 16],
    snapshot_lsn: u64,
    record_hash_at_lsn: u64,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> Vec<u8> {
    let payload = encode_payload(pairs);
    let header = SnapshotHeader {
        cluster_id,
        snapshot_lsn,
        record_hash_at_lsn,
        entry_count: pairs.len() as u64,
        payload_len: payload.len() as u64,
    };
    let mut buf = header.encode();
    buf.extend_from_slice(&payload);
    let crc = crc64_ecma(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// A fully decoded snapshot: its header plus the reconstructed key/value pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSnapshot {
    /// The validated header fields.
    pub header: SnapshotHeader,
    /// The key/value pairs, in the order they appear in the payload.
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Strictly decode a complete snapshot file (Technical-Design §7).
///
/// Validates the header (see [`SnapshotHeader::decode`]), the trailing
/// `snapshot_crc64` over the whole header and payload, that `payload_len`
/// matches the bytes present, that each entry's lengths stay within the
/// payload, that `entry_count` matches the decoded entries, and that no key is
/// duplicated. When `expected_cluster` is `Some`, the header `cluster_id` must
/// match it. Any violation returns a typed [`SnapshotError`]; the decoder
/// never returns a partial success.
pub fn decode(
    buf: &[u8],
    expected_cluster: Option<[u8; 16]>,
) -> Result<DecodedSnapshot, SnapshotError> {
    // Need at least header + trailing crc64.
    let min = SNAPSHOT_HEADER_LEN + SNAPSHOT_CRC64_LEN;
    if buf.len() < min {
        return Err(SnapshotError::Truncated {
            needed: min,
            got: buf.len(),
        });
    }
    let header = SnapshotHeader::decode(buf, expected_cluster)?;

    // Verify the trailing snapshot_crc64 over everything before it.
    let crc_start = buf.len() - SNAPSHOT_CRC64_LEN;
    let stored_crc = read_u64(buf, crc_start);
    let computed = crc64_ecma(&buf[0..crc_start]);
    if stored_crc != computed {
        return Err(SnapshotError::SnapshotCrcMismatch {
            stored: stored_crc,
            computed,
        });
    }

    // The payload occupies the bytes between the header and the trailing crc.
    let payload = &buf[SNAPSHOT_HEADER_LEN..crc_start];
    if header.payload_len != payload.len() as u64 {
        return Err(SnapshotError::PayloadLenMismatch {
            header: header.payload_len,
            actual: payload.len() as u64,
        });
    }

    // Walk the payload entry by entry.
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut off = 0usize;
    while off < payload.len() {
        let remaining = payload.len() - off;
        if remaining < ENTRY_FIXED_OVERHEAD {
            return Err(SnapshotError::EntryOverrun {
                offset: off,
                needed: ENTRY_FIXED_OVERHEAD,
                remaining,
            });
        }
        let key_len = read_u32(payload, off) as usize;
        let value_len = read_u32(payload, off + 4) as usize;
        let needed = ENTRY_FIXED_OVERHEAD
            .checked_add(key_len)
            .and_then(|n| n.checked_add(value_len))
            .ok_or(SnapshotError::EntryOverrun {
                offset: off,
                needed: usize::MAX,
                remaining,
            })?;
        if needed > remaining {
            return Err(SnapshotError::EntryOverrun {
                offset: off,
                needed,
                remaining,
            });
        }
        let key_start = off + ENTRY_FIXED_OVERHEAD;
        let key_end = key_start + key_len;
        let value_end = key_end + value_len;
        let key = payload[key_start..key_end].to_vec();
        let value = payload[key_end..value_end].to_vec();
        if !seen.insert(key.clone()) {
            return Err(SnapshotError::DuplicateKey(key));
        }
        pairs.push((key, value));
        off = value_end;
    }

    if header.entry_count != pairs.len() as u64 {
        return Err(SnapshotError::EntryCountMismatch {
            header: header.entry_count,
            actual: pairs.len() as u64,
        });
    }

    Ok(DecodedSnapshot { header, pairs })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Golden fixtures -------------------------------------------------
    //
    // These pin the EXACT bytes and the exact CRC32C / CRC64 values for known
    // inputs, mirroring the WAL golden tests in `wal::format`. They are the
    // acceptance gate for the snapshot byte format (Technical-Design §7). The
    // CRC primitives are trusted ground truth (CRC32C("123456789")==0xE3069283,
    // CRC64("123456789")==0x6C40DF5F0B497347). Changing any spec byte fails
    // these tests.

    const CLUSTER_ID: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];

    #[test]
    fn golden_empty_snapshot_lsn_0() {
        // Empty snapshot at LSN 0: entry_count=0, payload_len=0,
        // record_hash_at_lsn=0 (§7).
        let bytes = encode(CLUSTER_ID, 0, 0, &[]);
        // Complete file = header(64) || payload(0) || crc64(8).
        assert_eq!(bytes.len(), SNAPSHOT_HEADER_LEN + SNAPSHOT_CRC64_LEN);

        // Header offsets pinned.
        assert_eq!(&bytes[0..8], b"DDBSNP01");
        assert_eq!(read_u16(&bytes, 8), 1); // version
        assert_eq!(read_u16(&bytes, 10), 64); // header_len
        assert_eq!(&bytes[12..28], &CLUSTER_ID);
        assert_eq!(read_u64(&bytes, 28), 0); // snapshot_lsn
        assert_eq!(read_u64(&bytes, 36), 0); // record_hash_at_lsn
        assert_eq!(read_u64(&bytes, 44), 0); // entry_count
        assert_eq!(read_u64(&bytes, 52), 0); // payload_len

        // Frozen header_crc32c over bytes 0..60.
        let expected_header_crc = crc32c(&bytes[0..60]);
        assert_eq!(read_u32(&bytes, 60), expected_header_crc);

        // Frozen snapshot_crc64 over the whole header+payload (the first 64
        // bytes, since the payload is empty).
        let expected_snap_crc = crc64_ecma(&bytes[0..64]);
        assert_eq!(read_u64(&bytes, 64), expected_snap_crc);

        // Round-trip.
        let decoded = decode(&bytes, Some(CLUSTER_ID)).unwrap();
        assert_eq!(decoded.header.entry_count, 0);
        assert_eq!(decoded.header.payload_len, 0);
        assert_eq!(decoded.header.snapshot_lsn, 0);
        assert_eq!(decoded.header.record_hash_at_lsn, 0);
        assert!(decoded.pairs.is_empty());
    }

    #[test]
    fn golden_small_snapshot_nonzero_lsn() {
        // Three known pairs at a nonzero LSN with a nonzero record hash.
        let pairs = vec![
            (b"user:1".to_vec(), b"Aaron".to_vec()),
            (b"user:2".to_vec(), b"Bri".to_vec()),
            (b"k".to_vec(), Vec::new()),
        ];
        let snapshot_lsn = 50_000u64;
        let record_hash = 0x0123_4567_89AB_CDEFu64;
        let bytes = encode(CLUSTER_ID, snapshot_lsn, record_hash, &pairs);

        // Header fields pinned.
        assert_eq!(&bytes[0..8], b"DDBSNP01");
        assert_eq!(read_u16(&bytes, 8), 1);
        assert_eq!(read_u16(&bytes, 10), 64);
        assert_eq!(&bytes[12..28], &CLUSTER_ID);
        assert_eq!(read_u64(&bytes, 28), snapshot_lsn);
        assert_eq!(read_u64(&bytes, 36), record_hash);
        assert_eq!(read_u64(&bytes, 44), 3); // entry_count

        // payload_len = per-entry(8) * 3 + keys(6+6+1) + values(5+3+0)
        //             = 24 + 13 + 8 = 45.
        let expected_payload_len = 45u64;
        assert_eq!(read_u64(&bytes, 52), expected_payload_len);
        assert_eq!(
            bytes.len(),
            SNAPSHOT_HEADER_LEN + expected_payload_len as usize + SNAPSHOT_CRC64_LEN
        );

        // Payload entry bytes pinned at their exact offsets.
        let p = SNAPSHOT_HEADER_LEN; // payload start = 64
                                     // Entry 0: "user:1" -> "Aaron".
        assert_eq!(read_u32(&bytes, p), 6); // key_len
        assert_eq!(read_u32(&bytes, p + 4), 5); // value_len
        assert_eq!(&bytes[p + 8..p + 14], b"user:1");
        assert_eq!(&bytes[p + 14..p + 19], b"Aaron");
        // Entry 1 starts at p + 19: "user:2" -> "Bri".
        let e1 = p + 19;
        assert_eq!(read_u32(&bytes, e1), 6);
        assert_eq!(read_u32(&bytes, e1 + 4), 3);
        assert_eq!(&bytes[e1 + 8..e1 + 14], b"user:2");
        assert_eq!(&bytes[e1 + 14..e1 + 17], b"Bri");
        // Entry 2 starts at e1 + 17: "k" -> "" (empty value).
        let e2 = e1 + 17;
        assert_eq!(read_u32(&bytes, e2), 1);
        assert_eq!(read_u32(&bytes, e2 + 4), 0);
        assert_eq!(&bytes[e2 + 8..e2 + 9], b"k");
        // Payload ends exactly at the trailing crc.
        assert_eq!(e2 + 9, SNAPSHOT_HEADER_LEN + expected_payload_len as usize);

        // Frozen header_crc32c and snapshot_crc64.
        let expected_header_crc = crc32c(&bytes[0..60]);
        assert_eq!(read_u32(&bytes, 60), expected_header_crc);
        let crc_start = bytes.len() - SNAPSHOT_CRC64_LEN;
        let expected_snap_crc = crc64_ecma(&bytes[0..crc_start]);
        assert_eq!(read_u64(&bytes, crc_start), expected_snap_crc);

        // Round-trip yields the exact pairs in order.
        let decoded = decode(&bytes, Some(CLUSTER_ID)).unwrap();
        assert_eq!(decoded.header.snapshot_lsn, snapshot_lsn);
        assert_eq!(decoded.header.record_hash_at_lsn, record_hash);
        assert_eq!(decoded.pairs, pairs);
    }

    #[test]
    fn round_trip_empty_payload() {
        let bytes = encode(CLUSTER_ID, 0, 0, &[]);
        let decoded = decode(&bytes, None).unwrap();
        assert!(decoded.pairs.is_empty());
        assert_eq!(decoded.header.cluster_id, CLUSTER_ID);
    }

    #[test]
    fn round_trip_binary_non_utf8_pairs() {
        let pairs = vec![
            (vec![0x00, 0x80, 0xff, 0xc0], vec![0xfe, 0x00, 0x01, 0x80]),
            (vec![0xff], Vec::new()),
        ];
        assert!(std::str::from_utf8(&pairs[0].0).is_err());
        let bytes = encode(CLUSTER_ID, 7, 42, &pairs);
        let decoded = decode(&bytes, Some(CLUSTER_ID)).unwrap();
        assert_eq!(decoded.pairs, pairs);
    }

    #[test]
    fn decode_hand_built_bytes_is_endian_independent() {
        // Build a one-entry snapshot by hand, little-endian, and decode it.
        let mut header = vec![0u8; SNAPSHOT_HEADER_LEN];
        header[0..8].copy_from_slice(b"DDBSNP01");
        header[8..10].copy_from_slice(&1u16.to_le_bytes());
        header[10..12].copy_from_slice(&64u16.to_le_bytes());
        header[12..28].copy_from_slice(&CLUSTER_ID);
        header[28..36].copy_from_slice(&3u64.to_le_bytes()); // snapshot_lsn
        header[36..44].copy_from_slice(&99u64.to_le_bytes()); // record_hash
        header[44..52].copy_from_slice(&1u64.to_le_bytes()); // entry_count
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u32.to_le_bytes()); // key_len
        payload.extend_from_slice(&3u32.to_le_bytes()); // value_len
        payload.extend_from_slice(b"ab");
        payload.extend_from_slice(b"xyz");
        header[52..60].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let crc = crc32c(&header[0..60]);
        header[60..64].copy_from_slice(&crc.to_le_bytes());
        let mut buf = header;
        buf.extend_from_slice(&payload);
        let snap_crc = crc64_ecma(&buf);
        buf.extend_from_slice(&snap_crc.to_le_bytes());

        let decoded = decode(&buf, None).unwrap();
        assert_eq!(decoded.header.snapshot_lsn, 3);
        assert_eq!(decoded.pairs, vec![(b"ab".to_vec(), b"xyz".to_vec())]);
    }

    // ---- Rejection tests -------------------------------------------------

    #[test]
    fn reject_bad_magic() {
        let mut bytes = encode(CLUSTER_ID, 0, 0, &[]);
        bytes[0] = b'X';
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::BadMagic { .. })
        ));
    }

    #[test]
    fn reject_bad_version() {
        let mut bytes = encode(CLUSTER_ID, 0, 0, &[]);
        bytes[8] = 2; // version low byte
                      // Recompute header_crc so only the version is "wrong".
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        // Recompute the trailing snapshot crc too so only the version differs.
        let crc_start = bytes.len() - SNAPSHOT_CRC64_LEN;
        let snap = crc64_ecma(&bytes[0..crc_start]);
        bytes[crc_start..].copy_from_slice(&snap.to_le_bytes());
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::BadVersion(2))
        ));
    }

    #[test]
    fn reject_bad_header_len() {
        let mut bytes = encode(CLUSTER_ID, 0, 0, &[]);
        bytes[10..12].copy_from_slice(&65u16.to_le_bytes());
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::BadHeaderLen(65))
        ));
    }

    #[test]
    fn reject_cluster_mismatch() {
        let bytes = encode(CLUSTER_ID, 0, 0, &[]);
        let other = [0xAAu8; 16];
        assert!(matches!(
            decode(&bytes, Some(other)),
            Err(SnapshotError::ClusterMismatch { .. })
        ));
    }

    #[test]
    fn reject_tampered_header_crc() {
        let mut bytes = encode(CLUSTER_ID, 0, 0, &[]);
        // Flip a header byte without fixing the header CRC.
        bytes[28] ^= 0xFF; // snapshot_lsn low byte
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::HeaderCrcMismatch { .. })
        ));
    }

    #[test]
    fn reject_duplicate_key() {
        // Hand-build a payload with two identical keys, valid CRCs.
        let pairs = vec![
            (b"dup".to_vec(), b"a".to_vec()),
            (b"dup".to_vec(), b"b".to_vec()),
        ];
        let bytes = encode(CLUSTER_ID, 1, 1, &pairs);
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::DuplicateKey(_))
        ));
    }

    #[test]
    fn reject_entry_count_mismatch() {
        let pairs = vec![(b"a".to_vec(), b"1".to_vec())];
        let mut bytes = encode(CLUSTER_ID, 1, 1, &pairs);
        // Claim two entries in the header; fix header CRC and trailing CRC.
        bytes[44..52].copy_from_slice(&2u64.to_le_bytes());
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        let crc_start = bytes.len() - SNAPSHOT_CRC64_LEN;
        let snap = crc64_ecma(&bytes[0..crc_start]);
        bytes[crc_start..].copy_from_slice(&snap.to_le_bytes());
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::EntryCountMismatch {
                header: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn reject_payload_len_mismatch() {
        let pairs = vec![(b"a".to_vec(), b"1".to_vec())];
        let mut bytes = encode(CLUSTER_ID, 1, 1, &pairs);
        // Overstate payload_len; fix header CRC and trailing CRC so only
        // payload_len is inconsistent with the actual payload bytes.
        let actual = read_u64(&bytes, 52);
        bytes[52..60].copy_from_slice(&(actual + 1).to_le_bytes());
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        let crc_start = bytes.len() - SNAPSHOT_CRC64_LEN;
        let snap = crc64_ecma(&bytes[0..crc_start]);
        bytes[crc_start..].copy_from_slice(&snap.to_le_bytes());
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::PayloadLenMismatch { .. })
        ));
    }

    #[test]
    fn reject_tampered_snapshot_crc() {
        let mut bytes = encode(CLUSTER_ID, 1, 1, &[(b"a".to_vec(), b"1".to_vec())]);
        let crc_start = bytes.len() - SNAPSHOT_CRC64_LEN;
        bytes[crc_start] ^= 0xFF;
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::SnapshotCrcMismatch { .. })
        ));
    }

    #[test]
    fn reject_truncated_below_minimum() {
        let bytes = vec![0u8; 8];
        assert!(matches!(
            decode(&bytes, None),
            Err(SnapshotError::Truncated { .. })
        ));
    }

    #[test]
    fn reject_entry_overrun() {
        // Hand-build a header claiming one entry whose lengths overrun the
        // payload, with correct header CRC and trailing snapshot CRC.
        let mut header = vec![0u8; SNAPSHOT_HEADER_LEN];
        header[0..8].copy_from_slice(b"DDBSNP01");
        header[8..10].copy_from_slice(&1u16.to_le_bytes());
        header[10..12].copy_from_slice(&64u16.to_le_bytes());
        header[12..28].copy_from_slice(&CLUSTER_ID);
        header[44..52].copy_from_slice(&1u64.to_le_bytes()); // entry_count = 1
        let mut payload = Vec::new();
        payload.extend_from_slice(&10u32.to_le_bytes()); // key_len = 10 (overruns)
        payload.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
        payload.extend_from_slice(b"ab"); // only 2 key bytes present
        header[52..60].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let crc = crc32c(&header[0..60]);
        header[60..64].copy_from_slice(&crc.to_le_bytes());
        let mut buf = header;
        buf.extend_from_slice(&payload);
        let snap = crc64_ecma(&buf);
        buf.extend_from_slice(&snap.to_le_bytes());
        assert!(matches!(
            decode(&buf, None),
            Err(SnapshotError::EntryOverrun { .. })
        ));
    }
}
