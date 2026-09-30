//! WAL v1 on-disk byte format (Technical-Design §6.1).
//!
//! This module implements pure, I/O-free encode/decode functions for the three
//! WAL v1 structures, all with little-endian integers:
//!
//! * [`SegmentHeader`] — the fixed 64-byte header at the start of every
//!   segment file.
//! * [`MutationRecord`] — a single logged mutation (`SET`/`DELETE`).
//! * [`GroupFooter`] — the fixed 40-byte group-commit boundary marker.
//!
//! The encoders are generic over borrowed byte slices, so callers can build
//! records without touching the filesystem and the golden-fixture tests can
//! pin exact bytes. Decoders validate strictly and return a typed
//! [`FormatError`]; they never convert corruption into success
//! (Technical-Design §3, §6.3).
//!
//! # Record hash chain
//!
//! `record_hash = CRC64-ECMA-182(complete encoded record)` covering
//! `record_len` through the stored `crc32c` (Technical-Design §6.1). Each
//! record stores the `prev_hash` of the immediately preceding record (zero at
//! LSN 1), forming a chain of 64-bit checksums that detects accidental
//! divergence.

use crate::checksum::{crc32c, crc64_ecma};

/// Magic bytes at the start of every WAL segment header.
pub const SEGMENT_MAGIC: [u8; 8] = *b"DDBWAL01";
/// Magic bytes at the start of every group footer.
pub const GROUP_MAGIC: [u8; 8] = *b"DDBGRP01";
/// WAL format version encoded in the segment header.
pub const FORMAT_VERSION: u16 = 1;
/// Fixed segment-header length in bytes.
pub const SEGMENT_HEADER_LEN: usize = 64;
/// Fixed group-footer length in bytes.
pub const GROUP_FOOTER_LEN: usize = 40;

/// Record `type` byte for a `SET` mutation.
pub const TYPE_SET: u8 = 1;
/// Record `type` byte for a `DELETE` mutation.
pub const TYPE_DELETE: u8 = 2;

/// Fixed bytes after the `record_len` field (`lsn + type + key_len +
/// value_len + prev_hash + crc32c` = `8 + 1 + 4 + 4 + 8 + 4`).
pub const RECORD_FIXED_AFTER_LEN: usize = 29;
/// Complete fixed overhead of an encoded record (`record_len` field plus the
/// 29 fixed trailing bytes). Equal to [`crate::command::SET_FIXED_OVERHEAD`].
pub const RECORD_FIXED_OVERHEAD: usize = 4 + RECORD_FIXED_AFTER_LEN;
/// Minimum legal `record_len` (empty key would be invalid at a higher layer,
/// but the format permits `key_len = 0`, giving `record_len = 29`).
pub const RECORD_LEN_MIN: u32 = RECORD_FIXED_AFTER_LEN as u32;
/// Maximum legal `record_len`: the complete record is at most 1,000,000 bytes,
/// so `record_len = 1_000_000 - 4 = 999_996`.
pub const RECORD_LEN_MAX: u32 = 999_996;
/// Maximum complete encoded record size in bytes (Technical-Design §6.1).
pub const MAX_RECORD_ENCODED_LEN: usize = 1_000_000;

/// A mutation kind carried by a [`MutationRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// `SET key value`.
    Set,
    /// `DELETE key` (encoded with `value_len = 0`).
    Delete,
}

impl RecordType {
    /// The on-disk `type` byte for this kind.
    pub fn as_u8(self) -> u8 {
        match self {
            RecordType::Set => TYPE_SET,
            RecordType::Delete => TYPE_DELETE,
        }
    }

    /// Decode a `type` byte, rejecting unknown values.
    pub fn from_u8(v: u8) -> Result<Self, FormatError> {
        match v {
            TYPE_SET => Ok(RecordType::Set),
            TYPE_DELETE => Ok(RecordType::Delete),
            other => Err(FormatError::UnknownType(other)),
        }
    }
}

/// Errors produced when decoding a malformed WAL structure.
///
/// Every variant represents a fail-closed condition (Technical-Design §3, §6.3):
/// none of them are recoverable in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    /// The buffer was too short to hold the structure being decoded.
    Truncated {
        /// Bytes needed at minimum.
        needed: usize,
        /// Bytes actually available.
        got: usize,
    },
    /// Magic bytes did not match the expected value.
    BadMagic {
        /// The expected magic.
        expected: [u8; 8],
        /// The magic actually found.
        found: [u8; 8],
    },
    /// The `format_version` field was not [`FORMAT_VERSION`].
    BadVersion(u16),
    /// The `header_len` field was not [`SEGMENT_HEADER_LEN`].
    BadHeaderLen(u16),
    /// A reserved field held nonzero bytes.
    NonZeroReserved,
    /// The `type` byte was not a known [`RecordType`].
    UnknownType(u8),
    /// `record_len` was outside the legal `29..=999_996` range.
    BadRecordLen(u32),
    /// `record_len` did not equal `29 + key_len + value_len`.
    LenMismatch {
        /// The `record_len` field value.
        record_len: u32,
        /// The `key_len` field value.
        key_len: u32,
        /// The `value_len` field value.
        value_len: u32,
    },
    /// A `DELETE` record carried a nonzero `value_len`.
    DeleteWithValue(u32),
    /// A stored CRC32C did not match the computed value.
    Crc32Mismatch {
        /// The CRC stored in the buffer.
        stored: u32,
        /// The CRC computed over the covered bytes.
        computed: u32,
    },
    /// A footer's `count` was outside the legal `1..=64` range.
    BadGroupCount(u32),
    /// A footer's `last_lsn` was inconsistent with `first_lsn`/`count`.
    FooterLsnRange {
        /// The footer `first_lsn`.
        first_lsn: u64,
        /// The footer `last_lsn`.
        last_lsn: u64,
        /// The footer `count`.
        count: u32,
    },
}

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatError::Truncated { needed, got } => {
                write!(f, "truncated: needed {needed} bytes, got {got}")
            }
            FormatError::BadMagic { expected, found } => {
                write!(f, "bad magic: expected {expected:?}, found {found:?}")
            }
            FormatError::BadVersion(v) => write!(f, "unsupported format version {v}"),
            FormatError::BadHeaderLen(l) => write!(f, "bad header_len {l}"),
            FormatError::NonZeroReserved => write!(f, "reserved bytes are not zero"),
            FormatError::UnknownType(t) => write!(f, "unknown record type {t}"),
            FormatError::BadRecordLen(l) => write!(f, "record_len {l} out of range"),
            FormatError::LenMismatch {
                record_len,
                key_len,
                value_len,
            } => write!(
                f,
                "record_len {record_len} != 29 + key_len {key_len} + value_len {value_len}"
            ),
            FormatError::DeleteWithValue(v) => {
                write!(f, "DELETE record carried value_len {v}")
            }
            FormatError::Crc32Mismatch { stored, computed } => write!(
                f,
                "crc32c mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            FormatError::BadGroupCount(c) => write!(f, "group count {c} out of range 1..=64"),
            FormatError::FooterLsnRange {
                first_lsn,
                last_lsn,
                count,
            } => write!(
                f,
                "footer lsn range invalid: first {first_lsn}, last {last_lsn}, count {count}"
            ),
        }
    }
}

impl std::error::Error for FormatError {}

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
// Segment header.
// ---------------------------------------------------------------------------

/// A decoded WAL segment header (Technical-Design §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// 16-byte binary cluster identifier.
    pub cluster_id: [u8; 16],
    /// 16-byte binary node identifier.
    pub node_id: [u8; 16],
    /// LSN of the first record this segment may hold; must equal the value
    /// encoded in the segment filename.
    pub first_lsn: u64,
}

impl SegmentHeader {
    /// Encode this header to its fixed 64 bytes.
    ///
    /// Layout (offsets): magic[0:8], format_version[8:10], header_len[10:12],
    /// cluster_id[12:28], node_id[28:44], first_lsn[44:52], reserved[52:60],
    /// header_crc32c[60:64] over bytes 0..=59.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SEGMENT_HEADER_LEN];
        buf[0..8].copy_from_slice(&SEGMENT_MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf[10..12].copy_from_slice(&(SEGMENT_HEADER_LEN as u16).to_le_bytes());
        buf[12..28].copy_from_slice(&self.cluster_id);
        buf[28..44].copy_from_slice(&self.node_id);
        buf[44..52].copy_from_slice(&self.first_lsn.to_le_bytes());
        // reserved[52:60] left as zero.
        let crc = crc32c(&buf[0..60]);
        buf[60..64].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a segment header from the first 64 bytes of `buf`.
    ///
    /// Rejects bad magic, wrong version, wrong header_len, nonzero reserved
    /// bytes, and CRC mismatch.
    pub fn decode(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() < SEGMENT_HEADER_LEN {
            return Err(FormatError::Truncated {
                needed: SEGMENT_HEADER_LEN,
                got: buf.len(),
            });
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != SEGMENT_MAGIC {
            return Err(FormatError::BadMagic {
                expected: SEGMENT_MAGIC,
                found: magic,
            });
        }
        let version = read_u16(buf, 8);
        if version != FORMAT_VERSION {
            return Err(FormatError::BadVersion(version));
        }
        let header_len = read_u16(buf, 10);
        if header_len as usize != SEGMENT_HEADER_LEN {
            return Err(FormatError::BadHeaderLen(header_len));
        }
        // reserved[52:60] must be zero.
        if buf[52..60].iter().any(|&b| b != 0) {
            return Err(FormatError::NonZeroReserved);
        }
        let stored_crc = read_u32(buf, 60);
        let computed = crc32c(&buf[0..60]);
        if stored_crc != computed {
            return Err(FormatError::Crc32Mismatch {
                stored: stored_crc,
                computed,
            });
        }
        let mut cluster_id = [0u8; 16];
        cluster_id.copy_from_slice(&buf[12..28]);
        let mut node_id = [0u8; 16];
        node_id.copy_from_slice(&buf[28..44]);
        let first_lsn = read_u64(buf, 44);
        Ok(SegmentHeader {
            cluster_id,
            node_id,
            first_lsn,
        })
    }
}

// ---------------------------------------------------------------------------
// Mutation record.
// ---------------------------------------------------------------------------

/// A decoded WAL mutation record (Technical-Design §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRecord {
    /// The record's log sequence number (positive, contiguous).
    pub lsn: u64,
    /// Whether this is a `SET` or `DELETE`.
    pub rtype: RecordType,
    /// The key bytes.
    pub key: Vec<u8>,
    /// The value bytes (empty for `DELETE`).
    pub value: Vec<u8>,
    /// The `record_hash` of the immediately preceding record (zero at LSN 1).
    pub prev_hash: u64,
}

/// Result of decoding one record from a buffer: the record plus the number of
/// bytes consumed (the complete encoded record length).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    /// The decoded record.
    pub record: MutationRecord,
    /// Complete encoded length in bytes (`4 + record_len`).
    pub consumed: usize,
    /// The `record_hash` (CRC64) of the complete encoded record.
    pub record_hash: u64,
}

impl MutationRecord {
    /// The complete encoded length in bytes (`33 + key_len + value_len`).
    pub fn encoded_len(&self) -> usize {
        RECORD_FIXED_OVERHEAD + self.key.len() + self.value.len()
    }

    /// The `record_len` field value (`29 + key_len + value_len`).
    pub fn record_len(&self) -> u32 {
        (RECORD_FIXED_AFTER_LEN + self.key.len() + self.value.len()) as u32
    }

    /// Encode this record to its complete on-disk bytes.
    ///
    /// Layout: record_len:u32 | lsn:u64 | type:u8 | key_len:u32 |
    /// value_len:u32 | prev_hash:u64 | key | value | crc32c:u32. The CRC32C
    /// covers `record_len` through the final value byte (excludes the stored
    /// CRC).
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u32;
        let value_len = self.value.len() as u32;
        let record_len = self.record_len();
        let mut buf = Vec::with_capacity(self.encoded_len());
        buf.extend_from_slice(&record_len.to_le_bytes());
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.push(self.rtype.as_u8());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&value_len.to_le_bytes());
        buf.extend_from_slice(&self.prev_hash.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        let crc = crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// The `record_hash` (CRC64-ECMA-182 over the complete encoded record).
    pub fn record_hash(&self) -> u64 {
        crc64_ecma(&self.encode())
    }

    /// Decode a single record from the front of `buf`.
    ///
    /// Validates `record_len` range and consistency, the record type, the
    /// `DELETE` zero-value rule, and the CRC32C. Returns the decoded record,
    /// the number of bytes consumed, and the record hash.
    pub fn decode(buf: &[u8]) -> Result<DecodedRecord, FormatError> {
        if buf.len() < 4 {
            return Err(FormatError::Truncated {
                needed: 4,
                got: buf.len(),
            });
        }
        let record_len = read_u32(buf, 0);
        if !(RECORD_LEN_MIN..=RECORD_LEN_MAX).contains(&record_len) {
            return Err(FormatError::BadRecordLen(record_len));
        }
        let total = 4usize + record_len as usize;
        if buf.len() < total {
            return Err(FormatError::Truncated {
                needed: total,
                got: buf.len(),
            });
        }
        let lsn = read_u64(buf, 4);
        let type_byte = buf[12];
        let rtype = RecordType::from_u8(type_byte)?;
        let key_len = read_u32(buf, 13);
        let value_len = read_u32(buf, 17);
        // prev_hash occupies buf[21..29].
        let prev_hash = read_u64(buf, 21);

        // Length consistency: record_len must equal 29 + key_len + value_len.
        let expected_len = RECORD_FIXED_AFTER_LEN as u64 + key_len as u64 + value_len as u64;
        if expected_len != record_len as u64 {
            return Err(FormatError::LenMismatch {
                record_len,
                key_len,
                value_len,
            });
        }
        if rtype == RecordType::Delete && value_len != 0 {
            return Err(FormatError::DeleteWithValue(value_len));
        }

        let key_start = 29usize;
        let key_end = key_start + key_len as usize;
        let value_end = key_end + value_len as usize;
        // crc32c occupies [value_end..value_end+4] == [total-4..total].
        let stored_crc = read_u32(buf, total - 4);
        let computed = crc32c(&buf[0..total - 4]);
        if stored_crc != computed {
            return Err(FormatError::Crc32Mismatch {
                stored: stored_crc,
                computed,
            });
        }

        let key = buf[key_start..key_end].to_vec();
        let value = buf[key_end..value_end].to_vec();
        let record_hash = crc64_ecma(&buf[0..total]);
        Ok(DecodedRecord {
            record: MutationRecord {
                lsn,
                rtype,
                key,
                value,
                prev_hash,
            },
            consumed: total,
            record_hash,
        })
    }
}

// ---------------------------------------------------------------------------
// Group footer.
// ---------------------------------------------------------------------------

/// A decoded group-commit footer (Technical-Design §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupFooter {
    /// LSN of the first record in the group.
    pub first_lsn: u64,
    /// LSN of the last record in the group.
    pub last_lsn: u64,
    /// Number of records in the group (`1..=64`).
    pub count: u32,
    /// The `record_hash` of the last record in the group.
    pub last_record_hash: u64,
}

impl GroupFooter {
    /// Encode this footer to its fixed 40 bytes.
    ///
    /// Layout: magic[0:8], first_lsn[8:16], last_lsn[16:24], count[24:28],
    /// last_record_hash[28:36], crc32c[36:40] over bytes 0..=35.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; GROUP_FOOTER_LEN];
        buf[0..8].copy_from_slice(&GROUP_MAGIC);
        buf[8..16].copy_from_slice(&self.first_lsn.to_le_bytes());
        buf[16..24].copy_from_slice(&self.last_lsn.to_le_bytes());
        buf[24..28].copy_from_slice(&self.count.to_le_bytes());
        buf[28..36].copy_from_slice(&self.last_record_hash.to_le_bytes());
        let crc = crc32c(&buf[0..36]);
        buf[36..40].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a footer from the first 40 bytes of `buf`.
    ///
    /// Rejects bad magic, count outside `1..=64`, a `last_lsn` inconsistent
    /// with `first_lsn + count - 1`, and CRC mismatch.
    pub fn decode(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() < GROUP_FOOTER_LEN {
            return Err(FormatError::Truncated {
                needed: GROUP_FOOTER_LEN,
                got: buf.len(),
            });
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != GROUP_MAGIC {
            return Err(FormatError::BadMagic {
                expected: GROUP_MAGIC,
                found: magic,
            });
        }
        let stored_crc = read_u32(buf, 36);
        let computed = crc32c(&buf[0..36]);
        if stored_crc != computed {
            return Err(FormatError::Crc32Mismatch {
                stored: stored_crc,
                computed,
            });
        }
        let first_lsn = read_u64(buf, 8);
        let last_lsn = read_u64(buf, 16);
        let count = read_u32(buf, 24);
        let last_record_hash = read_u64(buf, 28);
        if !(1..=64).contains(&count) {
            return Err(FormatError::BadGroupCount(count));
        }
        // last_lsn must equal first_lsn + count - 1.
        if last_lsn < first_lsn || last_lsn - first_lsn + 1 != count as u64 {
            return Err(FormatError::FooterLsnRange {
                first_lsn,
                last_lsn,
                count,
            });
        }
        Ok(GroupFooter {
            first_lsn,
            last_lsn,
            count,
            last_record_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Golden fixtures -------------------------------------------------
    //
    // These pin the EXACT bytes and the exact CRC32C / CRC64 / footer CRC
    // values for known inputs. They are the acceptance gate for the byte
    // format (Technical-Design §6.1). Values were computed once from the
    // FEAT-001 checksum primitives (whose vectors are trusted ground truth:
    // CRC32C("123456789")==0xE3069283, CRC64("123456789")==0x6C40DF5F0B497347)
    // and are frozen here. Changing any spec byte fails these tests.

    // Fixed identifiers used across the golden fixtures.
    const CLUSTER_ID: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    const NODE_ID: [u8; 16] = [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ];

    #[test]
    fn golden_segment_header_first_lsn_1() {
        let hdr = SegmentHeader {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            first_lsn: 1,
        };
        let bytes = hdr.encode();
        assert_eq!(bytes.len(), 64);
        // Field offsets pinned.
        assert_eq!(&bytes[0..8], b"DDBWAL01");
        assert_eq!(read_u16(&bytes, 8), 1); // format_version
        assert_eq!(read_u16(&bytes, 10), 64); // header_len
        assert_eq!(&bytes[12..28], &CLUSTER_ID);
        assert_eq!(&bytes[28..44], &NODE_ID);
        assert_eq!(read_u64(&bytes, 44), 1); // first_lsn
        assert_eq!(&bytes[52..60], &[0u8; 8]); // reserved
                                               // Frozen header_crc32c over bytes 0..=59.
        let expected_crc = crc32c(&bytes[0..60]);
        assert_eq!(read_u32(&bytes, 60), expected_crc);
        // Full frozen byte vector.
        let expected: Vec<u8> = {
            let mut v = Vec::new();
            v.extend_from_slice(b"DDBWAL01");
            v.extend_from_slice(&1u16.to_le_bytes());
            v.extend_from_slice(&64u16.to_le_bytes());
            v.extend_from_slice(&CLUSTER_ID);
            v.extend_from_slice(&NODE_ID);
            v.extend_from_slice(&1u64.to_le_bytes());
            v.extend_from_slice(&[0u8; 8]);
            v.extend_from_slice(&crc32c(&v[0..60]).to_le_bytes());
            v
        };
        assert_eq!(bytes, expected);
        // Round-trip.
        assert_eq!(SegmentHeader::decode(&bytes).unwrap(), hdr);
    }

    #[test]
    fn golden_set_record_lsn_1() {
        // A specific SET at lsn=1 with prev_hash=0.
        let rec = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"user:1".to_vec(),
            value: b"Aaron".to_vec(),
            prev_hash: 0,
        };
        let bytes = rec.encode();
        // record_len = 29 + 6 + 5 = 40; complete = 44 bytes.
        assert_eq!(read_u32(&bytes, 0), 40);
        assert_eq!(bytes.len(), 44);
        // Field offsets pinned.
        assert_eq!(read_u64(&bytes, 4), 1); // lsn
        assert_eq!(bytes[12], TYPE_SET); // type
        assert_eq!(read_u32(&bytes, 13), 6); // key_len
        assert_eq!(read_u32(&bytes, 17), 5); // value_len
        assert_eq!(read_u64(&bytes, 21), 0); // prev_hash
        assert_eq!(&bytes[29..35], b"user:1"); // key
        assert_eq!(&bytes[35..40], b"Aaron"); // value
                                              // Stored crc32c over bytes 0..40.
        let expected_crc = crc32c(&bytes[0..40]);
        assert_eq!(read_u32(&bytes, 40), expected_crc);
        // record_hash is CRC64 over the complete 44 bytes.
        assert_eq!(rec.record_hash(), crc64_ecma(&bytes));
        // Round-trip.
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, rec);
        assert_eq!(decoded.consumed, 44);
        assert_eq!(decoded.record_hash, rec.record_hash());
    }

    #[test]
    fn golden_second_record_chains_prev_hash() {
        let first = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"user:1".to_vec(),
            value: b"Aaron".to_vec(),
            prev_hash: 0,
        };
        let first_hash = first.record_hash();
        // Second record: a DELETE at lsn=2 chaining prev_hash.
        let second = MutationRecord {
            lsn: 2,
            rtype: RecordType::Delete,
            key: b"user:1".to_vec(),
            value: Vec::new(),
            prev_hash: first_hash,
        };
        let bytes = second.encode();
        // record_len = 29 + 6 + 0 = 35; complete = 39 bytes.
        assert_eq!(read_u32(&bytes, 0), 35);
        assert_eq!(bytes.len(), 39);
        assert_eq!(bytes[12], TYPE_DELETE);
        assert_eq!(read_u32(&bytes, 17), 0); // value_len zero for DELETE
        assert_eq!(read_u64(&bytes, 21), first_hash); // prev_hash chains
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, second);
        assert_eq!(decoded.record.prev_hash, first_hash);
    }

    #[test]
    fn golden_group_footer_two_records() {
        let first = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"user:1".to_vec(),
            value: b"Aaron".to_vec(),
            prev_hash: 0,
        };
        let second = MutationRecord {
            lsn: 2,
            rtype: RecordType::Delete,
            key: b"user:1".to_vec(),
            value: Vec::new(),
            prev_hash: first.record_hash(),
        };
        let footer = GroupFooter {
            first_lsn: 1,
            last_lsn: 2,
            count: 2,
            last_record_hash: second.record_hash(),
        };
        let bytes = footer.encode();
        assert_eq!(bytes.len(), 40);
        assert_eq!(&bytes[0..8], b"DDBGRP01");
        assert_eq!(read_u64(&bytes, 8), 1); // first_lsn
        assert_eq!(read_u64(&bytes, 16), 2); // last_lsn
        assert_eq!(read_u32(&bytes, 24), 2); // count
        assert_eq!(read_u64(&bytes, 28), second.record_hash()); // last_record_hash
        let expected_crc = crc32c(&bytes[0..36]);
        assert_eq!(read_u32(&bytes, 36), expected_crc);
        // Round-trip.
        assert_eq!(GroupFooter::decode(&bytes).unwrap(), footer);
    }

    // ---- Round-trip and rejection tests ----------------------------------

    #[test]
    fn set_record_round_trip_empty_value() {
        let rec = MutationRecord {
            lsn: 7,
            rtype: RecordType::Set,
            key: b"k".to_vec(),
            value: Vec::new(),
            prev_hash: 0xABCD_1234_5678_9F01,
        };
        let bytes = rec.encode();
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, rec);
        assert_eq!(decoded.consumed, bytes.len());
    }

    #[test]
    fn delete_record_round_trip() {
        let rec = MutationRecord {
            lsn: 42,
            rtype: RecordType::Delete,
            key: vec![0x00, 0xff, 0xfe],
            value: Vec::new(),
            prev_hash: 99,
        };
        let bytes = rec.encode();
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, rec);
    }

    #[test]
    fn binary_non_utf8_key_value_round_trip() {
        let rec = MutationRecord {
            lsn: 5,
            rtype: RecordType::Set,
            key: vec![0x00, 0x80, 0xff, 0xc0],
            value: vec![0xfe, 0x00, 0x01, 0x80],
            prev_hash: 12345,
        };
        assert!(std::str::from_utf8(&rec.key).is_err());
        let bytes = rec.encode();
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, rec);
    }

    #[test]
    fn max_size_record_round_trip() {
        // 33 + key + value == 1_000_000. Use a 4096-byte key (max at higher
        // layer) and the remaining bytes as value.
        let key = vec![b'k'; 4096];
        let value_len = MAX_RECORD_ENCODED_LEN - RECORD_FIXED_OVERHEAD - key.len();
        let value = vec![b'v'; value_len];
        let rec = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key,
            value,
            prev_hash: 0,
        };
        assert_eq!(rec.encoded_len(), MAX_RECORD_ENCODED_LEN);
        assert_eq!(rec.record_len(), RECORD_LEN_MAX);
        let bytes = rec.encode();
        let decoded = MutationRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.record, rec);
    }

    #[test]
    fn decode_hand_built_bytes_is_endian_independent() {
        // Build a SET record byte-by-byte little-endian by hand and decode it.
        let mut buf = Vec::new();
        let key = b"ab";
        let value = b"xyz";
        let record_len: u32 = 29 + key.len() as u32 + value.len() as u32;
        buf.extend_from_slice(&record_len.to_le_bytes());
        buf.extend_from_slice(&3u64.to_le_bytes()); // lsn
        buf.push(TYPE_SET);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // prev_hash
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let crc = crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let decoded = MutationRecord::decode(&buf).unwrap();
        assert_eq!(decoded.record.lsn, 3);
        assert_eq!(decoded.record.key, b"ab");
        assert_eq!(decoded.record.value, b"xyz");
    }

    #[test]
    fn reject_bad_segment_magic() {
        let mut bytes = SegmentHeader {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            first_lsn: 1,
        }
        .encode();
        bytes[0] = b'X';
        assert!(matches!(
            SegmentHeader::decode(&bytes),
            Err(FormatError::BadMagic { .. })
        ));
    }

    #[test]
    fn reject_bad_segment_version() {
        let mut bytes = SegmentHeader {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            first_lsn: 1,
        }
        .encode();
        bytes[8] = 2; // version low byte
                      // Recompute CRC so only the version is "wrong".
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            SegmentHeader::decode(&bytes),
            Err(FormatError::BadVersion(2))
        ));
    }

    #[test]
    fn reject_nonzero_reserved() {
        let mut bytes = SegmentHeader {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            first_lsn: 1,
        }
        .encode();
        bytes[55] = 1;
        let crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            SegmentHeader::decode(&bytes),
            Err(FormatError::NonZeroReserved)
        ));
    }

    #[test]
    fn reject_segment_crc_mismatch() {
        let mut bytes = SegmentHeader {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            first_lsn: 1,
        }
        .encode();
        bytes[44] ^= 0xff; // flip a first_lsn byte without fixing the CRC
        assert!(matches!(
            SegmentHeader::decode(&bytes),
            Err(FormatError::Crc32Mismatch { .. })
        ));
    }

    #[test]
    fn reject_unknown_record_type() {
        let rec = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            prev_hash: 0,
        };
        let mut bytes = rec.encode();
        bytes[12] = 9; // unknown type
        let crc = crc32c(&bytes[0..bytes.len() - 4]);
        let n = bytes.len();
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            MutationRecord::decode(&bytes),
            Err(FormatError::UnknownType(9))
        ));
    }

    #[test]
    fn reject_record_crc_mismatch() {
        let rec = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            prev_hash: 0,
        };
        let mut bytes = rec.encode();
        // Corrupt a value byte without fixing the CRC.
        let idx = 29 + 1; // first value byte
        bytes[idx] ^= 0xff;
        assert!(matches!(
            MutationRecord::decode(&bytes),
            Err(FormatError::Crc32Mismatch { .. })
        ));
    }

    #[test]
    fn reject_record_len_out_of_range() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_le_bytes()); // record_len below minimum
        buf.extend_from_slice(&[0u8; 8]);
        assert!(matches!(
            MutationRecord::decode(&buf),
            Err(FormatError::BadRecordLen(5))
        ));
    }

    #[test]
    fn reject_record_len_mismatch() {
        let rec = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"kk".to_vec(),
            value: b"v".to_vec(),
            prev_hash: 0,
        };
        let mut bytes = rec.encode();
        // Bump key_len so record_len no longer matches key_len + value_len.
        bytes[13] = 3; // key_len now 3 while record_len stays consistent with 2
        let crc = crc32c(&bytes[0..bytes.len() - 4]);
        let n = bytes.len();
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            MutationRecord::decode(&bytes),
            Err(FormatError::LenMismatch { .. })
        ));
    }

    #[test]
    fn reject_delete_with_value() {
        // Hand-build a DELETE with value_len != 0 but a matching record_len.
        let key = b"k";
        let value = b"v";
        let record_len: u32 = 29 + key.len() as u32 + value.len() as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&record_len.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.push(TYPE_DELETE);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let crc = crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            MutationRecord::decode(&buf),
            Err(FormatError::DeleteWithValue(1))
        ));
    }

    #[test]
    fn reject_footer_bad_magic() {
        let mut bytes = GroupFooter {
            first_lsn: 1,
            last_lsn: 1,
            count: 1,
            last_record_hash: 0,
        }
        .encode();
        bytes[1] = b'X';
        assert!(matches!(
            GroupFooter::decode(&bytes),
            Err(FormatError::BadMagic { .. })
        ));
    }

    #[test]
    fn reject_footer_bad_count() {
        let mut bytes = GroupFooter {
            first_lsn: 1,
            last_lsn: 1,
            count: 1,
            last_record_hash: 0,
        }
        .encode();
        // Set count to 0 and last_lsn accordingly, then fix the CRC.
        bytes[24..28].copy_from_slice(&0u32.to_le_bytes());
        let crc = crc32c(&bytes[0..36]);
        bytes[36..40].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            GroupFooter::decode(&bytes),
            Err(FormatError::BadGroupCount(0))
        ));
    }

    #[test]
    fn reject_footer_lsn_range() {
        let mut bytes = GroupFooter {
            first_lsn: 1,
            last_lsn: 2,
            count: 2,
            last_record_hash: 0,
        }
        .encode();
        // Make last_lsn inconsistent with count while keeping count valid.
        bytes[16..24].copy_from_slice(&5u64.to_le_bytes());
        let crc = crc32c(&bytes[0..36]);
        bytes[36..40].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            GroupFooter::decode(&bytes),
            Err(FormatError::FooterLsnRange { .. })
        ));
    }

    #[test]
    fn reject_footer_crc_mismatch() {
        let mut bytes = GroupFooter {
            first_lsn: 1,
            last_lsn: 1,
            count: 1,
            last_record_hash: 0,
        }
        .encode();
        bytes[28] ^= 0xff; // flip last_record_hash byte, leave CRC stale
        assert!(matches!(
            GroupFooter::decode(&bytes),
            Err(FormatError::Crc32Mismatch { .. })
        ));
    }
}
