//! Checked generation pointer stored at the root of a data directory.
//!
//! The 28-byte v1 layout is specified in Technical-Design §8.2. A pointer is
//! validated before any WAL or snapshot is opened; directory names alone are
//! never used as authority for selecting a recovery generation.

use crate::checksum::crc32c;

pub const CURRENT_LEN: usize = 28;
const MAGIC: &[u8; 8] = b"DDBCUR01";
const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Current {
    pub generation: u64,
}

impl Current {
    pub fn encode(self) -> [u8; CURRENT_LEN] {
        let mut bytes = [0u8; CURRENT_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.generation.to_le_bytes());
        let crc = crc32c(&bytes[..24]);
        bytes[24..28].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != CURRENT_LEN {
            return Err("CURRENT length is not 28 bytes");
        }
        if &bytes[..8] != MAGIC {
            return Err("CURRENT magic mismatch");
        }
        if u16::from_le_bytes([bytes[8], bytes[9]]) != VERSION {
            return Err("unsupported CURRENT version");
        }
        if bytes[10..16].iter().any(|&b| b != 0) {
            return Err("CURRENT reserved bytes are nonzero");
        }
        let stored_crc = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        if stored_crc != crc32c(&bytes[..24]) {
            return Err("CURRENT crc32c mismatch");
        }
        let generation = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        if generation == 0 {
            return Err("CURRENT generation zero is invalid");
        }
        Ok(Self { generation })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_layout() {
        let bytes = Current { generation: 1 }.encode();
        assert_eq!(&bytes[..8], b"DDBCUR01");
        assert_eq!(&bytes[8..10], &[1, 0]);
        assert_eq!(&bytes[10..16], &[0; 6]);
        assert_eq!(&bytes[16..24], &[1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&bytes[24..28], &[0xb9, 0x26, 0x97, 0xac]);
        assert_eq!(Current::decode(&bytes), Ok(Current { generation: 1 }));
    }

    #[test]
    fn damaged_or_unknown_pointer_fails_closed() {
        let mut bytes = Current { generation: 7 }.encode();
        bytes[16] ^= 1;
        assert!(Current::decode(&bytes).is_err());
        let mut bytes = Current { generation: 7 }.encode();
        bytes[10] = 1;
        assert!(Current::decode(&bytes).is_err());
        let mut bytes = Current { generation: 7 }.encode();
        bytes[8] = 2;
        assert!(Current::decode(&bytes).is_err());
        assert!(Current::decode(b"0000000000000001").is_err());
    }
}
