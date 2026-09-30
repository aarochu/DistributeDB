//! Checksum primitives (CRC32C and CRC64-ECMA-182).
//!
//! These are the two integrity primitives the WAL v1 byte format depends on
//! (Technical-Design §6.1, SOW §7). They are implemented entirely in-crate
//! because the build environment has no crates.io access (ADR-001: std-only).
//!
//! # CRC32C (Castagnoli)
//!
//! Per Technical-Design §6.1, CRC32C uses the Castagnoli polynomial in its
//! reflected representation `0x82F63B78`, an initial register of `0xFFFFFFFF`,
//! and a final XOR of `0xFFFFFFFF`. This is the reflected (LSB-first) CRC used
//! by iSCSI and many storage formats. It protects the segment header (bytes
//! `0..=59`), each mutation record (from `record_len` through the final value
//! byte), and each group footer (its preceding 36 bytes).
//!
//! # CRC64-ECMA-182
//!
//! Per Technical-Design §6.1, CRC64-ECMA-182 uses polynomial
//! `0x42F0E1EBA9EA3693`, an initial register of zero, **no** input/output
//! reflection (MSB-first / big-endian bit order), and a final XOR of zero.
//! The WAL uses it for the per-record hash chain (`prev_hash`): the record
//! hash is the CRC64 over the complete encoded record.
//!
//! # Ground-truth vectors
//!
//! The standard CRC catalogue check value (the CRC of the ASCII string
//! `"123456789"`) is pinned by the unit tests below:
//! * `CRC32C("123456789") == 0xE3069283`
//! * `CRC64-ECMA-182("123456789") == 0x6C40DF5F0B497347`
//!
//! These vectors are the ground truth that makes later WAL golden fixtures
//! trustworthy.

/// Reflected polynomial for CRC32C (Castagnoli), per Technical-Design §6.1.
const CRC32C_POLY_REFLECTED: u32 = 0x82F6_3B78;

/// Non-reflected polynomial for CRC64-ECMA-182, per Technical-Design §6.1.
const CRC64_ECMA_POLY: u64 = 0x42F0_E1EB_A9EA_3693;

/// Precomputed 256-entry lookup table for reflected CRC32C.
const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0usize;
    while n < 256 {
        let mut crc = n as u32;
        let mut k = 0;
        while k < 8 {
            // Reflected (LSB-first) processing.
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC32C_POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            k += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}

/// Precomputed 256-entry lookup table for non-reflected CRC64-ECMA-182.
const fn build_crc64_ecma_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut n = 0usize;
    while n < 256 {
        // Non-reflected (MSB-first) processing: seed with the byte in the
        // top 8 bits of the 64-bit register.
        let mut crc = (n as u64) << 56;
        let mut k = 0;
        while k < 8 {
            if crc & 0x8000_0000_0000_0000 != 0 {
                crc = (crc << 1) ^ CRC64_ECMA_POLY;
            } else {
                crc <<= 1;
            }
            k += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}

/// CRC32C lookup table (computed once at compile time).
static CRC32C_TABLE: [u32; 256] = build_crc32c_table();

/// CRC64-ECMA-182 lookup table (computed once at compile time).
static CRC64_ECMA_TABLE: [u64; 256] = build_crc64_ecma_table();

/// Incremental CRC32C (Castagnoli) hasher.
///
/// Accumulate bytes with [`Crc32c::update`], then read the final value with
/// [`Crc32c::finalize`]. For a one-shot computation use the free function
/// [`crc32c`].
#[derive(Debug, Clone)]
pub struct Crc32c {
    // Running register, held without the final XOR applied.
    state: u32,
}

impl Crc32c {
    /// Create a hasher with the initial register `0xFFFFFFFF` (§6.1).
    pub fn new() -> Self {
        Crc32c { state: 0xFFFF_FFFF }
    }

    /// Fold `bytes` into the running CRC.
    pub fn update(&mut self, bytes: &[u8]) {
        let mut crc = self.state;
        for &b in bytes {
            let idx = ((crc ^ b as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32C_TABLE[idx];
        }
        self.state = crc;
    }

    /// Return the final CRC32C value with the `0xFFFFFFFF` final XOR applied.
    pub fn finalize(&self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental CRC64-ECMA-182 hasher.
///
/// Accumulate bytes with [`Crc64Ecma::update`], then read the final value with
/// [`Crc64Ecma::finalize`]. For a one-shot computation use the free function
/// [`crc64_ecma`].
#[derive(Debug, Clone)]
pub struct Crc64Ecma {
    // Running register; init is zero and final XOR is zero (§6.1).
    state: u64,
}

impl Crc64Ecma {
    /// Create a hasher with the initial register zero (§6.1).
    pub fn new() -> Self {
        Crc64Ecma { state: 0 }
    }

    /// Fold `bytes` into the running CRC (non-reflected, MSB-first).
    pub fn update(&mut self, bytes: &[u8]) {
        let mut crc = self.state;
        for &b in bytes {
            let idx = (((crc >> 56) as u8) ^ b) as usize;
            crc = (crc << 8) ^ CRC64_ECMA_TABLE[idx];
        }
        self.state = crc;
    }

    /// Return the final CRC64-ECMA-182 value (final XOR is zero).
    pub fn finalize(&self) -> u64 {
        self.state
    }
}

impl Default for Crc64Ecma {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the CRC32C (Castagnoli) of `bytes` in one shot (§6.1).
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut h = Crc32c::new();
    h.update(bytes);
    h.finalize()
}

/// Compute the CRC64-ECMA-182 of `bytes` in one shot (§6.1).
pub fn crc64_ecma(bytes: &[u8]) -> u64 {
    let mut h = Crc64Ecma::new();
    h.update(bytes);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard CRC catalogue check value for CRC32C (§6.1 ground truth).
    #[test]
    fn crc32c_catalogue_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    /// The standard CRC catalogue check value for CRC64-ECMA-182.
    #[test]
    fn crc64_ecma_catalogue_check_value() {
        assert_eq!(crc64_ecma(b"123456789"), 0x6C40_DF5F_0B49_7347);
    }

    #[test]
    fn crc32c_empty_input_is_zero() {
        // init ^ finalxor with no data folded: 0xFFFFFFFF ^ 0xFFFFFFFF == 0.
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn crc64_ecma_empty_input_is_zero() {
        // init zero, no data, final XOR zero.
        assert_eq!(crc64_ecma(b""), 0);
    }

    #[test]
    fn crc32c_single_byte_matches_incremental() {
        let one_shot = crc32c(b"A");
        let mut h = Crc32c::new();
        h.update(b"A");
        assert_eq!(one_shot, h.finalize());
    }

    #[test]
    fn crc64_ecma_single_byte_matches_incremental() {
        let one_shot = crc64_ecma(b"A");
        let mut h = Crc64Ecma::new();
        h.update(b"A");
        assert_eq!(one_shot, h.finalize());
    }

    #[test]
    fn crc32c_is_deterministic() {
        let data = b"the quick brown fox jumps over the lazy dog";
        assert_eq!(crc32c(data), crc32c(data));
    }

    #[test]
    fn crc64_ecma_is_deterministic() {
        let data = b"the quick brown fox jumps over the lazy dog";
        assert_eq!(crc64_ecma(data), crc64_ecma(data));
    }

    #[test]
    fn crc32c_incremental_equals_one_shot_across_chunks() {
        let data: Vec<u8> = (0u16..1000).map(|x| (x & 0xFF) as u8).collect();
        let one_shot = crc32c(&data);
        let mut h = Crc32c::new();
        for chunk in data.chunks(7) {
            h.update(chunk);
        }
        assert_eq!(one_shot, h.finalize());
    }

    #[test]
    fn crc64_ecma_incremental_equals_one_shot_across_chunks() {
        let data: Vec<u8> = (0u16..1000).map(|x| (x & 0xFF) as u8).collect();
        let one_shot = crc64_ecma(&data);
        let mut h = Crc64Ecma::new();
        for chunk in data.chunks(7) {
            h.update(chunk);
        }
        assert_eq!(one_shot, h.finalize());
    }

    #[test]
    fn crc32c_detects_single_bit_flip() {
        let a = crc32c(b"payload-bytes-0");
        let b = crc32c(b"payload-bytes-1");
        assert_ne!(a, b);
    }

    #[test]
    fn crc64_ecma_detects_single_bit_flip() {
        let a = crc64_ecma(b"payload-bytes-0");
        let b = crc64_ecma(b"payload-bytes-1");
        assert_ne!(a, b);
    }
}
