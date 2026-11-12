use std::fmt;
use serde::{Deserialize, Serialize};

/// 128-bit immutable Blob identifier inspired by YDB's `TLogoBlobID`,
/// extended with `project_id` for zero-overhead multi-tenant isolation.
///
/// Bit layout (big-endian lexicographically sortable):
/// - `project_id` : 32 bits (tenant / namespace)
/// - `owner_id`   : 32 bits (tablet / bucket / volume owner within project)
/// - `generation` : 24 bits (fencing epoch, max 16,777,215)
/// - `step`       : 24 bits (monotonic sequence within generation)
/// - `channel`    : 8 bits  (0 = WAL/log, 1 = snapshot/manifest, 2+ = data)
/// - `part`       : 8 bits  (0 = whole logical blob, 1..=N = erasure/mirror fragment)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobId(u128);

impl BlobId {
    pub const MAX_U24: u32 = 0x00FF_FFFF;

    pub fn new(
        project_id: u32,
        owner_id: u32,
        generation: u32,
        step: u32,
        channel: u8,
        part: u8,
    ) -> Self {
        assert!(generation <= Self::MAX_U24, "generation exceeds 24 bits");
        assert!(step <= Self::MAX_U24, "step exceeds 24 bits");
        let raw = ((project_id as u128) << 96)
            | ((owner_id as u128) << 64)
            | ((generation as u128) << 40)
            | ((step as u128) << 16)
            | ((channel as u128) << 8)
            | (part as u128);
        Self(raw)
    }

    #[inline]
    pub fn from_u128(v: u128) -> Self {
        Self(v)
    }

    #[inline]
    pub fn as_u128(self) -> u128 {
        self.0
    }

    #[inline]
    pub fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_be_bytes(b: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(b))
    }

    #[inline]
    pub fn project_id(self) -> u32 {
        (self.0 >> 96) as u32
    }

    #[inline]
    pub fn owner_id(self) -> u32 {
        (self.0 >> 64) as u32
    }

    #[inline]
    pub fn generation(self) -> u32 {
        ((self.0 >> 40) & 0x00FF_FFFF) as u32
    }

    #[inline]
    pub fn step(self) -> u32 {
        ((self.0 >> 16) & 0x00FF_FFFF) as u32
    }

    #[inline]
    pub fn channel(self) -> u8 {
        ((self.0 >> 8) & 0xFF) as u8
    }

    #[inline]
    pub fn part(self) -> u8 {
        (self.0 & 0xFF) as u8
    }

    /// Returns a copy of this ID pointing to fragment `part` (1-indexed for erasure coding).
    #[inline]
    pub fn with_part(self, part: u8) -> Self {
        Self((self.0 & !0xFFu128) | (part as u128))
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({})", self)
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[p{}:o{}:g{}:s{}:ch{}:pt{}]",
            self.project_id(),
            self.owner_id(),
            self.generation(),
            self.step(),
            self.channel(),
            self.part()
        )
    }
}
