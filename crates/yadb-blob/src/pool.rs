use yadb_io::StorageFs;

use crate::{BlobError, BlobId, ErasureCodec, ErasureScheme, VolumeStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairReport {
    pub intact_parts: usize,
    pub repaired_parts: usize,
}

/// Distributed Storage Proxy (`DSProxy` analog): stripes logical blobs across independent
/// failure-domain `VolumeStore`s using `Mirror` or `ReedSolomon(k+m)` and self-heals lost fragments.
pub struct PoolStriper<FS: StorageFs> {
    codec: ErasureCodec,
    domains: Vec<VolumeStore<FS>>,
}

impl<FS: StorageFs> PoolStriper<FS> {
    pub fn new(scheme: ErasureScheme, domains: Vec<VolumeStore<FS>>) -> Result<Self, BlobError> {
        let codec = ErasureCodec::new(scheme)?;
        if domains.len() < scheme.total_parts() {
            return Err(BlobError::InvalidErasure(format!(
                "pool scheme {:?} requires at least {} failure domains, got {}",
                scheme,
                scheme.total_parts(),
                domains.len()
            )));
        }
        Ok(Self { codec, domains })
    }

    pub fn domains(&self) -> &[VolumeStore<FS>] {
        &self.domains
    }

    /// Deterministic subgroup placement based on `BlobId` hash so load spreads evenly across domains.
    fn select_domain(&self, logical_id: BlobId, part_index: usize) -> usize {
        let h = (logical_id.with_part(0).as_u128() as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize;
        (h + part_index) % self.domains.len()
    }

    /// Encodes `payload` and writes fragments across distinct failure domains.
    /// Succeeds as long as at least a safe write quorum of fragments lands durably.
    pub fn put_blob(&self, logical_id: BlobId, payload: &[u8]) -> Result<(), BlobError> {
        let fragments = self.codec.encode(payload);
        let min_write_quorum = match self.codec.scheme() {
            ErasureScheme::Mirror { replicas } => (replicas as usize / 2) + 1,
            ErasureScheme::ReedSolomon { k, m } => k as usize + (m as usize).div_ceil(2),
        };

        let mut ok_writes = 0usize;
        let mut last_err = None;
        for (idx, frag) in fragments.iter().enumerate() {
            let part_no = (idx + 1) as u8;
            let part_id = logical_id.with_part(part_no);
            let dom_idx = self.select_domain(logical_id, idx);
            match self.domains[dom_idx].put(part_id, frag) {
                Ok(()) => ok_writes += 1,
                Err(e) => last_err = Some(e),
            }
        }

        if ok_writes >= min_write_quorum {
            Ok(())
        } else {
            Err(last_err.unwrap_or(BlobError::InsufficientFragments {
                available: ok_writes,
                required: min_write_quorum,
            }))
        }
    }

    /// Reads and reconstructs `logical_id`, transparently tolerating corrupted or offline domains.
    pub fn get_blob(&self, logical_id: BlobId) -> Result<Vec<u8>, BlobError> {
        let total = self.codec.scheme().total_parts();
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];

        for (idx, slot) in slots.iter_mut().enumerate() {
            let part_id = logical_id.with_part((idx + 1) as u8);
            let dom_idx = self.select_domain(logical_id, idx);
            if let Ok(bytes) = self.domains[dom_idx].get(part_id) {
                *slot = Some(bytes);
                // Fast exit for Mirror: first healthy replica is the whole blob!
                if matches!(self.codec.scheme(), ErasureScheme::Mirror { .. }) {
                    break;
                }
            }
        }

        self.codec.decode(&slots)
    }

    /// Verifies all fragments of `logical_id` and reconstructs any missing/corrupted fragments.
    pub fn repair_blob(&self, logical_id: BlobId) -> Result<RepairReport, BlobError> {
        let total = self.codec.scheme().total_parts();
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut intact_parts = 0usize;

        for (idx, slot) in slots.iter_mut().enumerate() {
            let part_id = logical_id.with_part((idx + 1) as u8);
            let dom_idx = self.select_domain(logical_id, idx);
            if let Ok(bytes) = self.domains[dom_idx].get(part_id) {
                *slot = Some(bytes);
                intact_parts += 1;
            }
        }

        if intact_parts == total {
            return Ok(RepairReport {
                intact_parts,
                repaired_parts: 0,
            });
        }

        let payload = self.codec.decode(&slots)?;
        let expected_fragments = self.codec.encode(&payload);
        let mut repaired_parts = 0usize;

        for (idx, slot) in slots.iter().enumerate() {
            if slot.is_none() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let dom_idx = self.select_domain(logical_id, idx);
                // Advance barrier or rewrite missing fragment
                self.domains[dom_idx].put(part_id, &expected_fragments[idx])?;
                repaired_parts += 1;
            }
        }

        Ok(RepairReport {
            intact_parts,
            repaired_parts,
        })
    }
}
