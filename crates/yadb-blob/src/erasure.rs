use crate::BlobError;
use serde::{Deserialize, Serialize};

/// Erasure / redundancy scheme per StoragePool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErasureScheme {
    /// Full replication across `replicas` failure domains (ideal for low-latency WAL/metadata).
    Mirror { replicas: u8 },
    /// Systematic Reed-Solomon over GF(2^8): `k` data shards + `m` parity shards.
    ReedSolomon { k: u8, m: u8 },
}

impl ErasureScheme {
    pub fn total_parts(self) -> usize {
        match self {
            ErasureScheme::Mirror { replicas } => replicas as usize,
            ErasureScheme::ReedSolomon { k, m } => (k + m) as usize,
        }
    }

    pub fn min_parts_for_read(self) -> usize {
        match self {
            ErasureScheme::Mirror { .. } => 1,
            ErasureScheme::ReedSolomon { k, .. } => k as usize,
        }
    }

    pub fn validate(self) -> Result<(), BlobError> {
        match self {
            ErasureScheme::Mirror { replicas } => {
                if replicas == 0 || replicas > 16 {
                    return Err(BlobError::InvalidErasure(
                        "mirror replicas must be in 1..=16".into(),
                    ));
                }
            }
            ErasureScheme::ReedSolomon { k, m } => {
                if k == 0 || m == 0 || (k as usize + m as usize) > 32 {
                    return Err(BlobError::InvalidErasure(
                        "reed-solomon requires k>=1, m>=1, k+m<=32".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Encodes and reconstructs blobs using either Mirroring or Systematic Reed-Solomon over GF(2^8).
///
/// Each fragment prepends an 8-byte original-length header (little-endian `u64` on data/mirror
/// or reconstructed via systematic layout) so arbitrary byte lengths are preserved bit-for-bit.
pub struct ErasureCodec {
    scheme: ErasureScheme,
    /// Row-major `(k + m) * k` encoding matrix; top `k * k` is identity.
    enc_matrix: Vec<u8>,
}

impl ErasureCodec {
    pub fn new(scheme: ErasureScheme) -> Result<Self, BlobError> {
        scheme.validate()?;
        let enc_matrix = match scheme {
            ErasureScheme::Mirror { .. } => Vec::new(),
            ErasureScheme::ReedSolomon { k, m } => build_systematic_matrix(k as usize, m as usize)?,
        };
        Ok(Self { scheme, enc_matrix })
    }

    pub fn scheme(&self) -> ErasureScheme {
        self.scheme
    }

    /// Splits `payload` into `total_parts()` fragments.
    pub fn encode(&self, payload: &[u8]) -> Vec<Vec<u8>> {
        match self.scheme {
            ErasureScheme::Mirror { replicas } => (0..replicas).map(|_| payload.to_vec()).collect(),
            ErasureScheme::ReedSolomon { k, m } => {
                let k = k as usize;
                let m = m as usize;
                // Prepend 4-byte exact payload length so padding zeros are stripped on decode
                let framed_len = 4 + payload.len();
                let shard_len = framed_len.div_ceil(k).max(1);
                let padded_total = shard_len * k;

                let mut framed = Vec::with_capacity(padded_total);
                framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                framed.extend_from_slice(payload);
                framed.resize(padded_total, 0);

                let mut shards: Vec<Vec<u8>> = Vec::with_capacity(k + m);
                for i in 0..k {
                    shards.push(framed[i * shard_len..(i + 1) * shard_len].to_vec());
                }

                // Compute `m` parity shards using rows `k .. k+m` of the systematic matrix
                for p in 0..m {
                    let mut parity = vec![0u8; shard_len];
                    let row = &self.enc_matrix[(k + p) * k..(k + p + 1) * k];
                    for (col, &coeff) in row.iter().enumerate() {
                        gf_mul_add_slice(&mut parity, &shards[col], coeff);
                    }
                    shards.push(parity);
                }
                shards
            }
        }
    }

    /// Reconstructs original payload from available fragments (`None` = missing/corrupted disk).
    pub fn decode(&self, parts: &[Option<Vec<u8>>]) -> Result<Vec<u8>, BlobError> {
        if parts.len() != self.scheme.total_parts() {
            return Err(BlobError::InvalidErasure(format!(
                "expected {} fragment slots, got {}",
                self.scheme.total_parts(),
                parts.len()
            )));
        }

        match self.scheme {
            ErasureScheme::Mirror { .. } => parts
                .iter()
                .find_map(|p| p.clone())
                .ok_or(BlobError::InsufficientFragments {
                    available: 0,
                    required: 1,
                }),
            ErasureScheme::ReedSolomon { k, .. } => {
                let k = k as usize;
                let present_count = parts.iter().filter(|p| p.is_some()).count();
                if present_count < k {
                    return Err(BlobError::InsufficientFragments {
                        available: present_count,
                        required: k,
                    });
                }

                let shard_len = parts
                    .iter()
                    .find_map(|p| p.as_ref().map(|v| v.len()))
                    .unwrap_or(0);
                for p in parts.iter().flatten() {
                    if p.len() != shard_len {
                        return Err(BlobError::InvalidErasure(
                            "mismatched shard byte lengths".into(),
                        ));
                    }
                }

                // Fast path: all `k` systematic data shards are intact!
                if parts[..k].iter().all(|p| p.is_some()) {
                    return unframe_systematic(
                        &parts[..k]
                            .iter()
                            .map(|o| o.as_ref().unwrap().as_slice())
                            .collect::<Vec<_>>(),
                    );
                }

                // Slow path: pick first `k` available shards and invert their `k * k` submatrix
                let mut sub_matrix = vec![0u8; k * k];
                let mut chosen_shards: Vec<&[u8]> = Vec::with_capacity(k);
                for (idx, slot) in parts.iter().enumerate() {
                    if let Some(buf) = slot {
                        let row_num = chosen_shards.len();
                        sub_matrix[row_num * k..(row_num + 1) * k]
                            .copy_from_slice(&self.enc_matrix[idx * k..(idx + 1) * k]);
                        chosen_shards.push(buf.as_slice());
                        if chosen_shards.len() == k {
                            break;
                        }
                    }
                }

                let inv = invert_matrix(&sub_matrix, k)?;
                let mut recovered_data: Vec<Vec<u8>> = Vec::with_capacity(k);
                for r in 0..k {
                    let mut data_shard = vec![0u8; shard_len];
                    let inv_row = &inv[r * k..(r + 1) * k];
                    for (c, &coeff) in inv_row.iter().enumerate() {
                        gf_mul_add_slice(&mut data_shard, chosen_shards[c], coeff);
                    }
                    recovered_data.push(data_shard);
                }

                let refs: Vec<&[u8]> = recovered_data.iter().map(|v| v.as_slice()).collect();
                unframe_systematic(&refs)
            }
        }
    }
}

fn unframe_systematic(data_shards: &[&[u8]]) -> Result<Vec<u8>, BlobError> {
    let total_cap: usize = data_shards.iter().map(|s| s.len()).sum();
    if total_cap < 4 {
        return Err(BlobError::InvalidErasure("corrupt framed shard (<4B)".into()));
    }
    let mut joined = Vec::with_capacity(total_cap);
    for s in data_shards {
        joined.extend_from_slice(s);
    }
    let orig_len = u32::from_le_bytes([joined[0], joined[1], joined[2], joined[3]]) as usize;
    if 4 + orig_len > joined.len() {
        return Err(BlobError::InvalidErasure(format!(
            "corrupt RS length header: {} exceeds {}",
            orig_len,
            joined.len() - 4
        )));
    }
    Ok(joined[4..4 + orig_len].to_vec())
}

// ============================================================================
// GF(2^8) Arithmetic (Primitive polynomial x^8 + x^4 + x^3 + x^2 + 1 = 0x11D)
// ============================================================================

const fn gf_mul_const(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0u8;
    let mut i = 0;
    while i < 8 {
        if (b & 1) != 0 {
            p ^= a;
        }
        let hi = a & 0x80;
        a <<= 1;
        if hi != 0 {
            a ^= 0x1D;
        }
        b >>= 1;
        i += 1;
    }
    p
}

#[inline]
fn gf_mul(a: u8, b: u8) -> u8 {
    gf_mul_const(a, b)
}

fn gf_pow(mut base: u8, mut exp: usize) -> u8 {
    let mut result = 1u8;
    while exp > 0 {
        if exp & 1 == 1 {
            result = gf_mul(result, base);
        }
        base = gf_mul(base, base);
        exp >>= 1;
    }
    result
}

fn gf_inv(a: u8) -> u8 {
    assert!(a != 0, "division by zero in GF(2^8)");
    // Fermat's little theorem in GF(2^8): a^254 = a^-1
    gf_pow(a, 254)
}

fn gf_mul_add_slice(dst: &mut [u8], src: &[u8], coeff: u8) {
    if coeff == 0 {
        return;
    }
    if coeff == 1 {
        for (d, &s) in dst.iter_mut().zip(src.iter()) {
            *d ^= s;
        }
        return;
    }
    // Precompute 256-byte multiplication row for fast tight loop on ARM64
    let mut tbl = [0u8; 256];
    for (x, slot) in tbl.iter_mut().enumerate() {
        *slot = gf_mul(coeff, x as u8);
    }
    for (d, &s) in dst.iter_mut().zip(src.iter()) {
        *d ^= tbl[s as usize];
    }
}

/// Builds a Cauchy encoding matrix in GF(2^8) with an identity top `k * k` block.
/// Every square submatrix of a Cauchy matrix over distinct elements is guaranteed invertible!
fn build_systematic_matrix(k: usize, m: usize) -> Result<Vec<u8>, BlobError> {
    let rows = k + m;
    let mut mat = vec![0u8; rows * k];
    for i in 0..k {
        mat[i * k + i] = 1;
    }
    // Cauchy parity block: C[p, c] = 1 / (x_p ^ y_c) where x_p = k + p, y_c = c
    for p in 0..m {
        let x = (k + p) as u8;
        for c in 0..k {
            let y = c as u8;
            mat[(k + p) * k + c] = gf_inv(x ^ y);
        }
    }
    Ok(mat)
}

fn invert_matrix(src: &[u8], n: usize) -> Result<Vec<u8>, BlobError> {
    let mut work = src.to_vec();
    let mut inv = vec![0u8; n * n];
    for i in 0..n {
        inv[i * n + i] = 1;
    }

    for col in 0..n {
        // Find pivot
        let pivot = (col..n)
            .find(|&r| work[r * n + col] != 0)
            .ok_or_else(|| BlobError::InvalidErasure("singular RS decode submatrix".into()))?;
        if pivot != col {
            for c in 0..n {
                work.swap(col * n + c, pivot * n + c);
                inv.swap(col * n + c, pivot * n + c);
            }
        }

        let diag = work[col * n + col];
        if diag != 1 {
            let scale = gf_inv(diag);
            for c in 0..n {
                work[col * n + c] = gf_mul(work[col * n + c], scale);
                inv[col * n + c] = gf_mul(inv[col * n + c], scale);
            }
        }

        // Eliminate all other rows
        for r in 0..n {
            if r != col {
                let factor = work[r * n + col];
                if factor != 0 {
                    for c in 0..n {
                        work[r * n + c] ^= gf_mul(factor, work[col * n + c]);
                        inv[r * n + c] ^= gf_mul(factor, inv[col * n + c]);
                    }
                }
            }
        }
    }

    Ok(inv)
}
