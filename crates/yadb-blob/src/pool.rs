use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use yadb_io::StorageFs;

use crate::{BlobError, BlobId, ErasureCodec, ErasureScheme, VolumeStats, VolumeStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairReport {
    /// Fragments readable after the repair, i.e. including the ones just rewritten.
    pub intact_parts: usize,
    pub repaired_parts: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolHealthState {
    Healthy,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotHealthSnapshot {
    pub slot_index: usize,
    pub domain: String,
    pub node_id: u32,
    pub addr: Option<String>,
    pub is_local: bool,
    pub online: bool,
    pub live_blobs: usize,
    pub live_bytes: u64,
    pub segment_count: usize,
    pub total_disk_bytes: u64,
    pub puts_ok: u64,
    pub puts_err: u64,
    pub gets_ok: u64,
    pub gets_err: u64,
    pub repairs_ok: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolHealthSnapshot {
    pub pool_name: String,
    pub scheme: String,
    pub status: PoolHealthState,
    pub total_slots: usize,
    pub online_slots: usize,
    pub min_read_slots: usize,
    pub min_write_slots: usize,
    pub total_repairs: u64,
    /// Writes refused because fewer than `total_slots` fragments landed, i.e.
    /// because the pool could not promise the redundancy it advertises. A refused
    /// write is not acknowledged and publishes nothing, so this counter is a loss
    /// of write availability, not a queue of work to finish later.
    pub refused_puts: u64,
    pub slots: Vec<SlotHealthSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarrierRequest {
    pub project_id: u32,
    pub owner_id: u32,
    pub channel: u8,
    pub up_to_generation: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarrierResponse {
    pub evicted: usize,
}

enum SlotBackend<FS: StorageFs> {
    Local(Arc<VolumeStore<FS>>),
    Remote {
        pool_name: String,
        slot_index: usize,
        peer_addr: String,
        token: Option<String>,
    },
}

struct SlotMetrics {
    online: AtomicBool,
    puts_ok: AtomicU64,
    puts_err: AtomicU64,
    gets_ok: AtomicU64,
    gets_err: AtomicU64,
    repairs_ok: AtomicU64,
}

impl Default for SlotMetrics {
    fn default() -> Self {
        Self {
            online: AtomicBool::new(true),
            puts_ok: AtomicU64::new(0),
            puts_err: AtomicU64::new(0),
            gets_ok: AtomicU64::new(0),
            gets_err: AtomicU64::new(0),
            repairs_ok: AtomicU64::new(0),
        }
    }
}

/// Virtual Disk (`VDisk` analog): represents one failure-domain storage slot in a `PoolStriper`.
/// Transparently executes zero-hop disk I/O if hosted on the local node, or fast HTTP/TCP RPC
/// (`/internal/vdisk/...`) if hosted on a remote cluster node.
pub struct PoolSlot<FS: StorageFs> {
    slot_index: usize,
    domain: String,
    node_id: u32,
    addr: Option<String>,
    backend: SlotBackend<FS>,
    metrics: SlotMetrics,
}

impl<FS: StorageFs> PoolSlot<FS> {
    pub fn local(
        slot_index: usize,
        domain: impl Into<String>,
        node_id: u32,
        addr: Option<String>,
        store: Arc<VolumeStore<FS>>,
    ) -> Self {
        Self {
            slot_index,
            domain: domain.into(),
            node_id,
            addr,
            backend: SlotBackend::Local(store),
            metrics: SlotMetrics::default(),
        }
    }

    pub fn remote(
        pool_name: impl Into<String>,
        slot_index: usize,
        domain: impl Into<String>,
        node_id: u32,
        peer_addr: impl Into<String>,
        token: Option<String>,
    ) -> Self {
        let peer = peer_addr.into();
        Self {
            slot_index,
            domain: domain.into(),
            node_id,
            addr: Some(peer.clone()),
            backend: SlotBackend::Remote {
                pool_name: pool_name.into(),
                slot_index,
                peer_addr: peer,
                token,
            },
            metrics: SlotMetrics::default(),
        }
    }

    #[inline]
    pub fn is_local(&self) -> bool {
        matches!(self.backend, SlotBackend::Local(_))
    }

    #[inline]
    pub fn slot_index(&self) -> usize {
        self.slot_index
    }

    #[inline]
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    #[inline]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn local_store(&self) -> Option<&Arc<VolumeStore<FS>>> {
        match &self.backend {
            SlotBackend::Local(s) => Some(s),
            SlotBackend::Remote { .. } => None,
        }
    }

    pub fn put(&self, id: BlobId, data: &[u8]) -> Result<(), BlobError> {
        let res = match &self.backend {
            SlotBackend::Local(store) => {
                let r = store.put(id, data);
                self.metrics.online.store(true, Ordering::Relaxed);
                r
            }
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!(
                    "/internal/vdisk/{pool_name}/{slot_index}/blob/{}",
                    id.to_hex()
                );
                match http_raw_request(
                    peer_addr,
                    "PUT",
                    &path,
                    "application/octet-stream",
                    data,
                    token.as_deref(),
                ) {
                    Ok((200 | 201, _)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Ok(())
                    }
                    Ok((409, _)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::ImmutableConflict(id))
                    }
                    Ok((412, _)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::FencedByBarrier { id, barrier_gen: 0 })
                    }
                    Ok((code, body)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::Io(std::io::Error::other(format!(
                            "remote vdisk PUT HTTP {code}: {}",
                            String::from_utf8_lossy(&body)
                        ))))
                    }
                    Err(e) => {
                        self.metrics.online.store(false, Ordering::Relaxed);
                        Err(BlobError::Io(e))
                    }
                }
            }
        };

        match &res {
            Ok(()) => {
                self.metrics.puts_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.metrics.puts_err.fetch_add(1, Ordering::Relaxed);
            }
        }
        res
    }

    pub fn get(&self, id: BlobId) -> Result<Vec<u8>, BlobError> {
        let res = match &self.backend {
            SlotBackend::Local(store) => {
                let r = store.get(id);
                self.metrics.online.store(true, Ordering::Relaxed);
                r
            }
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!(
                    "/internal/vdisk/{pool_name}/{slot_index}/blob/{}",
                    id.to_hex()
                );
                match http_raw_request(
                    peer_addr,
                    "GET",
                    &path,
                    "application/octet-stream",
                    &[],
                    token.as_deref(),
                ) {
                    Ok((200, body)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Ok(body)
                    }
                    Ok((404, _)) => {
                        // Node is online, it simply lacks this fragment (e.g. missed write while down)
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::NotFound(id))
                    }
                    Ok((422, _)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::CorruptData {
                            id,
                            reason: "remote CRC32 mismatch",
                        })
                    }
                    Ok((code, body)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Err(BlobError::Io(std::io::Error::other(format!(
                            "remote vdisk GET HTTP {code}: {}",
                            String::from_utf8_lossy(&body)
                        ))))
                    }
                    Err(e) => {
                        self.metrics.online.store(false, Ordering::Relaxed);
                        Err(BlobError::Io(e))
                    }
                }
            }
        };

        match &res {
            Ok(_) => {
                self.metrics.gets_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(BlobError::NotFound(_)) => {}
            Err(_) => {
                self.metrics.gets_err.fetch_add(1, Ordering::Relaxed);
            }
        }
        res
    }

    pub fn collect_garbage_barrier(
        &self,
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
    ) -> Result<usize, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => {
                store.collect_garbage_barrier(project_id, owner_id, channel, up_to_generation)
            }
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!("/internal/vdisk/{pool_name}/{slot_index}/barrier");
                let body = serde_json::to_vec(&BarrierRequest {
                    project_id,
                    owner_id,
                    channel,
                    up_to_generation,
                })
                .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                match http_raw_request(
                    peer_addr,
                    "POST",
                    &path,
                    "application/json",
                    &body,
                    token.as_deref(),
                ) {
                    Ok((200, resp)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        let parsed: BarrierResponse = serde_json::from_slice(&resp)
                            .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                        Ok(parsed.evicted)
                    }
                    Ok((code, resp)) => Err(BlobError::Io(std::io::Error::other(format!(
                        "remote barrier HTTP {code}: {}",
                        String::from_utf8_lossy(&resp)
                    )))),
                    Err(e) => {
                        self.metrics.online.store(false, Ordering::Relaxed);
                        Err(BlobError::Io(e))
                    }
                }
            }
        }
    }

    pub fn delete(&self, id: BlobId) -> Result<bool, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.delete(id),
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!(
                    "/internal/vdisk/{pool_name}/{slot_index}/blob/{}",
                    id.to_hex()
                );
                match http_raw_request(
                    peer_addr,
                    "DELETE",
                    &path,
                    "application/octet-stream",
                    &[],
                    token.as_deref(),
                ) {
                    Ok((200, body)) if body == b"deleted" || body == b"absent" => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        Ok(body == b"deleted")
                    }
                    Ok((code, body)) => Err(BlobError::Io(std::io::Error::other(format!(
                        "remote vdisk DELETE HTTP {code}: {}",
                        String::from_utf8_lossy(&body)
                    )))),
                    Err(e) => {
                        self.metrics.online.store(false, Ordering::Relaxed);
                        Err(BlobError::Io(e))
                    }
                }
            }
        }
    }

    pub fn probe_stats(&self, probe_remote: bool) -> (bool, VolumeStats) {
        match &self.backend {
            SlotBackend::Local(store) => {
                self.metrics.online.store(true, Ordering::Relaxed);
                (true, store.stats().unwrap_or_default())
            }
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                if !probe_remote {
                    return (
                        self.metrics.online.load(Ordering::Relaxed),
                        VolumeStats::default(),
                    );
                }
                let path = format!("/internal/vdisk/{pool_name}/{slot_index}/stats");
                match http_raw_request(
                    peer_addr,
                    "GET",
                    &path,
                    "application/json",
                    &[],
                    token.as_deref(),
                ) {
                    Ok((200, body)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        let st = serde_json::from_slice::<VolumeStats>(&body).unwrap_or_default();
                        (true, st)
                    }
                    _ => {
                        self.metrics.online.store(false, Ordering::Relaxed);
                        (false, VolumeStats::default())
                    }
                }
            }
        }
    }

    pub fn snapshot(&self, probe_remote: bool) -> SlotHealthSnapshot {
        let (online, stats) = self.probe_stats(probe_remote);
        SlotHealthSnapshot {
            slot_index: self.slot_index,
            domain: self.domain.clone(),
            node_id: self.node_id,
            addr: self.addr.clone(),
            is_local: self.is_local(),
            online,
            live_blobs: stats.live_blobs,
            live_bytes: stats.live_bytes,
            segment_count: stats.segment_count,
            total_disk_bytes: stats.total_disk_bytes,
            puts_ok: self.metrics.puts_ok.load(Ordering::Relaxed),
            puts_err: self.metrics.puts_err.load(Ordering::Relaxed),
            gets_ok: self.metrics.gets_ok.load(Ordering::Relaxed),
            gets_err: self.metrics.gets_err.load(Ordering::Relaxed),
            repairs_ok: self.metrics.repairs_ok.load(Ordering::Relaxed),
        }
    }
}

/// Distributed Storage Proxy (`DSProxy` analog): stripes logical blobs across independent
/// failure-domain `PoolSlot`s (`Local` or `Remote` `VDisk`s) using `Mirror` or `ReedSolomon(k+m)`
/// and self-heals lost or bit-rotted fragments.
pub struct PoolStriper<FS: StorageFs> {
    pool_name: String,
    codec: ErasureCodec,
    slots: Vec<PoolSlot<FS>>,
    has_remote: bool,
    /// Writes refused because fewer than `total_parts()` fragments landed.
    /// A refused write is not acknowledged, so there is nothing to repair: the
    /// counter is the observable signal that the pool lost write availability.
    refused_puts: AtomicU64,
}

impl<FS: StorageFs> PoolStriper<FS> {
    /// Creates a local-only striper from `VolumeStore` instances (single-node / unit tests).
    pub fn new(scheme: ErasureScheme, domains: Vec<VolumeStore<FS>>) -> Result<Self, BlobError> {
        let slots = domains
            .into_iter()
            .enumerate()
            .map(|(i, store)| PoolSlot::local(i, format!("local-{i}"), 1, None, Arc::new(store)))
            .collect();
        Self::from_slots("local", scheme, slots)
    }

    /// Creates a distributed striper mixing `Local` and `Remote` `PoolSlot`s across cluster nodes.
    pub fn from_slots(
        pool_name: impl Into<String>,
        scheme: ErasureScheme,
        slots: Vec<PoolSlot<FS>>,
    ) -> Result<Self, BlobError> {
        let codec = ErasureCodec::new(scheme)?;
        if slots.len() < scheme.total_parts() {
            return Err(BlobError::InvalidErasure(format!(
                "pool scheme {:?} requires at least {} slots, got {}",
                scheme,
                scheme.total_parts(),
                slots.len()
            )));
        }
        let has_remote = slots.iter().any(|s| !s.is_local());
        Ok(Self {
            pool_name: pool_name.into(),
            codec,
            slots,
            has_remote,
            refused_puts: AtomicU64::new(0),
        })
    }

    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    pub fn scheme(&self) -> ErasureScheme {
        self.codec.scheme()
    }

    pub fn slots(&self) -> &[PoolSlot<FS>] {
        &self.slots
    }

    pub fn domains(&self) -> &[PoolSlot<FS>] {
        &self.slots
    }

    pub fn min_read_quorum(&self) -> usize {
        match self.codec.scheme() {
            ErasureScheme::Mirror { .. } => 1,
            ErasureScheme::ReedSolomon { k, .. } => k as usize,
        }
    }

    /// Number of fragments a write must land before it may be acknowledged.
    ///
    /// That is every fragment: an ack promises the pool's full redundancy, so a
    /// missing fragment is a refused write rather than a degraded one. A pool
    /// with any slot offline is therefore read-only for new writes, which is the
    /// deliberate trade-off: an ack never means "readable but unprotected".
    /// Reads still need only `k` fragments (Reed-Solomon) or one replica (mirror).
    pub fn min_write_quorum(&self) -> usize {
        self.codec.scheme().total_parts()
    }

    /// Deterministic subgroup placement based on `BlobId` hash so load spreads evenly across slots
    /// and every node in the cluster computes the exact same slot mapping without extra coordination.
    fn select_slot(&self, logical_id: BlobId, part_index: usize) -> usize {
        let h =
            (logical_id.with_part(0).as_u128() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize;
        (h + part_index) % self.slots.len()
    }

    /// Encodes `payload` and writes fragments across failure-domain slots (in parallel over the network).
    /// Succeeds only when every fragment lands durably.
    ///
    /// `Ok` means "fully redundant": the caller's ack is a promise that losing any
    /// single failure domain still leaves the payload reconstructible. A short
    /// write is refused with `BlobError::RedundancyNotMet` and counted in
    /// `refused_puts`; the fragments that did land are never published, so a
    /// caller that saw the error holds no reference to them. They are *not*
    /// reclaimed automatically: the only orphan collection implemented is scoped
    /// to the S3 namespace and is disabled on Multi-Raft, so unreferenced
    /// fragments from a refused write — like those from a copy-on-write block
    /// replacement — accumulate until a pool-wide collector exists. Refusing is
    /// deliberate: an ack that only means "readable" would let a client believe
    /// data survived a node loss when it cannot.
    pub fn put_blob(&self, logical_id: BlobId, payload: &[u8]) -> Result<(), BlobError> {
        let fragments = self.codec.encode(payload);
        let required = self.min_write_quorum();

        let mut ok_writes = 0usize;

        if self.has_remote {
            let results: Vec<Result<(), BlobError>> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(fragments.len());
                for (idx, frag) in fragments.iter().enumerate() {
                    let part_id = logical_id.with_part((idx + 1) as u8);
                    let slot_idx = self.select_slot(logical_id, idx);
                    let slot = &self.slots[slot_idx];
                    handles.push(s.spawn(move || slot.put(part_id, frag)));
                }
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Err(BlobError::Io(std::io::Error::other(
                                "slot put thread panicked",
                            )))
                        })
                    })
                    .collect()
            });

            for r in results {
                match r {
                    Ok(()) => ok_writes += 1,
                    // The reason is recorded by the slot's own counters; the
                    // returned error reports only the shortfall.
                    Err(_) => {}
                }
            }
        } else {
            for (idx, frag) in fragments.iter().enumerate() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let slot_idx = self.select_slot(logical_id, idx);
                match self.slots[slot_idx].put(part_id, frag) {
                    Ok(()) => ok_writes += 1,
                    Err(_) => {}
                }
            }
        }

        if ok_writes == required {
            return Ok(());
        }
        // A client-facing body must not leak which peer refused, so the cause is
        // not part of the returned error; the per-slot `puts_err` counters keep it.
        self.refused_puts.fetch_add(1, Ordering::Relaxed);
        Err(BlobError::RedundancyNotMet {
            id: logical_id,
            written: ok_writes,
            required,
        })
    }

    /// Number of writes refused because they could not reach full redundancy.
    pub fn refused_put_count(&self) -> u64 {
        self.refused_puts.load(Ordering::Relaxed)
    }

    /// Tombstones every physical fragment. Success requires every target,
    /// unlike a data write quorum: an unavailable target must be retried so
    /// its orphan cannot be forgotten.
    pub fn delete(&self, logical_id: BlobId) -> Result<bool, BlobError> {
        let mut deleted = false;
        let mut error = None;
        for idx in 0..self.codec.scheme().total_parts() {
            let part_id = logical_id.with_part((idx + 1) as u8);
            let slot_idx = self.select_slot(logical_id, idx);
            match self.slots[slot_idx].delete(part_id) {
                Ok(existed) => deleted |= existed,
                Err(e) => error = Some(e),
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(deleted),
        }
    }

    /// Reads and reconstructs `logical_id`, preferring zero-hop local replicas for `Mirror`
    /// and transparently tolerating corrupted or offline remote nodes via Reed-Solomon decoding.
    ///
    /// A replica that answers "corrupt" is a positive observation about the data and is
    /// never reported as absence: a bit-rotted but fully present blob must not look like a
    /// blob that was never written, or an operator cannot tell corruption from a missing
    /// object at the API boundary. Corruption only wins when decoding could not produce a
    /// payload from the healthy replicas, so a corrupt mirror copy still reads from a good
    /// one. Offline replicas are deliberately *not* promoted this way: an unreachable node
    /// says nothing about the data, and reporting it as a non-`NotFound` error would turn
    /// the consistent-read path's "map is not a GC fence" `503` into a `500`.
    pub fn get_blob(&self, logical_id: BlobId) -> Result<Vec<u8>, BlobError> {
        let total = self.codec.scheme().total_parts();

        // Fast path for Mirror: try any local slot first (zero network hops!), then fall back to remote peers.
        if matches!(self.codec.scheme(), ErasureScheme::Mirror { .. }) {
            let mut order: Vec<usize> = (0..total).collect();
            order.sort_by_key(|&idx| {
                let slot_idx = self.select_slot(logical_id, idx);
                !self.slots[slot_idx].is_local()
            });

            let mut parts: Vec<Option<Vec<u8>>> = vec![None; total];
            let mut corruption: Option<BlobError> = None;
            for idx in order {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let slot_idx = self.select_slot(logical_id, idx);
                match self.slots[slot_idx].get(part_id) {
                    Ok(bytes) => {
                        parts[idx] = Some(bytes);
                        return self.codec.decode(&parts);
                    }
                    Err(e @ BlobError::CorruptData { .. }) => {
                        corruption.get_or_insert(e);
                    }
                    Err(_) => {}
                }
            }
            return Err(corruption.unwrap_or(BlobError::NotFound(logical_id)));
        }

        // Reed-Solomon: fetch fragments concurrently across cluster nodes if remote slots exist
        let mut parts: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut corruption: Option<BlobError> = None;
        if self.has_remote {
            let fetched: Vec<Result<Vec<u8>, BlobError>> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(total);
                for idx in 0..total {
                    let part_id = logical_id.with_part((idx + 1) as u8);
                    let slot_idx = self.select_slot(logical_id, idx);
                    let slot = &self.slots[slot_idx];
                    handles.push(s.spawn(move || slot.get(part_id)));
                }
                handles
                    .into_iter()
                    // A panicking fetch thread yields no fragment, which is what the
                    // old `join().ok().flatten()` did; treat it as unavailable rather
                    // than inventing an I/O cause.
                    .map(|h| h.join().unwrap_or(Err(BlobError::NotFound(logical_id))))
                    .collect()
            });
            for (idx, result) in fetched.into_iter().enumerate() {
                match result {
                    Ok(bytes) => parts[idx] = Some(bytes),
                    Err(e @ BlobError::CorruptData { .. }) => {
                        corruption.get_or_insert(e);
                    }
                    Err(_) => {}
                }
            }
        } else {
            for (idx, part_slot) in parts.iter_mut().enumerate() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let slot_idx = self.select_slot(logical_id, idx);
                match self.slots[slot_idx].get(part_id) {
                    Ok(bytes) => *part_slot = Some(bytes),
                    Err(e @ BlobError::CorruptData { .. }) => {
                        corruption.get_or_insert(e);
                    }
                    Err(_) => {}
                }
            }
        }

        match self.codec.decode(&parts) {
            Ok(payload) => Ok(payload),
            Err(e) => Err(corruption.unwrap_or(e)),
        }
    }

    /// Deep-scrubs all fragments of `logical_id` across local and remote nodes (verifying CRC32)
    /// and reconstructs + rewrites any missing or bit-rotted fragments!
    ///
    /// A corrupt fragment is deliberately treated as *missing* for reconstruction: that is
    /// what lets the scrub rewrite it from the healthy ones. It is only reported when the
    /// payload cannot be reconstructed at all, and then as corruption rather than as
    /// "insufficient fragments" — the damage was observed, not inferred from a shortfall.
    pub fn repair_blob(&self, logical_id: BlobId) -> Result<RepairReport, BlobError> {
        let total = self.codec.scheme().total_parts();
        let mut slots_data: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut corruption: Option<BlobError> = None;

        if self.has_remote {
            let fetched: Vec<Result<Vec<u8>, BlobError>> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(total);
                for idx in 0..total {
                    let part_id = logical_id.with_part((idx + 1) as u8);
                    let slot_idx = self.select_slot(logical_id, idx);
                    let slot = &self.slots[slot_idx];
                    handles.push(s.spawn(move || slot.get(part_id)));
                }
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or(Err(BlobError::NotFound(logical_id))))
                    .collect()
            });
            for (idx, result) in fetched.into_iter().enumerate() {
                match result {
                    Ok(bytes) => slots_data[idx] = Some(bytes),
                    Err(e @ BlobError::CorruptData { .. }) => {
                        corruption.get_or_insert(e);
                    }
                    Err(_) => {}
                }
            }
        } else {
            for (idx, item) in slots_data.iter_mut().enumerate() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let slot_idx = self.select_slot(logical_id, idx);
                match self.slots[slot_idx].get(part_id) {
                    Ok(bytes) => *item = Some(bytes),
                    Err(e @ BlobError::CorruptData { .. }) => {
                        corruption.get_or_insert(e);
                    }
                    Err(_) => {}
                }
            }
        }

        let intact_parts = slots_data.iter().filter(|x| x.is_some()).count();
        if intact_parts == total {
            return Ok(RepairReport {
                intact_parts,
                repaired_parts: 0,
            });
        }

        let payload = match self.codec.decode(&slots_data) {
            Ok(payload) => payload,
            Err(e) => return Err(corruption.unwrap_or(e)),
        };
        let expected_fragments = self.codec.encode(&payload);
        let mut repaired_parts = 0usize;

        for (idx, slot_val) in slots_data.iter().enumerate() {
            if slot_val.is_none() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                let slot_idx = self.select_slot(logical_id, idx);
                if self.slots[slot_idx]
                    .put(part_id, &expected_fragments[idx])
                    .is_ok()
                {
                    self.slots[slot_idx]
                        .metrics
                        .repairs_ok
                        .fetch_add(1, Ordering::Relaxed);
                    repaired_parts += 1;
                }
            }
        }

        // A failed rewrite is not reported as an error: the payload decoded, so the
        // blob is readable and no data is lost — only its redundancy is still short.
        // Callers compare `intact_parts` with the scheme's part count to see that,
        // which is why `intact_parts` counts fragments *after* the repair. Calling
        // this an error would make the scrub report a readable blob as unrecoverable.
        Ok(RepairReport {
            intact_parts: intact_parts + repaired_parts,
            repaired_parts,
        })
    }

    pub fn health_snapshot(&self, probe_remote: bool) -> PoolHealthSnapshot {
        let slot_snaps: Vec<SlotHealthSnapshot> = if self.has_remote && probe_remote {
            std::thread::scope(|s| {
                let handles: Vec<_> = self
                    .slots
                    .iter()
                    .map(|slot| s.spawn(move || slot.snapshot(true)))
                    .collect();
                handles.into_iter().filter_map(|h| h.join().ok()).collect()
            })
        } else {
            self.slots
                .iter()
                .map(|slot| slot.snapshot(probe_remote))
                .collect()
        };

        let total_slots = slot_snaps.len();
        let online_slots = slot_snaps.iter().filter(|s| s.online).count();
        let min_read_slots = self.min_read_quorum();
        let min_write_slots = self.min_write_quorum();
        let total_repairs = slot_snaps.iter().map(|s| s.repairs_ok).sum();

        let status = if online_slots == total_slots {
            PoolHealthState::Healthy
        } else if online_slots >= min_read_slots {
            PoolHealthState::Degraded
        } else {
            PoolHealthState::Offline
        };

        let scheme_str = match self.codec.scheme() {
            ErasureScheme::Mirror { replicas } => format!("mirror ({replicas}x)"),
            ErasureScheme::ReedSolomon { k, m } => format!("reed-solomon ({k}+{m})"),
        };

        PoolHealthSnapshot {
            pool_name: self.pool_name.clone(),
            scheme: scheme_str,
            status,
            total_slots,
            online_slots,
            min_read_slots,
            min_write_slots,
            total_repairs,
            refused_puts: self.refused_puts.load(Ordering::Relaxed),
            slots: slot_snaps,
        }
    }
}

fn http_raw_request(
    peer_addr: &str,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    token: Option<&str>,
) -> Result<(u16, Vec<u8>), std::io::Error> {
    // Reject header injection rather than allowing a malformed configured secret to alter framing.
    if token.is_some_and(|t| t.bytes().any(|b| b == b'\r' || b == b'\n')) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid Raft token header",
        ));
    }
    let clean = peer_addr
        .trim()
        .strip_prefix("http://")
        .unwrap_or(peer_addr.trim())
        .trim_end_matches('/');
    let sock: SocketAddr = clean
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_millis(300))?;
    stream.set_read_timeout(Some(Duration::from_millis(2000)))?;
    stream.set_write_timeout(Some(Duration::from_millis(2000)))?;

    let token_header = token
        .filter(|t| !t.is_empty())
        .map(|t| format!("x-yadb-raft-token: {t}\r\n"))
        .unwrap_or_default();
    let req_hdr = format!(
        "{method} {path} HTTP/1.1\r\nHost: {clean}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{token_header}Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req_hdr.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let Some(hdr_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed HTTP response from remote VDisk",
        ));
    };

    let hdr_str = String::from_utf8_lossy(&raw[..hdr_end]);
    let status_line = hdr_str.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid HTTP status code from remote VDisk",
            )
        })?;

    let payload = &raw[hdr_end + 4..];
    let is_chunked = hdr_str.lines().any(|l| {
        let lower = l.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });

    if is_chunked {
        Ok((status_code, decode_chunked(payload)?))
    } else {
        Ok((status_code, payload.to_vec()))
    }
}

fn decode_chunked(mut input: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = input.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_str = std::str::from_utf8(&input[..line_end])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let chunk_len = usize::from_str_radix(size_str, 16)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        input = &input[line_end + 2..];
        if chunk_len == 0 {
            break;
        }
        if input.len() < chunk_len + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated HTTP chunked body",
            ));
        }
        out.extend_from_slice(&input[..chunk_len]);
        input = &input[chunk_len + 2..];
    }
    Ok(out)
}
