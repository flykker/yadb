use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use yadb_io::StorageFs;

use crate::{
    BlobError, BlobId, ErasureCodec, ErasureScheme, PinGrant, PinId, PinView, VolumeStats,
    VolumeStore,
};

/// Lifetime of the pin a read takes before fetching a fragment.
///
/// A fragment fetch is a local read or one bounded HTTP request, so this is far
/// longer than any fetch it protects. It is deliberately the maximum the target
/// accepts rather than a short guess: a pin that expires under a slow fetch
/// stops protecting it, while a pin that outlives its reader only delays
/// collection by at most this long.
const READ_PIN_TTL_MS: u64 = crate::MAX_PIN_TTL_MS;

/// Releases a read pin when the fetch scope ends, on every exit path.
///
/// `unpin` is idempotent and never extends a deadline, so an early return, a
/// panic unwind and an explicit release are all safe. A release that fails
/// leaves the pin live until its TTL, which blocks collection rather than
/// admitting a delete - the safe direction, and not a reason to discard bytes
/// the read has already copied.
struct ReadPin<'a, FS: StorageFs> {
    slot: &'a PoolSlot<FS>,
    pin_id: PinId,
}

impl<FS: StorageFs> Drop for ReadPin<'_, FS> {
    fn drop(&mut self) {
        let _ = self.slot.unpin(self.pin_id);
    }
}

/// Whether a failed pin means "the target refused to be read" rather than
/// "the target did not answer".
///
/// The distinction decides what a read reports when no candidate supplied the
/// part. A refusal is evidence about the fence and must not be flattened into
/// `NotFound`; a transport failure is the same unavailability a down slot
/// already produces, and the pool has always read around it.
fn is_read_refusal(error: &BlobError) -> bool {
    matches!(
        error,
        BlobError::PinEpochNotCurrent { .. }
            | BlobError::PinClockUnreadable { .. }
            | BlobError::PinIdsExhausted
            | BlobError::StorageUnavailable
            | BlobError::RemoteFenceRefused { .. }
    )
}

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
    /// Spare slots that hold a part when its nominal slot does not answer. They
    /// are not counted in `total_slots`/`online_slots`: health describes whether
    /// the canonical placement is whole, and a handoff slot being offline is a
    /// consequence of the same node being offline.
    pub handoff_slots: usize,
    pub online_slots: usize,
    pub min_read_slots: usize,
    pub min_write_slots: usize,
    pub total_repairs: u64,
    /// Writes refused because fewer than the pool's write quorum of fragments
    /// landed. A refused write is not acknowledged and publishes nothing, so this
    /// counter is a loss of write availability, not a queue of work to finish
    /// later.
    pub refused_puts: u64,
    /// Writes acknowledged with fewer than `total_slots` fragments. Only a pool
    /// that opted into a reduced `minWriteQuorum` can produce one: the ack
    /// promised the quorum it configured, not the pool's full redundancy. Every
    /// such write is a blob that a repair pass still has to bring back to full
    /// redundancy, so this is the observable backlog.
    pub degraded_puts: u64,
    pub slots: Vec<SlotHealthSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarrierRequest {
    pub project_id: u32,
    pub owner_id: u32,
    pub channel: u8,
    pub up_to_generation: u32,
    /// The collector reports compaction separately from the barrier, so it asks
    /// for the durable barrier alone. A caller that omits the field keeps the
    /// previous barrier-then-compact behaviour.
    #[serde(default = "compact_by_default")]
    pub compact: bool,
    /// Deadline of the collector lease this barrier was issued under.
    ///
    /// It travels with the request because a barrier evicts a whole generation
    /// range in one durable record: once it is on the wire, the per-tombstone
    /// fence interval cannot bound it. The target compares this deadline with its
    /// own clock immediately before the eviction and refuses the barrier if the
    /// lease already lapsed, so a request that waited in a queue or crossed a
    /// paused peer does not delete on behalf of a pass that lost its fence.
    pub lease_expires_at_ms: u64,
}

fn compact_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BarrierResponse {
    pub evicted: usize,
    /// Bytes freed by the compaction that followed this barrier, when requested.
    #[serde(default)]
    pub reclaimed_bytes: u64,
    /// A compaction failure is reported, not swallowed: the barrier is already
    /// durable, but the caller must not believe space was reclaimed.
    #[serde(default)]
    pub compaction_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlobIdList {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactResponse {
    pub reclaimed_bytes: u64,
}

// Wire bodies of the storage-fence RPC. They are private to the transport: the
// public surface is `PoolSlot`'s methods, and the daemon speaks the same JSON
// through its own request types.
#[derive(Debug, Serialize, Deserialize)]
struct EpochWire {
    epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PinRefWire {
    pin_id: u64,
    epoch: u64,
    expires_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PinsWire {
    pins: Vec<PinRefWire>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PinCallWire {
    epoch: u64,
    ttl_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct UnpinCallWire {
    pin_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReleasedWire {
    released: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct AdvanceCallWire {
    expected_epoch: u64,
}

#[derive(Debug, Deserialize)]
struct FenceErrorWire {
    error: String,
    #[serde(default)]
    message: Option<String>,
}

/// Maps a non-200 answer from a fenced endpoint to a durable-state error.
///
/// A fence refusal is an answer, not a transport failure: turning it into
/// `BlobError::Io` would mark the slot offline and let the caller hand the work
/// to another slot, silently defeating the fence. Only a status-less transport
/// error is `Io`, and that is decided by the caller that owns the socket.
fn fence_refusal(status: u16, body: &[u8]) -> BlobError {
    let Ok(wire) = serde_json::from_slice::<FenceErrorWire>(body) else {
        return BlobError::RemoteFenceRefused {
            status,
            code: "unparsed".to_string(),
            detail: String::from_utf8_lossy(body).into_owned(),
        };
    };
    if wire.error == "storage_unavailable" {
        return BlobError::StorageUnavailable;
    }
    BlobError::RemoteFenceRefused {
        status,
        code: wire.error,
        detail: wire.message.unwrap_or_default(),
    }
}

/// Parses the answer of a fenced endpoint. A non-200 answer is mapped through
/// [`fence_refusal`] first, so its stable wire code is preserved; a 200 body
/// that does not match the contract is still an answer from the slot, so it
/// must not become `Io` either.
fn fence_json<T: DeserializeOwned>(status: u16, body: &[u8]) -> Result<T, BlobError> {
    if status != 200 {
        return Err(fence_refusal(status, body));
    }
    serde_json::from_slice(body).map_err(|error| BlobError::RemoteFenceRefused {
        status,
        code: "unparsed".to_string(),
        detail: format!("{error}: {}", String::from_utf8_lossy(body)),
    })
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

    /// Durable generation barrier on one physical slot. Evicts indexed fragments
    /// of `(project, owner, channel)` at or below `up_to_generation` and fences
    /// later writes of that range. Compaction is a separate step the collector
    /// drives once per slot so its bytes and failures can be reported.
    ///
    /// `lease_expires_at_ms` is the deadline of the collector lease the caller
    /// issued this barrier under. A local slot checks it here and a remote slot
    /// sends it so the target checks it again on arrival: the eviction is the one
    /// deletion that is not bounded by the fence interval, so it must not happen
    /// after the lease that authorised it has lapsed.
    pub fn apply_barrier(
        &self,
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
        compact: bool,
        lease_expires_at_ms: u64,
    ) -> Result<BarrierResponse, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => {
                let evicted = store.collect_garbage_barrier(
                    project_id,
                    owner_id,
                    channel,
                    up_to_generation,
                    lease_expires_at_ms,
                )?;
                if !compact {
                    return Ok(BarrierResponse {
                        evicted,
                        ..BarrierResponse::default()
                    });
                }
                let mut response = BarrierResponse {
                    evicted,
                    ..BarrierResponse::default()
                };
                match store.seal_and_compact() {
                    Ok(bytes) => response.reclaimed_bytes = bytes,
                    Err(error) => response.compaction_error = Some(error.to_string()),
                }
                Ok(response)
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
                    compact,
                    lease_expires_at_ms,
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
                        serde_json::from_slice(&resp)
                            .map_err(|e| BlobError::Io(std::io::Error::other(e)))
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

    /// Every physical fragment id this slot currently holds.
    pub fn scan_live(&self) -> Result<Vec<BlobId>, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => Ok(store.live_ids()),
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!("/internal/vdisk/{pool_name}/{slot_index}/blobs");
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
                        let list: BlobIdList = serde_json::from_slice(&body)
                            .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                        let mut ids = Vec::with_capacity(list.ids.len());
                        for hex in list.ids {
                            ids.push(BlobId::from_hex(&hex).ok_or_else(|| {
                                BlobError::Io(std::io::Error::other(format!(
                                    "remote slot returned a malformed BlobId {hex}"
                                )))
                            })?);
                        }
                        Ok(ids)
                    }
                    Ok((code, body)) => Err(BlobError::Io(std::io::Error::other(format!(
                        "remote vdisk blobs HTTP {code}: {}",
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

    /// Seals a wasteful active segment and compacts every sealed segment.
    pub fn seal_and_compact(&self) -> Result<u64, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.seal_and_compact(),
            SlotBackend::Remote {
                pool_name,
                slot_index,
                peer_addr,
                token,
            } => {
                let path = format!("/internal/vdisk/{pool_name}/{slot_index}/compact");
                match http_raw_request(
                    peer_addr,
                    "POST",
                    &path,
                    "application/json",
                    &[],
                    token.as_deref(),
                ) {
                    Ok((200, body)) => {
                        self.metrics.online.store(true, Ordering::Relaxed);
                        let parsed: CompactResponse = serde_json::from_slice(&body)
                            .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                        Ok(parsed.reclaimed_bytes)
                    }
                    Ok((code, body)) => Err(BlobError::Io(std::io::Error::other(format!(
                        "remote vdisk compact HTTP {code}: {}",
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

    /// Sends one storage-fence RPC to a remote slot.
    ///
    /// An HTTP response marks the slot online even when it refuses the request:
    /// a refusal is durable state the caller must stop on. Only a transport
    /// error is `Io` and marks the slot offline, because only then is "this slot
    /// did not answer" true.
    fn fence_request(
        &self,
        method: &str,
        suffix: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<(u16, Vec<u8>), BlobError> {
        let SlotBackend::Remote {
            pool_name,
            slot_index,
            peer_addr,
            token,
        } = &self.backend
        else {
            unreachable!("fence_request is only called for a remote slot");
        };
        let path = format!("/internal/vdisk/{pool_name}/{slot_index}/{suffix}");
        match http_raw_request(
            peer_addr,
            method,
            &path,
            content_type,
            body,
            token.as_deref(),
        ) {
            Ok((status, body)) => {
                self.metrics.online.store(true, Ordering::Relaxed);
                Ok((status, body))
            }
            Err(error) => {
                self.metrics.online.store(false, Ordering::Relaxed);
                Err(BlobError::Io(error))
            }
        }
    }

    /// The durable target epoch of the slot's volume.
    pub fn epoch(&self) -> Result<u64, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => {
                self.metrics.online.store(true, Ordering::Relaxed);
                Ok(store.epoch())
            }
            SlotBackend::Remote { .. } => {
                let (status, body) = self.fence_request("GET", "epoch", "application/json", &[])?;
                let wire: EpochWire = fence_json(status, &body)?;
                Ok(wire.epoch)
            }
        }
    }

    /// Every unexpired reader pin on the slot's volume.
    pub fn live_pins(&self) -> Result<Vec<PinView>, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.live_pins(),
            SlotBackend::Remote { .. } => {
                let (status, body) = self.fence_request("GET", "pins", "application/json", &[])?;
                let wire: PinsWire = fence_json(status, &body)?;
                Ok(wire
                    .pins
                    .into_iter()
                    .map(|pin| PinView {
                        pin_id: PinId::from_wire(pin.pin_id),
                        epoch: pin.epoch,
                        expires_at_ms: pin.expires_at_ms,
                    })
                    .collect())
            }
        }
    }

    /// Takes a reader pin on `epoch` for `ttl_ms`.
    pub fn pin(&self, epoch: u64, ttl_ms: u64) -> Result<PinGrant, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.pin(epoch, ttl_ms),
            SlotBackend::Remote { .. } => {
                let body = serde_json::to_vec(&PinCallWire { epoch, ttl_ms })
                    .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                let (status, body) =
                    self.fence_request("POST", "pin", "application/json", &body)?;
                let wire: PinRefWire = fence_json(status, &body)?;
                Ok(PinGrant {
                    pin_id: PinId::from_wire(wire.pin_id),
                    epoch: wire.epoch,
                    expires_at_ms: wire.expires_at_ms,
                })
            }
        }
    }

    /// Releases `pin_id`. Idempotent: `false` means it was already gone.
    pub fn unpin(&self, pin_id: PinId) -> Result<bool, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.unpin(pin_id),
            SlotBackend::Remote { .. } => {
                let body = serde_json::to_vec(&UnpinCallWire {
                    pin_id: pin_id.get(),
                })
                .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                let (status, body) =
                    self.fence_request("POST", "unpin", "application/json", &body)?;
                let wire: ReleasedWire = fence_json(status, &body)?;
                Ok(wire.released)
            }
        }
    }

    /// Raises the slot's epoch from exactly `expected_epoch` and returns the new
    /// value.
    pub fn advance_epoch(&self, expected_epoch: u64) -> Result<u64, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.advance_epoch(expected_epoch),
            SlotBackend::Remote { .. } => {
                let body = serde_json::to_vec(&AdvanceCallWire { expected_epoch })
                    .map_err(|e| BlobError::Io(std::io::Error::other(e)))?;
                let (status, body) =
                    self.fence_request("POST", "advance", "application/json", &body)?;
                let wire: EpochWire = fence_json(status, &body)?;
                Ok(wire.epoch)
            }
        }
    }

    /// Deletes `id` only if the slot holds it under exactly `expected_epoch`.
    pub fn delete_fenced(&self, id: BlobId, expected_epoch: u64) -> Result<bool, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.delete_fenced(id, expected_epoch),
            SlotBackend::Remote { .. } => {
                let suffix = format!("blob/{}?fence_epoch={expected_epoch}", id.to_hex());
                self.delete_fenced_response(&suffix)
            }
        }
    }

    /// Deletes `id` only if its epoch is older than `collector_epoch` and no
    /// unexpired pin names it.
    pub fn delete_collectable(&self, id: BlobId, collector_epoch: u64) -> Result<bool, BlobError> {
        match &self.backend {
            SlotBackend::Local(store) => store.delete_collectable(id, collector_epoch),
            SlotBackend::Remote { .. } => {
                let suffix = format!("blob/{}?collector_epoch={collector_epoch}", id.to_hex());
                self.delete_fenced_response(&suffix)
            }
        }
    }

    fn delete_fenced_response(&self, suffix: &str) -> Result<bool, BlobError> {
        let (status, body) =
            self.fence_request("DELETE", suffix, "application/octet-stream", &[])?;
        if status != 200 {
            return Err(fence_refusal(status, &body));
        }
        match body.as_slice() {
            b"deleted" => Ok(true),
            b"absent" => Ok(false),
            other => Err(BlobError::RemoteFenceRefused {
                status,
                code: "unexpected_body".to_string(),
                detail: String::from_utf8_lossy(other).into_owned(),
            }),
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
    /// Writes refused because fewer than `write_quorum` fragments landed.
    /// A refused write is not acknowledged, so there is nothing to repair: the
    /// counter is the observable signal that the pool lost write availability.
    refused_puts: AtomicU64,
    /// Writes acknowledged below the pool's full redundancy. Non-zero only for a
    /// pool whose configured quorum is below `total_parts()`.
    degraded_puts: AtomicU64,
    /// Fragments a write must land before it may be acknowledged. Defaults to
    /// every fragment; a pool opts down explicitly.
    write_quorum: usize,
    /// Number of *nominal* slots, i.e. `total_parts()`. Slots `[0, nominal_slots)`
    /// are the canonical placement of each part; slots `[nominal_slots, len)`
    /// are handoff slots.
    ///
    /// This mirrors YDB's DSProxy. In YDB a subgroup has `BlobSubgroupSize()`
    /// disks, the first `TotalPartCount()` of which are the main disks for the
    /// parts, and the rest are *handoff* disks: when a main disk does not answer,
    /// the part is written to a handoff disk instead and the write is still
    /// acknowledged with full redundancy. The number of handoff disks bounds how
    /// many disks may fail while the group can still place every part.
    ///
    /// Keeping the nominal count separate from `slots.len()` is what makes the
    /// layout stable: adding handoff slots must not move existing parts, so
    /// `select_slot` reduces modulo `nominal_slots`, not modulo the slot count.
    nominal_slots: usize,
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
            degraded_puts: AtomicU64::new(0),
            write_quorum: scheme.total_parts(),
            nominal_slots: scheme.total_parts(),
        })
    }

    /// Number of handoff slots: spare placements used when a nominal slot does
    /// not answer. Zero keeps the historical behaviour exactly.
    pub fn handoff_slot_count(&self) -> usize {
        self.slots.len() - self.nominal_slots
    }

    /// Lowers the fragment count a write must land before it may be acknowledged.
    ///
    /// Opt-in per pool. The default is every fragment, so an ack promises the
    /// pool's full redundancy and a missing fragment is a refused write. A pool
    /// that sets a lower quorum accepts a degraded write: the ack then promises
    /// only that `quorum` fragments are durable, and it is counted in
    /// `degraded_puts` until a repair pass restores full redundancy.
    ///
    /// The floor is the read threshold, because a quorum below it could
    /// acknowledge a write the pool cannot read back. The ceiling is
    /// `total_parts()`, because more fragments do not exist.
    pub fn with_min_write_quorum(mut self, quorum: usize) -> Result<Self, BlobError> {
        let floor = self.min_read_quorum();
        let ceiling = self.codec.scheme().total_parts();
        if quorum < floor || quorum > ceiling {
            return Err(BlobError::InvalidErasure(format!(
                "minWriteQuorum {quorum} for pool `{}` is outside {floor}..={ceiling} \
                 (scheme {:?})",
                self.pool_name,
                self.codec.scheme()
            )));
        }
        self.write_quorum = quorum;
        Ok(self)
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
    /// Defaults to every fragment: an ack then promises the pool's full
    /// redundancy, so a missing fragment is a refused write rather than a
    /// degraded one, and a pool with any slot offline is read-only for new
    /// writes. A pool that explicitly configured a lower `minWriteQuorum`
    /// accepts a degraded write instead, and every such ack is counted in
    /// `degraded_puts` until repair restores the missing fragments.
    /// Reads still need only `k` fragments (Reed-Solomon) or one replica (mirror).
    pub fn min_write_quorum(&self) -> usize {
        self.write_quorum
    }

    /// Deterministic subgroup placement based on `BlobId` hash so load spreads evenly across slots
    /// and every node in the cluster computes the exact same slot mapping without extra coordination.
    ///
    /// The modulus is `nominal_slots`, never `slots.len()`: appending handoff
    /// slots must not move a single existing part.
    fn select_slot(&self, logical_id: BlobId, part_index: usize) -> usize {
        let h =
            (logical_id.with_part(0).as_u128() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize;
        (h + part_index) % self.nominal_slots
    }

    /// Handoff slots in the order this blob should try them.
    ///
    /// Rotating by the blob hash spreads handoff load across the spare slots
    /// instead of always filling the first one. The order is deterministic for a
    /// given id, so two writers that both fall back pick the same slots.
    fn handoff_order(&self, logical_id: BlobId) -> impl Iterator<Item = usize> + '_ {
        let count = self.handoff_slot_count();
        let start = if count == 0 {
            0
        } else {
            let h = (logical_id.with_part(0).as_u128() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                as usize;
            h % count
        };
        (0..count).map(move |i| self.nominal_slots + (start + i) % count)
    }

    /// Test-only view of the spare order for one blob, so a test can fail the
    /// spare the writer would try first and assert the one it would try next.
    /// Deliberately not part of the public surface: callers must never pin a
    /// placement.
    #[cfg(test)]
    pub(crate) fn handoff_order_for(&self, logical_id: BlobId) -> Vec<usize> {
        self.handoff_order(logical_id).collect()
    }

    /// Every slot that may hold part `idx` of `logical_id`, nominal first.
    ///
    /// A part is written to exactly one slot, but which one depends on whether
    /// its nominal slot answered at write time. Reads therefore probe the nominal
    /// slot and then the handoff slots; there is no location index to consult,
    /// which is also why deletion must tombstone every candidate.
    fn part_candidates(&self, logical_id: BlobId, part_index: usize) -> Vec<usize> {
        let mut out = Vec::with_capacity(1 + self.handoff_slot_count());
        out.push(self.select_slot(logical_id, part_index));
        out.extend(self.handoff_order(logical_id));
        out
    }

    /// Reads one part from wherever it actually lives, under a pin on the
    /// target's current epoch.
    ///
    /// Returns the bytes and the slot that supplied them; `Ok(None)` means no
    /// candidate held the part. `Err` means at least one candidate could not be
    /// pinned and no other candidate supplied the part, so absence was never
    /// proved - reporting that as `NotFound` would turn "the target refused to
    /// be read" into "the bytes do not exist".
    ///
    /// A `CorruptData` answer is remembered but does not stop the search: a
    /// bit-rotted nominal copy must not hide a healthy handoff copy.
    ///
    /// The pin is taken before the fetch and released as soon as the bytes are
    /// copied, on every exit path. It is what makes a concurrent
    /// `delete_collectable` on the target refuse: the target consults any live
    /// pin, not only one that names the fragment's own epoch, because the only
    /// epoch a reader can pin is the current one.
    fn read_part(
        &self,
        logical_id: BlobId,
        part_index: usize,
        corruption: &mut Option<BlobError>,
    ) -> Result<Option<(Vec<u8>, usize)>, BlobError> {
        let part_id = logical_id.with_part((part_index + 1) as u8);
        let mut refusal: Option<BlobError> = None;
        for slot_idx in self.part_candidates(logical_id, part_index) {
            let slot = &self.slots[slot_idx];
            // Two calls, not one: the epoch must be read before it is named, and
            // a collector may advance in between. The pin then fails closed
            // rather than pinning an epoch that is no longer current.
            let grant = match slot
                .epoch()
                .and_then(|epoch| slot.pin(epoch, READ_PIN_TTL_MS))
            {
                Ok(grant) => grant,
                Err(error) => {
                    // Not "the fragment is absent": this candidate could not be
                    // proved safe to read. Another candidate has its own epoch
                    // and pin table, so it is still worth trying; the reason is
                    // kept in case none of them supplies the part.
                    //
                    // Only a refusal is kept. A transport failure is the same
                    // unavailability a down slot already produces, and turning
                    // it into the read's result would report a network fault as
                    // a statement about the fence.
                    if is_read_refusal(&error) {
                        refusal.get_or_insert(error);
                    }
                    continue;
                }
            };
            let _pin = ReadPin { slot, pin_id: grant.pin_id };
            match slot.get(part_id) {
                Ok(bytes) => return Ok(Some((bytes, slot_idx))),
                Err(e @ BlobError::CorruptData { .. }) => {
                    corruption.get_or_insert(e);
                }
                Err(_) => {}
            }
        }
        match refusal {
            Some(error) => Err(error),
            None => Ok(None),
        }
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
    ///
    /// A slot that *refuses* the write — `ImmutableConflict`,
    /// `FencedByBarrier`, `Tombstoned`, or any other non-`Io` failure — is not a
    /// missing slot. Such a refusal aborts the write and is returned unchanged:
    /// the part is never retried on a spare, because an ack that the refused
    /// bytes were durable elsewhere would be a lie about what reads return.
    /// Only `BlobError::Io` is handoff-eligible.
    pub fn put_blob(&self, logical_id: BlobId, payload: &[u8]) -> Result<(), BlobError> {
        let fragments = self.codec.encode(payload);
        let required = self.min_write_quorum();
        let total = self.codec.scheme().total_parts();

        // Nominal attempt: one part per nominal slot, all in parallel.
        let nominal: Vec<usize> = (0..total)
            .map(|idx| self.select_slot(logical_id, idx))
            .collect();
        let mut landed = vec![false; total];
        // Why each part did not land, when the slot gave a reason. Keeping the
        // reason is what lets a refusal be told apart from an unreachable slot;
        // flattening the result to `landed[idx] = r.is_ok()` loses that.
        let mut failures: Vec<Option<BlobError>> = (0..total).map(|_| None).collect();

        if self.has_remote {
            let results: Vec<(usize, Result<(), BlobError>)> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(fragments.len());
                for (idx, frag) in fragments.iter().enumerate() {
                    let part_id = logical_id.with_part((idx + 1) as u8);
                    let slot = &self.slots[nominal[idx]];
                    handles.push((idx, s.spawn(move || slot.put(part_id, frag))));
                }
                handles
                    .into_iter()
                    .map(|(idx, h)| {
                        (
                            idx,
                            h.join().unwrap_or_else(|_| {
                                Err(BlobError::Io(std::io::Error::other(
                                    "slot put thread panicked",
                                )))
                            }),
                        )
                    })
                    .collect()
            });

            for (idx, r) in results {
                // The reason is recorded by the slot's own counters; the write
                // reports the refusal itself when it is permanent.
                match r {
                    Ok(()) => landed[idx] = true,
                    Err(e) => failures[idx] = Some(e),
                }
            }
        } else {
            for (idx, frag) in fragments.iter().enumerate() {
                let part_id = logical_id.with_part((idx + 1) as u8);
                match self.slots[nominal[idx]].put(part_id, frag) {
                    Ok(()) => landed[idx] = true,
                    Err(e) => failures[idx] = Some(e),
                }
            }
        }

        // A permanent refusal is not a missing slot. `ImmutableConflict`,
        // `FencedByBarrier` and `Tombstoned` are durable statements about the
        // id: the pool was told "no", and writing the part to a spare would
        // acknowledge bytes that reads can never return (or resurrect a blob
        // that a barrier or a tombstone just fenced). Only a slot that could not
        // answer is handoff-eligible, and a refusal is propagated unchanged
        // rather than retried or reported as a generic shortfall. `refused_puts`
        // is deliberately not bumped: it counts lost write *availability*, and a
        // refusal here is a correctness decision, not an availability loss.
        if let Some(refusal) = failures.into_iter().flatten().find(|e| !e.is_unavailable()) {
            return Err(refusal);
        }

        // Handoff: a part whose nominal slot did not answer is placed on a spare
        // slot instead of failing the whole write. This is the YDB handoff-disk
        // rule, and it is why a node loss no longer has to cost redundancy: the
        // ack still means every part is durable, so the pool keeps its full
        // redundancy without lowering the write quorum.
        //
        // Two rules keep the placement honest:
        //  * a handoff slot holds at most one part of this blob, because YDB's
        //    `CountEffectiveReplicas` counts distinct disks — two parts on one
        //    disk are one replica, not two;
        //  * a handoff slot on the node that just failed is skipped, since that
        //    node is the one that did not answer in the first place.
        //
        // Every spare attempt in a round runs in parallel, like YDB's batched
        // puts. Doing them one after another made a node loss cost
        // `failed_parts * handoff_slots` connection timeouts inside a single
        // write, which on its own pushed a 24-way burst past the commit budget
        // and turned a slow node into an election. A round is bounded by one
        // slot's timeout instead, and the loop repeats for as long as some
        // unplaced part still has an untried eligible spare.
        //
        // Termination is by exhaustion, never by a progress check: an earlier
        // version broke out of the loop when a whole round failed, so one
        // unreachable spare stopped the search while a healthy spare beside it
        // was still untried. `used` grows by every assigned slot in every round
        // and never shrinks, so each round consumes at least one spare from the
        // finite handoff set and the loop runs at most `handoff_slot_count()`
        // times. `assignments.is_empty()` is the only exit.
        // Slots unavailable to this blob: its nominal slots, plus every spare
        // that already holds a part or refused one.
        let mut used: BTreeSet<usize> = nominal.iter().copied().collect();
        let mut rounds = 0usize;
        loop {
            // Assign each still-unplaced part a distinct spare, skipping spares
            // on the node whose nominal slot failed for that part.
            let mut assignments: Vec<(usize, usize)> = Vec::new();
            let mut taken: BTreeSet<usize> = used.clone();
            for idx in 0..total {
                if landed[idx] {
                    continue;
                }
                let failed_node = self.slots[nominal[idx]].node_id();
                let candidate = self.handoff_order(logical_id).find(|&slot| {
                    !taken.contains(&slot) && self.slots[slot].node_id() != failed_node
                });
                if let Some(slot) = candidate {
                    taken.insert(slot);
                    assignments.push((idx, slot));
                }
            }
            if assignments.is_empty() {
                break;
            }
            rounds += 1;
            debug_assert!(
                rounds <= self.handoff_slot_count(),
                "handoff loop must terminate by exhausting the {} spare slots, ran {rounds} rounds",
                self.handoff_slot_count()
            );
            let placed = if self.has_remote {
                std::thread::scope(|s| {
                    let handles: Vec<(usize, usize, _)> = assignments
                        .iter()
                        .map(|&(idx, slot)| {
                            let part_id = logical_id.with_part((idx + 1) as u8);
                            let frag = &fragments[idx];
                            let target = &self.slots[slot];
                            (idx, slot, s.spawn(move || target.put(part_id, frag)))
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|(idx, slot, h)| {
                            (
                                idx,
                                slot,
                                // A panicked thread is an unavailable slot, not
                                // a refusal: there is no reason to report.
                                h.join().unwrap_or_else(|_| {
                                    Err(BlobError::Io(std::io::Error::other(
                                        "slot put thread panicked",
                                    )))
                                }),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            } else {
                assignments
                    .iter()
                    .map(|&(idx, slot)| {
                        let part_id = logical_id.with_part((idx + 1) as u8);
                        (idx, slot, self.slots[slot].put(part_id, &fragments[idx]))
                    })
                    .collect()
            };
            for (idx, slot, result) in placed {
                // The slot is consumed either way: a spare that accepted the part
                // holds it, and one that refused must not be retried with a
                // different part of the same blob in the same write.
                used.insert(slot);
                match result {
                    Ok(()) => landed[idx] = true,
                    // A spare that refuses permanently refuses the whole write
                    // with that refusal. Moving the part to another spare would
                    // be exactly the bypass the nominal attempt already rejects.
                    Err(e) if !e.is_unavailable() => return Err(e),
                    // An unreachable spare only costs this attempt; the next
                    // round offers the part to the next untried spare.
                    Err(_) => {}
                }
            }
        }

        let ok_writes = landed.iter().filter(|x| **x).count();

        // `>=`, not `==`: a healthy pool lands every fragment, which is above a
        // reduced quorum. Comparing for equality would refuse a write on a fully
        // healthy cluster the moment a pool opts into a lower quorum.
        if ok_writes >= required {
            if ok_writes < total {
                self.degraded_puts.fetch_add(1, Ordering::Relaxed);
            }
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

    /// Number of writes refused because they could not reach the pool's quorum.
    pub fn refused_put_count(&self) -> u64 {
        self.refused_puts.load(Ordering::Relaxed)
    }

    /// Number of writes acknowledged below full redundancy.
    pub fn degraded_put_count(&self) -> u64 {
        self.degraded_puts.load(Ordering::Relaxed)
    }

    /// Tombstones every physical fragment. Success requires every target,
    /// unlike a data write quorum: an unavailable target must be retried so
    /// its orphan cannot be forgotten.
    pub fn delete(&self, logical_id: BlobId) -> Result<bool, BlobError> {
        // A part may sit on its nominal slot or on a handoff slot, and no
        // manifest records which. Every candidate is tombstoned: leaving a
        // handoff copy behind would let a deleted blob reappear the moment the
        // nominal slot answered `NotFound`. The candidates are swept in
        // parallel so a delete still costs one round trip, not one per slot.
        let mut targets = Vec::new();
        for idx in 0..self.codec.scheme().total_parts() {
            let part_id = logical_id.with_part((idx + 1) as u8);
            for slot_idx in self.part_candidates(logical_id, idx) {
                targets.push((part_id, slot_idx));
            }
        }

        let results: Vec<Result<bool, BlobError>> = if self.has_remote {
            std::thread::scope(|s| {
                let handles: Vec<_> = targets
                    .iter()
                    .map(|&(part_id, slot_idx)| {
                        let slot = &self.slots[slot_idx];
                        s.spawn(move || slot.delete(part_id))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Err(BlobError::Io(std::io::Error::other(
                                "slot delete thread panicked",
                            )))
                        })
                    })
                    .collect()
            })
        } else {
            targets
                .iter()
                .map(|&(part_id, slot_idx)| self.slots[slot_idx].delete(part_id))
                .collect()
        };

        let mut deleted = false;
        let mut error = None;
        for result in results {
            match result {
                Ok(existed) => deleted |= existed,
                // Every target must answer: an unreachable slot could still hold
                // a copy, so its tombstone has to be retried rather than assumed.
                Err(e) => error = error.or(Some(e)),
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

        // Fast path for Mirror: try parts whose nominal slot is local first
        // (zero network hops!), then the rest. `read_part` falls back to handoff
        // slots, so a part written to a spare slot is still found.
        if matches!(self.codec.scheme(), ErasureScheme::Mirror { .. }) {
            let mut order: Vec<usize> = (0..total).collect();
            order.sort_by_key(|&idx| !self.slots[self.select_slot(logical_id, idx)].is_local());

            let mut parts: Vec<Option<Vec<u8>>> = vec![None; total];
            let mut corruption: Option<BlobError> = None;
            let mut refusal: Option<BlobError> = None;
            for idx in order {
                match self.read_part(logical_id, idx, &mut corruption) {
                    Ok(Some((bytes, _))) => {
                        parts[idx] = Some(bytes);
                        return self.codec.decode(&parts);
                    }
                    // A refused copy does not make another replica unsafe to
                    // read: the next candidate takes its own pin.
                    Ok(None) => {}
                    Err(error) => {
                        refusal.get_or_insert(error);
                    }
                }
            }
            // Observed damage first, then "a target refused to be read", then
            // "nothing had it": the last answer must not hide the first two.
            return Err(corruption
                .or(refusal)
                .unwrap_or(BlobError::NotFound(logical_id)));
        }

        // Reed-Solomon: fetch fragments concurrently across cluster nodes if remote slots exist
        let mut parts: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut corruption: Option<BlobError> = None;
        let mut refusal: Option<BlobError> = None;
        if self.has_remote {
            let fetched: Vec<(Result<Option<(Vec<u8>, usize)>, BlobError>, Option<BlobError>)> =
                std::thread::scope(|s| {
                    let mut handles = Vec::with_capacity(total);
                    for idx in 0..total {
                        handles.push(s.spawn(move || {
                            let mut corruption = None;
                            let found = self.read_part(logical_id, idx, &mut corruption);
                            (found, corruption)
                        }));
                    }
                    handles
                        .into_iter()
                        // A panicking fetch thread yields no fragment, which is what
                        // the old `join().ok().flatten()` did; treat it as unavailable
                        // rather than inventing an I/O cause.
                        .map(|h| h.join().unwrap_or((Ok(None), None)))
                        .collect()
                });
            for (idx, (found, corrupt)) in fetched.into_iter().enumerate() {
                match found {
                    Ok(Some((bytes, _))) => parts[idx] = Some(bytes),
                    Ok(None) => {}
                    Err(error) => {
                        refusal.get_or_insert(error);
                    }
                }
                if let Some(e) = corrupt {
                    corruption.get_or_insert(e);
                }
            }
        } else {
            for idx in 0..total {
                match self.read_part(logical_id, idx, &mut corruption) {
                    Ok(Some((bytes, _))) => parts[idx] = Some(bytes),
                    Ok(None) => {}
                    Err(error) => {
                        refusal.get_or_insert(error);
                    }
                }
            }
        }

        match self.codec.decode(&parts) {
            // Every part that was used was read under its own pin, so a
            // reconstruction that succeeds is safe even if another copy refused.
            Ok(payload) => Ok(payload),
            // The payload could not be reconstructed. Observed damage and a
            // target that refused to be read are both more informative than
            // "not enough fragments", which would read as absence.
            Err(e) => Err(corruption.or(refusal).unwrap_or(e)),
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
        // Which slot actually supplied each part. Repair needs this to tell "on
        // its nominal slot" from "on a handoff slot that has to be moved back".
        let mut sources: Vec<Option<usize>> = vec![None; total];
        let mut corruption: Option<BlobError> = None;

        if self.has_remote {
            let fetched: Vec<(Result<Option<(Vec<u8>, usize)>, BlobError>, Option<BlobError>)> =
                std::thread::scope(|s| {
                    let mut handles = Vec::with_capacity(total);
                    for idx in 0..total {
                        handles.push(s.spawn(move || {
                            let mut corruption = None;
                            let found = self.read_part(logical_id, idx, &mut corruption);
                            (found, corruption)
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap_or((Ok(None), None)))
                        .collect()
                });
            for (idx, (found, corrupt)) in fetched.into_iter().enumerate() {
                match found {
                    Ok(Some((bytes, slot))) => {
                        slots_data[idx] = Some(bytes);
                        sources[idx] = Some(slot);
                    }
                    Ok(None) => {}
                    // A part that could not be pinned is not a missing part.
                    // Treating it as missing would reconstruct and rewrite a
                    // fragment a reader may be holding, so the repair stops.
                    Err(error) => return Err(error),
                }
                if let Some(e) = corrupt {
                    corruption.get_or_insert(e);
                }
            }
        } else {
            for idx in 0..total {
                match self.read_part(logical_id, idx, &mut corruption) {
                    Ok(Some((bytes, slot))) => {
                        slots_data[idx] = Some(bytes);
                        sources[idx] = Some(slot);
                    }
                    Ok(None) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        let intact_parts = slots_data.iter().filter(|x| x.is_some()).count();

        // Every part present and on its canonical slot: nothing to do.
        let all_nominal =
            (0..total).all(|idx| sources[idx] == Some(self.select_slot(logical_id, idx)));
        if intact_parts == total && all_nominal {
            return Ok(RepairReport {
                intact_parts,
                repaired_parts: 0,
            });
        }

        // Only a missing part needs reconstruction. A part that is present but
        // living on a handoff slot already has the right bytes; it is moved, not
        // rebuilt.
        let rebuilt = if intact_parts < total {
            match self.codec.decode(&slots_data) {
                Ok(payload) => Some(self.codec.encode(&payload)),
                Err(e) => return Err(corruption.unwrap_or(e)),
            }
        } else {
            None
        };

        let mut repaired_parts = 0usize;
        let mut new_parts = 0usize;

        for idx in 0..total {
            let nominal = self.select_slot(logical_id, idx);
            if sources[idx] == Some(nominal) {
                continue;
            }
            let data = match (&rebuilt, &slots_data[idx]) {
                (Some(fragments), _) => fragments[idx].clone(),
                (None, Some(bytes)) => bytes.clone(),
                // Unreachable: a part with no source implies `intact_parts <
                // total`, which means `rebuilt` is `Some`.
                (None, None) => continue,
            };
            let part_id = logical_id.with_part((idx + 1) as u8);
            if self.slots[nominal].put(part_id, &data).is_err() {
                continue;
            }
            self.slots[nominal]
                .metrics
                .repairs_ok
                .fetch_add(1, Ordering::Relaxed);
            repaired_parts += 1;
            if slots_data[idx].is_none() {
                new_parts += 1;
            }
            // The canonical copy is durable, so the handoff copy may go. This is
            // YDB's assimilation: a part returns to its main disk once that disk
            // answers again. Deleting only after the put succeeded is what keeps
            // a repair from dropping the last good copy; if the delete fails the
            // blob simply has two identical copies and the next pass retries.
            if let Some(src) = sources[idx] {
                if src != nominal {
                    let _ = self.slots[src].delete(part_id);
                }
            }
        }

        // A failed rewrite is not reported as an error: the payload decoded, so the
        // blob is readable and no data is lost — only its redundancy is still short.
        // Callers compare `intact_parts` with the scheme's part count to see that,
        // which is why `intact_parts` counts fragments *after* the repair. Calling
        // this an error would make the scrub report a readable blob as unrecoverable.
        Ok(RepairReport {
            intact_parts: intact_parts + new_parts,
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

        let total_slots = self.nominal_slots;
        let online_slots = slot_snaps[..self.nominal_slots]
            .iter()
            .filter(|s| s.online)
            .count();
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
            handoff_slots: self.handoff_slot_count(),
            online_slots,
            min_read_slots,
            min_write_slots,
            total_repairs,
            refused_puts: self.refused_puts.load(Ordering::Relaxed),
            degraded_puts: self.degraded_puts.load(Ordering::Relaxed),
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

#[cfg(test)]
mod fence_transport_tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn conflict_is_a_refusal_not_unavailability() {
        let error = fence_refusal(
            409,
            br#"{"error":"fragment_pinned","message":"a reader still pins epoch 3"}"#,
        );
        match &error {
            BlobError::RemoteFenceRefused {
                status,
                code,
                detail,
            } => {
                assert_eq!(*status, 409);
                assert_eq!(code, "fragment_pinned");
                assert_eq!(detail, "a reader still pins epoch 3");
            }
            other => panic!("expected RemoteFenceRefused, got {other:?}"),
        }
        assert!(!error.is_unavailable());
    }

    #[test]
    fn storage_unavailable_keeps_its_own_variant() {
        let error = fence_refusal(
            503,
            br#"{"error":"storage_unavailable","message":"restart and recover"}"#,
        );
        assert!(matches!(error, BlobError::StorageUnavailable));
        assert!(!error.is_unavailable());
    }

    #[test]
    fn non_json_error_body_is_an_unparsed_refusal() {
        let error = fence_refusal(409, b"plain text refusal, not JSON");
        match &error {
            BlobError::RemoteFenceRefused {
                status,
                code,
                detail,
            } => {
                assert_eq!(*status, 409);
                assert_eq!(code, "unparsed");
                assert_eq!(detail, "plain text refusal, not JSON");
            }
            other => panic!("expected RemoteFenceRefused, got {other:?}"),
        }
        assert!(!error.is_unavailable());
    }

    #[test]
    fn unauthorized_is_a_refusal_not_io() {
        let error = fence_refusal(401, br#"{"error":"unauthorized","message":"bad token"}"#);
        match &error {
            BlobError::RemoteFenceRefused { status, code, .. } => {
                assert_eq!(*status, 401);
                assert_eq!(code, "unauthorized");
            }
            other => panic!("expected RemoteFenceRefused, got {other:?}"),
        }
        assert!(!error.is_unavailable());
    }

    #[test]
    fn json_endpoint_preserves_the_wire_code_on_non_200() {
        let error = fence_json::<EpochWire>(
            409,
            br#"{"error":"epoch_mismatch","message":"caller is stale"}"#,
        );
        match error {
            Err(BlobError::RemoteFenceRefused {
                status,
                code,
                detail,
            }) => {
                assert_eq!(status, 409);
                assert_eq!(code, "epoch_mismatch");
                assert_eq!(detail, "caller is stale");
            }
            other => panic!("expected a mapped RemoteFenceRefused, got {other:?}"),
        }
    }

    /// Reads one HTTP/1.1 request, headers plus `Content-Length` bytes.
    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut raw = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            let Some(header_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&raw[..header_end]);
            let body_len = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if raw.len() >= header_end + 4 + body_len {
                break;
            }
        }
        String::from_utf8_lossy(&raw).into_owned()
    }

    #[test]
    fn remote_slot_round_trips_epoch_pin_unpin_and_advance_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().expect("accept request");
                let request = read_http_request(&mut stream);
                let request_line = request.lines().next().unwrap_or("").to_string();
                let target = request_line.split_whitespace().nth(1).unwrap_or("");
                let endpoint = target.split('?').next().unwrap_or("").rsplit('/').next();
                let body = match endpoint {
                    Some("epoch") => r#"{"epoch":7}"#,
                    Some("pin") => r#"{"pin_id":41,"epoch":7,"expires_at_ms":900}"#,
                    Some("unpin") => r#"{"released":true}"#,
                    Some("advance") => r#"{"epoch":8}"#,
                    other => panic!("unexpected endpoint {other:?}"),
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
                requests.push(request);
            }
            requests
        });

        let slot: PoolSlot<yadb_io::OsFs> = PoolSlot::remote(
            "pool",
            0,
            "domain",
            2,
            address.to_string().as_str(),
            Some("secret".to_string()),
        );
        assert_eq!(slot.epoch().expect("epoch"), 7);
        let grant = slot.pin(7, 60_000).expect("pin");
        assert_eq!(grant.pin_id, PinId::from_wire(41));
        assert_eq!(grant.epoch, 7);
        assert_eq!(grant.expires_at_ms, 900);
        assert!(slot.unpin(grant.pin_id).expect("unpin"));
        assert_eq!(slot.advance_epoch(7).expect("advance"), 8);

        let requests = server.join().expect("test server");
        assert_eq!(requests.len(), 4);
        for (request, method) in requests.iter().zip(["GET", "POST", "POST", "POST"]) {
            assert!(
                request.starts_with(&format!("{method} /internal/vdisk/pool/0/")),
                "unexpected request line: {request}"
            );
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-yadb-raft-token: secret"),
                "token header missing: {request}"
            );
        }
    }
}
