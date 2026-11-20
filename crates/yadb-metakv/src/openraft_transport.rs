//! Peer-authenticated envelope for the OpenRaft transport.
//!
//! Every node holds its own Ed25519 private key. Peers only exchange public
//! keys through trusted configuration, so a node that receives a message
//! cannot impersonate another sender: it does not hold that sender's private
//! key. This is the property the earlier shared/per-node HMAC prototype
//! lacked.
//!
//! The envelope also binds group identity, incarnation, sender, recipient and
//! a per-sender monotonic sequence number. The in-memory replay guard blocks
//! old messages only for its own lifetime, not across restart. It does not provide confidentiality: transport
//! encryption still requires TLS. Reordering and duplication of *distinct*
//! Raft messages remain OpenRaft's responsibility.
//!
//! Certificate-pin changes are runtime state, not configuration. They are
//! written to the replica directory so that a restart cannot resurrect a
//! retired or revoked credential: the config file is the initial set, and the
//! stored changes are merged over it. The store refuses to load rather than
//! falling back to the config, because that fallback is exactly the regression
//! it exists to prevent.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};

use crate::multiraft::{GroupIdentity, ReplicaIdentity};

const CONTEXT: &[u8] = b"yadb/openraft/rpc/v1";
const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
const PUBLIC_KEY_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;

/// The durable pin state inside a replica directory, beside `replica.json`.
///
/// Only changes are stored, never the whole accepted set: the config file stays
/// the initial set, so an operator's later edit is still merged honestly and a
/// pin that was never touched is not duplicated into two places.
const PIN_STATE_FILE: &str = "certificate-pins.json";
/// The unpublished copy. Only the rename in [`write_pin_state`] publishes it,
/// so a crash before that leaves the previous file intact.
const PIN_STATE_TEMP: &str = "certificate-pins.json.tmp";
const PIN_STATE_VERSION: u32 = 1;
/// Generous for a few hundred pins; a larger file is treated as damage.
const MAX_PIN_STATE: u64 = 256 * 1024;

/// One installed pin, bound to the node it was installed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinRecord {
    node: u32,
    pin: [u8; 32],
}

/// The persisted changes to the config's initial pins.
///
/// `checksum` covers the semantic content, so a byte flip that still parses as
/// JSON is refused instead of silently changing which credential is trusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinState {
    version: u32,
    checksum: u32,
    installed: Vec<PinRecord>,
    retired: Vec<[u8; 32]>,
    revoked: Vec<[u8; 32]>,
}

impl PinState {
    fn new(
        installed: Vec<PinRecord>,
        retired: Vec<[u8; 32]>,
        revoked: Vec<[u8; 32]>,
    ) -> Self {
        let mut state = Self {
            version: PIN_STATE_VERSION,
            checksum: 0,
            installed,
            retired,
            revoked,
        };
        state.seal();
        state
    }

    fn sort(&mut self) {
        self.installed.sort_by_key(|record| (record.node, record.pin));
        self.installed.dedup();
        self.retired.sort();
        self.retired.dedup();
        self.revoked.sort();
        self.revoked.dedup();
    }

    /// Deterministic over the semantic content, independent of JSON formatting
    /// and field order, so the check does not depend on the serializer.
    fn digest(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(b"yadb/certificate-pin-state/v1");
        hasher.update(&self.version.to_le_bytes());
        let mut installed = self.installed.clone();
        installed.sort_by_key(|record| (record.node, record.pin));
        installed.dedup();
        for record in &installed {
            hasher.update(&record.node.to_le_bytes());
            hasher.update(&record.pin);
        }
        hasher.update(b"retired");
        let mut retired = self.retired.clone();
        retired.sort();
        retired.dedup();
        for pin in &retired {
            hasher.update(pin);
        }
        hasher.update(b"revoked");
        let mut revoked = self.revoked.clone();
        revoked.sort();
        revoked.dedup();
        for pin in &revoked {
            hasher.update(pin);
        }
        hasher.finalize()
    }

    fn seal(&mut self) {
        self.sort();
        self.checksum = self.digest();
    }
}

/// Reads the durable pin state from a replica directory.
///
/// A missing file means "no changes yet", not "no pins": the config's pins are
/// still the initial set. Every other failure is returned rather than treated
/// as absent, because silently reverting to the config would restore a
/// credential the operator retired or revoked.
fn read_pin_state(dir: &Path) -> Result<Option<PinState>, TransportError> {
    let path = dir.join(PIN_STATE_FILE);
    let unreadable = |error: io::Error| {
        pin_state_error(format!(
            "certificate pin state {} is unreadable: {error}",
            path.display()
        ))
    };
    let file = match yadb_io::sys::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(unreadable(error)),
    };
    let len = file.metadata().map_err(unreadable)?.len();
    if len == 0 || len > MAX_PIN_STATE {
        return Err(pin_state_error(format!(
            "certificate pin state {} is empty or larger than {MAX_PIN_STATE} bytes",
            path.display()
        )));
    }
    let mut bytes = vec![0u8; len as usize];
    let mut filled = 0;
    while filled < bytes.len() {
        match yadb_io::sys::read_at(&file, filled as u64, &mut bytes[filled..]) {
            Ok(0) => {
                return Err(pin_state_error(format!(
                    "certificate pin state {} ended before its recorded length",
                    path.display()
                )))
            }
            Ok(read) => filled += read,
            Err(error) => return Err(unreadable(error)),
        }
    }
    let mut state: PinState = serde_json::from_slice(&bytes).map_err(|error| {
        pin_state_error(format!(
            "certificate pin state {} is corrupt: {error}",
            path.display()
        ))
    })?;
    if state.version != PIN_STATE_VERSION {
        return Err(pin_state_error(format!(
            "certificate pin state {} has unsupported version {}",
            path.display(),
            state.version
        )));
    }
    state.sort();
    if state.checksum != state.digest() {
        return Err(pin_state_error(format!(
            "certificate pin state {} failed its integrity check",
            path.display()
        )));
    }
    Ok(Some(state))
}

/// Publishes the pin state durably and atomically.
///
/// The order is the one the rest of this crate uses: write a temporary file,
/// `sync_data` it, `rename` it over the final name, then `sync_dir` the
/// directory entry. The temporary file is never authoritative, so a crash
/// before the rename leaves the previous state, and the rename cannot expose a
/// half-written file. The I/O goes through `yadb_io::sys` so the crash window
/// is reachable from a test with the same fault seam as the other stores.
fn write_pin_state(dir: &Path, state: &PinState) -> io::Result<()> {
    let bytes = serde_json::to_vec(state).map_err(io::Error::other)?;
    if bytes.len() as u64 > MAX_PIN_STATE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certificate pin state exceeds the size limit",
        ));
    }
    let path = dir.join(PIN_STATE_FILE);
    let temp = dir.join(PIN_STATE_TEMP);
    let result = (|| -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        yadb_io::sys::write_all(&file, &bytes)?;
        yadb_io::sys::sync_data(&file)?;
        yadb_io::sys::rename(&temp, &path)?;
        yadb_io::sys::sync_dir(dir)?;
        Ok(())
    })();
    if result.is_err() {
        // A failed write must not leave a stale temporary file that a later
        // retry could mistake for progress.
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn pin_state_error(message: impl Into<String>) -> TransportError {
    TransportError::PinState(message.into())
}

/// One node's private signing key. It is never sent to peers.
pub struct NodeSigningKey {
    inner: Arc<Ed25519KeyPair>,
}

impl std::fmt::Debug for NodeSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeSigningKey(<redacted>)")
    }
}

impl NodeSigningKey {
    /// Generates a fresh key from the system entropy source.
    pub fn generate() -> Result<Self, TransportError> {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 =
            Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| TransportError::KeyGeneration)?;
        Self::from_pkcs8(pkcs8.as_ref())
    }

    /// Derives a deterministic key from a seed. Intended for tests and for
    /// reproducible provisioning; production keys must come from a secret store.
    pub fn from_seed(seed: &[u8]) -> Result<Self, TransportError> {
        let pair =
            Ed25519KeyPair::from_seed_unchecked(seed).map_err(|_| TransportError::KeyGeneration)?;
        Ok(Self {
            inner: Arc::new(pair),
        })
    }

    pub fn from_pkcs8(pkcs8: &[u8]) -> Result<Self, TransportError> {
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| TransportError::KeyGeneration)?;
        Ok(Self {
            inner: Arc::new(pair),
        })
    }

    /// The public key that peers must be configured with.
    pub fn public_key(&self) -> Vec<u8> {
        self.inner.public_key().as_ref().to_vec()
    }

    /// Signs an arbitrary caller-supplied message. Callers must apply their
    /// own domain separation; the transport and fence-token encodings do so
    /// with different context prefixes.
    pub(crate) fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.inner.sign(message).as_ref().to_vec()
    }
}

/// Verifies an Ed25519 signature with a trusted public key.
pub(crate) fn verify_signature(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(message, signature)
        .is_ok()
}

/// One operator-requested change to a node's accepted certificate pins.
///
/// The three operations are deliberately not symmetric. `Install` adds a pin
/// and keeps the old ones, which is what makes rotation overlap. `Retire`
/// removes one but refuses to leave a node with no pin. `Revoke` is the
/// emergency path and may remove the last pin, taking effect on the next frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinChange {
    Install { node: u32, pin: [u8; 32] },
    Retire { node: u32, pin: [u8; 32] },
    Revoke { node: u32, pin: [u8; 32] },
}

/// Trusted peer public keys, keyed by node id.
#[derive(Debug, Clone, Default)]
pub struct PeerKeyRegistry {
    keys: BTreeMap<u32, Vec<u8>>,
    pins: Arc<RwLock<CertificatePinRegistry>>,
    /// Serialises pin changes so the state written to disk is the state that
    /// is published. Deliberately separate from `pins`: the file write happens
    /// while this is held, and a slow sync must not block frame verification.
    changes: Arc<Mutex<()>>,
}

#[derive(Debug, Default, Clone)]
struct CertificatePinRegistry {
    active: BTreeMap<u32, BTreeSet<[u8; 32]>>,
    owners: BTreeMap<[u8; 32], u32>,
    revoked: BTreeSet<[u8; 32]>,
    /// Pins removed by `Retire`. Kept because a retired pin must stay retired
    /// across a restart even though the config still names it as its initial
    /// pin. `Install` clears it again: an explicit re-install overrides a
    /// retirement, while a revocation is permanent.
    retired: BTreeSet<[u8; 32]>,
    initial: Option<BTreeMap<u32, [u8; 32]>>,
    /// The replica directory holding the durable state, when this registry
    /// belongs to one. An unattached registry is memory-only, which is what
    /// tests and in-process transports use.
    dir: Option<PathBuf>,
}

impl CertificatePinRegistry {
    /// The changes to persist: every active pin that is not the config's
    /// initial pin for its node, plus the recorded retirements and revocations.
    fn image(&self) -> PinState {
        let mut installed = Vec::new();
        for (&node, active) in &self.active {
            let config = self
                .initial
                .as_ref()
                .and_then(|initial| initial.get(&node))
                .copied();
            for &pin in active {
                if Some(pin) != config {
                    installed.push(PinRecord { node, pin });
                }
            }
        }
        PinState::new(
            installed,
            self.retired.iter().copied().collect(),
            self.revoked.iter().copied().collect(),
        )
    }
}

impl PeerKeyRegistry {
    pub fn new(keys: BTreeMap<u32, Vec<u8>>) -> Result<Self, TransportError> {
        if keys.is_empty() || keys.values().any(|key| key.len() != PUBLIC_KEY_LEN) {
            return Err(TransportError::InvalidInput("invalid peer public key"));
        }
        Ok(Self {
            keys,
            pins: Arc::new(RwLock::new(CertificatePinRegistry::default())),
            changes: Arc::new(Mutex::new(())),
        })
    }

    /// Builds the registry a replica starts with.
    ///
    /// The config's pins are the initial set and the durable state in `dir` is
    /// merged over them: stored installs add, stored retirements remove and a
    /// stored revocation wins over a config pin. A missing file means "no
    /// changes yet"; any other read, decode or merge failure refuses
    /// construction, because falling back to the config would resurrect the
    /// credential the file exists to keep out.
    pub fn open_for_replica(
        keys: BTreeMap<u32, Vec<u8>>,
        initial: &BTreeMap<u32, [u8; 32]>,
        dir: &Path,
    ) -> Result<Self, TransportError> {
        let registry = Self::new(keys)?;
        {
            let mut pins = registry
                .pins
                .write()
                .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
            pins.dir = Some(dir.to_path_buf());
            Self::seed_initial(&mut pins, initial)?;
        }
        if let Some(state) = read_pin_state(dir)? {
            registry.merge_pin_state(state)?;
        }
        Ok(registry)
    }

    /// Merges the stored changes over the initial set, refusing a state that
    /// contradicts the config or leaves this node unable to authenticate.
    fn merge_pin_state(&self, state: PinState) -> Result<(), TransportError> {
        let mut pins = self
            .pins
            .write()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        // Installs are additions. A pin stays bound to the node it was first
        // installed for, and a revocation already on record cannot be undone
        // by the same file also claiming an install.
        for record in &state.installed {
            if !self.keys.contains_key(&record.node) {
                return Err(pin_state_error(format!(
                    "stored pin state installs a certificate for unknown node {}",
                    record.node
                )));
            }
            if pins.revoked.contains(&record.pin) {
                return Err(pin_state_error(
                    "stored pin state both installs and revokes a certificate",
                ));
            }
            if pins
                .owners
                .get(&record.pin)
                .is_some_and(|owner| *owner != record.node)
            {
                return Err(pin_state_error(
                    "stored pin state installs a certificate already bound to another node",
                ));
            }
            pins.owners.insert(record.pin, record.node);
            pins.active
                .entry(record.node)
                .or_default()
                .insert(record.pin);
            pins.retired.remove(&record.pin);
        }
        // A stored revocation wins over a config pin: this is the case the
        // store exists for. It may leave a node with no pin, which is the
        // documented emergency semantics of revocation.
        for pin in &state.revoked {
            if let Some(&node) = pins.owners.get(pin) {
                if let Some(active) = pins.active.get_mut(&node) {
                    active.remove(pin);
                }
            }
            pins.revoked.insert(*pin);
            pins.retired.remove(pin);
        }
        // A retirement removes a pin but never the last one. The runtime
        // refuses that, so a state that needs it contradicts the config.
        for pin in &state.retired {
            if pins.revoked.contains(pin) {
                continue;
            }
            if let Some(&owner) = pins.owners.get(pin) {
                let Some(active) = pins.active.get_mut(&owner) else {
                    return Err(pin_state_error(format!(
                        "stored pin state retires a certificate with no active set for node {owner}"
                    )));
                };
                if active.contains(pin) {
                    if active.len() <= 1 {
                        return Err(pin_state_error(format!(
                            "stored pin state would leave node {owner} with no active certificate pin"
                        )));
                    }
                    active.remove(pin);
                }
            }
            pins.retired.insert(*pin);
        }
        if pins.active.values().all(BTreeSet::is_empty) {
            return Err(pin_state_error(
                "certificate pin state would leave this node with no active pin",
            ));
        }
        Ok(())
    }

    pub fn with_peer(mut self, node_id: u32, public_key: Vec<u8>) -> Result<Self, TransportError> {
        if public_key.len() != PUBLIC_KEY_LEN {
            return Err(TransportError::InvalidInput("invalid peer public key"));
        }
        self.keys.insert(node_id, public_key);
        Ok(self)
    }

    pub fn public_key(&self, node_id: u32) -> Option<&[u8]> {
        self.keys.get(&node_id).map(Vec::as_slice)
    }

    /// Installs a certificate pin while retaining all currently active pins.
    /// Install the replacement before retiring the old pin: this ordering is
    /// the overlap rule. Retire refuses to leave zero pins; the operator must
    /// also confirm that the replacement credential works before retirement,
    /// which a local registry cannot establish. A pin is bound to its first
    /// node for this registry's lifetime and a revoked pin cannot be reinstalled.
    pub fn install_pin(&self, node_id: u32, pin: [u8; 32]) -> Result<(), TransportError> {
        self.pin_change(&PinChange::Install { node: node_id, pin })
    }

    /// Retires one pin, but never the last active pin for a node. Callers must
    /// first install the replacement and deploy it to every peer.
    pub fn retire_pin(&self, node_id: u32, pin: [u8; 32]) -> Result<(), TransportError> {
        self.pin_change(&PinChange::Retire { node: node_id, pin })
    }

    /// Revocation is deliberately allowed to remove the last pin. It is an
    /// emergency operation and takes effect on the next frame check.
    pub fn revoke_pin(&self, node_id: u32, pin: [u8; 32]) -> Result<(), TransportError> {
        self.pin_change(&PinChange::Revoke { node: node_id, pin })
    }

    /// Reports whether `pin_change` would succeed, without changing anything.
    ///
    /// A daemon serves several groups, each with its own registry. Checking
    /// every group first and applying only when all of them accept is what
    /// stops a rotation from leaving one group trusting a credential the
    /// others have already refused. The check is advisory: it is only sound
    /// while the caller serialises changes, because another writer between the
    /// check and the apply would make it stale.
    pub fn check_pin_change(&self, change: &PinChange) -> Result<(), TransportError> {
        let pins = self
            .pins
            .read()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        // Cloned only on the operator path, never per frame: `apply_pin_change`
        // mutates, and running it against a copy is what keeps the check and
        // the apply from being two copies of the same rules.
        let mut probe = pins.clone();
        Self::apply_pin_change(&self.keys, &mut probe, change, false)
    }

    /// Applies one pin change. See `check_pin_change` for the ordering rule.
    ///
    /// The next state is validated and written to the replica directory before
    /// it becomes visible, so a failed write leaves the registry exactly as it
    /// was and no frame is authenticated under a pin that a restart would not
    /// have. The change is serialised by an internal mutex, which is separate
    /// from the frame-path lock and is held across the file write.
    pub fn pin_change(&self, change: &PinChange) -> Result<(), TransportError> {
        let _serialised = self
            .changes
            .lock()
            .map_err(|_| TransportError::InvalidInput("certificate pin change lock poisoned"))?;
        let (state, dir) = {
            let pins = self
                .pins
                .read()
                .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
            // Clone only on the operator path, never per frame. Applying to a
            // probe is what lets the file be written before the change is
            // published; it is the same code path the apply uses.
            let mut probe = pins.clone();
            Self::apply_pin_change(&self.keys, &mut probe, change, true)?;
            (probe.image(), pins.dir.clone())
        };
        if let Some(dir) = dir {
            write_pin_state(&dir, &state).map_err(|error| {
                pin_state_error(format!(
                    "cannot persist certificate pin state in {}: {error}",
                    dir.display()
                ))
            })?;
        }
        let mut pins = self
            .pins
            .write()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        Self::apply_pin_change(&self.keys, &mut pins, change, true)
    }

    fn apply_pin_change(
        keys: &BTreeMap<u32, Vec<u8>>,
        pins: &mut CertificatePinRegistry,
        change: &PinChange,
        apply: bool,
    ) -> Result<(), TransportError> {
        match *change {
            PinChange::Install { node, pin } => {
                if !keys.contains_key(&node)
                    || pins.revoked.contains(&pin)
                    || pins.owners.get(&pin).is_some_and(|owner| *owner != node)
                {
                    return Err(TransportError::InvalidInput(
                        "certificate pin belongs to another, revoked or unknown node",
                    ));
                }
                if apply {
                    pins.owners.insert(pin, node);
                    pins.active.entry(node).or_default().insert(pin);
                    // An explicit re-install overrides a retirement, but a
                    // revocation was rejected above and stays permanent.
                    pins.retired.remove(&pin);
                }
                Ok(())
            }
            PinChange::Retire { node, pin } => {
                let active = pins.active.get(&node).ok_or(TransportError::UnknownSender)?;
                // The last pin may not be retired: a node with no accepted pin
                // can never authenticate again.
                if active.len() <= 1 || !active.contains(&pin) {
                    return Err(TransportError::InvalidInput(
                        "certificate pin absent or last active pin",
                    ));
                }
                if apply {
                    pins.active
                        .get_mut(&node)
                        .expect("checked above")
                        .remove(&pin);
                    // Remembered so that a restart cannot resurrect it from the
                    // config, which still lists it as its initial pin.
                    pins.retired.insert(pin);
                }
                Ok(())
            }
            PinChange::Revoke { node, pin } => {
                if !pins.active.get(&node).is_some_and(|a| a.contains(&pin)) {
                    return Err(TransportError::InvalidInput("certificate pin not active"));
                }
                if apply {
                    pins.active
                        .get_mut(&node)
                        .expect("checked above")
                        .remove(&pin);
                    pins.revoked.insert(pin);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn register_initial_pins(
        &self,
        initial: &BTreeMap<u32, [u8; 32]>,
    ) -> Result<(), TransportError> {
        let mut pins = self
            .pins
            .write()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        Self::seed_initial(&mut pins, initial)
    }

    /// Seeds the initial set exactly once. A later binding may repeat the same
    /// set (the transport binds pins after the registry is built) but must not
    /// replace it, because a replacement would resurrect a retired pin.
    fn seed_initial(
        pins: &mut CertificatePinRegistry,
        initial: &BTreeMap<u32, [u8; 32]>,
    ) -> Result<(), TransportError> {
        if let Some(existing) = &pins.initial {
            return if existing == initial {
                Ok(())
            } else {
                Err(TransportError::InvalidInput(
                    "conflicting initial certificate pins",
                ))
            };
        }
        if !pins.active.is_empty() {
            return Err(TransportError::InvalidInput(
                "invalid initial certificate pins",
            ));
        }
        for (&node, &pin) in initial {
            pins.active.insert(node, BTreeSet::from([pin]));
            pins.owners.insert(pin, node);
        }
        pins.initial = Some(initial.clone());
        Ok(())
    }

    pub(crate) fn pin_owner(&self, pin: [u8; 32]) -> Result<u32, TransportError> {
        let pins = self
            .pins
            .read()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        let node = *pins
            .owners
            .get(&pin)
            .ok_or(TransportError::AuthenticationFailed)?;
        if pins
            .active
            .get(&node)
            .is_some_and(|active| active.contains(&pin))
        {
            Ok(node)
        } else {
            Err(TransportError::AuthenticationFailed)
        }
    }

    /// Per-frame cost is one uncontended RwLock read (atomic acquire/release,
    /// no allocation or syscall), plus ordered-map lookups. It can contend
    /// with lifecycle writers under heavy fan-in; the lock is never held over
    /// I/O or an await.
    pub(crate) fn check_pin(&self, node_id: u32, pin: [u8; 32]) -> Result<(), TransportError> {
        let pins = self
            .pins
            .read()
            .map_err(|_| TransportError::InvalidInput("certificate pin registry poisoned"))?;
        if pins
            .active
            .get(&node_id)
            .is_some_and(|active| active.contains(&pin))
        {
            Ok(())
        } else {
            Err(TransportError::AuthenticationFailed)
        }
    }
}

/// Rejects replayed or reordered authenticated messages from the same sender.
#[derive(Debug, Default)]
pub struct ReplayGuard {
    highest: BTreeMap<u32, u64>,
}

impl ReplayGuard {
    /// Accepts only a strictly increasing sequence number per sender.
    pub fn accept(&mut self, sender: u32, sequence: u64) -> bool {
        let entry = self.highest.entry(sender).or_insert(0);
        if sequence <= *entry {
            return false;
        }
        *entry = sequence;
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedRaftEnvelope {
    pub version: u16,
    pub group: GroupIdentity,
    pub sender: u32,
    pub recipient: u32,
    pub sequence: u64,
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    InvalidInput(&'static str),
    /// The durable certificate-pin state could not be read, decoded, merged or
    /// written. The message names the file and the reason; startup surfaces it
    /// verbatim instead of falling back to the config.
    PinState(String),
    KeyGeneration,
    IdentityMismatch,
    SenderMismatch,
    RecipientMismatch,
    UnknownSender,
    AuthenticationFailed,
    Replayed,
    PayloadTooLarge,
    Encoding,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PinState(message) => f.write_str(message),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for TransportError {}

fn canonical_bytes(
    version: u16,
    group: &GroupIdentity,
    sender: u32,
    recipient: u32,
    sequence: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CONTEXT.len() + 2 + 16 + 8 + 8 + 4 + 4 + 8 + payload.len());
    bytes.extend_from_slice(CONTEXT);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&group.cluster);
    bytes.extend_from_slice(&group.group.to_le_bytes());
    bytes.extend_from_slice(&group.incarnation.to_le_bytes());
    bytes.extend_from_slice(&sender.to_le_bytes());
    bytes.extend_from_slice(&recipient.to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

impl SignedRaftEnvelope {
    pub fn sign(
        identity: &ReplicaIdentity,
        recipient: u32,
        sequence: u64,
        payload: Vec<u8>,
        key: &NodeSigningKey,
    ) -> Result<Self, TransportError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(TransportError::PayloadTooLarge);
        }
        if recipient == 0 || recipient == identity.node() || !identity.voters().contains(&recipient)
        {
            return Err(TransportError::RecipientMismatch);
        }
        let group = identity.group().clone();
        let signature = key.sign(&canonical_bytes(
            1,
            &group,
            identity.node(),
            recipient,
            sequence,
            &payload,
        ));
        Ok(Self {
            version: 1,
            group,
            sender: identity.node(),
            recipient,
            sequence,
            payload,
            signature,
        })
    }

    /// Verifies the sender's signature against a trusted public key. The
    /// public key is never taken from the message itself.
    ///
    /// `transport_peer` is the peer identity derived from the connection, when
    /// the transport can establish one. When present it must equal the claimed
    /// sender, so a peer cannot claim another identity even with a valid key.
    pub fn verify(
        &self,
        local: &ReplicaIdentity,
        transport_peer: Option<u32>,
        registry: &PeerKeyRegistry,
    ) -> Result<&[u8], TransportError> {
        if self.version != 1 {
            return Err(TransportError::AuthenticationFailed);
        }
        if self.group != *local.group() {
            return Err(TransportError::IdentityMismatch);
        }
        if let Some(peer) = transport_peer {
            if peer != self.sender {
                return Err(TransportError::SenderMismatch);
            }
        }
        if self.sender == local.node() || !local.voters().contains(&self.sender) {
            return Err(TransportError::SenderMismatch);
        }
        if self.recipient != local.node() {
            return Err(TransportError::RecipientMismatch);
        }
        if self.payload.len() > MAX_PAYLOAD {
            return Err(TransportError::PayloadTooLarge);
        }
        if self.signature.len() != SIGNATURE_LEN {
            return Err(TransportError::AuthenticationFailed);
        }
        let public_key = registry
            .public_key(self.sender)
            .ok_or(TransportError::UnknownSender)?;
        if !verify_signature(
            public_key,
            &canonical_bytes(
                self.version,
                &self.group,
                self.sender,
                self.recipient,
                self.sequence,
                &self.payload,
            ),
            &self.signature,
        ) {
            return Err(TransportError::AuthenticationFailed);
        }
        Ok(&self.payload)
    }

    /// Verifies the signature and additionally enforces replay protection.
    /// The sequence check happens only after the signature is valid, so an
    /// attacker cannot burn sequence numbers of an honest sender.
    pub fn verify_and_accept(
        &self,
        local: &ReplicaIdentity,
        transport_peer: Option<u32>,
        registry: &PeerKeyRegistry,
        guard: &mut ReplayGuard,
    ) -> Result<&[u8], TransportError> {
        self.verify(local, transport_peer, registry)?;
        if !guard.accept(self.sender, self.sequence) {
            return Err(TransportError::Replayed);
        }
        Ok(&self.payload)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, TransportError> {
        serde_json::to_vec(self).map_err(|_| TransportError::Encoding)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self, TransportError> {
        serde_json::from_slice(bytes).map_err(|_| TransportError::Encoding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch directory under `YADB_TEST_TMPDIR` (or the system temporary
    /// directory), removed on drop.
    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let base = std::env::var_os("YADB_TEST_TMPDIR")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            loop {
                let nonce = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let path = base.join(format!(
                    "yadb-certificate-pins-{nonce}-{}-{sequence}",
                    std::process::id()
                ));
                match std::fs::create_dir(&path) {
                    Ok(()) => return Self(path),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create test directory {}: {error}", path.display()),
                }
            }
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn identities() -> (ReplicaIdentity, ReplicaIdentity) {
        let group = GroupIdentity {
            cluster: [4; 16],
            group: 3,
            incarnation: 8,
        };
        (
            ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap(),
            ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap(),
        )
    }

    fn keys() -> (NodeSigningKey, NodeSigningKey) {
        // Ed25519 seeds must be exactly 32 bytes.
        let mut seed_one = [0u8; 32];
        seed_one[0] = 1;
        let mut seed_two = [0u8; 32];
        seed_two[0] = 2;
        (
            NodeSigningKey::from_seed(&seed_one).unwrap(),
            NodeSigningKey::from_seed(&seed_two).unwrap(),
        )
    }

    fn registry(one: &NodeSigningKey, two: &NodeSigningKey) -> PeerKeyRegistry {
        PeerKeyRegistry::new(BTreeMap::from([
            (1, one.public_key()),
            (2, two.public_key()),
        ]))
        .unwrap()
    }

    /// The peer keys and the config's initial pins a two-node replica starts
    /// from, shared by the restart tests.
    fn replica_inputs(
        one: &NodeSigningKey,
        two: &NodeSigningKey,
    ) -> (BTreeMap<u32, Vec<u8>>, BTreeMap<u32, [u8; 32]>) {
        (
            BTreeMap::from([(1, one.public_key()), (2, two.public_key())]),
            BTreeMap::from([(1, [11; 32]), (2, [22; 32])]),
        )
    }

    /// A revocation is the emergency path and must survive a restart even
    /// though the config still names the revoked pin as node 1's initial pin.
    #[test]
    fn a_revoked_pin_stays_revoked_across_a_restart() {
        let (one, two) = keys();
        let dir = TempDir::new();
        let (keys, initial) = replica_inputs(&one, &two);
        let registry = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0).unwrap();
        registry.revoke_pin(1, [11; 32]).unwrap();
        assert_eq!(
            registry.check_pin(1, [11; 32]),
            Err(TransportError::AuthenticationFailed)
        );

        let restarted = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0).unwrap();
        assert_eq!(
            restarted.check_pin(1, [11; 32]),
            Err(TransportError::AuthenticationFailed),
            "the config's revoked pin must not come back after a restart"
        );
        assert!(
            restarted.install_pin(1, [11; 32]).is_err(),
            "a revoked pin must stay uninstallable after a restart"
        );
        restarted.check_pin(2, [22; 32]).unwrap();
    }

    /// A retired pin is still listed in the config as the initial pin, so the
    /// stored retirement is the only thing keeping it out after a restart.
    #[test]
    fn a_retired_pin_stays_retired_across_a_restart() {
        let (one, two) = keys();
        let dir = TempDir::new();
        let (keys, initial) = replica_inputs(&one, &two);
        let registry = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0).unwrap();
        registry.install_pin(1, [12; 32]).unwrap();
        registry.retire_pin(1, [11; 32]).unwrap();

        let restarted = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0).unwrap();
        assert_eq!(
            restarted.check_pin(1, [11; 32]),
            Err(TransportError::AuthenticationFailed),
            "the config's retired pin must not come back after a restart"
        );
        restarted.check_pin(1, [12; 32]).unwrap();
        restarted.check_pin(2, [22; 32]).unwrap();
    }

    /// The replacement installed during a rotation must not be lost, and the
    /// config pin it was installed beside must still work.
    #[test]
    fn an_installed_pin_is_still_accepted_across_a_restart() {
        let (one, two) = keys();
        let dir = TempDir::new();
        let (keys, initial) = replica_inputs(&one, &two);
        let registry = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0).unwrap();
        registry.install_pin(1, [12; 32]).unwrap();

        let restarted = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0).unwrap();
        restarted.check_pin(1, [12; 32]).unwrap();
        restarted.check_pin(1, [11; 32]).unwrap();
        // A restart must not silently rebind the installed pin.
        assert!(restarted.install_pin(2, [12; 32]).is_err());
    }

    /// A damaged file is not an absent file: reverting to the config would
    /// undo whatever the operator last did.
    #[test]
    fn a_corrupt_or_truncated_pin_state_refuses_startup() {
        let (one, two) = keys();
        let (keys, initial) = replica_inputs(&one, &two);

        // A real file to truncate and to flip one field of, so the damaged
        // cases are the shapes this store actually writes.
        let source = TempDir::new();
        let registry =
            PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &source.0).unwrap();
        registry.install_pin(1, [12; 32]).unwrap();
        let valid = std::fs::read(source.0.join(PIN_STATE_FILE)).unwrap();
        let mut flipped: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        let checksum = flipped["checksum"].as_u64().unwrap();
        flipped["checksum"] = serde_json::json!(checksum ^ 1);
        let flipped = serde_json::to_vec(&flipped).unwrap();

        for (case, bytes, expected) in [
            ("garbage", b"not a pin state".to_vec(), "is corrupt"),
            (
                "truncated",
                valid[..valid.len() - 5].to_vec(),
                "is corrupt",
            ),
            ("flipped checksum", flipped, "failed its integrity check"),
        ] {
            let dir = TempDir::new();
            std::fs::write(dir.0.join(PIN_STATE_FILE), &bytes).unwrap();
            let error = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0)
                .err()
                .unwrap_or_else(|| panic!("{case}: a damaged pin state must refuse startup"));
            let text = error.to_string();
            assert!(
                matches!(error, TransportError::PinState(_)),
                "{case}: {error}"
            );
            assert!(text.contains(expected), "{case}: {text}");
            assert!(text.contains(PIN_STATE_FILE), "{case}: {text}");
        }
    }

    /// The rename is the publication point. A failure between the temporary
    /// file's sync and that rename must leave the previous state on disk and
    /// in memory, so a restart reads the state before the failed change.
    #[test]
    fn a_crash_before_the_rename_keeps_the_previous_pin_state() {
        use yadb_io::fault::{renames, RenameFault};

        let (one, two) = keys();
        let dir = TempDir::new();
        let (keys, initial) = replica_inputs(&one, &two);
        let registry = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0).unwrap();
        // One durable change first, so there is a previous state to keep.
        registry.install_pin(1, [12; 32]).unwrap();

        let armed = renames(&[RenameFault::Error(io::ErrorKind::Other)]);
        let error = registry.revoke_pin(1, [11; 32]).unwrap_err();
        drop(armed);
        let text = error.to_string();
        assert!(
            text.contains("cannot persist certificate pin state"),
            "{text}"
        );
        assert!(text.contains("injected rename fault"), "{text}");

        // Nothing was published: the pin is still accepted here and the file
        // is still the state from before the failed revoke.
        registry.check_pin(1, [11; 32]).unwrap();
        assert!(
            !dir.0.join(PIN_STATE_TEMP).exists(),
            "a failed publication must not leave the temporary file behind"
        );
        let restarted = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0).unwrap();
        restarted.check_pin(1, [11; 32]).unwrap();
        restarted.check_pin(1, [12; 32]).unwrap();
    }

    /// Revocation may legitimately empty the registry at runtime, but a
    /// restart that merges to no active pin at all cannot authenticate anyone.
    #[test]
    fn a_state_that_leaves_no_active_pin_refuses_startup() {
        let (one, _two) = keys();
        let dir = TempDir::new();
        let keys = BTreeMap::from([(1, one.public_key())]);
        let initial = BTreeMap::from([(1, [11; 32])]);
        let registry = PeerKeyRegistry::open_for_replica(keys.clone(), &initial, &dir.0).unwrap();
        registry.revoke_pin(1, [11; 32]).unwrap();

        let error = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0)
            .err()
            .expect("an empty merged pin set must refuse startup");
        assert!(
            error.to_string().contains("no active pin"),
            "{}",
            error.to_string()
        );
    }

    /// The runtime refuses to retire a node's last pin, so a stored state that
    /// asks for it contradicts the config and must not be applied silently.
    #[test]
    fn a_stored_retirement_that_would_empty_a_node_refuses_startup() {
        let (one, two) = keys();
        let dir = TempDir::new();
        let (keys, initial) = replica_inputs(&one, &two);
        // A file the runtime could not have produced: retire node 1's only pin.
        let state = PinState::new(Vec::new(), vec![[11; 32]], Vec::new());
        write_pin_state(&dir.0, &state).unwrap();

        let error = PeerKeyRegistry::open_for_replica(keys, &initial, &dir.0)
            .err()
            .expect("retiring the last pin must refuse startup");
        assert!(
            error.to_string().contains("would leave node 1"),
            "{error}"
        );
    }

    #[test]
    fn rotation_requires_overlap_before_retiring_a_working_pin() {
        let (one, two) = keys();
        let registry = registry(&one, &two);
        let old = [11; 32];
        let new = [12; 32];
        registry.install_pin(1, old).unwrap();
        assert!(
            registry.retire_pin(1, old).is_err(),
            "last pin was retired before replacement"
        );
        registry.check_pin(1, old).unwrap();
        registry.install_pin(1, new).unwrap();
        registry.check_pin(1, old).unwrap();
        registry.check_pin(1, new).unwrap();
        registry.retire_pin(1, old).unwrap();
        assert_eq!(
            registry.check_pin(1, old),
            Err(TransportError::AuthenticationFailed)
        );
        registry.check_pin(1, new).unwrap();
    }

    #[test]
    fn a_pin_change_can_be_checked_without_being_applied() {
        let (one, two) = keys();
        let registry = registry(&one, &two);
        let old = [11; 32];
        let new = [12; 32];
        registry.install_pin(1, old).unwrap();

        // A check reports what the apply would do and changes nothing, which is
        // what lets a daemon refuse a change on one group before touching any.
        registry
            .check_pin_change(&PinChange::Install { node: 1, pin: new })
            .unwrap();
        assert_eq!(
            registry.check_pin(1, new),
            Err(TransportError::AuthenticationFailed),
            "a check must not install the pin"
        );
        assert_eq!(
            registry.check_pin_change(&PinChange::Retire { node: 1, pin: old }),
            Err(TransportError::InvalidInput(
                "certificate pin absent or last active pin"
            )),
            "a check must report the same refusal the apply would"
        );
        registry.check_pin(1, old).unwrap();

        registry
            .pin_change(&PinChange::Install { node: 1, pin: new })
            .unwrap();
        registry.check_pin(1, new).unwrap();
        registry
            .pin_change(&PinChange::Retire { node: 1, pin: old })
            .unwrap();
        assert_eq!(
            registry.check_pin(1, old),
            Err(TransportError::AuthenticationFailed)
        );
    }

    #[test]
    fn rotated_pins_cannot_be_rebound_to_a_different_node() {
        let (one, two) = keys();
        let registry = registry(&one, &two);
        let old = [11; 32];
        let new = [12; 32];
        let node_two = [22; 32];
        registry.install_pin(1, old).unwrap();
        registry.install_pin(2, node_two).unwrap();
        registry.install_pin(1, new).unwrap();
        assert!(
            registry.install_pin(1, node_two).is_err(),
            "node 2 pin was rebound to node 1"
        );
        assert_eq!(
            registry.check_pin(1, node_two),
            Err(TransportError::AuthenticationFailed)
        );
        assert_eq!(registry.pin_owner(new).unwrap(), 1);
        assert_eq!(registry.pin_owner(node_two).unwrap(), 2);
        let (one_id, two_id) = identities();
        let forged = SignedRaftEnvelope::sign(&two_id, 1, 1, b"forged".to_vec(), &two).unwrap();
        assert_eq!(
            forged.verify(&one_id, Some(1), &registry),
            Err(TransportError::SenderMismatch)
        );
    }

    #[test]
    fn authenticates_sender_with_trusted_public_key() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let envelope =
            SignedRaftEnvelope::sign(&one, 2, 1, b"append-entries".to_vec(), &key_one).unwrap();
        assert_eq!(
            envelope.verify(&two, Some(1), &registry).unwrap(),
            b"append-entries"
        );
        let decoded = SignedRaftEnvelope::decode_json(&envelope.encode_json().unwrap()).unwrap();
        assert_eq!(
            decoded.verify(&two, Some(1), &registry).unwrap(),
            b"append-entries"
        );
    }

    #[test]
    fn a_node_cannot_impersonate_another_sender() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        // Node 2 signs a message that claims to come from node 1. The
        // verifier trusts node 1's public key, so the signature must fail.
        let group = one.group().clone();
        let signature = key_two.sign(&canonical_bytes(1, &group, 1, 2, 1, b"forged"));
        let forged = SignedRaftEnvelope {
            version: 1,
            group,
            sender: 1,
            recipient: 2,
            sequence: 1,
            payload: b"forged".to_vec(),
            signature,
        };
        assert_eq!(
            forged.verify(&two, Some(1), &registry),
            Err(TransportError::AuthenticationFailed)
        );
    }

    #[test]
    fn rejects_tampering_wrong_key_and_unknown_sender() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let mut envelope =
            SignedRaftEnvelope::sign(&one, 2, 1, b"vote".to_vec(), &key_one).unwrap();
        envelope.payload[0] ^= 1;
        assert_eq!(
            envelope.verify(&two, Some(1), &registry),
            Err(TransportError::AuthenticationFailed)
        );
        envelope = SignedRaftEnvelope::sign(&one, 2, 1, b"vote".to_vec(), &key_one).unwrap();
        envelope.sequence = 2;
        assert_eq!(
            envelope.verify(&two, Some(1), &registry),
            Err(TransportError::AuthenticationFailed)
        );
        let unknown = PeerKeyRegistry::new(BTreeMap::from([(2, key_two.public_key())])).unwrap();
        assert_eq!(
            envelope.verify(&two, Some(1), &unknown),
            Err(TransportError::UnknownSender)
        );
    }

    #[test]
    fn rejects_group_route_and_replay_violations() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let envelope = SignedRaftEnvelope::sign(&one, 2, 5, b"vote".to_vec(), &key_one).unwrap();
        // A message addressed to node 2 is not accepted by node 1.
        assert_eq!(
            envelope.verify(&one, Some(1), &registry),
            Err(TransportError::SenderMismatch)
        );
        assert_eq!(
            envelope.verify(&two, Some(2), &registry),
            Err(TransportError::SenderMismatch)
        );
        let other_group = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [5; 16],
                group: 3,
                incarnation: 8,
            },
            2,
            vec![1, 2],
        )
        .unwrap();
        assert_eq!(
            envelope.verify(&other_group, Some(1), &registry),
            Err(TransportError::IdentityMismatch)
        );

        let mut guard = ReplayGuard::default();
        assert!(guard.accept(1, 5));
        assert!(!guard.accept(1, 5));
        assert!(!guard.accept(1, 4));
        assert!(guard.accept(1, 6));
        assert!(guard.accept(2, 1));
    }

    #[test]
    fn duplicate_rpc_messages_use_distinct_sequences() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let first = SignedRaftEnvelope::sign(&one, 2, 1, b"same-index".to_vec(), &key_one).unwrap();
        let second =
            SignedRaftEnvelope::sign(&one, 2, 2, b"same-index".to_vec(), &key_one).unwrap();
        let mut guard = ReplayGuard::default();
        assert!(guard.accept(first.sender, first.sequence));
        assert!(guard.accept(second.sender, second.sequence));
        assert_eq!(
            first.verify(&two, Some(1), &registry).unwrap(),
            second.verify(&two, Some(1), &registry).unwrap()
        );
    }

    #[test]
    fn verify_and_accept_rejects_replayed_messages() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let envelope = SignedRaftEnvelope::sign(&one, 2, 1, b"vote".to_vec(), &key_one).unwrap();
        let mut guard = ReplayGuard::default();
        assert_eq!(
            envelope
                .verify_and_accept(&two, Some(1), &registry, &mut guard)
                .unwrap(),
            b"vote"
        );
        assert_eq!(
            envelope.verify_and_accept(&two, Some(1), &registry, &mut guard),
            Err(TransportError::Replayed)
        );
        // A forged message must not consume the honest sender's sequence.
        let group = one.group().clone();
        let forged = SignedRaftEnvelope {
            version: 1,
            group,
            sender: 1,
            recipient: 2,
            sequence: 2,
            payload: b"forged".to_vec(),
            signature: key_two.sign(&canonical_bytes(
                1,
                &one.group().clone(),
                1,
                2,
                2,
                b"forged",
            )),
        };
        assert_eq!(
            forged.verify_and_accept(&two, Some(1), &registry, &mut guard),
            Err(TransportError::AuthenticationFailed)
        );
        let honest = SignedRaftEnvelope::sign(&one, 2, 2, b"vote-2".to_vec(), &key_one).unwrap();
        assert_eq!(
            honest
                .verify_and_accept(&two, Some(1), &registry, &mut guard)
                .unwrap(),
            b"vote-2"
        );
    }
}
