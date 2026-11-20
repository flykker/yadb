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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};

use crate::multiraft::{GroupIdentity, ReplicaIdentity};

const CONTEXT: &[u8] = b"yadb/openraft/rpc/v1";
const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
const PUBLIC_KEY_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;

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
}

#[derive(Debug, Default, Clone)]
struct CertificatePinRegistry {
    active: BTreeMap<u32, BTreeSet<[u8; 32]>>,
    owners: BTreeMap<[u8; 32], u32>,
    revoked: BTreeSet<[u8; 32]>,
    initial: Option<BTreeMap<u32, [u8; 32]>>,
}

impl PeerKeyRegistry {
    pub fn new(keys: BTreeMap<u32, Vec<u8>>) -> Result<Self, TransportError> {
        if keys.is_empty() || keys.values().any(|key| key.len() != PUBLIC_KEY_LEN) {
            return Err(TransportError::InvalidInput("invalid peer public key"));
        }
        Ok(Self {
            keys,
            pins: Arc::new(RwLock::new(CertificatePinRegistry::default())),
        })
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
    pub fn pin_change(&self, change: &PinChange) -> Result<(), TransportError> {
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
        write!(f, "{self:?}")
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
