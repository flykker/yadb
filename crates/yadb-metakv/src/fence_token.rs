//! Signed fence tokens for cross-node storage-fence operations.
//!
//! A [`FenceToken`] is a small, self-contained statement signed by one cluster
//! node and addressed to another. It proves that a trusted node, identified by
//! the same Ed25519 public key the Raft transport already trusts for that node,
//! acting for one specific [`GroupIdentity`], issued a [`FenceOperation`] for a
//! specific epoch with an absolute deadline. The recipient re-derives the
//! sender's public key from its trusted [`PeerKeyRegistry`]; the key is never
//! taken from the token itself.
//!
//! What a token does **not** prove:
//!
//! * It is not authorization for arbitrary blob identifiers. The token binds a
//!   group, sender, recipient, operation and epoch, but carries no payload and
//!   names no blobs. Any blob-id authorization must be enforced separately by
//!   the operation that consumes the token.
//! * It is not replay-free by itself. There is deliberately no sequence number:
//!   replay is bounded by the target's epoch compare-and-swap plus the deadline.
//!   A token can be replayed until the target has advanced past its epoch or the
//!   deadline has passed, so every consumer must apply both checks atomically.
//!
//! Signatures use a dedicated domain-separation prefix that differs from the
//! Raft envelope prefix, so a signature over one structure can never verify as
//! the other. The encoding used for signing is a fixed, unambiguous byte string,
//! never JSON; JSON is only a transport encoding for the token itself.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::openraft_transport::{verify_signature, NodeSigningKey, PeerKeyRegistry};

/// Wire and signing version of the fence token.
pub const FENCE_TOKEN_VERSION: u16 = 1;

/// Domain separation prefix. This must never equal the Raft transport context,
/// otherwise an envelope signature could be replayed as a fence token.
const FENCE_CONTEXT: &[u8] = b"YADB-FENCE-V1";

/// Ed25519 signatures are always 64 bytes.
const SIGNATURE_LEN: usize = 64;

/// The operation a fence token authorizes.
///
/// The explicit discriminants are part of the signed encoding and must stay
/// stable. Serde rejects unknown variant names on deserialization.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FenceOperation {
    AdvanceEpoch = 1,
    DeleteCollectable = 2,
    DeleteFenced = 3,
}

/// A node's signed statement that it issued a fence operation for a group at an
/// epoch, addressed to one specific recipient and valid until a deadline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FenceToken {
    pub version: u16,
    pub group: GroupIdentity,
    pub sender: u32,
    pub recipient: u32,
    pub operation: FenceOperation,
    pub epoch: u64,
    pub expires_at_ms: u64,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FenceTokenError {
    #[error("fence token recipient is not another voter in the sender's group")]
    InvalidRecipient,

    #[error("fence token group does not match the local group identity")]
    IdentityMismatch,

    #[error("fence token sender is not another voter in the local group")]
    SenderMismatch,

    #[error("fence token was addressed to a different node")]
    RecipientMismatch,

    #[error("no trusted public key is registered for the fence token sender")]
    UnknownSender,

    #[error("fence token operation mismatch: expected {expected:?}, presented {presented:?}")]
    OperationMismatch {
        expected: FenceOperation,
        presented: FenceOperation,
    },

    #[error("fence token epoch mismatch: expected {expected}, presented {presented}")]
    EpochMismatch { expected: u64, presented: u64 },

    #[error("fence token signature is invalid or unsupported")]
    AuthenticationFailed,

    #[error("clock is unreadable, so the fence token deadline cannot be checked: {reason}")]
    ClockUnreadable { reason: String },

    #[error("fence token expired at {expires_at_ms} ms (now {now_ms} ms)")]
    Expired { expires_at_ms: u64, now_ms: u64 },

    #[error("fence token encoding is invalid")]
    Encoding,
}

/// Canonical signing bytes, in fixed order:
///
/// `FENCE_CONTEXT` || `version: u16 LE` || `group.cluster: [u8; 16]` ||
/// `group.group: u64 LE` || `group.incarnation: u64 LE` ||
/// `sender: u32 LE` || `recipient: u32 LE` || `operation: u8` ||
/// `epoch: u64 LE` || `expires_at_ms: u64 LE`.
///
/// Every field has a fixed width, so no lengths or delimiters are needed. There
/// is no payload.
fn canonical_bytes(
    version: u16,
    group: &GroupIdentity,
    sender: u32,
    recipient: u32,
    operation: FenceOperation,
    epoch: u64,
    expires_at_ms: u64,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FENCE_CONTEXT.len() + 2 + 16 + 8 + 8 + 4 + 4 + 1 + 8 + 8);
    bytes.extend_from_slice(FENCE_CONTEXT);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&group.cluster);
    bytes.extend_from_slice(&group.group.to_le_bytes());
    bytes.extend_from_slice(&group.incarnation.to_le_bytes());
    bytes.extend_from_slice(&sender.to_le_bytes());
    bytes.extend_from_slice(&recipient.to_le_bytes());
    bytes.push(operation as u8);
    bytes.extend_from_slice(&epoch.to_le_bytes());
    bytes.extend_from_slice(&expires_at_ms.to_le_bytes());
    bytes
}

impl FenceToken {
    /// Creates and signs a token for `recipient` in the sender's own group.
    ///
    /// The recipient must be a different voter in the same group, matching the
    /// routing rule used by the Raft transport envelope.
    pub fn sign(
        identity: &ReplicaIdentity,
        recipient: u32,
        operation: FenceOperation,
        epoch: u64,
        expires_at_ms: u64,
        key: &NodeSigningKey,
    ) -> Result<Self, FenceTokenError> {
        if recipient == 0 || recipient == identity.node() || !identity.voters().contains(&recipient)
        {
            return Err(FenceTokenError::InvalidRecipient);
        }
        let group = identity.group().clone();
        let sender = identity.node();
        let signature = key.sign(&canonical_bytes(
            FENCE_TOKEN_VERSION,
            &group,
            sender,
            recipient,
            operation,
            epoch,
            expires_at_ms,
        ));
        Ok(Self {
            version: FENCE_TOKEN_VERSION,
            group,
            sender,
            recipient,
            operation,
            epoch,
            expires_at_ms,
            signature,
        })
    }

    /// Verifies this token for the local identity, trusted peer keys, expected
    /// operation and epoch, and an explicit clock reading.
    ///
    /// Checks run in this order: version, group, sender, recipient, operation,
    /// epoch, signature length, trusted key lookup, signature, then deadline. An
    /// unreadable clock fails closed with [`FenceTokenError::ClockUnreadable`].
    /// The deadline is strict: `now_ms == expires_at_ms` is already expired.
    pub fn verify(
        &self,
        local: &ReplicaIdentity,
        registry: &PeerKeyRegistry,
        expected_operation: FenceOperation,
        expected_epoch: u64,
        now_ms: Result<u64, String>,
    ) -> Result<(), FenceTokenError> {
        if self.version != FENCE_TOKEN_VERSION {
            return Err(FenceTokenError::AuthenticationFailed);
        }
        if self.group != *local.group() {
            return Err(FenceTokenError::IdentityMismatch);
        }
        if self.sender == local.node() || !local.voters().contains(&self.sender) {
            return Err(FenceTokenError::SenderMismatch);
        }
        if self.recipient != local.node() {
            return Err(FenceTokenError::RecipientMismatch);
        }
        if self.operation != expected_operation {
            return Err(FenceTokenError::OperationMismatch {
                expected: expected_operation,
                presented: self.operation,
            });
        }
        if self.epoch != expected_epoch {
            return Err(FenceTokenError::EpochMismatch {
                expected: expected_epoch,
                presented: self.epoch,
            });
        }
        if self.signature.len() != SIGNATURE_LEN {
            return Err(FenceTokenError::AuthenticationFailed);
        }
        let public_key = registry
            .public_key(self.sender)
            .ok_or(FenceTokenError::UnknownSender)?;
        if !verify_signature(
            public_key,
            &canonical_bytes(
                self.version,
                &self.group,
                self.sender,
                self.recipient,
                self.operation,
                self.epoch,
                self.expires_at_ms,
            ),
            &self.signature,
        ) {
            return Err(FenceTokenError::AuthenticationFailed);
        }
        let now = now_ms.map_err(|reason| FenceTokenError::ClockUnreadable { reason })?;
        if now >= self.expires_at_ms {
            return Err(FenceTokenError::Expired {
                expires_at_ms: self.expires_at_ms,
                now_ms: now,
            });
        }
        Ok(())
    }

    /// Encodes the token for transport. This does not authenticate it; callers
    /// must still call [`FenceToken::verify`] after decoding.
    pub fn encode_json(&self) -> Result<Vec<u8>, FenceTokenError> {
        serde_json::to_vec(self).map_err(|_| FenceTokenError::Encoding)
    }

    /// Decodes a token from transport bytes. Unknown operations and malformed
    /// fields are rejected by serde. This does not authenticate the token.
    pub fn decode_json(bytes: &[u8]) -> Result<Self, FenceTokenError> {
        serde_json::from_slice(bytes).map_err(|_| FenceTokenError::Encoding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openraft_transport::{SignedRaftEnvelope, TransportError};
    use std::collections::BTreeMap;

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
    fn valid_token_verifies_for_the_addressed_recipient() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let token = FenceToken::sign(
            &one,
            2,
            FenceOperation::AdvanceEpoch,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        assert_eq!(token.version, FENCE_TOKEN_VERSION);
        assert_eq!(token.sender, 1);
        assert_eq!(token.recipient, 2);
        assert_eq!(token.operation, FenceOperation::AdvanceEpoch);
        assert_eq!(token.epoch, 7);
        assert_eq!(token.expires_at_ms, 10_000);
        token
            .verify(
                &two,
                &registry,
                FenceOperation::AdvanceEpoch,
                7,
                Ok(9_999),
            )
            .unwrap();
    }

    #[test]
    fn tampered_epoch_is_authentication_failure() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let mut token = FenceToken::sign(
            &one,
            2,
            FenceOperation::AdvanceEpoch,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        token.epoch = 8;
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::AdvanceEpoch,
                8,
                Ok(9_999),
            ),
            Err(FenceTokenError::AuthenticationFailed)
        );
    }

    #[test]
    fn advance_epoch_token_is_not_accepted_as_delete_collectable() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let token = FenceToken::sign(
            &one,
            2,
            FenceOperation::AdvanceEpoch,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::DeleteCollectable,
                7,
                Ok(9_999),
            ),
            Err(FenceTokenError::OperationMismatch {
                expected: FenceOperation::DeleteCollectable,
                presented: FenceOperation::AdvanceEpoch,
            })
        );
    }

    #[test]
    fn rejects_wrong_recipient_unknown_sender_and_foreign_group() {
        let group = GroupIdentity {
            cluster: [4; 16],
            group: 3,
            incarnation: 8,
        };
        let one = ReplicaIdentity::new(group.clone(), 1, vec![1, 2, 3]).unwrap();
        let three = ReplicaIdentity::new(group.clone(), 3, vec![1, 2, 3]).unwrap();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let token = FenceToken::sign(
            &one,
            2,
            FenceOperation::DeleteFenced,
            7,
            10_000,
            &key_one,
        )
        .unwrap();

        // Node 3 is a voter in the right group but is not the addressed node.
        assert_eq!(
            token.verify(
                &three,
                &registry,
                FenceOperation::DeleteFenced,
                7,
                Ok(9_999),
            ),
            Err(FenceTokenError::RecipientMismatch)
        );

        let two = ReplicaIdentity::new(group.clone(), 2, vec![1, 2, 3]).unwrap();
        let unknown = PeerKeyRegistry::new(BTreeMap::from([(2, key_two.public_key())])).unwrap();
        assert_eq!(
            token.verify(
                &two,
                &unknown,
                FenceOperation::DeleteFenced,
                7,
                Ok(9_999),
            ),
            Err(FenceTokenError::UnknownSender)
        );

        let foreign_group = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [5; 16],
                group: 3,
                incarnation: 8,
            },
            2,
            vec![1, 2, 3],
        )
        .unwrap();
        assert_eq!(
            token.verify(
                &foreign_group,
                &registry,
                FenceOperation::DeleteFenced,
                7,
                Ok(9_999),
            ),
            Err(FenceTokenError::IdentityMismatch)
        );
    }

    #[test]
    fn deadline_is_strict_and_unreadable_clock_fails_closed() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);
        let token = FenceToken::sign(
            &one,
            2,
            FenceOperation::DeleteCollectable,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::DeleteCollectable,
                7,
                Ok(10_000),
            ),
            Err(FenceTokenError::Expired {
                expires_at_ms: 10_000,
                now_ms: 10_000,
            })
        );
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::DeleteCollectable,
                7,
                Err("clock unavailable".to_string()),
            ),
            Err(FenceTokenError::ClockUnreadable {
                reason: "clock unavailable".to_string(),
            })
        );
        // A late clock is not accepted either.
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::DeleteCollectable,
                7,
                Ok(10_001),
            ),
            Err(FenceTokenError::Expired {
                expires_at_ms: 10_000,
                now_ms: 10_001,
            })
        );
    }

    #[test]
    fn raft_envelope_signature_cannot_be_replayed_as_a_fence_token() {
        let (one, two) = identities();
        let (key_one, key_two) = keys();
        let registry = registry(&key_one, &key_two);

        // Same sender, recipient, sequence/epoch and (empty) payload as a valid
        // fence token, so only the domain separation and operation/deadline
        // fields differ.
        let envelope = SignedRaftEnvelope::sign(&one, 2, 7, Vec::new(), &key_one).unwrap();
        let token = FenceToken {
            version: FENCE_TOKEN_VERSION,
            group: one.group().clone(),
            sender: envelope.sender,
            recipient: envelope.recipient,
            operation: FenceOperation::AdvanceEpoch,
            epoch: envelope.sequence,
            expires_at_ms: 10_000,
            signature: envelope.signature.clone(),
        };
        // Ed25519 verification failing under the same trusted key proves the
        // signed byte strings differ; the explicit context check documents the
        // intended reason.
        assert_ne!(FENCE_CONTEXT, b"yadb/openraft/rpc/v1");
        assert_eq!(
            token.verify(
                &two,
                &registry,
                FenceOperation::AdvanceEpoch,
                7,
                Ok(9_999),
            ),
            Err(FenceTokenError::AuthenticationFailed)
        );

        // The converse must fail too: a fence-token signature cannot be used to
        // authenticate a Raft envelope.
        let fence = FenceToken::sign(
            &one,
            2,
            FenceOperation::AdvanceEpoch,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        let forged = SignedRaftEnvelope {
            version: 1,
            group: fence.group.clone(),
            sender: fence.sender,
            recipient: fence.recipient,
            sequence: fence.epoch,
            payload: Vec::new(),
            signature: fence.signature.clone(),
        };
        assert_eq!(
            forged.verify(&two, Some(1), &registry),
            Err(TransportError::AuthenticationFailed)
        );
    }

    #[test]
    fn rejects_invalid_recipient_at_signing_time() {
        let (one, _) = identities();
        let (key_one, _) = keys();
        assert_eq!(
            FenceToken::sign(
                &one,
                0,
                FenceOperation::AdvanceEpoch,
                7,
                10_000,
                &key_one,
            ),
            Err(FenceTokenError::InvalidRecipient)
        );
        assert_eq!(
            FenceToken::sign(
                &one,
                1,
                FenceOperation::AdvanceEpoch,
                7,
                10_000,
                &key_one,
            ),
            Err(FenceTokenError::InvalidRecipient)
        );
        assert_eq!(
            FenceToken::sign(
                &one,
                3,
                FenceOperation::AdvanceEpoch,
                7,
                10_000,
                &key_one,
            ),
            Err(FenceTokenError::InvalidRecipient)
        );
    }

    #[test]
    fn json_roundtrip_preserves_the_token_and_rejects_unknown_operations() {
        let (one, _) = identities();
        let (key_one, _) = keys();
        let token = FenceToken::sign(
            &one,
            2,
            FenceOperation::DeleteCollectable,
            7,
            10_000,
            &key_one,
        )
        .unwrap();
        let encoded = token.encode_json().unwrap();
        assert_eq!(FenceToken::decode_json(&encoded).unwrap(), token);

        let unknown = String::from_utf8(encoded)
            .unwrap()
            .replace("\"DeleteCollectable\"", "\"UnknownOperation\"");
        assert_eq!(
            FenceToken::decode_json(unknown.as_bytes()),
            Err(FenceTokenError::Encoding)
        );
    }
}
