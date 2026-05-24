//! Wire format and remote-capable message traits.
//!
//! Cross-node user messages implement [`RemoteMessage`]; actors that may be
//! addressed remotely implement [`RemoteActor`]. Local-only control plane items
//! (`Exit`, stop, OS signals) stay off the wire in Phase 8.

use spawned_address::ActorAddress;
use std::fmt;

/// Stable string identifier for a remotely addressable actor type.
pub trait RemoteActor {
    const REMOTE_ID: &'static str;
}

/// Stable string identifier for a message type that may cross the network.
pub trait RemoteMessage: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync {
    const REMOTE_ID: &'static str;
}

/// Envelope sent between nodes (transport-agnostic).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireEnvelope {
    /// Destination actor address.
    pub to: ActorAddress,
    /// [`RemoteMessage::REMOTE_ID`] of the payload.
    pub remote_msg_id: String,
    /// Serialized message body.
    pub payload: Vec<u8>,
    /// Correlation id for request/response pairing (`0` = fire-and-forget).
    pub correlation_id: u64,
}

impl WireEnvelope {
    /// Build a fire-and-forget envelope for a remote message.
    pub fn fire_and_forget<M: RemoteMessage>(
        to: ActorAddress,
        message: &M,
    ) -> Result<Self, WireError> {
        Ok(Self {
            to,
            remote_msg_id: M::REMOTE_ID.to_string(),
            payload: encode_payload(message)?,
            correlation_id: 0,
        })
    }

    /// Build a request envelope with a correlation id.
    pub fn request<M: RemoteMessage>(
        to: ActorAddress,
        message: &M,
        correlation_id: u64,
    ) -> Result<Self, WireError> {
        Ok(Self {
            to,
            remote_msg_id: M::REMOTE_ID.to_string(),
            payload: encode_payload(message)?,
            correlation_id,
        })
    }
}

/// Errors encoding or decoding wire data.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("remote message id mismatch: expected {expected}, got {actual}")]
    MessageIdMismatch { expected: &'static str, actual: String },
}

/// Serialize a remote message body with postcard.
pub fn encode_payload<M: RemoteMessage>(message: &M) -> Result<Vec<u8>, WireError> {
    postcard::to_allocvec(message).map_err(|e| WireError::Encode(e.to_string()))
}

/// Deserialize a remote message body after verifying the wire id.
pub fn decode_payload<M: RemoteMessage>(envelope: &WireEnvelope) -> Result<M, WireError> {
    if envelope.remote_msg_id != M::REMOTE_ID {
        return Err(WireError::MessageIdMismatch {
            expected: M::REMOTE_ID,
            actual: envelope.remote_msg_id.clone(),
        });
    }
    postcard::from_bytes(&envelope.payload).map_err(|e| WireError::Decode(e.to_string()))
}

/// Serialize a full envelope (length-framed transports use this).
pub fn encode_envelope(envelope: &WireEnvelope) -> Result<Vec<u8>, WireError> {
    postcard::to_allocvec(envelope).map_err(|e| WireError::Encode(e.to_string()))
}

/// Deserialize a full envelope.
pub fn decode_envelope(bytes: &[u8]) -> Result<WireEnvelope, WireError> {
    postcard::from_bytes(bytes).map_err(|e| WireError::Decode(e.to_string()))
}

impl fmt::Display for WireEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WireEnvelope {{ to: {}, msg: {}, correlation: {} }}",
            self.to, self.remote_msg_id, self.correlation_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spawned_address::{ActorId, NodeId};

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Ping {
        n: u32,
    }

    impl RemoteMessage for Ping {
        const REMOTE_ID: &'static str = "spawned.test.Ping/v1";
    }

    struct Worker;

    impl RemoteActor for Worker {
        const REMOTE_ID: &'static str = "spawned.test.Worker/v1";
    }

    #[test]
    fn remote_ids_are_stable() {
        assert_eq!(Ping::REMOTE_ID, "spawned.test.Ping/v1");
        assert_eq!(Worker::REMOTE_ID, "spawned.test.Worker/v1");
    }

    #[test]
    fn payload_roundtrip() {
        let envelope = WireEnvelope::fire_and_forget(
            ActorAddress::on(NodeId::new("a@host"), ActorId::from_raw(1)),
            &Ping { n: 99 },
        )
        .unwrap();
        let ping: Ping = decode_payload(&envelope).unwrap();
        assert_eq!(ping, Ping { n: 99 });
    }

    #[test]
    fn envelope_roundtrip() {
        let envelope = WireEnvelope::request(
            ActorAddress::local(ActorId::from_raw(3)),
            &Ping { n: 1 },
            42,
        )
        .unwrap();
        let bytes = encode_envelope(&envelope).unwrap();
        let back = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, back);
    }

    #[test]
    fn message_id_mismatch() {
        let mut envelope = WireEnvelope::fire_and_forget(
            ActorAddress::local(ActorId::from_raw(1)),
            &Ping { n: 0 },
        )
        .unwrap();
        envelope.remote_msg_id = "wrong".into();
        let err = decode_payload::<Ping>(&envelope).unwrap_err();
        assert!(matches!(err, WireError::MessageIdMismatch { .. }));
    }
}
