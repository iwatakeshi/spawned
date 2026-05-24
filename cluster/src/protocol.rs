use spawned_address::NodeId;

/// Wire protocol version for TCP handshake.
pub const PROTOCOL_VERSION: u32 = 1;

/// Initial handshake exchanged after TCP connect.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Handshake {
    pub version: u32,
    pub node: NodeId,
}

impl Handshake {
    pub fn local(node: NodeId) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            node,
        }
    }
}

/// Response to a correlated request envelope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireReply {
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}

pub fn encode_handshake(handshake: &Handshake) -> Result<Vec<u8>, spawned_wire::WireError> {
    postcard::to_allocvec(handshake).map_err(|e| spawned_wire::WireError::Encode(e.to_string()))
}

pub fn decode_handshake(bytes: &[u8]) -> Result<Handshake, spawned_wire::WireError> {
    postcard::from_bytes(bytes).map_err(|e| spawned_wire::WireError::Decode(e.to_string()))
}

pub fn encode_reply(reply: &WireReply) -> Result<Vec<u8>, spawned_wire::WireError> {
    postcard::to_allocvec(reply).map_err(|e| spawned_wire::WireError::Encode(e.to_string()))
}

pub fn decode_reply_frame(bytes: &[u8]) -> Result<WireReply, spawned_wire::WireError> {
    postcard::from_bytes(bytes).map_err(|e| spawned_wire::WireError::Decode(e.to_string()))
}
