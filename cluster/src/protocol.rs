use spawned_address::{ActorAddress, NodeId};

/// Wire protocol version for TCP handshake.
pub const PROTOCOL_VERSION: u32 = 2;

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

/// Control-plane registry replication event (Phase 10.1).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RegistryEvent {
    Register {
        name: String,
        address: ActorAddress,
    },
    Unregister {
        name: String,
        address: ActorAddress,
    },
    /// Full snapshot of a peer's locally-owned registrations.
    Snapshot {
        entries: Vec<(String, ActorAddress)>,
    },
}

/// Control-plane process group replication event (Phase 10.2).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PgEvent {
    Join {
        scope: String,
        group: String,
        address: ActorAddress,
    },
    Leave {
        scope: String,
        group: String,
        address: ActorAddress,
    },
    /// Full snapshot of a peer's locally-owned pg memberships.
    Snapshot {
        entries: Vec<PgMemberEntry>,
    },
}

/// One pg membership entry in a federated snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PgMemberEntry {
    pub scope: String,
    pub group: String,
    pub address: ActorAddress,
}

/// Top-level TCP frame after handshake (actor data plane or control plane).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusterFrame {
    Actor(spawned_wire::WireEnvelope),
    Registry(RegistryEvent),
    Pg(PgEvent),
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

pub fn encode_cluster_frame(frame: &ClusterFrame) -> Result<Vec<u8>, spawned_wire::WireError> {
    postcard::to_allocvec(frame).map_err(|e| spawned_wire::WireError::Encode(e.to_string()))
}

pub fn decode_cluster_frame(bytes: &[u8]) -> Result<ClusterFrame, spawned_wire::WireError> {
    postcard::from_bytes(bytes).map_err(|e| spawned_wire::WireError::Decode(e.to_string()))
}

pub fn encode_reply(reply: &WireReply) -> Result<Vec<u8>, spawned_wire::WireError> {
    postcard::to_allocvec(reply).map_err(|e| spawned_wire::WireError::Encode(e.to_string()))
}

pub fn decode_reply_frame(bytes: &[u8]) -> Result<WireReply, spawned_wire::WireError> {
    postcard::from_bytes(bytes).map_err(|e| spawned_wire::WireError::Decode(e.to_string()))
}
