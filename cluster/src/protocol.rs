use spawned_address::{ActorAddress, NodeId};

/// Wire protocol version for TCP handshake.
pub const PROTOCOL_VERSION: u32 = 3;

/// Maximum `RemoteSpawnSpec::Worker::init` payload size (64 KiB).
pub const MAX_REMOTE_SPAWN_INIT_BYTES: usize = 64 * 1024;

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

/// Wire mirror of [`spawned_concurrency::ExitReason`] (no serde on the runtime type).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WireExitReason {
    Normal,
    Shutdown,
    Panic(String),
    Kill,
}

/// How a remotely spawned child is restarted (mirrors `RestartType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WireRestartType {
    Permanent,
    Transient,
    Temporary,
}

/// Optional overrides when spawning from a named child spec on a remote node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteSpecOverrides {
    pub restart: Option<WireRestartType>,
    pub pg_scope: Option<String>,
    pub pg_group: Option<String>,
}

/// How to spawn a child on a remote node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteSpawnSpec {
    Worker {
        worker_type: String,
        init: Vec<u8>,
    },
    NamedSpec {
        name: String,
        overrides: RemoteSpecOverrides,
    },
}

/// Supervision command signal (stop / shutdown / kill).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SupervisionSignal {
    Stop,
    Shutdown,
    Kill,
}

/// Supervision control-plane event (routed unicast, not federated broadcast).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SupervisionEvent {
    /// RPC: spawn a child on the placement node (`correlation_id != 0`).
    SpawnRequest {
        parent: ActorAddress,
        placement: NodeId,
        spec: RemoteSpawnSpec,
        link: bool,
    },
    /// RPC reply: spawn succeeded.
    SpawnOk { child: ActorAddress },
    /// RPC reply: spawn failed.
    SpawnErr { error: String },

    /// Command: signal a local actor (`correlation_id == 0`).
    Signal {
        target: ActorAddress,
        signal: SupervisionSignal,
    },

    /// Event: child exited (`correlation_id == 0`; routed to parent's node).
    ChildExit {
        child: ActorAddress,
        parent: ActorAddress,
        reason: WireExitReason,
    },
    /// Event: monitor fired (`correlation_id == 0`; routed to owner's node).
    Down {
        owner: ActorAddress,
        monitor_ref: u64,
        child: ActorAddress,
        reason: WireExitReason,
    },

    /// Registration: install a monitor on the target node.
    Monitor {
        owner: ActorAddress,
        target: ActorAddress,
        monitor_ref: u64,
    },
    Demonitor {
        owner: ActorAddress,
        target: ActorAddress,
        monitor_ref: u64,
    },
    Link {
        a: ActorAddress,
        b: ActorAddress,
    },
    Unlink {
        a: ActorAddress,
        b: ActorAddress,
    },
}

/// Top-level supervision payload on the wire.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SupervisionEnvelope {
    /// 0 = fire-and-forget; non-zero expects a supervision reply with matching id.
    pub correlation_id: u64,
    pub event: SupervisionEvent,
}

/// Top-level TCP frame after handshake (actor data plane or control plane).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusterFrame {
    Actor(spawned_wire::WireEnvelope),
    Registry(RegistryEvent),
    Pg(PgEvent),
    Supervision(SupervisionEnvelope),
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
