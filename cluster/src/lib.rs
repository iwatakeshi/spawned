//! Cluster routing and pluggable transport for Spawned.
//!
//! Phase 8b provides [`ClusterRouter`] and [`Transport`]; Phase 8c adds
//! [`TcpTransport`] and [`TcpClusterListener`] for length-framed TCP.

mod async_transport;
mod control;
mod dispatch;
mod error;
mod frame;
mod inbound;
mod pg_sync;
mod protocol;
mod registry;
mod router;
mod supervision_sync;
mod supervision_validate;
mod transport;
mod tcp;
#[cfg(feature = "libp2p")]
mod libp2p;

pub use async_transport::AsyncTransport;
pub use control::ControlPlaneHooks;
pub use dispatch::AddressDispatch;
pub use frame::{read_frame, write_frame};
pub use inbound::InboundDispatch;
pub use pg_sync::{PgHooks, PgInbound, PgSnapshot};
pub use protocol::{
    decode_cluster_frame, decode_handshake, encode_handshake, ClusterFrame, Handshake, PgEvent,
    PgMemberEntry, RegistryEvent, RemoteSpawnSpec, RemoteSpecOverrides, SupervisionEnvelope,
    SupervisionEvent, SupervisionSignal, WireExitReason, WireReply, WireRestartType,
    PROTOCOL_VERSION, MAX_REMOTE_SPAWN_INIT_BYTES,
};
pub use error::TransportError;
pub use supervision_validate::{requires_correlation, validate_envelope, validate_reply};
pub use registry::{RegistryHooks, RegistryInbound, RegistrySnapshot};
pub use supervision_sync::{
    apply_supervision, decode_supervision, decode_supervision_reply, encode_supervision,
    encode_supervision_frame, stub_supervision_hooks, SupervisionHooks, SupervisionInbound,
};
pub use router::ClusterRouter;
pub use tcp::{TcpAsyncTransport, TcpClusterListener, TcpTransport};
pub use transport::{Transport, UnavailableTransport};
#[cfg(feature = "libp2p")]
pub use libp2p_identity as identity;
#[cfg(feature = "libp2p")]
pub use libp2p_identity::PeerId;
#[cfg(feature = "libp2p")]
pub use multiaddr::Multiaddr;
#[cfg(feature = "libp2p")]
pub use libp2p::{Libp2pCluster, Libp2pPeer, CLUSTER_PROTOCOL as LIBP2P_CLUSTER_PROTOCOL};
