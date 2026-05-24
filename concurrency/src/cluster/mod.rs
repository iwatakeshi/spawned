//! Cluster-aware messaging (requires the `cluster` feature).
//!
//! [`RemoteActorRef`] routes by [`ActorAddress`] locality: local recipients use
//! the existing `Recipient` path; remote addresses go through [`ClusterRouter`].

mod named_registry;
mod node;
mod pg_sync;
mod registry_sync;
mod remote_actor_ref;
mod wire_dispatch;

pub use named_registry::{
    apply_remote_event, local_snapshot, lookup_address, lookup_handle, register_named,
    unregister_named, NamedRegistryError,
};
pub use node::{Node, NodeBuilder, NodeError};
pub use pg_sync::install_pg_sync;
pub use registry_sync::install_registry_sync;
pub use remote_actor_ref::{RemoteActorRef, RemoteRequest};
pub use wire_dispatch::{tasks_wire_dispatch, threads_wire_dispatch};

pub use crate::pg::{
    apply_remote_pg_event, local_pg_snapshot, member_addresses, member_addresses_scoped,
};

pub use spawned_cluster::{
    AddressDispatch, AsyncTransport, ClusterFrame, ClusterRouter, ControlPlaneHooks, InboundDispatch,
    PgEvent, PgMemberEntry, RegistryEvent, RegistryHooks, TcpAsyncTransport, TcpClusterListener,
    TcpTransport, Transport, TransportError, UnavailableTransport, WireReply, PROTOCOL_VERSION,
};

#[cfg(feature = "cluster-libp2p")]
pub use spawned_cluster::{
    identity, Libp2pCluster, Libp2pPeer, Multiaddr, PeerId, LIBP2P_CLUSTER_PROTOCOL,
};

pub(crate) fn remove_named_by_actor_id(id: crate::child_handle::ActorId) {
    named_registry::remove_by_actor_id(id);
}

pub(crate) fn publish_pg_event(event: PgEvent) {
    pg_sync::publish(event);
}
