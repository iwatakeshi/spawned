//! Cluster-aware messaging (requires the `cluster` feature).
//!
//! [`RemoteActorRef`] routes by [`ActorAddress`] locality: local recipients use
//! the existing `Recipient` path; remote addresses go through [`ClusterRouter`].

mod named_registry;
mod node;
mod remote_actor_ref;
mod wire_dispatch;

pub use named_registry::{
    lookup_address, lookup_handle, register_named, unregister_named, NamedRegistryError,
};
pub use node::{Node, NodeBuilder, NodeError};
pub use remote_actor_ref::{RemoteActorRef, RemoteRequest};
pub use wire_dispatch::{tasks_wire_dispatch, threads_wire_dispatch};

pub use spawned_cluster::{
    AddressDispatch, ClusterRouter, InboundDispatch, TcpClusterListener, TcpTransport, Transport,
    TransportError, UnavailableTransport, WireReply, PROTOCOL_VERSION,
};

pub(crate) fn remove_named_by_actor_id(id: crate::child_handle::ActorId) {
    named_registry::remove_by_actor_id(id);
}
