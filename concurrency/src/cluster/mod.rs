//! Cluster-aware messaging (requires the `cluster` feature).
//!
//! [`RemoteActorRef`] routes by [`ActorAddress`] locality: local recipients use
//! the existing `Recipient` path; remote addresses go through [`ClusterRouter`].

mod named_registry;
mod remote_actor_ref;

pub use named_registry::{
    lookup_address, register_named, unregister_named, NamedRegistryError,
};
pub use remote_actor_ref::RemoteActorRef;

pub use spawned_cluster::{ClusterRouter, Transport, TransportError, UnavailableTransport};

pub(crate) fn remove_named_by_actor_id(id: crate::child_handle::ActorId) {
    named_registry::remove_by_actor_id(id);
}
