//! Cluster-aware messaging (requires the `cluster` feature).
//!
//! [`RemoteActorRef`] routes by [`ActorAddress`] locality: local recipients use
//! the existing `Recipient` path; remote addresses go through [`ClusterRouter`].

mod named_registry;
mod node;
mod pg_sync;
mod registry_sync;
mod remote_actor_ref;
pub(crate) mod remote_spawn;
mod supervision_broker;
mod supervision_exit;
mod supervision_link;
mod supervision_monitor;
mod supervision_remote;
mod supervision_routing;
mod supervision_sync;
mod wire_dispatch;

pub use named_registry::{
    apply_remote_event, local_snapshot, lookup_address, lookup_handle, register_named,
    unregister_named, NamedRegistryError,
};
pub use node::{Node, NodeBuilder, NodeError};
pub use pg_sync::install_pg_sync;
pub use registry_sync::install_registry_sync;
pub use remote_spawn::{
    install_tasks_runtime, request_spawn, Placement, RemoteChildHandle, RemoteSpawnError,
};
pub use supervision_remote::{
    complete_remote_shutdown_wait, overrides_from_spec, remote_spawn_spec_from_inner,
    request_spawn_async, request_spawn_blocking, request_spawn_with_retry_async,
    request_spawn_with_retry_blocking, shutdown_remote, shutdown_remote_and_wait,
    shutdown_remote_and_wait_blocking, RemoteShutdownError, RemoteSpawnMeta,
    RemoteSpawnRetryPolicy,
};
pub use supervision_link::{propagate_remote_link_exits, publish_link, publish_unlink};
pub use supervision_monitor::{
    is_local_address, publish_demonitor, publish_monitor, register_supervision_monitor_owner,
};
pub use supervision_broker::{
    install_supervision_broker, local_handle, register_supervision_actor, start_supervision_broker,
    SupervisionBroker, SupervisionBrokerInner,
};
pub use supervision_routing::{publish_routed, route_node};
pub use supervision_sync::{install_supervision_request, install_supervision_sync};
pub use remote_actor_ref::{RemoteActorRef, RemoteRequest};
pub use wire_dispatch::{tasks_wire_dispatch, threads_wire_dispatch};

pub use crate::pg::{
    apply_remote_pg_event, local_pg_snapshot, member_addresses, member_addresses_scoped,
};

pub use spawned_cluster::{
    AddressDispatch, AsyncTransport, ClusterFrame, ClusterRouter, ControlPlaneHooks, InboundDispatch,
    PgEvent, PgMemberEntry, RegistryEvent, RegistryHooks, RemoteSpawnSpec, RemoteSpecOverrides,
    SupervisionEnvelope, SupervisionEvent, SupervisionHooks, SupervisionSignal, TcpAsyncTransport,
    TcpClusterListener, TcpTransport, Transport, TransportError, UnavailableTransport,
    WireExitReason, WireReply, WireRestartType, PROTOCOL_VERSION, MAX_REMOTE_SPAWN_INIT_BYTES,
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
