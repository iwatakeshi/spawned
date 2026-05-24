//! Cluster node bootstrap (Phase 8d + 10.1 federated registry).
//!
//! [`NodeBuilder`] wires TCP listen/transport, installs the global
//! [`ClusterRouter`], and optionally registers OS signal shutdown.

use crate::child_handle::ChildHandle;
use crate::message::Message;
use crate::shutdown_signal::{register_shutdown_on_signal, spawn_shutdown_signal_dispatcher_tasks};
use crate::shutdown_signal::SignalGuard;
use crate::RemoteMessage;
use spawned_address::{local_node, set_local_node_for_test, ActorAddress, NodeId};
use spawned_cluster::{
    AddressDispatch, ClusterRouter, ControlPlaneHooks, TcpClusterListener, TcpTransport, Transport,
    TransportError, UnavailableTransport,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Errors starting a cluster [`Node`].
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("node name was already initialized before build")]
    NameAlreadySet,
}

/// Running cluster node: router, optional TCP listener, signal guards.
pub struct Node {
    local_node: NodeId,
    router: Arc<ClusterRouter>,
    tcp: Option<Arc<TcpTransport>>,
    listener: Option<TcpClusterListener>,
    dispatch: Arc<AddressDispatch>,
    _signal_guards: Vec<SignalGuard>,
}

impl Node {
    /// Start configuring a node.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// This node's identity.
    pub fn local_node(&self) -> &NodeId {
        &self.local_node
    }

    /// Cluster router installed for this process (also the global router).
    pub fn router(&self) -> Arc<ClusterRouter> {
        self.router.clone()
    }

    /// Cluster listen address, if configured.
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().map(|l| l.local_addr())
    }

    /// Register a tasks-runtime actor for inbound wire delivery.
    pub fn register_tasks_wire<M>(
        &self,
        address: ActorAddress,
        recipient: crate::tasks::Recipient<M>,
    ) where
        M: Message + RemoteMessage,
        M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
    {
        let handler = super::tasks_wire_dispatch(address.clone(), recipient);
        self.dispatch.register(address, handler);
    }

    /// Register a threads-runtime actor for inbound wire delivery.
    pub fn register_threads_wire<M>(
        &self,
        address: ActorAddress,
        recipient: crate::threads::Recipient<M>,
    ) where
        M: Message + RemoteMessage,
        M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
    {
        let handler = super::threads_wire_dispatch(address.clone(), recipient);
        self.dispatch.register(address, handler);
    }

    /// Gracefully stop the cluster listener.
    pub fn shutdown(self) {
        drop(self);
    }

    /// Exchange registry snapshots with all configured peers.
    pub fn sync_registry(&self) -> Result<(), TransportError> {
        if let Some(tcp) = &self.tcp {
            tcp.sync_peers()?;
        }
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.shutdown();
        }
    }
}

/// Configures and builds a [`Node`].
#[derive(Default)]
pub struct NodeBuilder {
    name: Option<NodeId>,
    listen: Option<SocketAddr>,
    peers: HashMap<NodeId, SocketAddr>,
    shutdown_handles: Vec<ChildHandle>,
}

impl NodeBuilder {
    /// Set the node name (must be before the first [`local_node()`] call).
    pub fn name(mut self, name: impl Into<NodeId>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Listen for inbound cluster TCP connections.
    pub fn listen(mut self, addr: SocketAddr) -> Self {
        self.listen = Some(addr);
        self
    }

    /// Add a remote peer reachable at the given socket address.
    pub fn peer(mut self, node: impl Into<NodeId>, addr: SocketAddr) -> Self {
        self.peers.insert(node.into(), addr);
        self
    }

    /// Actors to receive priority shutdown on Ctrl+C / SIGTERM.
    pub fn shutdown_on_signal(mut self, handles: &[ChildHandle]) -> Self {
        self.shutdown_handles.extend_from_slice(handles);
        self
    }

    /// Build the node: transport, optional listener, global router, signal hooks.
    pub fn build(self) -> Result<Node, NodeError> {
        if let Some(name) = self.name.clone() {
            set_local_node_for_test(name.clone());
            if local_node() != name {
                return Err(NodeError::NameAlreadySet);
            }
        }

        let local = local_node();
        let dispatch = Arc::new(AddressDispatch::new());
        let federated = self.listen.is_some() || !self.peers.is_empty();

        let control = if federated {
            ControlPlaneHooks::federated(
                Arc::new(|event| super::named_registry::apply_remote_event(event)),
                Arc::new(super::named_registry::local_snapshot),
                Arc::new(|event| crate::pg::apply_remote_pg_event(event)),
                Arc::new(crate::pg::local_pg_snapshot),
            )
        } else {
            ControlPlaneHooks::none()
        };

        let tcp: Option<Arc<TcpTransport>> = if self.peers.is_empty() {
            None
        } else {
            Some(Arc::new(
                TcpTransport::new(local.clone(), self.peers.clone())
                    .with_control_plane_hooks(control.clone()),
            ))
        };

        if let Some(tcp) = &tcp {
            if federated {
                let tcp_registry = tcp.clone();
                super::registry_sync::install(move |event| {
                    let _ = tcp_registry.broadcast_registry(event);
                });
                let tcp_pg = tcp.clone();
                super::pg_sync::install(move |event| {
                    let _ = tcp_pg.broadcast_pg(event);
                });
            }
        }

        let transport: Arc<dyn Transport> = if let Some(tcp) = tcp.clone() {
            tcp
        } else {
            Arc::new(UnavailableTransport)
        };

        let router = Arc::new(ClusterRouter::new(transport));
        let _ = ClusterRouter::set_global(router.clone());

        let listener = if let Some(addr) = self.listen {
            Some(TcpClusterListener::bind_with_control_plane(
                addr,
                local.clone(),
                dispatch.clone(),
                control,
            )?)
        } else {
            None
        };

        let _signal_guards = if self.shutdown_handles.is_empty() {
            Vec::new()
        } else {
            spawn_shutdown_signal_dispatcher_tasks();
            register_shutdown_on_signal(&self.shutdown_handles)
        };

        Ok(Node {
            local_node: local,
            router,
            tcp,
            listener,
            dispatch,
            _signal_guards,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_only_node_builds() {
        let node = Node::builder().build().unwrap();
        assert!(node.listen_addr().is_none());
    }
}
