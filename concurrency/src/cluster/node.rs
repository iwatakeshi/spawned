//! Cluster node bootstrap (Phase 8d + 10.1 federated registry + 11.1 libp2p).
//!
//! [`NodeBuilder`] wires TCP or libp2p listen/transport, installs the global
//! [`ClusterRouter`], and optionally registers OS signal shutdown.

use crate::child_handle::ChildHandle;
use crate::message::Message;
use crate::shutdown_signal::{register_shutdown_on_signal, spawn_shutdown_signal_dispatcher_tasks};
use crate::shutdown_signal::SignalGuard;
use crate::RemoteMessage;
use spawned_address::{local_node, set_local_node_for_test, ActorAddress, NodeId};
use super::{start_supervision_broker, SupervisionBroker, SupervisionBrokerInner};
use spawned_cluster::{
    AddressDispatch, AsyncTransport, ClusterRouter, ControlPlaneHooks, SupervisionHooks,
    TcpAsyncTransport, TcpClusterListener, TcpTransport, Transport, TransportError,
    UnavailableTransport,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

#[cfg(feature = "cluster-libp2p")]
use spawned_cluster::{Libp2pCluster, Libp2pPeer};

/// Errors starting a cluster [`Node`].
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("node name was already initialized before build")]
    NameAlreadySet,
    #[error("invalid node configuration: {0}")]
    InvalidConfig(String),
}

enum ClusterBackend {
    Tcp {
        transport: Option<Arc<TcpTransport>>,
        listener: Option<TcpClusterListener>,
    },
    #[cfg(feature = "cluster-libp2p")]
    Libp2p(Arc<Libp2pCluster>),
    None,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum TransportKind {
    #[default]
    Tcp,
    #[cfg(feature = "cluster-libp2p")]
    Libp2p,
}

#[cfg(feature = "cluster-libp2p")]
#[derive(Clone)]
struct Libp2pPeerConfig {
    node: NodeId,
    peer_id: spawned_cluster::PeerId,
    addr: spawned_cluster::Multiaddr,
}

/// Running cluster node: router, optional cluster backend, signal guards.
pub struct Node {
    local_node: NodeId,
    router: Arc<ClusterRouter>,
    backend: ClusterBackend,
    dispatch: Arc<AddressDispatch>,
    supervision: Option<Arc<SupervisionBrokerInner>>,
    _supervision_broker: Option<crate::tasks::ActorRef<SupervisionBroker>>,
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

    /// Cluster listen address, if configured (TCP only).
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        match &self.backend {
            ClusterBackend::Tcp { listener, .. } => listener.as_ref().map(|l| l.local_addr()),
            _ => None,
        }
    }

    /// libp2p listen multiaddrs, when using the libp2p backend.
    #[cfg(feature = "cluster-libp2p")]
    pub fn listen_multiaddrs(&self) -> Vec<spawned_cluster::Multiaddr> {
        match &self.backend {
            ClusterBackend::Libp2p(cluster) => cluster.listen_addrs(),
            _ => Vec::new(),
        }
    }

    /// Register a local actor for inbound remote supervision signals.
    pub fn register_supervision(
        &self,
        address: ActorAddress,
        handle: ChildHandle,
    ) -> Result<(), NodeError> {
        self.supervision
            .as_ref()
            .ok_or_else(|| {
                NodeError::InvalidConfig(
                    "supervision broker not running (cluster listen/peer required)".into(),
                )
            })?
            .register(address, handle)
            .map_err(NodeError::Transport)
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
        match &self.backend {
            ClusterBackend::Tcp { transport, .. } => {
                if let Some(tcp) = transport {
                    tcp.sync_peers()?;
                }
            }
            #[cfg(feature = "cluster-libp2p")]
            ClusterBackend::Libp2p(cluster) => cluster.sync_peers()?,
            ClusterBackend::None => {}
        }
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        match &mut self.backend {
            ClusterBackend::Tcp { listener, .. } => {
                if let Some(listener) = listener.take() {
                    listener.shutdown();
                }
            }
            #[cfg(feature = "cluster-libp2p")]
            ClusterBackend::Libp2p(cluster) => {
                cluster.signal_shutdown();
                cluster.join_thread();
            }
            ClusterBackend::None => {}
        }
    }
}

/// Configures and builds a [`Node`].
#[derive(Default)]
pub struct NodeBuilder {
    name: Option<NodeId>,
    transport: TransportKind,
    listen: Option<SocketAddr>,
    peers: HashMap<NodeId, SocketAddr>,
    #[cfg(feature = "cluster-libp2p")]
    keypair: Option<spawned_cluster::identity::Keypair>,
    #[cfg(feature = "cluster-libp2p")]
    listen_libp2p: Option<spawned_cluster::Multiaddr>,
    #[cfg(feature = "cluster-libp2p")]
    libp2p_peers: Vec<Libp2pPeerConfig>,
    shutdown_handles: Vec<ChildHandle>,
}

impl NodeBuilder {
    /// Set the node name (must be before the first [`local_node()`] call).
    pub fn name(mut self, name: impl Into<NodeId>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Use TCP transport (default).
    pub fn transport_tcp(mut self) -> Self {
        self.transport = TransportKind::Tcp;
        self
    }

    /// Use libp2p transport (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn transport_libp2p(mut self, keypair: Option<spawned_cluster::identity::Keypair>) -> Self {
        self.transport = TransportKind::Libp2p;
        self.keypair = keypair;
        self
    }

    /// Listen for inbound cluster TCP connections.
    pub fn listen(mut self, addr: SocketAddr) -> Self {
        self.listen = Some(addr);
        self
    }

    /// Listen for inbound libp2p connections (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn listen_libp2p(mut self, addr: spawned_cluster::Multiaddr) -> Self {
        self.listen_libp2p = Some(addr);
        self
    }

    /// Add a remote peer reachable at the given socket address (TCP).
    pub fn peer(mut self, node: impl Into<NodeId>, addr: SocketAddr) -> Self {
        self.peers.insert(node.into(), addr);
        self
    }

    /// Add a remote libp2p peer (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn libp2p_peer(
        mut self,
        node: impl Into<NodeId>,
        peer_id: spawned_cluster::PeerId,
        addr: spawned_cluster::Multiaddr,
    ) -> Self {
        self.libp2p_peers.push(Libp2pPeerConfig {
            node: node.into(),
            peer_id,
            addr,
        });
        self
    }

    /// Actors to receive priority shutdown on Ctrl+C / SIGTERM.
    pub fn shutdown_on_signal(mut self, handles: &[ChildHandle]) -> Self {
        self.shutdown_handles.extend_from_slice(handles);
        self
    }

    fn validate(&self) -> Result<(), NodeError> {
        #[cfg(feature = "cluster-libp2p")]
        {
            if self.transport == TransportKind::Libp2p {
                if self.listen.is_some() || !self.peers.is_empty() {
                    return Err(NodeError::InvalidConfig(
                        "libp2p transport cannot be combined with TCP listen/peer options".into(),
                    ));
                }
            } else if self.listen_libp2p.is_some() || !self.libp2p_peers.is_empty() {
                return Err(NodeError::InvalidConfig(
                    "TCP transport cannot be combined with libp2p listen/peer options".into(),
                ));
            }
        }
        Ok(())
    }

    /// Build the node: transport, optional listener, global router, signal hooks.
    pub fn build(self) -> Result<Node, NodeError> {
        self.validate()?;

        if let Some(name) = self.name.clone() {
            set_local_node_for_test(name.clone());
            if local_node() != name {
                return Err(NodeError::NameAlreadySet);
            }
        }

        let local = local_node();
        if let Ok(handle) = spawned_rt::tasks::Handle::try_current() {
            super::remote_spawn::install_tasks_runtime(handle);
        }
        let dispatch = Arc::new(AddressDispatch::new());

        let cluster_active = {
            #[cfg(feature = "cluster-libp2p")]
            {
                match self.transport {
                    TransportKind::Libp2p => {
                        self.listen_libp2p.is_some() || !self.libp2p_peers.is_empty()
                    }
                    TransportKind::Tcp => self.listen.is_some() || !self.peers.is_empty(),
                }
            }
            #[cfg(not(feature = "cluster-libp2p"))]
            {
                self.listen.is_some() || !self.peers.is_empty()
            }
        };

        let (supervision_broker, supervision_inner) = if cluster_active {
            let (broker, inner) = start_supervision_broker(local.clone());
            (Some(broker), Some(inner))
        } else {
            (None, None)
        };

        #[cfg(feature = "cluster-libp2p")]
        let federated = match self.transport {
            TransportKind::Libp2p => {
                self.listen_libp2p.is_some() || !self.libp2p_peers.is_empty()
            }
            TransportKind::Tcp => self.listen.is_some() || !self.peers.is_empty(),
        };

        #[cfg(not(feature = "cluster-libp2p"))]
        let federated = self.listen.is_some() || !self.peers.is_empty();

        let mut control = if federated {
            ControlPlaneHooks::federated(
                Arc::new(|event| super::named_registry::apply_remote_event(event)),
                Arc::new(super::named_registry::local_snapshot),
                Arc::new(|event| crate::pg::apply_remote_pg_event(event)),
                Arc::new(crate::pg::local_pg_snapshot),
            )
        } else {
            ControlPlaneHooks::none()
        };

        if let Some(inner) = supervision_inner.as_ref() {
            let broker_inner = inner.clone();
            control = control.with_supervision(SupervisionHooks::from_fn(Arc::new(
                move |envelope| broker_inner.apply(envelope),
            )));
        }

        #[cfg(feature = "cluster-libp2p")]
        if self.transport == TransportKind::Libp2p {
            return self.build_libp2p(
                local,
                dispatch,
                control,
                federated,
                supervision_broker,
                supervision_inner,
            );
        }

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
            if let Some(inner) = supervision_inner.as_ref() {
                let local_sup = local.clone();
                let inner_sup = inner.clone();
                let tcp_sup = tcp.clone();
                super::supervision_sync::install_publish(move |envelope| {
                    let _ = super::supervision_routing::publish_routed(
                        &local_sup,
                        envelope,
                        |env| inner_sup.apply(env).map(|_| ()),
                        |node, env| tcp_sup.send_supervision(&node, env),
                    );
                });
                let tcp_req = tcp.clone();
                super::supervision_sync::install_request(move |node, envelope| {
                    tcp_req.request_supervision(node, envelope)
                });
            }
        }

        let transport: Arc<dyn Transport> = if let Some(tcp) = tcp.clone() {
            tcp.clone()
        } else {
            Arc::new(UnavailableTransport)
        };

        let async_transport: Option<Arc<dyn AsyncTransport>> = tcp
            .as_ref()
            .map(|tcp| Arc::new(TcpAsyncTransport(tcp.clone())) as Arc<dyn AsyncTransport>);

        let router = Arc::new(ClusterRouter::with_async(transport, async_transport));
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
            backend: ClusterBackend::Tcp {
                transport: tcp,
                listener,
            },
            dispatch,
            supervision: supervision_inner,
            _supervision_broker: supervision_broker,
            _signal_guards,
        })
    }

    #[cfg(feature = "cluster-libp2p")]
    fn build_libp2p(
        self,
        local: NodeId,
        dispatch: Arc<AddressDispatch>,
        control: ControlPlaneHooks,
        federated: bool,
        supervision_broker: Option<crate::tasks::ActorRef<SupervisionBroker>>,
        supervision_inner: Option<Arc<SupervisionBrokerInner>>,
    ) -> Result<Node, NodeError> {
        let listen = match self.listen_libp2p {
            Some(addr) => addr,
            None if !self.libp2p_peers.is_empty() => {
                let port = Libp2pCluster::ephemeral_tcp_port()?;
                format!("/ip4/127.0.0.1/tcp/{port}")
                    .parse()
                    .map_err(|e| TransportError::Protocol(format!("invalid multiaddr: {e}")))?
            }
            None => {
                return Err(NodeError::InvalidConfig(
                    "libp2p node requires listen_libp2p or at least one libp2p_peer".into(),
                ));
            }
        };

        let keypair = self
            .keypair
            .unwrap_or_else(|| spawned_cluster::identity::Keypair::generate_ed25519());

        let peers: Vec<Libp2pPeer> = self
            .libp2p_peers
            .into_iter()
            .map(|peer| Libp2pPeer {
                node: peer.node,
                peer_id: peer.peer_id,
                addr: peer.addr,
            })
            .collect();

        let cluster = Arc::new(Libp2pCluster::start(
            keypair,
            local.clone(),
            listen,
            peers,
            dispatch.clone(),
            control.clone(),
        )?);

        if federated {
            let cluster_registry = cluster.clone();
            super::registry_sync::install(move |event| {
                let _ = cluster_registry.broadcast_registry(event);
            });
            let cluster_pg = cluster.clone();
            super::pg_sync::install(move |event| {
                let _ = cluster_pg.broadcast_pg(event);
            });
        }

        if let Some(inner) = supervision_inner.as_ref() {
            let local_sup = local.clone();
            let inner_sup = inner.clone();
            let cluster_sup = cluster.clone();
            super::supervision_sync::install_publish(move |envelope| {
                let _ = super::supervision_routing::publish_routed(
                    &local_sup,
                    envelope,
                    |env| inner_sup.apply(env).map(|_| ()),
                    |node, env| cluster_sup.send_supervision_to(&node, env),
                );
            });
            let cluster_req = cluster.clone();
            super::supervision_sync::install_request(move |node, envelope| {
                cluster_req.request_supervision_from(node, envelope)
            });
        }

        let transport: Arc<dyn Transport> = cluster.clone();
        let async_transport: Arc<dyn AsyncTransport> = cluster.clone();
        let router = Arc::new(ClusterRouter::with_async(
            transport,
            Some(async_transport),
        ));
        let _ = ClusterRouter::set_global(router.clone());

        let _signal_guards = if self.shutdown_handles.is_empty() {
            Vec::new()
        } else {
            spawn_shutdown_signal_dispatcher_tasks();
            register_shutdown_on_signal(&self.shutdown_handles)
        };

        Ok(Node {
            local_node: local,
            router,
            backend: ClusterBackend::Libp2p(cluster),
            dispatch,
            supervision: supervision_inner,
            _supervision_broker: supervision_broker,
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
