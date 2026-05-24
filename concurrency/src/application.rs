//! OTP-style application entry point (Phase 9.2).
//!
//! [`Application::builder()`] composes optional cluster [`Node`] bootstrap, a startup
//! callback that returns root shutdown handles, OS signal registration, and
//! [`Application::run`] to await graceful exit.

use crate::child_handle::ChildHandle;
use crate::shutdown_signal::{register_shutdown_on_signal, spawn_shutdown_signal_dispatcher_tasks};
use crate::shutdown_signal::SignalGuard;

#[cfg(feature = "cluster")]
use crate::cluster::{Node, NodeBuilder, NodeError};

/// Errors starting an [`Application`].
#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[cfg(feature = "cluster")]
    #[error("node error: {0}")]
    Node(#[from] NodeError),
    #[error("{0}")]
    Startup(String),
}

impl From<String> for ApplicationError {
    fn from(value: String) -> Self {
        Self::Startup(value)
    }
}

impl From<&str> for ApplicationError {
    fn from(value: &str) -> Self {
        Self::Startup(value.to_string())
    }
}

/// Context for the application startup callback.
pub struct ApplicationContext<'a> {
    #[cfg(feature = "cluster")]
    pub node: Option<&'a Node>,
    #[cfg(not(feature = "cluster"))]
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

/// Running application: optional cluster node, signal guards, shutdown roots.
pub struct Application {
    #[cfg(feature = "cluster")]
    node: Option<Node>,
    shutdown_handles: Vec<ChildHandle>,
    _signal_guards: Vec<SignalGuard>,
}

/// Configures an [`Application`].
#[derive(Default)]
pub struct ApplicationBuilder {
    #[cfg(feature = "cluster")]
    node_builder: NodeBuilder,
    #[cfg(feature = "cluster")]
    use_node: bool,
}

impl Application {
    pub fn builder() -> ApplicationBuilder {
        ApplicationBuilder::default()
    }

    /// Root handles registered for OS signal shutdown.
    pub fn shutdown_handles(&self) -> &[ChildHandle] {
        &self.shutdown_handles
    }

    /// Cluster node, when configured (requires `cluster` feature).
    #[cfg(feature = "cluster")]
    pub fn node(&self) -> Option<&Node> {
        self.node.as_ref()
    }

    /// Wait until all root handles have exited (tasks runtime).
    pub async fn run(self) {
        for handle in &self.shutdown_handles {
            let _ = handle.wait_exit_async().await;
        }
    }

    /// Wait until all root handles have exited (threads runtime).
    pub fn run_blocking(self) {
        for handle in &self.shutdown_handles {
            let _ = handle.wait_exit_blocking();
        }
    }
}

impl ApplicationBuilder {
    /// Set the node name (`cluster` feature).
    #[cfg(feature = "cluster")]
    pub fn name(mut self, name: impl Into<spawned_address::NodeId>) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.name(name);
        self
    }

    /// Listen for inbound cluster TCP (`cluster` feature).
    #[cfg(feature = "cluster")]
    pub fn listen(mut self, addr: std::net::SocketAddr) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.listen(addr);
        self
    }

    /// Add a remote cluster peer (`cluster` feature).
    #[cfg(feature = "cluster")]
    pub fn peer(mut self, node: impl Into<spawned_address::NodeId>, addr: std::net::SocketAddr) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.peer(node, addr);
        self
    }

    /// Use libp2p transport (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn transport_libp2p(
        mut self,
        keypair: Option<crate::cluster::identity::Keypair>,
    ) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.transport_libp2p(keypair);
        self
    }

    /// Listen for inbound libp2p connections (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn listen_libp2p(mut self, addr: crate::cluster::Multiaddr) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.listen_libp2p(addr);
        self
    }

    /// Add a remote libp2p peer (`cluster-libp2p` feature).
    #[cfg(feature = "cluster-libp2p")]
    pub fn libp2p_peer(
        mut self,
        node: impl Into<spawned_address::NodeId>,
        peer_id: crate::cluster::PeerId,
        addr: crate::cluster::Multiaddr,
    ) -> Self {
        self.use_node = true;
        self.node_builder = self.node_builder.libp2p_peer(node, peer_id, addr);
        self
    }

    /// Build optional cluster node, run startup, register OS signal shutdown.
    pub async fn start<F, Fut>(self, startup: F) -> Result<Application, ApplicationError>
    where
        F: for<'a> FnOnce(&'a ApplicationContext<'_>) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<ChildHandle>, ApplicationError>>,
    {
        #[cfg(feature = "cluster")]
        let node = if self.use_node {
            Some(self.node_builder.build()?)
        } else {
            None
        };

        let ctx = ApplicationContext {
            #[cfg(feature = "cluster")]
            node: node.as_ref(),
            #[cfg(not(feature = "cluster"))]
            _marker: std::marker::PhantomData,
        };

        let handles = startup(&ctx).await?;
        let guards = register_signal_shutdown(&handles);

        Ok(Application {
            #[cfg(feature = "cluster")]
            node,
            shutdown_handles: handles,
            _signal_guards: guards,
        })
    }

    /// Blocking startup for the threads runtime.
    pub fn start_blocking<F>(self, startup: F) -> Result<Application, ApplicationError>
    where
        F: for<'a> FnOnce(&'a ApplicationContext<'_>) -> Result<Vec<ChildHandle>, ApplicationError>,
    {
        #[cfg(feature = "cluster")]
        let node = if self.use_node {
            Some(self.node_builder.build()?)
        } else {
            None
        };

        let ctx = ApplicationContext {
            #[cfg(feature = "cluster")]
            node: node.as_ref(),
            #[cfg(not(feature = "cluster"))]
            _marker: std::marker::PhantomData,
        };

        let handles = startup(&ctx)?;
        let guards = register_signal_shutdown_threads(&handles);

        Ok(Application {
            #[cfg(feature = "cluster")]
            node,
            shutdown_handles: handles,
            _signal_guards: guards,
        })
    }
}

fn register_signal_shutdown(handles: &[ChildHandle]) -> Vec<SignalGuard> {
    if handles.is_empty() {
        return Vec::new();
    }
    spawn_shutdown_signal_dispatcher_tasks();
    register_shutdown_on_signal(handles)
}

fn register_signal_shutdown_threads(handles: &[ChildHandle]) -> Vec<SignalGuard> {
    if handles.is_empty() {
        return Vec::new();
    }
    crate::shutdown_signal::spawn_shutdown_signal_dispatcher_threads();
    register_shutdown_on_signal(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exit_request::{new_requested_exit_reason, new_skip_stopped_flag};
    use crate::link::{new_link_table, new_linked_exit_reason, new_trap_exit_flag};
    use std::sync::{Arc, Condvar, Mutex};

    fn dummy_handle() -> ChildHandle {
        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let no_op_send_exit: crate::link::SendExitFn = Arc::new(|_| Ok(()));
        let no_op_send_signal: crate::child_handle::SendSignalFn = Arc::new(|_| Ok(()));
        ChildHandle::from_threads(
            crate::child_handle::ActorId::next(),
            Arc::new(|| {}),
            completion,
            new_trap_exit_flag(),
            new_link_table(),
            new_linked_exit_reason(),
            no_op_send_exit,
            no_op_send_signal,
            new_requested_exit_reason(),
            new_skip_stopped_flag(),
        )
    }

    #[test]
    fn application_start_without_cluster_node() {
        let runtime = spawned_rt::tasks::Runtime::new().unwrap();
        runtime.block_on(async {
            let app = Application::builder()
                .start(|_ctx| async { Ok(vec![dummy_handle()]) })
                .await
                .unwrap();
            assert_eq!(app.shutdown_handles().len(), 1);
        });
    }
}
