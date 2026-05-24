//! Per-node supervision broker: inbound wire events and local actor registry.

use crate::child_handle::{ActorId, ChildHandle};
use crate::cluster::remote_spawn;
use crate::link::Exit;
use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    SupervisionEnvelope, SupervisionEvent, SupervisionSignal, TransportError,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Shared broker state (sync inbound hook + actor shell).
#[derive(Debug)]
pub struct SupervisionBrokerInner {
    local: NodeId,
    handles: RwLock<HashMap<ActorId, ChildHandle>>,
    /// Remote parent address for each locally spawned child (Phase 12.4 ChildExit).
    parents: RwLock<HashMap<ActorId, ActorAddress>>,
    broker_handle: RwLock<Option<ChildHandle>>,
}

impl SupervisionBrokerInner {
    pub fn new(local: NodeId) -> Self {
        Self {
            local,
            handles: RwLock::new(HashMap::new()),
            parents: RwLock::new(HashMap::new()),
            broker_handle: RwLock::new(None),
        }
    }

    pub fn local_node(&self) -> &NodeId {
        &self.local
    }

    pub(crate) fn set_broker_handle(&self, handle: ChildHandle) {
        *self.broker_handle.write().unwrap_or_else(|p| p.into_inner()) = Some(handle);
    }

    /// Register a locally-running actor for inbound supervision signals.
    pub fn register(&self, address: ActorAddress, handle: ChildHandle) -> Result<(), TransportError> {
        if address.node != self.local {
            return Err(TransportError::Protocol(format!(
                "supervision register: address {} is not local ({})",
                address.node, self.local
            )));
        }
        if handle.id() != address.actor_id {
            return Err(TransportError::Protocol(format!(
                "supervision register: handle id {} does not match address {}",
                handle.id(),
                address.actor_id
            )));
        }
        self.handles
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(address.actor_id, handle);
        Ok(())
    }

    pub fn unregister(&self, actor_id: ActorId) {
        self.handles
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&actor_id);
        self.parents
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&actor_id);
    }

    /// Handle an inbound supervision envelope on this node.
    pub fn apply(
        &self,
        envelope: SupervisionEnvelope,
    ) -> Result<Option<SupervisionEnvelope>, TransportError> {
        match envelope.event {
            SupervisionEvent::Signal { target, signal } => {
                self.apply_signal(&target, signal)?;
                Ok(None)
            }
            SupervisionEvent::SpawnRequest {
                parent,
                placement,
                spec,
                link,
            } => self.apply_spawn(envelope.correlation_id, parent, placement, spec, link),
            _ => Ok(None),
        }
    }

    fn apply_spawn(
        &self,
        correlation_id: u64,
        parent: ActorAddress,
        placement: NodeId,
        spec: spawned_cluster::RemoteSpawnSpec,
        link: bool,
    ) -> Result<Option<SupervisionEnvelope>, TransportError> {
        if placement != self.local {
            return Ok(Some(SupervisionEnvelope {
                correlation_id,
                event: SupervisionEvent::SpawnErr {
                    error: format!(
                        "placement node {placement} does not match local node {}",
                        self.local
                    ),
                },
            }));
        }

        let broker_parent = if link {
            self.broker_handle
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
        } else {
            None
        };

        if link && broker_parent.is_none() {
            return Ok(Some(SupervisionEnvelope {
                correlation_id,
                event: SupervisionEvent::SpawnErr {
                    error: "supervision broker not ready".into(),
                },
            }));
        }

        match remote_spawn::spawn_local(spec, link, broker_parent.as_ref()) {
            Ok(handle) => {
                let child = ActorAddress::on(self.local.clone(), handle.id());
                self.register(child.clone(), handle.clone())?;
                self.parents
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(child.actor_id, parent);
                Ok(Some(SupervisionEnvelope {
                    correlation_id,
                    event: SupervisionEvent::SpawnOk { child },
                }))
            }
            Err(error) => Ok(Some(SupervisionEnvelope {
                correlation_id,
                event: SupervisionEvent::SpawnErr { error },
            })),
        }
    }

    fn apply_signal(
        &self,
        target: &ActorAddress,
        signal: SupervisionSignal,
    ) -> Result<(), TransportError> {
        if target.node != self.local {
            return Err(TransportError::Protocol(format!(
                "supervision signal target {} is not on this node ({})",
                target.node, self.local
            )));
        }
        let handle = self
            .handles
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&target.actor_id)
            .cloned()
            .ok_or_else(|| {
                TransportError::Protocol(format!(
                    "supervision signal: unknown local actor {}",
                    target.actor_id
                ))
            })?;
        match signal {
            SupervisionSignal::Stop => handle.stop(),
            SupervisionSignal::Shutdown => handle.shutdown(),
            SupervisionSignal::Kill => handle.kill(),
        }
        Ok(())
    }
}

/// Tasks-runtime supervision broker actor (keeps broker alive on the node).
pub struct SupervisionBroker {
    inner: Arc<SupervisionBrokerInner>,
}

impl SupervisionBroker {
    pub fn inner(&self) -> Arc<SupervisionBrokerInner> {
        self.inner.clone()
    }
}

use crate::tasks::{Actor, ActorStart, Context, Handler};

impl Actor for SupervisionBroker {
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
        self.inner.set_broker_handle(ctx.child_handle());
    }

    async fn exit_received(&mut self, exit: Exit, _ctx: &Context<Self>) {
        let _ = exit;
        // ChildExit propagation to remote parent — Phase 12.4.
    }
}

/// Start the tasks supervision broker and return its handle + shared inner state.
pub fn start_supervision_broker(
    local: NodeId,
) -> (
    crate::tasks::ActorRef<SupervisionBroker>,
    Arc<SupervisionBrokerInner>,
) {
    let inner = Arc::new(SupervisionBrokerInner::new(local));
    let actor = SupervisionBroker {
        inner: inner.clone(),
    };
    let actor_ref = actor.start();
    (actor_ref, inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::tasks::{Handler};

    struct Target;

    impl Actor for Target {}

    #[derive(Clone, Copy)]
    struct Ping;

    impl Message for Ping {
        type Result = ();
    }

    impl Handler<Ping> for Target {
        async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) {}
    }

    #[tokio::test]
    async fn local_shutdown_signal_via_broker() {
        let local = NodeId::new("broker@127.0.0.1");
        unsafe {
            std::env::set_var("SPAWNED_NODE_NAME", local.as_str());
        }
        spawned_address::set_local_node_for_test(local.clone());
        let (_broker, inner) = start_supervision_broker(local.clone());
        let actor = Target.start();
        let address = ActorAddress::on(local.clone(), actor.id());
        inner
            .register(address.clone(), actor.child_handle())
            .unwrap();

        inner
            .apply(SupervisionEnvelope {
                correlation_id: 0,
                event: SupervisionEvent::Signal {
                    target: address,
                    signal: SupervisionSignal::Shutdown,
                },
            })
            .unwrap();

        assert_eq!(
            actor.child_handle().wait_exit_async().await,
            crate::ExitReason::Shutdown
        );
    }
}
