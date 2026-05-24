//! Per-node supervision broker: inbound wire events and local actor registry.

use crate::child_handle::{ActorId, ChildHandle};
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
}

impl SupervisionBrokerInner {
    pub fn new(local: NodeId) -> Self {
        Self {
            local,
            handles: RwLock::new(HashMap::new()),
        }
    }

    pub fn local_node(&self) -> &NodeId {
        &self.local
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
            SupervisionEvent::SpawnRequest { .. } => Ok(Some(SupervisionEnvelope {
                correlation_id: envelope.correlation_id,
                event: SupervisionEvent::SpawnErr {
                    error: "remote spawn not implemented".into(),
                },
            })),
            _ => Ok(None),
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

use crate::tasks::{Actor, ActorStart};

impl Actor for SupervisionBroker {}

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
    use crate::tasks::{Actor, ActorStart, Context, Handler};

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
