//! Per-node supervision broker: inbound wire events and local actor registry.

use crate::child_handle::{ActorId, ChildHandle};
use crate::cluster::remote_spawn;
use crate::cluster::supervision_exit::{child_exit_envelope, wire_to_exit_reason};
use crate::cluster::supervision_remote::complete_remote_shutdown_wait;
use crate::cluster::supervision_monitor::{self, SendDownFn};
use crate::error::ExitReason;
use crate::link::Exit;
use crate::monitor::{Down, MonitorRef};
use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    SupervisionEnvelope, SupervisionEvent, SupervisionSignal, TransportError, WireExitReason,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

static BROKER: OnceLock<RwLock<Option<Arc<SupervisionBrokerInner>>>> = OnceLock::new();

fn broker_slot() -> &'static RwLock<Option<Arc<SupervisionBrokerInner>>> {
    BROKER.get_or_init(|| RwLock::new(None))
}

/// Install the local supervision broker for actor self-registration.
pub fn install_supervision_broker(inner: Arc<SupervisionBrokerInner>) {
    *broker_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(inner);
}

/// Register a local actor that may receive inbound supervision signals or ChildExit delivery.
pub fn register_supervision_actor(
    address: ActorAddress,
    handle: ChildHandle,
) -> Result<(), TransportError> {
    let guard = broker_slot().read().unwrap_or_else(|p| p.into_inner());
    let inner = guard.as_ref().ok_or_else(|| {
        TransportError::Protocol("supervision broker not installed".into())
    })?;
    inner.register(address, handle)
}

/// Register a local actor to receive inbound remote [`Down`] messages.
pub fn register_down_owner(
    address: ActorAddress,
    send_down: SendDownFn,
) -> Result<(), TransportError> {
    let guard = broker_slot().read().unwrap_or_else(|p| p.into_inner());
    let inner = guard.as_ref().ok_or_else(|| {
        TransportError::Protocol("supervision broker not installed".into())
    })?;
    inner.register_down_owner(address, send_down)
}

/// Lookup a locally registered supervision handle (if any).
pub fn local_handle(actor_id: ActorId) -> Option<ChildHandle> {
    let guard = broker_slot().read().unwrap_or_else(|p| p.into_inner());
    let inner = guard.as_ref()?;
    inner.local_handle(actor_id)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MonitorKey {
    owner: ActorAddress,
    monitor_ref: u64,
}

struct InstalledMonitor {
    active: Arc<AtomicBool>,
}

/// Shared broker state (sync inbound hook + actor shell).
pub struct SupervisionBrokerInner {
    local: NodeId,
    handles: RwLock<HashMap<ActorId, ChildHandle>>,
    /// Remote parent address for each locally spawned child (Phase 12.4 ChildExit).
    parents: RwLock<HashMap<ActorId, ActorAddress>>,
    /// Remote monitors installed on this node (Phase 12.5).
    monitors: Arc<RwLock<HashMap<MonitorKey, InstalledMonitor>>>,
    down_owners: RwLock<HashMap<ActorId, SendDownFn>>,
    /// Exit reasons for actors removed from `handles` (late remote monitors).
    exited: RwLock<HashMap<ActorId, ExitReason>>,
    /// Remote link peers for each local actor (Phase 12.6).
    link_peers: Arc<RwLock<HashMap<ActorId, Vec<ActorAddress>>>>,
    link_waits: Arc<RwLock<HashSet<ActorId>>>,
    broker_handle: RwLock<Option<ChildHandle>>,
}

impl SupervisionBrokerInner {
    pub fn new(local: NodeId) -> Self {
        Self {
            local,
            handles: RwLock::new(HashMap::new()),
            parents: RwLock::new(HashMap::new()),
            monitors: Arc::new(RwLock::new(HashMap::new())),
            down_owners: RwLock::new(HashMap::new()),
            exited: RwLock::new(HashMap::new()),
            link_peers: Arc::new(RwLock::new(HashMap::new())),
            link_waits: Arc::new(RwLock::new(HashSet::new())),
            broker_handle: RwLock::new(None),
        }
    }

    pub fn local_handle(&self, actor_id: ActorId) -> Option<ChildHandle> {
        self.handles
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&actor_id)
            .cloned()
    }

    pub fn register_down_owner(
        &self,
        address: ActorAddress,
        send_down: SendDownFn,
    ) -> Result<(), TransportError> {
        if address.node != self.local {
            return Err(TransportError::Protocol(format!(
                "down owner {} is not local ({})",
                address.node, self.local
            )));
        }
        self.down_owners
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(address.actor_id, send_down);
        Ok(())
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
            SupervisionEvent::ChildExit {
                child,
                parent,
                reason,
            } => {
                self.apply_child_exit(child, parent, reason)?;
                Ok(None)
            }
            SupervisionEvent::Monitor {
                owner,
                target,
                monitor_ref,
            } => {
                self.apply_monitor(owner, target, monitor_ref)?;
                Ok(None)
            }
            SupervisionEvent::Demonitor {
                owner,
                target,
                monitor_ref,
            } => {
                self.apply_demonitor(owner, target, monitor_ref);
                Ok(None)
            }
            SupervisionEvent::Down {
                owner,
                monitor_ref,
                child: _,
                reason,
            } => {
                self.apply_down(owner, monitor_ref, reason)?;
                Ok(None)
            }
            SupervisionEvent::Link { a, b } => {
                self.apply_link(a, b)?;
                Ok(None)
            }
            SupervisionEvent::Unlink { a, b } => {
                self.apply_unlink(a, b);
                Ok(None)
            }
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

    fn apply_child_exit(
        &self,
        child: ActorAddress,
        parent: ActorAddress,
        reason: WireExitReason,
    ) -> Result<(), TransportError> {
        if parent.node != self.local {
            return Err(TransportError::Protocol(format!(
                "ChildExit parent {} is not on this node ({})",
                parent.node, self.local
            )));
        }
        let handle = self
            .handles
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&parent.actor_id)
            .cloned()
            .ok_or_else(|| {
                TransportError::Protocol(format!(
                    "ChildExit: unknown local parent actor {}",
                    parent.actor_id
                ))
            })?;
        complete_remote_shutdown_wait(child.actor_id);
        let exit = Exit {
            from: child,
            reason: wire_to_exit_reason(reason),
        };
        (handle.send_exit_fn())(exit).map_err(|e| TransportError::Protocol(e.to_string()))?;
        Ok(())
    }

    fn apply_monitor(
        &self,
        owner: ActorAddress,
        target: ActorAddress,
        monitor_ref: u64,
    ) -> Result<(), TransportError> {
        if target.node != self.local {
            return Err(TransportError::Protocol(format!(
                "Monitor target {} is not on this node ({})",
                target.node, self.local
            )));
        }
        let handle = match self
            .handles
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&target.actor_id)
            .cloned()
        {
            Some(handle) => handle,
            None => {
                if let Some(reason) = self
                    .exited
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&target.actor_id)
                    .cloned()
                {
                    supervision_monitor::publish_down(
                        owner,
                        MonitorRef::from_raw(monitor_ref),
                        target,
                        &reason,
                    );
                    return Ok(());
                }
                return Err(TransportError::Protocol(format!(
                    "Monitor: unknown local target actor {}",
                    target.actor_id
                )));
            }
        };

        let key = MonitorKey {
            owner: owner.clone(),
            monitor_ref,
        };
        let active = Arc::new(AtomicBool::new(true));
        self.monitors
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone(), InstalledMonitor { active: active.clone() });

        if let Some(reason) = handle.exit_reason() {
            self.monitors
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
            supervision_monitor::publish_down(
                owner,
                MonitorRef::from_raw(monitor_ref),
                target,
                &reason,
            );
            return Ok(());
        }

        let monitors = self.monitors.clone();
        remote_spawn::spawn_async_on_runtime(async move {
            let reason = handle.wait_exit_async().await;
            let should_notify = monitors
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key)
                .map(|entry| entry.active.load(Ordering::Acquire))
                .unwrap_or(false);
            if should_notify {
                supervision_monitor::publish_down(
                    owner,
                    MonitorRef::from_raw(monitor_ref),
                    target,
                    &reason,
                );
            }
        });
        Ok(())
    }

    fn apply_demonitor(&self, owner: ActorAddress, target: ActorAddress, monitor_ref: u64) {
        let _ = target;
        let key = MonitorKey {
            owner,
            monitor_ref,
        };
        if let Some(entry) = self
            .monitors
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key)
        {
            entry.active.store(false, Ordering::Release);
        }
    }

    fn apply_down(
        &self,
        owner: ActorAddress,
        monitor_ref: u64,
        reason: WireExitReason,
    ) -> Result<(), TransportError> {
        if owner.node != self.local {
            return Err(TransportError::Protocol(format!(
                "Down owner {} is not on this node ({})",
                owner.node, self.local
            )));
        }
        let send_down = self
            .down_owners
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&owner.actor_id)
            .cloned()
            .ok_or_else(|| {
                TransportError::Protocol(format!(
                    "Down: unknown local owner actor {}",
                    owner.actor_id
                ))
            })?;
        let down = Down {
            monitor_ref: MonitorRef::from_raw(monitor_ref),
            reason: wire_to_exit_reason(reason),
        };
        send_down(down).map_err(|e| TransportError::Protocol(e.to_string()))
    }

    fn apply_link(&self, a: ActorAddress, b: ActorAddress) -> Result<(), TransportError> {
        let (local, remote) = if a.node == self.local {
            (a, b)
        } else if b.node == self.local {
            (b, a)
        } else {
            return Err(TransportError::Protocol(format!(
                "Link: neither {} nor {} is on this node ({})",
                a.node, b.node, self.local
            )));
        };

        {
            let mut peers = self
                .link_peers
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let entry = peers.entry(local.actor_id).or_default();
            if !entry.iter().any(|peer| peer == &remote) {
                entry.push(remote.clone());
            }
        }

        if let Some(reason) = self
            .exited
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&local.actor_id)
            .cloned()
        {
            super::supervision_sync::publish_supervision(child_exit_envelope(
                local.clone(),
                remote,
                &reason,
            ));
            return Ok(());
        }

        let handle = self
            .handles
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&local.actor_id)
            .cloned()
            .ok_or_else(|| {
                TransportError::Protocol(format!(
                    "Link: unknown local actor {}",
                    local.actor_id
                ))
            })?;

        if let Some(reason) = handle.exit_reason() {
            super::supervision_sync::publish_supervision(child_exit_envelope(
                local,
                remote,
                &reason,
            ));
            return Ok(());
        }

        {
            let mut waits = self
                .link_waits
                .write()
                .unwrap_or_else(|p| p.into_inner());
            if !waits.insert(local.actor_id) {
                return Ok(());
            }
        }

        let link_peers = self.link_peers.clone();
        let link_waits = self.link_waits.clone();
        let local_id = local.actor_id;
        let local_addr = local;

        remote_spawn::spawn_async_on_runtime(async move {
            let reason = handle.wait_exit_async().await;
            link_waits
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&local_id);
            let peers = link_peers
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&local_id)
                .unwrap_or_default();
            for peer in peers {
                super::supervision_sync::publish_supervision(child_exit_envelope(
                    local_addr.clone(),
                    peer,
                    &reason,
                ));
            }
        });
        Ok(())
    }

    fn apply_unlink(&self, a: ActorAddress, b: ActorAddress) {
        let (local, remote) = if a.node == self.local {
            (a, b)
        } else if b.node == self.local {
            (b, a)
        } else {
            return;
        };
        if let Some(peers) = self
            .link_peers
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&local.actor_id)
        {
            peers.retain(|peer| peer != &remote);
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

    /// Propagate a linked remote-spawned child exit to its home supervisor node.
    pub(crate) fn propagate_child_exit(&self, exit: Exit) {
        let actor_id = exit.from.actor_id;
        let child = ActorAddress::on(self.local.clone(), actor_id);
        let Some(parent) = self
            .parents
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&actor_id)
            .cloned()
        else {
            return;
        };

        self.unregister(actor_id);
        self.exited
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(actor_id, exit.reason.clone());
        super::supervision_sync::publish_supervision(child_exit_envelope(
            child,
            parent,
            &exit.reason,
        ));
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

use crate::tasks::{Actor, ActorStart, Context};

impl Actor for SupervisionBroker {
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
        self.inner.set_broker_handle(ctx.child_handle());
    }

    async fn exit_received(&mut self, exit: Exit, _ctx: &Context<Self>) {
        self.inner.propagate_child_exit(exit);
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
    install_supervision_broker(inner.clone());
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
    use crate::tasks::Handler;

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
