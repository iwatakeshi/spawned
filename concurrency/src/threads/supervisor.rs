use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::child_handle::{ActorId, ChildHandle};
use crate::child_spec::{
    shutdown_child_blocking, ChildSpec as InnerChildSpec, RestartBackoff, RestartIntensity,
    ShutdownType,
};
use crate::link::Exit;
use crate::supervisor::{
    ChildHandleSlot, ChildPolicy, SupervisorAction, SupervisorLogic, SupervisorStrategy,
};

#[cfg(feature = "cluster")]
use crate::cluster::{
    remote_spawn_spec_from_inner, request_spawn_blocking, shutdown_remote, RemoteChildHandle,
    RemoteSpawnMeta,
};
#[cfg(feature = "cluster")]
use crate::cluster::Placement;

use super::actor::{Actor, ActorRef, ActorStart, Context};

pub struct SupervisorBuilder {
    strategy: SupervisorStrategy,
    intensity: RestartIntensity,
    specs: Vec<InnerChildSpec>,
}

impl SupervisorBuilder {
    pub fn new() -> Self {
        Self {
            strategy: SupervisorStrategy::OneForOne,
            intensity: RestartIntensity::default(),
            specs: Vec::new(),
        }
    }

    pub fn strategy(mut self, strategy: SupervisorStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn intensity(mut self, intensity: RestartIntensity) -> Self {
        self.intensity = intensity;
        self
    }

    pub fn child(mut self, spec: impl Into<InnerChildSpec>) -> Self {
        self.specs.push(spec.into());
        self
    }

    pub fn start(self) -> ActorRef<Supervisor> {
        Supervisor {
            logic: SupervisorLogic::new(self.strategy, self.intensity),
            specs: self.specs,
            handles: HashMap::new(),
            #[cfg(feature = "cluster")]
            remote_handles: HashMap::new(),
            #[cfg(feature = "cluster")]
            remote_spawn_meta: HashMap::new(),
            #[cfg(feature = "cluster")]
            remote_actor_to_id: HashMap::new(),
        }
        .start()
    }
}

impl Default for SupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Supervisor {
    logic: SupervisorLogic,
    specs: Vec<InnerChildSpec>,
    handles: HashMap<String, ChildHandle>,
    #[cfg(feature = "cluster")]
    remote_handles: HashMap<String, RemoteChildHandle>,
    #[cfg(feature = "cluster")]
    remote_spawn_meta: HashMap<String, RemoteSpawnMeta>,
    #[cfg(feature = "cluster")]
    remote_actor_to_id: HashMap<ActorId, String>,
}

impl Supervisor {
    pub fn builder() -> SupervisorBuilder {
        SupervisorBuilder::new()
    }

    fn start_child(&mut self, ctx: &Context<Self>, spec: &InnerChildSpec, start_index: usize) {
        let handle = spec.start_child(&ctx.child_handle());
        self.logic.register_child(
            ChildPolicy {
                id: spec.id.clone(),
                restart: spec.restart,
                start_index,
            },
            ChildHandleSlot {
                id: handle.id(),
                alive: true,
            },
        );
        self.handles.insert(spec.id.clone(), handle);
    }

    #[cfg(feature = "cluster")]
    fn start_child_remote(&mut self, ctx: &Context<Self>, spec: &InnerChildSpec, start_index: usize) {
        let Some(wire_spec) = remote_spawn_spec_from_inner(spec) else {
            tracing::error!(child_id = %spec.id, "remote child spec missing wire descriptor");
            return;
        };
        let Placement::Remote(placement) = spec.placement.clone() else {
            tracing::error!(child_id = %spec.id, "remote child requires Remote placement");
            return;
        };
        let parent = ctx.actor_address();
        let address = match request_spawn_blocking(&placement, parent.clone(), wire_spec.clone(), true) {
            Ok(address) => address,
            Err(err) => {
                tracing::error!(child_id = %spec.id, error = %err, "remote child start failed");
                return;
            }
        };
        let remote = RemoteChildHandle::new(address.clone(), parent);
        self.logic.register_child(
            ChildPolicy {
                id: spec.id.clone(),
                restart: spec.restart,
                start_index,
            },
            ChildHandleSlot {
                id: address.actor_id,
                alive: true,
            },
        );
        self.remote_handles
            .insert(spec.id.clone(), remote);
        self.remote_actor_to_id
            .insert(address.actor_id, spec.id.clone());
        self.remote_spawn_meta.insert(
            spec.id.clone(),
            RemoteSpawnMeta {
                spec: wire_spec,
                placement,
                link: true,
            },
        );
    }

    #[cfg(feature = "cluster")]
    fn restart_remote_child(&mut self, ctx: &Context<Self>, child_id: &str) {
        let Some(meta) = self.remote_spawn_meta.get(child_id).cloned() else {
            return;
        };
        self.remote_handles.remove(child_id);
        self.remote_actor_to_id.retain(|_, id| id != child_id);

        let parent = ctx.actor_address();
        let address = match request_spawn_blocking(
            &meta.placement,
            parent.clone(),
            meta.spec.clone(),
            meta.link,
        ) {
            Ok(address) => address,
            Err(err) => {
                tracing::error!(child_id, error = %err, "remote child restart failed");
                self.logic.remove_child_by_id(child_id);
                self.remote_spawn_meta.remove(child_id);
                return;
            }
        };

        let remote = RemoteChildHandle::new(address.clone(), parent);
        self.logic.replace_child_handle(
            child_id,
            ChildHandleSlot {
                id: address.actor_id,
                alive: true,
            },
        );
        self.remote_handles.insert(child_id.to_string(), remote);
        self.remote_actor_to_id
            .insert(address.actor_id, child_id.to_string());
    }

    fn restart_child(&mut self, ctx: &Context<Self>, child_id: &str) {
        let Some(spec) = self.specs.iter().find(|spec| spec.id == child_id) else {
            return;
        };
        let handle = spec.start_child(&ctx.child_handle());
        self.logic.replace_child_handle(
            child_id,
            ChildHandleSlot {
                id: handle.id(),
                alive: true,
            },
        );
        self.handles.insert(child_id.to_string(), handle);
    }

    fn terminate_children(&mut self, ids: &[String]) {
        for id in ids {
            let shutdown = self
                .specs
                .iter()
                .find(|spec| spec.id == *id)
                .map(|spec| spec.shutdown)
                .unwrap_or(ShutdownType::Infinity);
            #[cfg(feature = "cluster")]
            if let Some(remote) = self.remote_handles.get(id) {
                if self.logic.is_child_alive(id) {
                    let _ = shutdown_remote(remote, shutdown);
                }
                continue;
            }
            if let Some(handle) = self.handles.get(id) {
                shutdown_child_blocking(handle, shutdown);
                self.logic.mark_child_dead(handle.id());
            }
        }
    }

    fn backoff_for(&self, child_id: &str) -> RestartBackoff {
        self.specs
            .iter()
            .find(|spec| spec.id == child_id)
            .map(|spec| spec.backoff)
            .unwrap_or_default()
    }

    fn restart_child_with_backoff(&mut self, ctx: &Context<Self>, child_id: &str) {
        let backoff = self.backoff_for(child_id);
        let delay = self.logic.backoff_delay(child_id, backoff);
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        #[cfg(feature = "cluster")]
        if self.remote_spawn_meta.contains_key(child_id) {
            self.restart_remote_child(ctx, child_id);
            return;
        }
        self.restart_child(ctx, child_id);
    }

    fn maybe_complete_batch_restart(&mut self, ctx: &Context<Self>) {
        if !self.logic.pending_batch_restart_complete() {
            return;
        }
        if let Some(ids) = self.logic.take_pending_restart_ids() {
            #[cfg(feature = "cluster")]
            {
                let (remote_ids, local_ids): (Vec<_>, Vec<_>) = ids.into_iter().partition(|id| {
                    self.remote_spawn_meta.contains_key(id.as_str())
                });
                for id in remote_ids {
                    self.restart_child_with_backoff(ctx, &id);
                }
                for id in local_ids {
                    self.restart_child_with_backoff(ctx, &id);
                }
            }
            #[cfg(not(feature = "cluster"))]
            for id in ids {
                self.restart_child_with_backoff(ctx, &id);
            }
        }
    }

    fn handle_exit(&mut self, exit: Exit, ctx: &Context<Self>) {
        if self.logic.suppress_restarts() {
            self.logic.note_exit_during_batch(exit.from.actor_id);
            self.maybe_complete_batch_restart(ctx);
            return;
        }

        match self
            .logic
            .on_child_exit(exit.from.actor_id, &exit.reason, Instant::now())
        {
            SupervisorAction::Ignore => {}
            SupervisorAction::RestartOne(id) => self.restart_child_with_backoff(ctx, &id),
            SupervisorAction::TerminateBatch(ids) => {
                self.terminate_children(&ids);
                self.maybe_complete_batch_restart(ctx);
            }
            SupervisorAction::Meltdown => {
                tracing::error!(
                    supervisor = ?ctx.id(),
                    "supervisor exceeded restart intensity — shutting down"
                );
                ctx.stop();
            }
        }
    }
}

impl Actor for Supervisor {
    fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
        #[cfg(feature = "cluster")]
        let _ = crate::cluster::register_supervision_actor(
            ctx.actor_address(),
            ctx.child_handle(),
        );
        let specs = self.specs.clone();
        for (index, spec) in specs.iter().enumerate() {
            #[cfg(feature = "cluster")]
            if spec.is_remote() {
                self.start_child_remote(ctx, spec, index);
                continue;
            }
            self.start_child(ctx, spec, index);
        }
    }

    fn exit_received(&mut self, exit: Exit, ctx: &Context<Self>) {
        self.handle_exit(exit, ctx);
    }

    fn stopped(&mut self, _ctx: &Context<Self>) {
        self.logic.set_suppress_restarts(true);
        let order = self.logic.shutdown_order();
        for id in order {
            let shutdown = self
                .specs
                .iter()
                .find(|spec| spec.id == id)
                .map(|spec| spec.shutdown)
                .unwrap_or(ShutdownType::Infinity);
            if let Some(handle) = self.handles.get(&id) {
                shutdown_child_blocking(handle, shutdown);
            }
            #[cfg(feature = "cluster")]
            if let Some(remote) = self.remote_handles.get(&id) {
                let _ = shutdown_remote(remote, shutdown);
            }
        }
        self.handles.clear();
        #[cfg(feature = "cluster")]
        {
            self.remote_handles.clear();
            self.remote_spawn_meta.clear();
            self.remote_actor_to_id.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::child_handle::ActorId;
    use crate::child_spec::RestartType;
    use crate::error::ExitReason;
    use crate::message::Message;
    use crate::threads::actor::Handler;
    use crate::threads::child_spec::ChildSpec;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    struct FlakyWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for FlakyWorker {
        fn started(&mut self, _ctx: &Context<Self>) {
            let n = self.starts.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                panic!("first start dies");
            }
        }
    }

    struct GetChildId(String);
    impl Message for GetChildId {
        type Result = ActorId;
    }

    impl Handler<GetChildId> for Supervisor {
        fn handle(&mut self, msg: GetChildId, _ctx: &Context<Self>) -> ActorId {
            self.handles
                .get(&msg.0)
                .map(ChildHandle::id)
                .expect("child not found")
        }
    }

    #[test]
    fn one_for_one_restarts_panicked_child() {
        let starts = Arc::new(AtomicUsize::new(0));

        let sup = Supervisor::builder()
            .child(ChildSpec::worker(
                "worker",
                {
                    let starts = starts.clone();
                    move || FlakyWorker {
                        starts: starts.clone(),
                    }
                },
                RestartType::Permanent,
            ))
            .start();

        for _ in 0..50 {
            if starts.load(Ordering::SeqCst) >= 2 {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(starts.load(Ordering::SeqCst), 2);
        assert!(sup.exit_reason().is_none());

        let _ = sup.request(GetChildId("worker".into())).unwrap();

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn supervisor_shutdown_does_not_restart_permanent_child() {
        let starts = Arc::new(AtomicUsize::new(0));

        struct CountingIdler {
            starts: Arc<AtomicUsize>,
        }

        impl Actor for CountingIdler {
            fn started(&mut self, _ctx: &Context<Self>) {
                self.starts.fetch_add(1, Ordering::SeqCst);
            }
        }

        let sup = Supervisor::builder()
            .child(ChildSpec::worker(
                "worker",
                {
                    let starts = starts.clone();
                    move || CountingIdler {
                        starts: starts.clone(),
                    }
                },
                RestartType::Permanent,
            ))
            .start();

        thread::sleep(Duration::from_millis(50));
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        let handle = sup.child_handle();
        handle.stop();
        let reason = handle.wait_exit_blocking();
        assert_eq!(reason, ExitReason::Normal);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }
}
