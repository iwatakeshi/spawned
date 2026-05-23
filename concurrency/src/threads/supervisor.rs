use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::child_handle::ChildHandle;
use crate::child_spec::{ChildType, RestartIntensity, RestartType, ShutdownType};
use crate::link::Exit;
use crate::supervisor::{
    ChildHandleSlot, ChildPolicy, SupervisorAction, SupervisorLogic, SupervisorStrategy,
};

use super::actor::{Actor, ActorRef, ActorStart, Context};

/// Specification for a supervised child in threads mode.
pub struct ChildSpec {
    pub id: String,
    start: Arc<dyn Fn(&Context<Supervisor>) -> ChildHandle + Send + Sync>,
    pub restart: RestartType,
    pub shutdown: ShutdownType,
    pub child_type: ChildType,
}

impl Clone for ChildSpec {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            start: self.start.clone(),
            restart: self.restart,
            shutdown: self.shutdown,
            child_type: self.child_type,
        }
    }
}

impl ChildSpec {
    pub fn worker<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self {
            id: id.into(),
            start: Arc::new(move |ctx| start().start_linked(ctx).child_handle()),
            restart,
            shutdown: ShutdownType::Infinity,
            child_type: ChildType::Worker,
        }
    }

    pub fn supervisor<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self {
            id: id.into(),
            start: Arc::new(move |ctx| start().start_linked(ctx).child_handle()),
            restart,
            shutdown: ShutdownType::Infinity,
            child_type: ChildType::Supervisor,
        }
    }

    pub fn with_shutdown(mut self, shutdown: ShutdownType) -> Self {
        self.shutdown = shutdown;
        self
    }
}

pub struct SupervisorBuilder {
    strategy: SupervisorStrategy,
    intensity: RestartIntensity,
    specs: Vec<ChildSpec>,
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

    pub fn child(mut self, spec: ChildSpec) -> Self {
        self.specs.push(spec);
        self
    }

    pub fn start(self) -> ActorRef<Supervisor> {
        Supervisor {
            logic: SupervisorLogic::new(self.strategy, self.intensity),
            specs: self.specs,
            handles: HashMap::new(),
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
    specs: Vec<ChildSpec>,
    handles: HashMap<String, ChildHandle>,
}

impl Supervisor {
    pub fn builder() -> SupervisorBuilder {
        SupervisorBuilder::new()
    }

    fn start_child(&mut self, ctx: &Context<Self>, spec: &ChildSpec, start_index: usize) {
        let handle = (spec.start)(ctx);
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

    fn restart_child(&mut self, ctx: &Context<Self>, child_id: &str) {
        let Some(spec) = self.specs.iter().find(|spec| spec.id == child_id) else {
            return;
        };
        let handle = (spec.start)(ctx);
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
            if let Some(spec) = self.specs.iter().find(|spec| spec.id == *id) {
                if let Some(handle) = self.handles.get(id) {
                    stop_child(handle, spec.shutdown);
                }
            }
        }
    }

    fn maybe_complete_batch_restart(&mut self, ctx: &Context<Self>) {
        if !self.logic.pending_batch_restart_complete() {
            return;
        }
        if let Some(ids) = self.logic.take_pending_restart_ids() {
            for id in ids {
                self.restart_child(ctx, &id);
            }
        }
    }

    fn handle_exit(&mut self, exit: Exit, ctx: &Context<Self>) {
        if self.logic.suppress_restarts() {
            self.logic.note_exit_during_batch(exit.from);
            self.maybe_complete_batch_restart(ctx);
            return;
        }

        match self
            .logic
            .on_child_exit(exit.from, &exit.reason, Instant::now())
        {
            SupervisorAction::Ignore => {}
            SupervisorAction::RestartOne(id) => self.restart_child(ctx, &id),
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
        let specs: Vec<_> = self.specs.iter().cloned().collect();
        for (index, spec) in specs.iter().enumerate() {
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
                stop_child(handle, shutdown);
                let _ = handle.wait_exit_blocking();
            }
        }
    }
}

fn stop_child(handle: &ChildHandle, shutdown: ShutdownType) {
    match shutdown {
        ShutdownType::Infinity | ShutdownType::Timeout(_) => handle.shutdown(),
        ShutdownType::BrutalKill => handle.kill(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::child_handle::ActorId;
    use crate::error::ExitReason;
    use crate::message::Message;
    use crate::threads::actor::Handler;
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

    struct Idler;
    impl Actor for Idler {}

    struct CountingIdler {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for CountingIdler {
        fn started(&mut self, _ctx: &Context<Self>) {
            self.starts.fetch_add(1, Ordering::SeqCst);
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
            .strategy(SupervisorStrategy::OneForOne)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(5),
            })
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

        let _child_id = sup.request(GetChildId("worker".into())).unwrap();

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn supervisor_shutdown_does_not_restart_permanent_child() {
        let starts = Arc::new(AtomicUsize::new(0));

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
