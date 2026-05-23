use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::child_handle::{ActorId, ChildHandle};
use crate::child_spec::{
    shutdown_child_async, warn_supervisor_timeout, ChildType, RestartIntensity, RestartType,
    ShutdownType, DEFAULT_WORKER_SHUTDOWN,
};
use crate::dynamic_supervisor::{instance_id, DynamicChildInfo, DynamicSupervisorError};
use crate::link::Exit;
use crate::mailbox::MailboxConfig;
use crate::registry;
use crate::response::Response;
use crate::supervisor::{
    ChildHandleSlot, ChildPolicy, SupervisorAction, SupervisorLogic, SupervisorStrategy,
};

use super::actor::{Actor, ActorRef, ActorStart, Context, Handler};

type ChildStartFn =
    Arc<dyn Fn(&Context<DynamicSupervisor>, MailboxConfig) -> ChildHandle + Send + Sync>;

/// Specification for a dynamically started child in tasks mode.
pub struct ChildSpec {
    pub id: String,
    start: ChildStartFn,
    pub restart: RestartType,
    pub shutdown: ShutdownType,
    pub child_type: ChildType,
    pub mailbox: MailboxConfig,
}

impl Clone for ChildSpec {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            start: self.start.clone(),
            restart: self.restart,
            shutdown: self.shutdown,
            child_type: self.child_type,
            mailbox: self.mailbox,
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
            start: Arc::new(move |ctx, mailbox| {
                start()
                    .start_linked_with_mailbox(ctx, mailbox)
                    .child_handle()
            }),
            restart,
            shutdown: DEFAULT_WORKER_SHUTDOWN,
            child_type: ChildType::Worker,
            mailbox: MailboxConfig::unbounded(),
        }
    }

    pub fn supervisor<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self {
            id: id.into(),
            start: Arc::new(move |ctx, mailbox| {
                start()
                    .start_linked_with_mailbox(ctx, mailbox)
                    .child_handle()
            }),
            restart,
            shutdown: ShutdownType::Infinity,
            child_type: ChildType::Supervisor,
            mailbox: MailboxConfig::unbounded(),
        }
    }

    pub fn with_shutdown(mut self, shutdown: ShutdownType) -> Self {
        warn_supervisor_timeout(self.child_type, shutdown);
        self.shutdown = shutdown;
        self
    }

    pub fn with_mailbox(mut self, mailbox: MailboxConfig) -> Self {
        self.mailbox = mailbox;
        self
    }
}

/// Builder for a [`DynamicSupervisor`] actor.
pub struct DynamicSupervisorBuilder {
    intensity: RestartIntensity,
    max_children: Option<usize>,
}

impl DynamicSupervisorBuilder {
    pub fn new() -> Self {
        Self {
            intensity: RestartIntensity::default(),
            max_children: None,
        }
    }

    pub fn intensity(mut self, intensity: RestartIntensity) -> Self {
        self.intensity = intensity;
        self
    }

    pub fn max_children(mut self, max: usize) -> Self {
        self.max_children = Some(max);
        self
    }

    pub fn start(self) -> ActorRef<DynamicSupervisor> {
        DynamicSupervisor {
            logic: SupervisorLogic::new(SupervisorStrategy::OneForOne, self.intensity),
            specs: HashMap::new(),
            handles: HashMap::new(),
            actor_to_id: HashMap::new(),
            reg_names: HashMap::new(),
            id_counter: 0,
            max_children: self.max_children,
            stopping: false,
        }
        .start()
    }
}

impl Default for DynamicSupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Dynamic supervisor for runtime child pools (OneForOne only).
pub struct DynamicSupervisor {
    logic: SupervisorLogic,
    specs: HashMap<String, ChildSpec>,
    handles: HashMap<String, ChildHandle>,
    actor_to_id: HashMap<ActorId, String>,
    reg_names: HashMap<String, String>,
    id_counter: u64,
    max_children: Option<usize>,
    stopping: bool,
}

/// Request to start a child at runtime.
pub struct StartChild {
    pub spec: ChildSpec,
    pub reg_name: Option<String>,
}

impl crate::message::Message for StartChild {
    type Result = Result<ChildHandle, DynamicSupervisorError>;
}

/// Request to terminate a child by actor id.
pub struct TerminateChild {
    pub actor_id: ActorId,
}

impl crate::message::Message for TerminateChild {
    type Result = Result<(), DynamicSupervisorError>;
}

/// Request the number of alive children.
pub struct CountChildren;

impl crate::message::Message for CountChildren {
    type Result = usize;
}

/// Request metadata for all registered children.
pub struct WhichChildren;

impl crate::message::Message for WhichChildren {
    type Result = Vec<DynamicChildInfo>;
}

/// Convenience API on [`ActorRef<DynamicSupervisor>`].
pub trait DynamicSupervisorApi {
    fn start_child(
        &self,
        spec: ChildSpec,
        reg_name: Option<String>,
    ) -> Response<Result<ChildHandle, DynamicSupervisorError>>;

    fn terminate_child(&self, actor_id: ActorId) -> Response<Result<(), DynamicSupervisorError>>;

    fn count_children(&self) -> Response<usize>;

    fn which_children(&self) -> Response<Vec<DynamicChildInfo>>;
}

impl DynamicSupervisorApi for ActorRef<DynamicSupervisor> {
    fn start_child(
        &self,
        spec: ChildSpec,
        reg_name: Option<String>,
    ) -> Response<Result<ChildHandle, DynamicSupervisorError>> {
        Response::from(self.request_raw(StartChild { spec, reg_name }))
    }

    fn terminate_child(&self, actor_id: ActorId) -> Response<Result<(), DynamicSupervisorError>> {
        Response::from(self.request_raw(TerminateChild { actor_id }))
    }

    fn count_children(&self) -> Response<usize> {
        Response::from(self.request_raw(CountChildren))
    }

    fn which_children(&self) -> Response<Vec<DynamicChildInfo>> {
        Response::from(self.request_raw(WhichChildren))
    }
}

impl DynamicSupervisor {
    pub fn builder() -> DynamicSupervisorBuilder {
        DynamicSupervisorBuilder::new()
    }

    fn start_child(
        &mut self,
        ctx: &Context<Self>,
        spec: ChildSpec,
        reg_name: Option<String>,
    ) -> Result<ChildHandle, DynamicSupervisorError> {
        if self.stopping {
            return Err(DynamicSupervisorError::SupervisorStopping);
        }

        if let Some(max) = self.max_children {
            if self.logic.child_count() >= max {
                return Err(DynamicSupervisorError::MaxChildrenExceeded);
            }
        }

        self.id_counter += 1;
        let child_id = instance_id(&spec.id, self.id_counter);
        if self.logic.has_child_id(&child_id) {
            return Err(DynamicSupervisorError::DuplicateChildId(child_id));
        }

        let start_index = self.logic.next_start_index();
        let handle = (spec.start)(ctx, spec.mailbox);
        self.logic.register_child(
            ChildPolicy {
                id: child_id.clone(),
                restart: spec.restart,
                start_index,
            },
            ChildHandleSlot {
                id: handle.id(),
                alive: true,
            },
        );
        self.specs.insert(child_id.clone(), spec);
        self.handles.insert(child_id.clone(), handle.clone());
        self.actor_to_id.insert(handle.id(), child_id.clone());

        if let Some(name) = reg_name {
            if let Err(err) = registry::register(&name, handle.clone()) {
                self.cleanup_child(&child_id);
                return Err(DynamicSupervisorError::Registry(err.to_string()));
            }
            self.reg_names.insert(name, child_id);
        }

        Ok(handle)
    }

    fn restart_child(&mut self, ctx: &Context<Self>, child_id: &str) {
        let Some(spec) = self.specs.get(child_id).cloned() else {
            return;
        };
        let handle = (spec.start)(ctx, spec.mailbox);
        if let Some(old) = self.handles.get(child_id) {
            self.actor_to_id.remove(&old.id());
        }
        self.logic.replace_child_handle(
            child_id,
            ChildHandleSlot {
                id: handle.id(),
                alive: true,
            },
        );
        self.handles.insert(child_id.to_string(), handle.clone());
        self.actor_to_id.insert(handle.id(), child_id.to_string());
    }

    fn cleanup_child(&mut self, child_id: &str) {
        if let Some(name) = self
            .reg_names
            .iter()
            .find_map(|(name, id)| (id == child_id).then_some(name.clone()))
        {
            registry::unregister(&name);
            self.reg_names.remove(&name);
        }
        if let Some(handle) = self.handles.remove(child_id) {
            self.actor_to_id.remove(&handle.id());
        }
        self.specs.remove(child_id);
        self.logic.remove_child_by_id(child_id);
    }

    async fn terminate_child_by_actor(
        &mut self,
        actor_id: ActorId,
    ) -> Result<(), DynamicSupervisorError> {
        let Some(child_id) = self.actor_to_id.get(&actor_id).cloned() else {
            return Err(DynamicSupervisorError::ChildNotFound(actor_id));
        };
        let shutdown = self
            .specs
            .get(&child_id)
            .map(|spec| spec.shutdown)
            .unwrap_or(ShutdownType::Infinity);
        let Some(handle) = self.handles.get(&child_id).cloned() else {
            return Err(DynamicSupervisorError::ChildNotFound(actor_id));
        };

        self.logic.remove_child_by_id(&child_id);
        shutdown_child_async(&handle, shutdown).await;
        self.cleanup_child(&child_id);
        Ok(())
    }

    async fn handle_exit(&mut self, exit: Exit, ctx: &Context<Self>) {
        match self
            .logic
            .on_child_exit(exit.from, &exit.reason, Instant::now())
        {
            SupervisorAction::Ignore => {
                if let Some(child_id) = self
                    .logic
                    .child_id_by_actor_any(exit.from)
                    .map(str::to_string)
                {
                    self.cleanup_child(&child_id);
                }
            }
            SupervisorAction::RestartOne(id) => self.restart_child(ctx, &id),
            SupervisorAction::TerminateBatch(_) | SupervisorAction::Meltdown => {
                tracing::error!(
                    supervisor = ?ctx.id(),
                    "unexpected batch/meltdown action in dynamic supervisor"
                );
                ctx.stop();
            }
        }
    }
}

impl Actor for DynamicSupervisor {
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
    }

    async fn exit_received(&mut self, exit: Exit, ctx: &Context<Self>) {
        self.handle_exit(exit, ctx).await;
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        self.stopping = true;
        self.logic.set_suppress_restarts(true);
        let order = self.logic.shutdown_order();
        for id in order {
            let shutdown = self
                .specs
                .get(&id)
                .map(|spec| spec.shutdown)
                .unwrap_or(ShutdownType::Infinity);
            if let Some(handle) = self.handles.get(&id) {
                shutdown_child_async(handle, shutdown).await;
            }
        }
        self.specs.clear();
        self.handles.clear();
        self.actor_to_id.clear();
        for name in self.reg_names.keys().cloned().collect::<Vec<_>>() {
            registry::unregister(&name);
        }
        self.reg_names.clear();
    }
}

impl Handler<StartChild> for DynamicSupervisor {
    async fn handle(
        &mut self,
        msg: StartChild,
        ctx: &Context<Self>,
    ) -> Result<ChildHandle, DynamicSupervisorError> {
        self.start_child(ctx, msg.spec, msg.reg_name)
    }
}

impl Handler<TerminateChild> for DynamicSupervisor {
    async fn handle(
        &mut self,
        msg: TerminateChild,
        ctx: &Context<Self>,
    ) -> Result<(), DynamicSupervisorError> {
        let _ = ctx;
        self.terminate_child_by_actor(msg.actor_id).await
    }
}

impl Handler<CountChildren> for DynamicSupervisor {
    async fn handle(&mut self, _msg: CountChildren, _ctx: &Context<Self>) -> usize {
        self.logic.child_count()
    }
}

impl Handler<WhichChildren> for DynamicSupervisor {
    async fn handle(&mut self, _msg: WhichChildren, _ctx: &Context<Self>) -> Vec<DynamicChildInfo> {
        self.logic
            .list_children()
            .into_iter()
            .map(|child| {
                let shutdown = self
                    .specs
                    .get(&child.id)
                    .map(|spec| spec.shutdown)
                    .unwrap_or(ShutdownType::Infinity);
                DynamicChildInfo {
                    id: child.id,
                    actor_id: child.actor_id,
                    alive: child.alive,
                    restart: child.restart,
                    shutdown,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct FlakyWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for FlakyWorker {
        async fn started(&mut self, _ctx: &Context<Self>) {
            let n = self.starts.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                panic!("first start dies");
            }
        }
    }

    struct CountingIdler {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for CountingIdler {
        async fn started(&mut self, _ctx: &Context<Self>) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Idler;
    impl Actor for Idler {}

    fn run<F: std::future::Future>(f: F) {
        spawned_rt::tasks::Runtime::new().unwrap().block_on(f);
    }

    #[test]
    fn start_count_which_and_terminate() {
        run(async {
            let sup = DynamicSupervisor::builder().start();

            let handle = sup
                .start_child(
                    ChildSpec::worker("w", || Idler, RestartType::Permanent),
                    None,
                )
                .await
                .unwrap()
                .unwrap();

            assert_eq!(sup.count_children().await.unwrap(), 1);
            let children = sup.which_children().await.unwrap();
            assert_eq!(children.len(), 1);
            assert!(children[0].id.starts_with("w#"));
            assert_eq!(children[0].actor_id, handle.id());

            sup.terminate_child(handle.id()).await.unwrap().unwrap();
            assert_eq!(sup.count_children().await.unwrap(), 0);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn one_for_one_restarts_panicked_child() {
        run(async {
            let starts = Arc::new(AtomicUsize::new(0));
            let sup = DynamicSupervisor::builder()
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(5),
                })
                .start();

            let _handle = sup
                .start_child(
                    ChildSpec::worker(
                        "worker",
                        {
                            let starts = starts.clone();
                            move || FlakyWorker {
                                starts: starts.clone(),
                            }
                        },
                        RestartType::Permanent,
                    ),
                    None,
                )
                .await
                .unwrap()
                .unwrap();

            for _ in 0..50 {
                if starts.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                spawned_rt::tasks::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(starts.load(Ordering::SeqCst), 2);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn max_children_is_enforced() {
        run(async {
            let sup = DynamicSupervisor::builder().max_children(1).start();

            sup.start_child(
                ChildSpec::worker("w", || Idler, RestartType::Permanent),
                None,
            )
            .await
            .unwrap()
            .unwrap();
            let err = sup
                .start_child(
                    ChildSpec::worker("w", || Idler, RestartType::Permanent),
                    None,
                )
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(err, DynamicSupervisorError::MaxChildrenExceeded);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn terminate_does_not_restart_permanent_child() {
        run(async {
            let starts = Arc::new(AtomicUsize::new(0));
            let sup = DynamicSupervisor::builder().start();

            let handle = sup
                .start_child(
                    ChildSpec::worker(
                        "worker",
                        {
                            let starts = starts.clone();
                            move || CountingIdler {
                                starts: starts.clone(),
                            }
                        },
                        RestartType::Permanent,
                    ),
                    None,
                )
                .await
                .unwrap()
                .unwrap();

            spawned_rt::tasks::sleep(Duration::from_millis(50)).await;
            assert_eq!(starts.load(Ordering::SeqCst), 1);

            sup.terminate_child(handle.id()).await.unwrap().unwrap();
            spawned_rt::tasks::sleep(Duration::from_millis(50)).await;
            assert_eq!(starts.load(Ordering::SeqCst), 1);
            assert_eq!(sup.count_children().await.unwrap(), 0);
        });
    }

    #[test]
    fn registry_name_is_registered_and_unregistered() {
        run(async {
            let sup = DynamicSupervisor::builder().start();

            sup.start_child(
                ChildSpec::worker("w", || Idler, RestartType::Permanent),
                Some("dyn-worker".into()),
            )
            .await
            .unwrap()
            .unwrap();

            assert!(registry::whereis::<ChildHandle>("dyn-worker").is_some());

            let children = sup.which_children().await.unwrap();
            sup.terminate_child(children[0].actor_id)
                .await
                .unwrap()
                .unwrap();
            assert!(registry::whereis::<ChildHandle>("dyn-worker").is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }
}
