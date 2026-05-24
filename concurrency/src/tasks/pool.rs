//! Supervised actor pools for the async runtime (DynamicSupervisor + pg dispatch).

use crate::child_handle::ChildHandle;
use crate::message::Message;
use crate::pool::{PoolDispatcher, PoolError, PoolStrategy};
use crate::tasks::child_spec::ChildSpec;
use crate::tasks::pg;
use crate::tasks::{Actor, ActorRef, DynamicSupervisor, DynamicSupervisorApi, Handler};

/// A supervised worker pool: `DynamicSupervisor` + pg group + routed dispatch.
pub struct ActorPool {
    supervisor: ActorRef<DynamicSupervisor>,
    dispatcher: PoolDispatcher,
}

/// Configures an [`ActorPool`].
#[derive(Debug)]
pub struct ActorPoolBuilder {
    group: String,
    strategy: PoolStrategy,
    max_children: Option<usize>,
    scope: Option<String>,
}

/// Fire-and-forget dispatch to one member selected by `dispatcher`'s strategy.
pub fn dispatch<A, M>(dispatcher: &PoolDispatcher, msg: M) -> Result<(), PoolError>
where
    A: Actor + Handler<M>,
    M: Message,
{
    let members = pg::members_scoped::<A>(&dispatcher.scope, &dispatcher.group);
    if members.is_empty() {
        return Err(PoolError::NoMembers);
    }
    let depths: Vec<_> = members.iter().map(|m| m.mailbox_depth()).collect();
    let idx = dispatcher
        .select_index(&depths)
        .expect("non-empty members implies Some index");
    members[idx].send(msg).map_err(PoolError::from)
}

/// Request/reply dispatch to one member selected by `dispatcher`'s strategy.
pub async fn call_one<A, M>(dispatcher: &PoolDispatcher, msg: M) -> Result<M::Result, PoolError>
where
    A: Actor + Handler<M>,
    M: Message + Clone,
{
    let members = pg::members_scoped::<A>(&dispatcher.scope, &dispatcher.group);
    if members.is_empty() {
        return Err(PoolError::NoMembers);
    }
    let depths: Vec<_> = members.iter().map(|m| m.mailbox_depth()).collect();
    let idx = dispatcher
        .select_index(&depths)
        .expect("non-empty members implies Some index");
    members[idx]
        .request(msg)
        .await
        .map_err(PoolError::from)
}

impl ActorPool {
    pub fn builder(group: impl Into<String>) -> ActorPoolBuilder {
        ActorPoolBuilder::new(group)
    }

    pub fn supervisor(&self) -> &ActorRef<DynamicSupervisor> {
        &self.supervisor
    }

    pub fn child_handle(&self) -> ChildHandle {
        self.supervisor.child_handle()
    }

    pub fn dispatcher(&self) -> &PoolDispatcher {
        &self.dispatcher
    }

    pub fn group(&self) -> &str {
        self.dispatcher.group()
    }

    pub fn dispatch<A, M>(&self, msg: M) -> Result<(), PoolError>
    where
        A: Actor + Handler<M>,
        M: Message,
    {
        dispatch::<A, M>(&self.dispatcher, msg)
    }

    pub async fn call_one<A, M>(&self, msg: M) -> Result<M::Result, PoolError>
    where
        A: Actor + Handler<M>,
        M: Message + Clone,
    {
        call_one::<A, M>(&self.dispatcher, msg).await
    }
}

impl ActorPoolBuilder {
    pub fn new(group: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            strategy: PoolStrategy::RoundRobin,
            max_children: None,
            scope: None,
        }
    }

    pub fn strategy(mut self, strategy: PoolStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn max_children(mut self, max: usize) -> Self {
        self.max_children = Some(max);
        self
    }

    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Start `count` supervised workers and return a pool handle.
    ///
    /// Workers must join the pool's pg group in `started()` (same `group` name passed
    /// to [`Self::new`]). Use [`Self::group`] when building specs if helpful.
    pub async fn start<F>(self, count: usize, spec_for: F) -> ActorPool
    where
        F: Fn(usize) -> ChildSpec,
    {
        let mut builder = DynamicSupervisor::builder();
        if let Some(max) = self.max_children {
            builder = builder.max_children(max);
        }
        let supervisor = builder.start();

        for i in 0..count {
            supervisor
                .start_child(spec_for(i), None)
                .await
                .expect("start pool worker")
                .expect("start pool worker");
        }

        let mut dispatcher = PoolDispatcher::new(&self.group, self.strategy);
        if let Some(scope) = self.scope {
            dispatcher = dispatcher.with_scope(scope);
        }

        ActorPool {
            supervisor,
            dispatcher,
        }
    }

    pub fn group(&self) -> &str {
        &self.group
    }
}
