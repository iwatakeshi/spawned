//! Supervised actor pools for the threads runtime (DynamicSupervisor + pg dispatch).

use crate::child_handle::ChildHandle;
use crate::message::Message;
use crate::pool::{PoolDispatcher, PoolError, PoolStrategy};
use crate::threads::child_spec::ChildSpec;
use crate::threads::pg;
use crate::threads::{Actor, ActorRef, DynamicSupervisor, DynamicSupervisorApi, Handler};
#[cfg(feature = "cluster")]
use crate::{cluster::RemoteActorRef, RemoteMessage};
use spawned_address::ActorAddress;

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

fn select_local_target<A: Actor>(
    dispatcher: &PoolDispatcher,
) -> Result<ActorRef<A>, PoolError> {
    let members = pg::members_scoped::<A>(&dispatcher.scope, &dispatcher.group);
    if members.is_empty() {
        return Err(PoolError::NoMembers);
    }
    let depths: Vec<usize> = members.iter().map(|m| m.mailbox_depth()).collect();
    let idx = dispatcher
        .select_index(&depths)
        .expect("non-empty members implies Some index");
    Ok(members[idx].clone())
}

#[cfg(feature = "cluster")]
enum FederatedTarget<A: Actor> {
    Local(ActorRef<A>),
    Remote(ActorAddress),
}

#[cfg(feature = "cluster")]
fn select_federated_target<A: Actor>(
    dispatcher: &PoolDispatcher,
) -> Result<FederatedTarget<A>, PoolError> {
    let members = pg::members_scoped::<A>(&dispatcher.scope, &dispatcher.group);
    let mut depths: Vec<usize> = members.iter().map(|m| m.mailbox_depth()).collect();
    let remote_addrs: Vec<ActorAddress> = crate::pg::member_addresses_scoped(
        &dispatcher.scope,
        &dispatcher.group,
    )
    .into_iter()
    .filter(|address| !address.is_local())
    .collect();
    depths.extend(std::iter::repeat_n(0, remote_addrs.len()));

    if depths.is_empty() {
        return Err(PoolError::NoMembers);
    }

    let idx = dispatcher
        .select_index(&depths)
        .expect("non-empty depths implies Some index");

    if idx < members.len() {
        Ok(FederatedTarget::Local(members[idx].clone()))
    } else {
        Ok(FederatedTarget::Remote(
            remote_addrs[idx - members.len()].clone(),
        ))
    }
}

pub fn dispatch<A, M>(dispatcher: &PoolDispatcher, msg: M) -> Result<(), PoolError>
where
    A: Actor + Handler<M>,
    M: Message,
{
    select_local_target::<A>(dispatcher)?
        .send(msg)
        .map_err(PoolError::from)
}

pub fn call_one<A, M>(dispatcher: &PoolDispatcher, msg: M) -> Result<M::Result, PoolError>
where
    A: Actor + Handler<M>,
    M: Message + Clone,
{
    select_local_target::<A>(dispatcher)?
        .request(msg)
        .map_err(PoolError::from)
}

#[cfg(feature = "cluster")]
pub fn dispatch_federated<A, M>(dispatcher: &PoolDispatcher, msg: M) -> Result<(), PoolError>
where
    A: Actor + Handler<M>,
    M: Message + RemoteMessage,
{
    match select_federated_target::<A>(dispatcher)? {
        FederatedTarget::Local(member) => member.send(msg).map_err(PoolError::from),
        FederatedTarget::Remote(address) => {
            RemoteActorRef::<M>::remote_global(address)
                .send(msg)
                .map_err(PoolError::from)
        }
    }
}

#[cfg(feature = "cluster")]
pub fn call_one_federated<A, M>(
    dispatcher: &PoolDispatcher,
    msg: M,
) -> Result<M::Result, PoolError>
where
    A: Actor + Handler<M>,
    M: Message + Clone + RemoteMessage,
    M::Result: for<'de> serde::Deserialize<'de> + Send,
{
    use crate::cluster::RemoteRequest;

    match select_federated_target::<A>(dispatcher)? {
        FederatedTarget::Local(member) => member.request(msg).map_err(PoolError::from),
        FederatedTarget::Remote(address) => match RemoteActorRef::<M>::remote_global(address)
            .request_raw(msg)?
        {
            RemoteRequest::Remote(payload) => spawned_wire::decode_reply(&payload)
                .map_err(|_| PoolError::from(crate::error::ActorError::RemoteUnreachable)),
            RemoteRequest::Local(_) => {
                Err(PoolError::from(crate::error::ActorError::RemoteUnreachable))
            }
        },
    }
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

    pub fn call_one<A, M>(&self, msg: M) -> Result<M::Result, PoolError>
    where
        A: Actor + Handler<M>,
        M: Message + Clone,
    {
        call_one::<A, M>(&self.dispatcher, msg)
    }

    #[cfg(feature = "cluster")]
    pub fn dispatch_federated<A, M>(&self, msg: M) -> Result<(), PoolError>
    where
        A: Actor + Handler<M>,
        M: Message + RemoteMessage,
    {
        dispatch_federated::<A, M>(&self.dispatcher, msg)
    }

    #[cfg(feature = "cluster")]
    pub fn call_one_federated<A, M>(&self, msg: M) -> Result<M::Result, PoolError>
    where
        A: Actor + Handler<M>,
        M: Message + Clone + RemoteMessage,
        M::Result: for<'de> serde::Deserialize<'de> + Send,
    {
        call_one_federated::<A, M>(&self.dispatcher, msg)
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

    /// Workers are auto-joined to the pool's pg group unless the spec already calls
    /// [`with_pg_group`](crate::threads::ChildSpec::with_pg_group).
    pub fn start<F>(self, count: usize, spec_for: F) -> ActorPool
    where
        F: Fn(usize) -> ChildSpec,
    {
        let mut builder = DynamicSupervisor::builder();
        if let Some(max) = self.max_children {
            builder = builder.max_children(max);
        }
        let supervisor = builder.start();

        for i in 0..count {
            let spec = {
                let s = spec_for(i);
                if s.has_pg_membership() {
                    s
                } else {
                    s.with_pg_group(&self.group)
                }
            };
            supervisor
                .start_child(spec, None)
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
