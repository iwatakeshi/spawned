//! Typed process group helpers for the blocking [`super::ActorRef`] runtime.

use crate::child_handle::ActorId;
use crate::message::Message;
use crate::pg::{self, PgCallReport, PgError, PgSendReport, DEFAULT_SCOPE};
use crate::threads::{Actor, ActorRef, Handler};
#[cfg(feature = "cluster")]
use crate::{cluster::RemoteActorRef, RemoteMessage};

/// Join an actor to a process group in the default scope for later typed dispatch.
pub fn join<A: Actor>(group: impl AsRef<str>, actor: &ActorRef<A>) {
    join_scoped(DEFAULT_SCOPE, group, actor);
}

/// Join an actor to a scoped process group for later typed dispatch.
pub fn join_scoped<A: Actor>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    actor: &ActorRef<A>,
) {
    pg::join_scoped(scope.as_ref(), group.as_ref(), actor.child_handle());
    pg::typed_join(scope, group, actor.id(), actor.clone());
}

/// Leave a group once in the default scope (decrement join count).
pub fn leave(group: impl AsRef<str>, id: ActorId) -> Result<(), PgError> {
    pg::leave(group, id)
}

/// Leave a scoped group once (decrement join count).
pub fn leave_scoped(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    id: ActorId,
) -> Result<(), PgError> {
    pg::leave_scoped(scope, group, id)
}

/// Returns all live [`ActorRef`] members of a group in the default scope.
pub fn members<A: Actor>(group: impl AsRef<str>) -> Vec<ActorRef<A>> {
    members_scoped(DEFAULT_SCOPE, group)
}

/// Returns all live [`ActorRef`] members of a scoped group.
pub fn members_scoped<A: Actor>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
) -> Vec<ActorRef<A>> {
    pg::typed_members::<ActorRef<A>>(scope, group)
        .into_iter()
        .filter(|actor| actor.exit_reason().is_none())
        .collect()
}

/// Returns all live members on the local node in the default scope.
///
/// Identical to [`members`] in single-node deployments.
pub fn local_members<A: Actor>(group: impl AsRef<str>) -> Vec<ActorRef<A>> {
    members(group)
}

/// Returns all live members on the local node in a scope.
pub fn local_members_scoped<A: Actor>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
) -> Vec<ActorRef<A>> {
    members_scoped(scope, group)
}

pub fn cast<A: Actor, M: Message + Clone>(
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    cast_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

pub fn cast_scoped<A: Actor, M: Message + Clone>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    let scope = scope.as_ref();
    let group = group.as_ref();
    let mut report = PgSendReport::default();
    for member in members_scoped::<A>(scope, group) {
        match member.send(msg.clone()) {
            Ok(()) => report.delivered += 1,
            Err(err) => report.failed.push((member.id(), err)),
        }
    }
    report
}

#[cfg(feature = "cluster")]
pub fn cast_federated<A: Actor, M: Message + Clone + RemoteMessage>(
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    cast_federated_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

#[cfg(feature = "cluster")]
pub fn cast_federated_scoped<A: Actor, M: Message + Clone + RemoteMessage>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    let scope = scope.as_ref();
    let group = group.as_ref();
    let mut report = cast_scoped::<A, M>(scope, group, msg.clone());
    cast_remote::<M>(scope, group, msg, &mut report);
    report
}

#[cfg(feature = "cluster")]
fn cast_remote<M: Message + Clone + RemoteMessage>(
    scope: &str,
    group: &str,
    msg: M,
    report: &mut PgSendReport,
) {
    for address in pg::member_addresses_scoped(scope, group) {
        if address.is_local() {
            continue;
        }
        let remote = RemoteActorRef::<M>::remote_global(address.clone());
        match remote.send(msg.clone()) {
            Ok(()) => report.delivered += 1,
            Err(err) => report.failed.push((address.actor_id, err)),
        }
    }
}

pub fn call<A: Actor, M: Message + Clone>(
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
{
    call_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

pub fn call_scoped<A: Actor, M: Message + Clone>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
{
    let scope = scope.as_ref();
    let group = group.as_ref();
    let mut report = PgCallReport {
        ok: Vec::new(),
        failed: Vec::new(),
    };
    for member in members_scoped::<A>(scope, group) {
        let id = member.id();
        match member.request(msg.clone()) {
            Ok(result) => report.ok.push((id, result)),
            Err(err) => report.failed.push((id, err)),
        }
    }
    report
}

#[cfg(feature = "cluster")]
pub fn call_federated<A: Actor, M: Message + Clone + RemoteMessage>(
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
    M::Result: for<'de> serde::Deserialize<'de> + Send,
{
    call_federated_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

#[cfg(feature = "cluster")]
pub fn call_federated_scoped<A: Actor, M: Message + Clone + RemoteMessage>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
    M::Result: for<'de> serde::Deserialize<'de> + Send,
{
    let scope = scope.as_ref();
    let group = group.as_ref();
    let mut report = call_scoped::<A, M>(scope, group, msg.clone());
    call_remote::<M>(scope, group, msg, &mut report);
    report
}

#[cfg(feature = "cluster")]
fn call_remote<M: Message + Clone + RemoteMessage>(
    scope: &str,
    group: &str,
    msg: M,
    report: &mut PgCallReport<M::Result>,
) where
    M::Result: for<'de> serde::Deserialize<'de> + Send,
{
    use crate::cluster::RemoteRequest;

    for address in pg::member_addresses_scoped(scope, group) {
        if address.is_local() {
            continue;
        }
        let id = address.actor_id;
        let remote = RemoteActorRef::<M>::remote_global(address);
        match remote.request_raw(msg.clone()) {
            Ok(RemoteRequest::Remote(payload)) => {
                match spawned_wire::decode_reply(&payload) {
                    Ok(result) => report.ok.push((id, result)),
                    Err(_) => report.failed.push((id, crate::error::ActorError::RemoteUnreachable)),
                }
            }
            Ok(RemoteRequest::Local(_)) => {
                report.failed.push((id, crate::error::ActorError::RemoteUnreachable));
            }
            Err(err) => report.failed.push((id, err)),
        }
    }
}
