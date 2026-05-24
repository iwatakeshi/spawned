//! Typed process group helpers for the blocking [`super::ActorRef`] runtime.

use crate::child_handle::ActorId;
use crate::message::Message;
use crate::pg::{self, PgCallReport, PgError, PgSendReport, DEFAULT_SCOPE};
use crate::threads::{Actor, ActorRef, Handler};

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

/// Fire-and-forget broadcast to all live members in the default scope.
pub fn cast<A: Actor, M: Message + Clone>(
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    cast_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

/// Fire-and-forget broadcast to all live members in a scope.
pub fn cast_scoped<A: Actor, M: Message + Clone>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgSendReport
where
    A: Handler<M>,
{
    let mut report = PgSendReport::default();
    for member in members_scoped::<A>(scope, group) {
        match member.send(msg.clone()) {
            Ok(()) => report.delivered += 1,
            Err(err) => report.failed.push((member.id(), err)),
        }
    }
    report
}

/// Request/reply broadcast to all live members in the default scope.
pub fn call<A: Actor, M: Message + Clone>(
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
{
    call_scoped::<A, M>(DEFAULT_SCOPE, group, msg)
}

/// Request/reply broadcast to all live members in a scope.
pub fn call_scoped<A: Actor, M: Message + Clone>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    msg: M,
) -> PgCallReport<M::Result>
where
    A: Handler<M>,
{
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
