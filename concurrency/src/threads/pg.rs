//! Typed process group helpers for the blocking [`super::ActorRef`] runtime.

use crate::child_handle::ActorId;
use crate::pg::{self, PgError};
use crate::threads::{Actor, ActorRef};

/// Join an actor to a process group for later typed dispatch.
pub fn join<A: Actor>(group: impl AsRef<str>, actor: &ActorRef<A>) {
    pg::join(group.as_ref(), actor.child_handle());
    pg::typed_join(group, actor.id(), actor.clone());
}

/// Leave a group once (decrement join count).
pub fn leave(group: impl AsRef<str>, id: ActorId) -> Result<(), PgError> {
    pg::leave(group, id)
}

/// Returns all live [`ActorRef`] members of a group.
pub fn members<A: Actor>(group: impl AsRef<str>) -> Vec<ActorRef<A>> {
    pg::typed_members::<ActorRef<A>>(group)
        .into_iter()
        .filter(|actor| actor.exit_reason().is_none())
        .collect()
}

/// Returns all live members on the local node.
///
/// Identical to [`members`] in single-node deployments.
pub fn local_members<A: Actor>(group: impl AsRef<str>) -> Vec<ActorRef<A>> {
    members(group)
}
