//! Process-group pool dispatch (Phase 9.6).
//!
//! OTP-style worker pools compose [`DynamicSupervisor`](crate::tasks::DynamicSupervisor)
//! + [`pg`](crate::pg) membership + [`PoolDispatcher`] for routed send/request.
//! No separate pool actor — see [CLUSTERING.md](https://github.com/lambdaclass/spawned/blob/main/docs/CLUSTERING.md).

use std::sync::atomic::{AtomicUsize, Ordering};

/// How to pick a single member from a process group for routed dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolStrategy {
    /// Cycle through members in join order.
    #[default]
    RoundRobin,
    /// Pick the member with the lowest mailbox depth.
    LeastLoaded,
}

/// Errors routing a message to one pool member.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// The process group has no live members.
    #[error("pool has no members")]
    NoMembers,
    #[error(transparent)]
    Actor(#[from] crate::error::ActorError),
}

impl PartialEq for PoolError {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (Self::NoMembers, Self::NoMembers))
    }
}

/// Stateful dispatcher for one pg group + scope.
#[derive(Debug)]
pub struct PoolDispatcher {
    pub(crate) scope: String,
    pub(crate) group: String,
    strategy: PoolStrategy,
    next: AtomicUsize,
}

impl PoolDispatcher {
    /// Create a dispatcher for `group` in the default pg scope.
    pub fn new(group: impl Into<String>, strategy: PoolStrategy) -> Self {
        Self {
            scope: crate::pg::DEFAULT_SCOPE.to_string(),
            group: group.into(),
            strategy,
            next: AtomicUsize::new(0),
        }
    }

    /// Use a non-default pg scope (see [`pg::join_scoped`](crate::pg::join_scoped)).
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = scope.into();
        self
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn strategy(&self) -> PoolStrategy {
        self.strategy
    }

    pub(crate) fn select_index(&self, depths: &[usize]) -> Option<usize> {
        select_member_index(depths.len(), self.strategy, depths, &self.next)
    }
}

/// Shared member selection for tasks and threads dispatchers.
pub(crate) fn select_member_index(
    member_count: usize,
    strategy: PoolStrategy,
    depths: &[usize],
    next: &AtomicUsize,
) -> Option<usize> {
    if member_count == 0 {
        return None;
    }
    match strategy {
        PoolStrategy::RoundRobin => {
            let idx = next.fetch_add(1, Ordering::Relaxed) % member_count;
            Some(idx)
        }
        PoolStrategy::LeastLoaded => {
            let mut best = 0usize;
            let mut min = depths.first().copied().unwrap_or(0);
            for (i, &depth) in depths.iter().enumerate().skip(1) {
                if depth < min {
                    min = depth;
                    best = i;
                }
            }
            Some(best)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles() {
        let next = AtomicUsize::new(0);
        assert_eq!(
            select_member_index(3, PoolStrategy::RoundRobin, &[0, 0, 0], &next),
            Some(0)
        );
        assert_eq!(
            select_member_index(3, PoolStrategy::RoundRobin, &[0, 0, 0], &next),
            Some(1)
        );
        assert_eq!(
            select_member_index(3, PoolStrategy::RoundRobin, &[0, 0, 0], &next),
            Some(2)
        );
        assert_eq!(
            select_member_index(3, PoolStrategy::RoundRobin, &[0, 0, 0], &next),
            Some(0)
        );
    }

    #[test]
    fn least_loaded_picks_shallowest() {
        let next = AtomicUsize::new(0);
        assert_eq!(
            select_member_index(4, PoolStrategy::LeastLoaded, &[3, 1, 2, 1], &next),
            Some(1)
        );
    }

    #[test]
    fn empty_pool_returns_none() {
        let next = AtomicUsize::new(0);
        assert_eq!(
            select_member_index(0, PoolStrategy::RoundRobin, &[], &next),
            None
        );
    }
}
