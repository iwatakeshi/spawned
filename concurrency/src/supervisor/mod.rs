use crate::child_handle::ActorId;
use crate::child_spec::{should_restart, RestartIntensity, RestartType};
use crate::error::ExitReason;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::time::Instant;

/// How a supervisor reacts when a supervised child exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorStrategy {
    /// Restart only the child that exited.
    OneForOne,
    /// Terminate all children, then restart all.
    OneForAll,
    /// Terminate the dead child and all children started after it, then restart those.
    RestForOne,
}

/// Decision returned by [`SupervisorLogic::on_child_exit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorAction {
    /// No restart or termination needed.
    Ignore,
    /// Restart a single child by id.
    RestartOne(String),
    /// Shut down these children before a batch restart (OneForAll / RestForOne).
    TerminateBatch(Vec<String>),
    /// Supervisor exceeded restart intensity — supervisor should exit abnormally.
    Meltdown,
}

/// Policy fields shared by supervised children (mode-agnostic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildPolicy {
    pub id: String,
    pub restart: RestartType,
    pub start_index: usize,
}

/// Runtime state for one supervised child inside a supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedChild {
    pub policy: ChildPolicy,
    pub handle: Option<ChildHandleSlot>,
}

/// Lightweight handle slot used by shared supervisor logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildHandleSlot {
    pub id: ActorId,
    pub alive: bool,
}

/// Summary of a supervised child for listing APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildInfo {
    pub id: String,
    pub actor_id: ActorId,
    pub alive: bool,
    pub restart: RestartType,
    pub start_index: usize,
}

/// Tracks restart intensity within a sliding time window.
#[derive(Debug, Clone)]
pub struct IntensityTracker {
    intensity: RestartIntensity,
    restart_timestamps: VecDeque<Instant>,
}

/// Intensity window exceeded — too many restarts in the configured period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntensityExceeded;

impl IntensityTracker {
    pub fn new(intensity: RestartIntensity) -> Self {
        Self {
            intensity,
            restart_timestamps: VecDeque::new(),
        }
    }

    /// Record a restart attempt. Returns `Err` when intensity is exceeded.
    pub fn record_restart(&mut self, now: Instant) -> Result<(), IntensityExceeded> {
        while let Some(front) = self.restart_timestamps.front() {
            if now.duration_since(*front) > self.intensity.within {
                self.restart_timestamps.pop_front();
            } else {
                break;
            }
        }
        if self.restart_timestamps.len() >= self.intensity.max_restarts as usize {
            return Err(IntensityExceeded);
        }
        self.restart_timestamps.push_back(now);
        Ok(())
    }

    pub fn restart_count(&self) -> usize {
        self.restart_timestamps.len()
    }
}

/// Shared restart/termination policy engine for supervisors.
#[derive(Debug, Clone)]
pub struct SupervisorLogic {
    strategy: SupervisorStrategy,
    intensity: IntensityTracker,
    children: Vec<SupervisedChild>,
    suppress_restarts: bool,
    pending_batch_restart: Option<BatchRestart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BatchRestart {
    All,
    FromIndex(usize),
}

impl SupervisorLogic {
    pub fn new(strategy: SupervisorStrategy, intensity: RestartIntensity) -> Self {
        Self {
            strategy,
            intensity: IntensityTracker::new(intensity),
            children: Vec::new(),
            suppress_restarts: false,
            pending_batch_restart: None,
        }
    }

    pub fn strategy(&self) -> SupervisorStrategy {
        self.strategy
    }

    pub fn suppress_restarts(&self) -> bool {
        self.suppress_restarts
    }

    pub fn set_suppress_restarts(&mut self, value: bool) {
        self.suppress_restarts = value;
    }

    pub fn children(&self) -> &[SupervisedChild] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [SupervisedChild] {
        &mut self.children
    }

    pub fn child_index_by_actor(&self, actor_id: ActorId) -> Option<usize> {
        self.children.iter().position(|child| {
            child
                .handle
                .is_some_and(|slot| slot.id == actor_id && slot.alive)
        })
    }

    pub fn child_id_by_actor(&self, actor_id: ActorId) -> Option<&str> {
        self.child_index_by_actor(actor_id)
            .map(|idx| self.children[idx].policy.id.as_str())
    }

    /// Resolve a child id by actor id, including children already marked dead.
    pub fn child_id_by_actor_any(&self, actor_id: ActorId) -> Option<&str> {
        self.children
            .iter()
            .find(|child| child.handle.is_some_and(|slot| slot.id == actor_id))
            .map(|child| child.policy.id.as_str())
    }

    /// Number of children currently marked alive.
    pub fn child_count(&self) -> usize {
        self.children
            .iter()
            .filter(|child| child.handle.is_some_and(|slot| slot.alive))
            .count()
    }

    /// List all registered children with liveness metadata.
    pub fn list_children(&self) -> Vec<ChildInfo> {
        self.children
            .iter()
            .filter_map(|child| {
                let slot = child.handle?;
                Some(ChildInfo {
                    id: child.policy.id.clone(),
                    actor_id: slot.id,
                    alive: slot.alive,
                    restart: child.policy.restart,
                    start_index: child.policy.start_index,
                })
            })
            .collect()
    }

    pub fn has_child_id(&self, id: &str) -> bool {
        self.children.iter().any(|child| child.policy.id == id)
    }

    /// Next start index for a dynamically added child.
    pub fn next_start_index(&self) -> usize {
        self.children
            .iter()
            .map(|child| child.policy.start_index)
            .max()
            .map(|index| index + 1)
            .unwrap_or(0)
    }

    pub fn remove_child_by_id(&mut self, child_id: &str) -> bool {
        let Some(index) = self
            .children
            .iter()
            .position(|child| child.policy.id == child_id)
        else {
            return false;
        };
        self.children.remove(index);
        true
    }

    pub fn remove_child_by_actor(&mut self, actor_id: ActorId) -> bool {
        let Some(index) = self
            .children
            .iter()
            .position(|child| child.handle.is_some_and(|slot| slot.id == actor_id))
        else {
            return false;
        };
        self.children.remove(index);
        true
    }

    pub fn mark_child_dead(&mut self, actor_id: ActorId) {
        if let Some(idx) = self.child_index_by_actor(actor_id) {
            if let Some(slot) = &mut self.children[idx].handle {
                slot.alive = false;
            }
        }
    }

    pub fn register_child(&mut self, policy: ChildPolicy, handle: ChildHandleSlot) {
        self.children.push(SupervisedChild {
            policy,
            handle: Some(handle),
        });
    }

    pub fn replace_child_handle(&mut self, child_id: &str, handle: ChildHandleSlot) {
        if let Some(child) = self
            .children
            .iter_mut()
            .find(|child| child.policy.id == child_id)
        {
            child.handle = Some(handle);
        }
    }

    /// Decide what to do when a linked child exits.
    pub fn on_child_exit(
        &mut self,
        actor_id: ActorId,
        reason: &ExitReason,
        now: Instant,
    ) -> SupervisorAction {
        if self.suppress_restarts {
            return SupervisorAction::Ignore;
        }

        let Some(child_index) = self.child_index_by_actor(actor_id) else {
            return SupervisorAction::Ignore;
        };

        let child_id = self.children[child_index].policy.id.clone();
        let restart = self.children[child_index].policy.restart;
        let start_index = self.children[child_index].policy.start_index;

        self.mark_child_dead(actor_id);

        if !should_restart(restart, reason) {
            return SupervisorAction::Ignore;
        }

        if self.intensity.record_restart(now).is_err() {
            return SupervisorAction::Meltdown;
        }

        match self.strategy {
            SupervisorStrategy::OneForOne => SupervisorAction::RestartOne(child_id),
            SupervisorStrategy::OneForAll => {
                self.suppress_restarts = true;
                self.pending_batch_restart = Some(BatchRestart::All);
                SupervisorAction::TerminateBatch(all_child_ids(&self.children))
            }
            SupervisorStrategy::RestForOne => {
                self.suppress_restarts = true;
                self.pending_batch_restart = Some(BatchRestart::FromIndex(start_index));
                SupervisorAction::TerminateBatch(rest_for_one_ids(&self.children, start_index))
            }
        }
    }

    /// Record an exit while batch termination is in progress.
    pub fn note_exit_during_batch(&mut self, actor_id: ActorId) {
        self.mark_child_dead(actor_id);
    }

    /// After batch termination completes, return ids to restart (if any).
    pub fn take_pending_restart_ids(&mut self) -> Option<Vec<String>> {
        let pending = self.pending_batch_restart.take()?;
        self.suppress_restarts = false;
        Some(match pending {
            BatchRestart::All => all_child_ids(&self.children),
            BatchRestart::FromIndex(index) => rest_for_one_ids(&self.children, index),
        })
    }

    /// Returns true when all children targeted by a pending batch restart are dead.
    pub fn pending_batch_restart_complete(&self) -> bool {
        let Some(pending) = &self.pending_batch_restart else {
            return false;
        };
        match pending {
            BatchRestart::All => !self
                .children
                .iter()
                .any(|child| child.handle.is_some_and(|slot| slot.alive)),
            BatchRestart::FromIndex(index) => self.children.get(*index..).is_some_and(|slice| {
                !slice
                    .iter()
                    .any(|child| child.handle.is_some_and(|slot| slot.alive))
            }),
        }
    }

    /// Child ids in reverse start order for graceful shutdown.
    pub fn shutdown_order(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .children
            .iter()
            .map(|child| (child.policy.start_index, child.policy.id.clone()))
            .collect();
        ids.sort_by_key(|b| Reverse(b.0));
        ids.into_iter().map(|(_, id)| id).collect()
    }
}

fn all_child_ids(children: &[SupervisedChild]) -> Vec<String> {
    children
        .iter()
        .map(|child| child.policy.id.clone())
        .collect()
}

fn rest_for_one_ids(children: &[SupervisedChild], from_index: usize) -> Vec<String> {
    children
        .iter()
        .filter(|child| child.policy.start_index >= from_index)
        .map(|child| child.policy.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::child_spec::RestartType;
    use std::time::Duration;

    fn slot(_id: u64) -> ChildHandleSlot {
        ChildHandleSlot {
            id: ActorId::next(),
            alive: true,
        }
    }

    fn child(
        id: &str,
        restart: RestartType,
        start_index: usize,
        handle: ChildHandleSlot,
    ) -> SupervisedChild {
        SupervisedChild {
            policy: ChildPolicy {
                id: id.into(),
                restart,
                start_index,
            },
            handle: Some(handle),
        }
    }

    #[test]
    fn one_for_one_restarts_on_abnormal_exit() {
        let mut logic = SupervisorLogic::new(
            SupervisorStrategy::OneForOne,
            RestartIntensity {
                max_restarts: 3,
                within: Duration::from_secs(5),
            },
        );
        let handle = slot(1);
        logic.register_child(
            ChildPolicy {
                id: "w1".into(),
                restart: RestartType::Permanent,
                start_index: 0,
            },
            handle,
        );

        let action = logic.on_child_exit(handle.id, &ExitReason::Panic("x".into()), Instant::now());
        assert_eq!(action, SupervisorAction::RestartOne("w1".into()));
    }

    #[test]
    fn one_for_one_ignores_shutdown_exit() {
        let mut logic =
            SupervisorLogic::new(SupervisorStrategy::OneForOne, RestartIntensity::default());
        let handle = slot(1);
        logic.register_child(
            ChildPolicy {
                id: "w1".into(),
                restart: RestartType::Permanent,
                start_index: 0,
            },
            handle,
        );

        let action = logic.on_child_exit(handle.id, &ExitReason::Shutdown, Instant::now());
        assert_eq!(action, SupervisorAction::Ignore);
    }

    #[test]
    fn intensity_meltdown_after_max_restarts() {
        let mut logic = SupervisorLogic::new(
            SupervisorStrategy::OneForOne,
            RestartIntensity {
                max_restarts: 2,
                within: Duration::from_secs(5),
            },
        );
        let handle = slot(1);
        logic.register_child(
            ChildPolicy {
                id: "w1".into(),
                restart: RestartType::Permanent,
                start_index: 0,
            },
            handle,
        );
        let now = Instant::now();

        assert_eq!(
            logic.on_child_exit(handle.id, &ExitReason::Kill, now),
            SupervisorAction::RestartOne("w1".into())
        );
        logic.replace_child_handle("w1", slot(2));
        assert_eq!(
            logic.on_child_exit(
                logic.children()[0].handle.unwrap().id,
                &ExitReason::Kill,
                now
            ),
            SupervisorAction::RestartOne("w1".into())
        );
        logic.replace_child_handle("w1", slot(3));
        assert_eq!(
            logic.on_child_exit(
                logic.children()[0].handle.unwrap().id,
                &ExitReason::Kill,
                now
            ),
            SupervisorAction::Meltdown
        );
    }

    #[test]
    fn one_for_all_terminates_everyone() {
        let mut logic =
            SupervisorLogic::new(SupervisorStrategy::OneForAll, RestartIntensity::default());
        let h1 = slot(1);
        let h2 = slot(2);
        logic.children = vec![
            child("a", RestartType::Permanent, 0, h1),
            child("b", RestartType::Permanent, 1, h2),
        ];

        let action = logic.on_child_exit(h1.id, &ExitReason::Panic("x".into()), Instant::now());
        assert_eq!(
            action,
            SupervisorAction::TerminateBatch(vec!["a".into(), "b".into()])
        );
        assert!(logic.suppress_restarts());
    }

    #[test]
    fn rest_for_one_terminates_from_dead_index() {
        let mut logic =
            SupervisorLogic::new(SupervisorStrategy::RestForOne, RestartIntensity::default());
        let h0 = slot(0);
        let h1 = slot(1);
        let h2 = slot(2);
        logic.children = vec![
            child("a", RestartType::Permanent, 0, h0),
            child("b", RestartType::Permanent, 1, h1),
            child("c", RestartType::Permanent, 2, h2),
        ];

        let action = logic.on_child_exit(h1.id, &ExitReason::Kill, Instant::now());
        assert_eq!(
            action,
            SupervisorAction::TerminateBatch(vec!["b".into(), "c".into()])
        );
    }

    #[test]
    fn pending_batch_restart_ids_after_all_dead() {
        let mut logic =
            SupervisorLogic::new(SupervisorStrategy::OneForAll, RestartIntensity::default());
        let h1 = slot(1);
        let h2 = slot(2);
        logic.children = vec![
            child("a", RestartType::Permanent, 0, h1),
            child("b", RestartType::Permanent, 1, h2),
        ];
        let _ = logic.on_child_exit(h1.id, &ExitReason::Panic("x".into()), Instant::now());
        logic.mark_child_dead(h2.id);

        assert!(logic.pending_batch_restart_complete());
        let ids = logic.take_pending_restart_ids().unwrap();
        assert_eq!(ids, vec![String::from("a"), String::from("b")]);
        assert!(!logic.suppress_restarts());
    }

    #[test]
    fn child_count_list_remove_and_next_start_index() {
        let mut logic =
            SupervisorLogic::new(SupervisorStrategy::OneForOne, RestartIntensity::default());
        let h0 = slot(1);
        let h1 = slot(2);
        logic.register_child(
            ChildPolicy {
                id: "a".into(),
                restart: RestartType::Permanent,
                start_index: 0,
            },
            h0,
        );
        logic.register_child(
            ChildPolicy {
                id: "b".into(),
                restart: RestartType::Transient,
                start_index: 3,
            },
            h1,
        );

        assert_eq!(logic.child_count(), 2);
        assert!(logic.has_child_id("a"));
        assert_eq!(logic.next_start_index(), 4);

        let listed = logic.list_children();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, "a");
        assert_eq!(listed[0].actor_id, h0.id);

        logic.mark_child_dead(h0.id);
        assert_eq!(logic.child_count(), 1);
        assert_eq!(logic.child_id_by_actor_any(h0.id), Some("a"));

        assert!(logic.remove_child_by_id("a"));
        assert!(!logic.has_child_id("a"));
        assert!(logic.remove_child_by_actor(h1.id));
        assert!(logic.children.is_empty());
    }

    #[test]
    fn shutdown_order_is_reverse_start_index() {
        let logic = SupervisorLogic {
            strategy: SupervisorStrategy::OneForOne,
            intensity: IntensityTracker::new(RestartIntensity::default()),
            children: vec![
                child("first", RestartType::Permanent, 0, slot(1)),
                child("second", RestartType::Permanent, 1, slot(2)),
                child("third", RestartType::Permanent, 2, slot(3)),
            ],
            suppress_restarts: false,
            pending_batch_restart: None,
        };
        assert_eq!(
            logic.shutdown_order(),
            vec![
                String::from("third"),
                String::from("second"),
                String::from("first"),
            ]
        );
    }
}
