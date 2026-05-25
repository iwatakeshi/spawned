//! Structured supervision tracing and pluggable metric hooks (Phase 14).

use crate::child_handle::ActorId;
use crate::error::ExitReason;
use crate::supervisor::SupervisorStrategy;
use spawned_address::NodeId;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Tracing target for supervisor lifecycle events.
pub const TARGET_SUPERVISION: &str = "spawned.supervision";

/// Tracing target for cluster broker / remote supervision events.
pub const TARGET_CLUSTER: &str = "spawned.cluster";

/// Pluggable hook for supervision counters and latency recording.
pub trait SupervisionRecorder: Send + Sync {
    fn inc_restart(&self, supervisor: ActorId, child_id: &str, remote: bool);
    fn inc_meltdown(&self, supervisor: ActorId);
    fn record_remote_spawn(&self, placement: &NodeId, duration: Duration, ok: bool);
    fn inc_remote_spawn_retry(&self, placement: &NodeId);
}

struct NoopRecorder;

impl SupervisionRecorder for NoopRecorder {
    fn inc_restart(&self, _supervisor: ActorId, _child_id: &str, _remote: bool) {}
    fn inc_meltdown(&self, _supervisor: ActorId) {}
    fn record_remote_spawn(&self, _placement: &NodeId, _duration: Duration, _ok: bool) {}
    fn inc_remote_spawn_retry(&self, _placement: &NodeId) {}
}

static RECORDER: OnceLock<std::sync::RwLock<Option<Arc<dyn SupervisionRecorder>>>> = OnceLock::new();

fn recorder_slot() -> &'static std::sync::RwLock<Option<Arc<dyn SupervisionRecorder>>> {
    RECORDER.get_or_init(|| std::sync::RwLock::new(None))
}

/// Install a process-global supervision recorder (returns false if already set).
pub fn install_supervision_recorder(recorder: Arc<dyn SupervisionRecorder>) -> bool {
    recorder_slot()
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .replace(recorder)
        .is_none()
}

/// Returns the installed recorder, or a no-op default.
pub fn supervision_recorder() -> Arc<dyn SupervisionRecorder> {
    recorder_slot()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .unwrap_or_else(|| Arc::new(NoopRecorder))
}

#[cfg(test)]
pub(crate) fn reset_supervision_recorder() {
    *recorder_slot()
        .write()
        .unwrap_or_else(|p| p.into_inner()) = None;
}

pub fn child_exit(supervisor: ActorId, child_id: &str, actor_id: ActorId, reason: &ExitReason) {
    tracing::debug!(
        target: TARGET_SUPERVISION,
        event = "child_exit",
        %supervisor,
        child_id,
        %actor_id,
        %reason,
    );
}

pub fn restart_scheduled(
    supervisor: ActorId,
    child_id: &str,
    backoff: Duration,
    remote: bool,
) {
    tracing::info!(
        target: TARGET_SUPERVISION,
        event = "restart_scheduled",
        %supervisor,
        child_id,
        backoff_ms = backoff.as_millis() as u64,
        remote,
    );
    supervision_recorder().inc_restart(supervisor, child_id, remote);
}

pub fn batch_terminate(
    supervisor: ActorId,
    strategy: SupervisorStrategy,
    child_ids: &[String],
) {
    tracing::info!(
        target: TARGET_SUPERVISION,
        event = "batch_terminate",
        %supervisor,
        ?strategy,
        child_count = child_ids.len(),
        child_ids = ?child_ids,
    );
}

pub fn meltdown(supervisor: ActorId) {
    tracing::warn!(
        target: TARGET_SUPERVISION,
        event = "meltdown",
        %supervisor,
    );
    supervision_recorder().inc_meltdown(supervisor);
}

pub fn remote_spawn(
    placement: &NodeId,
    child_id: &str,
    duration: Duration,
    ok: bool,
) {
    if ok {
        tracing::info!(
            target: TARGET_SUPERVISION,
            event = "remote_spawn",
            %placement,
            child_id,
            duration_ms = duration.as_millis() as u64,
            outcome = "ok",
        );
    } else {
        tracing::warn!(
            target: TARGET_SUPERVISION,
            event = "remote_spawn",
            %placement,
            child_id,
            duration_ms = duration.as_millis() as u64,
            outcome = "err",
        );
    }
    supervision_recorder().record_remote_spawn(placement, duration, ok);
}

pub fn remote_spawn_retry(placement: &NodeId, attempt: u32, max_attempts: u32) {
    tracing::warn!(
        target: TARGET_SUPERVISION,
        event = "remote_spawn_retry",
        %placement,
        attempt,
        max_attempts,
    );
    supervision_recorder().inc_remote_spawn_retry(placement);
}

pub fn remote_shutdown_wait(child_id: &str, shutdown: &str, ok: bool) {
    if ok {
        tracing::debug!(
            target: TARGET_SUPERVISION,
            event = "remote_shutdown_wait",
            child_id,
            shutdown,
            outcome = "ok",
        );
    } else {
        tracing::warn!(
            target: TARGET_SUPERVISION,
            event = "remote_shutdown_wait",
            child_id,
            shutdown,
            outcome = "err",
        );
    }
}

pub fn broker_spawn(correlation_id: u64, parent: &str, placement: &NodeId) {
    tracing::debug!(
        target: TARGET_CLUSTER,
        event = "broker_spawn",
        correlation_id,
        parent,
        %placement,
    );
}

pub fn broker_child_exit(child: &str, parent: &str, reason: &str) {
    tracing::debug!(
        target: TARGET_CLUSTER,
        event = "broker_child_exit",
        child,
        parent,
        reason,
    );
}

pub fn broker_signal(target: &str, signal: &str) {
    tracing::debug!(
        target: TARGET_CLUSTER,
        event = "broker_signal",
        target,
        signal,
    );
}

#[cfg(test)]
pub struct TestRecorder {
    pub restarts: std::sync::atomic::AtomicU32,
    pub meltdowns: std::sync::atomic::AtomicU32,
    pub remote_spawns: std::sync::atomic::AtomicU32,
    pub remote_spawn_retries: std::sync::atomic::AtomicU32,
}

#[cfg(test)]
impl TestRecorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            restarts: std::sync::atomic::AtomicU32::new(0),
            meltdowns: std::sync::atomic::AtomicU32::new(0),
            remote_spawns: std::sync::atomic::AtomicU32::new(0),
            remote_spawn_retries: std::sync::atomic::AtomicU32::new(0),
        })
    }
}

#[cfg(test)]
impl SupervisionRecorder for TestRecorder {
    fn inc_restart(&self, _supervisor: ActorId, _child_id: &str, _remote: bool) {
        self.restarts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn inc_meltdown(&self, _supervisor: ActorId) {
        self.meltdowns.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_remote_spawn(&self, _placement: &NodeId, _duration: Duration, _ok: bool) {
        self.remote_spawns.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn inc_remote_spawn_retry(&self, _placement: &NodeId) {
        self.remote_spawn_retries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recorder_increments_on_restart_scheduled() {
        reset_supervision_recorder();
        let recorder = TestRecorder::new();
        install_supervision_recorder(recorder.clone());
        restart_scheduled(ActorId::from_raw(1), "worker", Duration::from_millis(10), false);
        assert_eq!(recorder.restarts.load(std::sync::atomic::Ordering::SeqCst), 1);
        reset_supervision_recorder();
    }

    #[test]
    fn test_recorder_increments_on_meltdown_direct() {
        let recorder = TestRecorder::new();
        SupervisionRecorder::inc_meltdown(&*recorder, ActorId::from_raw(2));
        assert_eq!(recorder.meltdowns.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
