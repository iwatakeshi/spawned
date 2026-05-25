//! Shared helpers for static and dynamic remote supervisor children.

use crate::child_handle::ActorId;
use crate::child_spec::{ChildSpec as InnerChildSpec, RestartType, ShutdownType};
use crate::cluster::remote_spawn::{self, RemoteSpawnError};
use crate::observability::remote_spawn_retry;
use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{RemoteSpawnSpec, RemoteSpecOverrides, WireRestartType};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Metadata stored per remote child for restart.
#[derive(Debug, Clone)]
pub struct RemoteSpawnMeta {
    pub spec: RemoteSpawnSpec,
    pub placement: NodeId,
    pub link: bool,
}

/// Errors from remote shutdown wait helpers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteShutdownError {
    #[error("remote shutdown signal failed: {0}")]
    Signal(String),
    #[error("remote shutdown timed out")]
    Timeout,
    #[error("remote shutdown wait interrupted")]
    Interrupted,
}

/// Retry policy for transient transport errors during remote spawn.
#[derive(Debug, Clone, Copy)]
pub struct RemoteSpawnRetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RemoteSpawnRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

const KILL_GRACE: Duration = Duration::from_millis(100);

static SHUTDOWN_WAITS: OnceLock<Mutex<HashMap<ActorId, std::sync::mpsc::SyncSender<()>>>> =
    OnceLock::new();

fn shutdown_waits() -> &'static Mutex<HashMap<ActorId, std::sync::mpsc::SyncSender<()>>> {
    SHUTDOWN_WAITS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Complete a pending remote shutdown wait when ChildExit arrives at the home broker.
pub fn complete_remote_shutdown_wait(actor_id: ActorId) {
    if let Some(tx) = shutdown_waits()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&actor_id)
    {
        let _ = tx.send(());
    }
}

fn register_shutdown_wait(actor_id: ActorId) -> std::sync::mpsc::Receiver<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    shutdown_waits()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(actor_id, tx);
    rx
}

fn clear_shutdown_wait(actor_id: ActorId) {
    shutdown_waits()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&actor_id);
}

fn signal_error(err: RemoteSpawnError) -> RemoteShutdownError {
    RemoteShutdownError::Signal(err.to_string())
}

fn wait_rx_blocking(
    rx: &std::sync::mpsc::Receiver<()>,
    timeout: Option<Duration>,
) -> Result<(), RemoteShutdownError> {
    match timeout {
        None => rx
            .recv()
            .map_err(|_| RemoteShutdownError::Interrupted)
            .map(|_| ()),
        Some(duration) => rx.recv_timeout(duration).map_err(|err| match err {
            std::sync::mpsc::RecvTimeoutError::Timeout => RemoteShutdownError::Timeout,
            _ => RemoteShutdownError::Interrupted,
        }),
    }
}

fn run_shutdown_policy_blocking(
    remote: &remote_spawn::RemoteChildHandle,
    shutdown: ShutdownType,
    rx: &std::sync::mpsc::Receiver<()>,
) -> Result<(), RemoteShutdownError> {
    match shutdown {
        ShutdownType::BrutalKill => {
            remote.kill().map_err(signal_error)?;
            wait_rx_blocking(rx, Some(KILL_GRACE))
        }
        ShutdownType::Infinity => {
            remote.shutdown().map_err(signal_error)?;
            wait_rx_blocking(rx, None)
        }
        ShutdownType::Timeout(duration) => {
            remote.shutdown().map_err(signal_error)?;
            if wait_rx_blocking(rx, Some(duration)).is_err() {
                tracing::warn!(
                    child = %remote.address(),
                    ?duration,
                    "remote child shutdown timed out — escalating to kill"
                );
                remote.kill().map_err(signal_error)?;
                wait_rx_blocking(rx, Some(KILL_GRACE))
            } else {
                Ok(())
            }
        }
    }
}

pub fn restart_to_wire(restart: RestartType) -> WireRestartType {
    match restart {
        RestartType::Permanent => WireRestartType::Permanent,
        RestartType::Transient => WireRestartType::Transient,
        RestartType::Temporary => WireRestartType::Temporary,
    }
}

/// Build wire overrides from a child spec's restart and pg membership.
pub fn overrides_from_spec(spec: &InnerChildSpec) -> RemoteSpecOverrides {
    RemoteSpecOverrides {
        restart: Some(restart_to_wire(spec.restart)),
        pg_scope: spec.pg_membership.as_ref().map(|pg| pg.scope.clone()),
        pg_group: spec.pg_membership.as_ref().map(|pg| pg.group.clone()),
    }
}

/// Build a wire spawn spec from an inner child spec (remote children only).
pub fn remote_spawn_spec_from_inner(spec: &InnerChildSpec) -> Option<RemoteSpawnSpec> {
    use crate::child_spec::RemoteChildSpec;
    match spec.remote.as_ref()? {
        RemoteChildSpec::Named { spec_name } => Some(RemoteSpawnSpec::NamedSpec {
            name: spec_name.clone(),
            overrides: overrides_from_spec(spec),
        }),
        RemoteChildSpec::Worker { worker_type, init } => Some(RemoteSpawnSpec::Worker {
            worker_type: worker_type.clone(),
            init: init.clone(),
        }),
    }
}

fn transport_retry_delay(attempt: u32, policy: RemoteSpawnRetryPolicy) -> Duration {
    let exp = attempt.saturating_sub(1).min(16);
    let delay = policy.base_delay.saturating_mul(1 << exp);
    delay.min(policy.max_delay)
}

fn is_transport_error(err: &RemoteSpawnError) -> bool {
    matches!(err, RemoteSpawnError::Transport(_))
}

/// Issue a correlated spawn RPC from a tasks-runtime async context.
pub async fn request_spawn_async(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
) -> Result<ActorAddress, RemoteSpawnError> {
    let placement = placement.clone();
    spawned_rt::tasks::spawn_blocking(move || remote_spawn::request_spawn(&placement, parent, spec, link))
        .await
        .map_err(|_| RemoteSpawnError::SpawnFailed("spawn task failed".into()))?
}

/// Issue a correlated spawn RPC with transport retries (tasks runtime).
#[tracing::instrument(
    skip(placement, parent, spec, policy),
    fields(placement = %placement, link),
    level = "debug"
)]
pub async fn request_spawn_with_retry_async(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
    policy: RemoteSpawnRetryPolicy,
) -> Result<ActorAddress, RemoteSpawnError> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match request_spawn_async(placement, parent.clone(), spec.clone(), link).await {
            Ok(address) => return Ok(address),
            Err(err) if is_transport_error(&err) && attempt < policy.max_attempts => {
                let delay = transport_retry_delay(attempt, policy);
                remote_spawn_retry(placement, attempt, policy.max_attempts);
                spawned_rt::tasks::sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Issue a correlated spawn RPC from a blocking / threads context.
pub fn request_spawn_blocking(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
) -> Result<ActorAddress, RemoteSpawnError> {
    remote_spawn::request_spawn(placement, parent, spec, link)
}

/// Issue a correlated spawn RPC with transport retries (blocking / threads).
pub fn request_spawn_with_retry_blocking(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
    policy: RemoteSpawnRetryPolicy,
) -> Result<ActorAddress, RemoteSpawnError> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match request_spawn_blocking(placement, parent.clone(), spec.clone(), link) {
            Ok(address) => return Ok(address),
            Err(err) if is_transport_error(&err) && attempt < policy.max_attempts => {
                let delay = transport_retry_delay(attempt, policy);
                remote_spawn_retry(placement, attempt, policy.max_attempts);
                std::thread::sleep(delay);
            }
            Err(err) => return Err(err),
        }
    }
}

/// Apply shutdown policy to a remote child (fire-and-forget signal).
pub fn shutdown_remote(
    remote: &remote_spawn::RemoteChildHandle,
    shutdown: ShutdownType,
) -> Result<(), RemoteSpawnError> {
    match shutdown {
        ShutdownType::BrutalKill => remote.kill(),
        ShutdownType::Infinity | ShutdownType::Timeout(_) => remote.shutdown(),
    }
}

/// Signal a remote child and block until ChildExit completes the wait registry.
pub fn shutdown_remote_and_wait_blocking(
    remote: &remote_spawn::RemoteChildHandle,
    shutdown: ShutdownType,
) -> Result<(), RemoteShutdownError> {
    let actor_id = remote.address().actor_id;
    let rx = register_shutdown_wait(actor_id);
    let result = run_shutdown_policy_blocking(remote, shutdown, &rx);
    clear_shutdown_wait(actor_id);
    result
}

/// Signal a remote child and wait until ChildExit completes the wait registry.
#[tracing::instrument(
    skip(remote, shutdown),
    fields(child = %remote.address(), ?shutdown),
    level = "debug"
)]
pub async fn shutdown_remote_and_wait(
    remote: &remote_spawn::RemoteChildHandle,
    shutdown: ShutdownType,
) -> Result<(), RemoteShutdownError> {
    let actor_id = remote.address().actor_id;
    let rx = register_shutdown_wait(actor_id);
    remote_signal(remote, shutdown).map_err(signal_error)?;
    let result = match shutdown {
        ShutdownType::BrutalKill => wait_rx_async(rx, Some(KILL_GRACE)).await,
        ShutdownType::Infinity => wait_rx_async(rx, None).await,
        ShutdownType::Timeout(duration) => {
            match wait_rx_async(rx, Some(duration)).await {
                Ok(()) => Ok(()),
                Err(RemoteShutdownError::Timeout) => {
                    tracing::warn!(
                        child = %remote.address(),
                        ?duration,
                        "remote child shutdown timed out — escalating to kill"
                    );
                    remote.kill().map_err(signal_error)?;
                    clear_shutdown_wait(actor_id);
                    let rx2 = register_shutdown_wait(actor_id);
                    wait_rx_async(rx2, Some(KILL_GRACE)).await
                }
                Err(err) => Err(err),
            }
        }
    };
    clear_shutdown_wait(actor_id);
    result
}

fn remote_signal(
    remote: &remote_spawn::RemoteChildHandle,
    shutdown: ShutdownType,
) -> Result<(), RemoteSpawnError> {
    match shutdown {
        ShutdownType::BrutalKill => remote.kill(),
        ShutdownType::Infinity | ShutdownType::Timeout(_) => remote.shutdown(),
    }
}

async fn wait_rx_async(
    rx: std::sync::mpsc::Receiver<()>,
    timeout: Option<Duration>,
) -> Result<(), RemoteShutdownError> {
    spawned_rt::tasks::spawn_blocking(move || wait_rx_blocking(&rx, timeout))
        .await
        .map_err(|_| RemoteShutdownError::Interrupted)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_remote_shutdown_wait_unblocks_receiver() {
        let actor_id = ActorId::from_raw(42);
        let rx = register_shutdown_wait(actor_id);
        complete_remote_shutdown_wait(actor_id);
        assert!(rx.recv().is_ok());
        clear_shutdown_wait(actor_id);
    }

    #[test]
    fn transport_retry_delay_caps_at_max() {
        let policy = RemoteSpawnRetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(200),
        };
        assert_eq!(
            transport_retry_delay(1, policy),
            Duration::from_millis(50)
        );
        assert_eq!(
            transport_retry_delay(3, policy),
            Duration::from_millis(200)
        );
        assert_eq!(
            transport_retry_delay(10, policy),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn is_transport_error_matches_transport_variant() {
        assert!(is_transport_error(&RemoteSpawnError::Transport(
            "reset".into()
        )));
        assert!(!is_transport_error(&RemoteSpawnError::SpawnFailed(
            "bad spec".into()
        )));
    }
}
