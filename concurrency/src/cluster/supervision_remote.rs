//! Shared helpers for static and dynamic remote supervisor children.

use crate::child_spec::{ChildSpec as InnerChildSpec, RestartType, ShutdownType};
use crate::cluster::remote_spawn::{self, RemoteSpawnError};
use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{RemoteSpawnSpec, RemoteSpecOverrides, WireRestartType};

/// Metadata stored per remote child for restart.
#[derive(Debug, Clone)]
pub struct RemoteSpawnMeta {
    pub spec: RemoteSpawnSpec,
    pub placement: NodeId,
    pub link: bool,
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

/// Issue a correlated spawn RPC from a blocking / threads context.
pub fn request_spawn_blocking(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
) -> Result<ActorAddress, RemoteSpawnError> {
    remote_spawn::request_spawn(placement, parent, spec, link)
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
