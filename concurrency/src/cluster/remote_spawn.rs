//! Remote spawn registries, client RPC, and [`RemoteChildHandle`].

use crate::child_handle::ChildHandle;
use crate::child_spec::{ChildSpec as InnerChildSpec, PgMembership, RestartType};
use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    RemoteSpawnSpec, RemoteSpecOverrides, SupervisionEnvelope, SupervisionEvent,
    SupervisionSignal, TransportError, WireRestartType,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

static CORRELATION: AtomicU64 = AtomicU64::new(1);
static TASKS_RUNTIME: OnceLock<spawned_rt::tasks::Handle> = OnceLock::new();

/// Install the tasks runtime used to start workers from non-async cluster threads.
pub fn install_tasks_runtime(handle: spawned_rt::tasks::Handle) {
    let _ = TASKS_RUNTIME.set(handle);
}

pub(crate) fn dispatch_tasks_spawn<F>(start: F) -> Result<ChildHandle, String>
where
    F: FnOnce() -> ChildHandle + Send + 'static,
{
    let handle = TASKS_RUNTIME
        .get()
        .ok_or_else(|| "tasks runtime not installed for remote spawn".to_string())?
        .clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let _ = tx.send(start());
    });
    rx.recv()
        .map_err(|e| format!("remote worker spawn failed: {e}"))
}

/// Where a supervised child should run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    Local,
    Remote(NodeId),
}

/// Errors from remote spawn client/registry operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteSpawnError {
    #[error("supervision transport error: {0}")]
    Transport(String),
    #[error("remote spawn failed: {0}")]
    SpawnFailed(String),
    #[error("invalid spawn reply")]
    InvalidReply,
    #[error("supervision publish not installed")]
    PublishNotInstalled,
    #[error("supervision request not installed")]
    RequestNotInstalled,
}

impl From<TransportError> for RemoteSpawnError {
    fn from(err: TransportError) -> Self {
        Self::Transport(err.to_string())
    }
}

type WorkerSpawnFn =
    Arc<dyn Fn(&[u8], Option<&ChildHandle>) -> Result<ChildHandle, String> + Send + Sync>;
type NamedSpecFn = Arc<dyn Fn() -> InnerChildSpec + Send + Sync>;

struct RuntimeRegistry {
    workers: HashMap<String, WorkerSpawnFn>,
    named_specs: HashMap<String, NamedSpecFn>,
}

impl RuntimeRegistry {
    fn new() -> Self {
        Self {
            workers: HashMap::new(),
            named_specs: HashMap::new(),
        }
    }
}

struct RemoteSpawnStore {
    tasks: RuntimeRegistry,
    threads: RuntimeRegistry,
}

fn store() -> &'static RwLock<RemoteSpawnStore> {
    static STORE: OnceLock<RwLock<RemoteSpawnStore>> = OnceLock::new();
    STORE.get_or_init(|| {
        RwLock::new(RemoteSpawnStore {
            tasks: RuntimeRegistry::new(),
            threads: RuntimeRegistry::new(),
        })
    })
}
#[derive(Debug, Clone)]
pub struct RemoteChildHandle {
    address: ActorAddress,
    parent: ActorAddress,
}

impl RemoteChildHandle {
    pub fn new(address: ActorAddress, parent: ActorAddress) -> Self {
        Self { address, parent }
    }

    pub fn address(&self) -> &ActorAddress {
        &self.address
    }

    pub fn parent(&self) -> &ActorAddress {
        &self.parent
    }

    pub fn stop(&self) -> Result<(), RemoteSpawnError> {
        self.signal(SupervisionSignal::Stop)
    }

    pub fn shutdown(&self) -> Result<(), RemoteSpawnError> {
        self.signal(SupervisionSignal::Shutdown)
    }

    pub fn kill(&self) -> Result<(), RemoteSpawnError> {
        self.signal(SupervisionSignal::Kill)
    }

    fn signal(&self, signal: SupervisionSignal) -> Result<(), RemoteSpawnError> {
        super::supervision_sync::publish_supervision(SupervisionEnvelope {
            correlation_id: 0,
            event: SupervisionEvent::Signal {
                target: self.address.clone(),
                signal,
            },
        });
        Ok(())
    }
}

pub(crate) fn next_correlation_id() -> u64 {
    CORRELATION.fetch_add(1, Ordering::Relaxed)
}

/// Issue a correlated spawn RPC to a placement node.
pub fn request_spawn(
    placement: &NodeId,
    parent: ActorAddress,
    spec: RemoteSpawnSpec,
    link: bool,
) -> Result<ActorAddress, RemoteSpawnError> {
    let correlation_id = next_correlation_id();
    let envelope = SupervisionEnvelope {
        correlation_id,
        event: SupervisionEvent::SpawnRequest {
            parent,
            placement: placement.clone(),
            spec,
            link,
        },
    };
    let reply = super::supervision_sync::request_supervision(placement, envelope)?;
    if reply.correlation_id != correlation_id {
        return Err(RemoteSpawnError::InvalidReply);
    }
    match reply.event {
        SupervisionEvent::SpawnOk { child } => Ok(child),
        SupervisionEvent::SpawnErr { error } => Err(RemoteSpawnError::SpawnFailed(error)),
        _ => Err(RemoteSpawnError::InvalidReply),
    }
}

pub(crate) fn register_worker_tasks(
    worker_type: String,
    spawn: WorkerSpawnFn,
) -> Result<(), RemoteSpawnError> {
    let mut guard = store().write().unwrap_or_else(|p| p.into_inner());
    if guard.tasks.workers.contains_key(&worker_type) {
        return Err(RemoteSpawnError::SpawnFailed(format!(
            "worker type already registered: {worker_type}"
        )));
    }
    guard.tasks.workers.insert(worker_type, spawn);
    Ok(())
}

pub(crate) fn register_worker_threads(
    worker_type: String,
    spawn: WorkerSpawnFn,
) -> Result<(), RemoteSpawnError> {
    let mut guard = store().write().unwrap_or_else(|p| p.into_inner());
    if guard.threads.workers.contains_key(&worker_type) {
        return Err(RemoteSpawnError::SpawnFailed(format!(
            "worker type already registered: {worker_type}"
        )));
    }
    guard.threads.workers.insert(worker_type, spawn);
    Ok(())
}

pub(crate) fn register_named_spec_tasks(
    name: String,
    factory: NamedSpecFn,
) -> Result<(), RemoteSpawnError> {
    let mut guard = store().write().unwrap_or_else(|p| p.into_inner());
    if guard.tasks.named_specs.contains_key(&name) {
        return Err(RemoteSpawnError::SpawnFailed(format!(
            "named spec already registered: {name}"
        )));
    }
    guard.tasks.named_specs.insert(name, factory);
    Ok(())
}

pub(crate) fn register_named_spec_threads(
    name: String,
    factory: NamedSpecFn,
) -> Result<(), RemoteSpawnError> {
    let mut guard = store().write().unwrap_or_else(|p| p.into_inner());
    if guard.threads.named_specs.contains_key(&name) {
        return Err(RemoteSpawnError::SpawnFailed(format!(
            "named spec already registered: {name}"
        )));
    }
    guard.threads.named_specs.insert(name, factory);
    Ok(())
}

/// Spawn a child on this node from an inbound wire spec (broker inbound path).
pub(crate) fn spawn_local(
    spec: RemoteSpawnSpec,
    link: bool,
    parent: Option<&ChildHandle>,
) -> Result<ChildHandle, String> {
    match spec {
        RemoteSpawnSpec::Worker { worker_type, init } => {
            spawn_worker(&worker_type, &init, link, parent)
        }
        RemoteSpawnSpec::NamedSpec { name, overrides } => {
            spawn_named_spec(&name, &overrides, link, parent)
        }
    }
}

fn spawn_worker(
    worker_type: &str,
    init: &[u8],
    link: bool,
    parent: Option<&ChildHandle>,
) -> Result<ChildHandle, String> {
    let parent_for_link = if link { parent } else { None };
    let guard = store().read().unwrap_or_else(|p| p.into_inner());
    if let Some(spawn) = guard.tasks.workers.get(worker_type) {
        return spawn(init, parent_for_link);
    }
    if let Some(spawn) = guard.threads.workers.get(worker_type) {
        return spawn(init, parent_for_link);
    }
    Err(format!("unknown worker type: {worker_type}"))
}

fn spawn_named_spec(
    name: &str,
    overrides: &RemoteSpecOverrides,
    link: bool,
    parent: Option<&ChildHandle>,
) -> Result<ChildHandle, String> {
    let Some(parent) = parent else {
        return Err("named remote spec requires link=true (parent handle)".into());
    };
    if !link {
        return Err("named remote spec requires link=true".into());
    }
    let mut inner = resolve_named_spec(name)?;
    apply_overrides(&mut inner, overrides);
    Ok(inner.start_child(parent))
}

fn resolve_named_spec(name: &str) -> Result<InnerChildSpec, String> {
    let guard = store().read().unwrap_or_else(|p| p.into_inner());
    if let Some(factory) = guard.tasks.named_specs.get(name) {
        return Ok(factory());
    }
    if let Some(factory) = guard.threads.named_specs.get(name) {
        return Ok(factory());
    }
    Err(format!("unknown named spec: {name}"))
}

fn apply_overrides(spec: &mut InnerChildSpec, overrides: &RemoteSpecOverrides) {
    if let Some(restart) = overrides.restart {
        spec.restart = wire_restart(restart);
    }
    if let (Some(scope), Some(group)) = (&overrides.pg_scope, &overrides.pg_group) {
        spec.pg_membership = Some(PgMembership {
            scope: scope.clone(),
            group: group.clone(),
        });
    }
}

fn wire_restart(restart: WireRestartType) -> RestartType {
    match restart {
        WireRestartType::Permanent => RestartType::Permanent,
        WireRestartType::Transient => RestartType::Transient,
        WireRestartType::Temporary => RestartType::Temporary,
    }
}
