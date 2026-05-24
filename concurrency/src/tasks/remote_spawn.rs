//! Tasks-runtime remote worker and named-spec registration.

use crate::child_handle::ChildHandle;
use crate::cluster::remote_spawn::{self, RemoteSpawnError};
use crate::mailbox::MailboxConfig;
use crate::tasks::{Actor, ActorStart};
use std::sync::Arc;

fn start_on_runtime<F>(start: F) -> Result<ChildHandle, String>
where
    F: FnOnce() -> ChildHandle + Send + 'static,
{
    match spawned_rt::tasks::Handle::try_current() {
        Ok(handle) => Ok(handle.block_on(async move { start() })),
        Err(_) => remote_spawn::dispatch_tasks_spawn(start),
    }
}

/// Register a tasks-runtime worker type for inbound remote spawn on this node.
pub fn register_remote_worker<A, Init>(
    worker_type: impl Into<String>,
    start: impl Fn(Init) -> A + Send + Sync + 'static,
) -> Result<(), RemoteSpawnError>
where
    A: Actor + ActorStart,
    Init: serde::de::DeserializeOwned + Send + 'static,
{
    let worker_type = worker_type.into();
    let start = Arc::new(start);
    remote_spawn::register_worker_tasks(
        worker_type,
        Arc::new(move |init_bytes, parent| {
            let init: Init = postcard::from_bytes(init_bytes)
                .map_err(|e| format!("decode init: {e}"))?;
            let start = start.clone();
            let parent = parent.cloned();
            start_on_runtime(move || {
                if let Some(parent) = parent {
                    start(init)
                        .start_linked_to_handle(&parent, MailboxConfig::default_worker())
                        .child_handle()
                } else {
                    start(init).start().child_handle()
                }
            })
        }),
    )
}

/// Register a named tasks [`ChildSpec`](super::ChildSpec) template for remote spawn.
pub fn register_remote_spec(
    name: impl Into<String>,
    factory: impl Fn() -> super::ChildSpec + Send + Sync + 'static,
) -> Result<(), RemoteSpawnError> {
    let factory = Arc::new(factory);
    remote_spawn::register_named_spec_tasks(name.into(), Arc::new(move || factory().0))
}
