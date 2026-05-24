//! Threads-runtime remote worker and named-spec registration.

use crate::cluster::remote_spawn;
use crate::cluster::RemoteSpawnError;
use crate::mailbox::MailboxConfig;
use crate::threads::{ActorStart, Actor};
use std::sync::Arc;

/// Register a threads-runtime worker type for inbound remote spawn on this node.
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
    remote_spawn::register_worker_threads(
        worker_type,
        Arc::new(move |init_bytes, parent| {
            let init: Init = postcard::from_bytes(init_bytes)
                .map_err(|e| format!("decode init: {e}"))?;
            if let Some(parent) = parent {
                Ok(start(init)
                    .start_linked_to_handle(parent, MailboxConfig::default_worker())
                    .child_handle())
            } else {
                Ok(start(init).start().child_handle())
            }
        }),
    )
}

/// Register a named threads [`ChildSpec`](super::ChildSpec) template for remote spawn.
pub fn register_remote_spec(
    name: impl Into<String>,
    factory: impl Fn() -> super::ChildSpec + Send + Sync + 'static,
) -> Result<(), RemoteSpawnError> {
    let factory = Arc::new(factory);
    remote_spawn::register_named_spec_threads(name.into(), Arc::new(move || factory().0))
}
