//! Federated registry publish hook (installed by [`super::node::NodeBuilder`]).

use spawned_cluster::RegistryEvent;
use std::sync::{OnceLock, RwLock};

type PublishFn = Box<dyn Fn(RegistryEvent) + Send + Sync>;

static PUBLISH: OnceLock<RwLock<Option<PublishFn>>> = OnceLock::new();

fn publish_slot() -> &'static RwLock<Option<PublishFn>> {
    PUBLISH.get_or_init(|| RwLock::new(None))
}

pub(crate) fn install(publish: impl Fn(RegistryEvent) + Send + Sync + 'static) {
    *publish_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(publish));
}

/// Install the outbound registry replication hook (typically via [`NodeBuilder`](super::NodeBuilder)).
pub fn install_registry_sync(publish: impl Fn(RegistryEvent) + Send + Sync + 'static) {
    install(publish);
}

pub(crate) fn publish(event: RegistryEvent) {
    let guard = publish_slot().read().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = guard.as_ref() {
        f(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spawned_address::{ActorAddress, ActorId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn publish_calls_installed_hook() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_for_hook = count.clone();
        install(move |event| {
            if matches!(
                event,
                RegistryEvent::Unregister { ref name, .. } if name == "registry_sync_test_only"
            ) {
                count_for_hook.fetch_add(1, Ordering::Relaxed);
            }
        });
        publish(RegistryEvent::Unregister {
            name: "registry_sync_test_only".into(),
            address: ActorAddress::local(ActorId::from_raw(1)),
        });
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
