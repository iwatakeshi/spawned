//! Federated supervision publish hook (installed in Phase 12.2).

use spawned_cluster::SupervisionEnvelope;
use std::sync::{OnceLock, RwLock};

type PublishFn = Box<dyn Fn(SupervisionEnvelope) + Send + Sync>;

static PUBLISH: OnceLock<RwLock<Option<PublishFn>>> = OnceLock::new();

fn publish_slot() -> &'static RwLock<Option<PublishFn>> {
    PUBLISH.get_or_init(|| RwLock::new(None))
}

pub(crate) fn install(publish: impl Fn(SupervisionEnvelope) + Send + Sync + 'static) {
    *publish_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(publish));
}

pub fn install_supervision_sync(publish: impl Fn(SupervisionEnvelope) + Send + Sync + 'static) {
    install(publish);
}

pub(crate) fn publish_supervision(envelope: SupervisionEnvelope) {
    let guard = publish_slot().read().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = guard.as_ref() {
        f(envelope);
    }
}
