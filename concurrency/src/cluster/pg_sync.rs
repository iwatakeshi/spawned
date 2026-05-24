//! Federated pg publish hook (installed by [`super::node::NodeBuilder`]).

use spawned_cluster::PgEvent;
use std::sync::{OnceLock, RwLock};

type PublishFn = Box<dyn Fn(PgEvent) + Send + Sync>;

static PUBLISH: OnceLock<RwLock<Option<PublishFn>>> = OnceLock::new();

fn publish_slot() -> &'static RwLock<Option<PublishFn>> {
    PUBLISH.get_or_init(|| RwLock::new(None))
}

pub(crate) fn install(publish: impl Fn(PgEvent) + Send + Sync + 'static) {
    *publish_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(publish));
}

pub fn install_pg_sync(publish: impl Fn(PgEvent) + Send + Sync + 'static) {
    install(publish);
}

pub(crate) fn publish(event: PgEvent) {
    let guard = publish_slot().read().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = guard.as_ref() {
        f(event);
    }
}
