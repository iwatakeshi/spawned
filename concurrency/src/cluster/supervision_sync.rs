//! Federated supervision publish + request hooks (installed by [`super::node::NodeBuilder`]).

use spawned_address::NodeId;
use spawned_cluster::{SupervisionEnvelope, TransportError};
use std::sync::{OnceLock, RwLock};

type PublishFn = Box<dyn Fn(SupervisionEnvelope) + Send + Sync>;
type RequestFn = Box<dyn Fn(&NodeId, SupervisionEnvelope) -> Result<SupervisionEnvelope, TransportError> + Send + Sync>;

static PUBLISH: OnceLock<RwLock<Option<PublishFn>>> = OnceLock::new();
static REQUEST: OnceLock<RwLock<Option<RequestFn>>> = OnceLock::new();

fn publish_slot() -> &'static RwLock<Option<PublishFn>> {
    PUBLISH.get_or_init(|| RwLock::new(None))
}

fn request_slot() -> &'static RwLock<Option<RequestFn>> {
    REQUEST.get_or_init(|| RwLock::new(None))
}

pub(crate) fn install_publish(publish: impl Fn(SupervisionEnvelope) + Send + Sync + 'static) {
    *publish_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(publish));
}

pub(crate) fn install_request(
    request: impl Fn(&NodeId, SupervisionEnvelope) -> Result<SupervisionEnvelope, TransportError>
        + Send
        + Sync
        + 'static,
) {
    *request_slot().write().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(request));
}

pub fn install_supervision_sync(publish: impl Fn(SupervisionEnvelope) + Send + Sync + 'static) {
    install_publish(publish);
}

pub fn install_supervision_request(
    request: impl Fn(&NodeId, SupervisionEnvelope) -> Result<SupervisionEnvelope, TransportError>
        + Send
        + Sync
        + 'static,
) {
    install_request(request);
}

pub(crate) fn publish_supervision(envelope: SupervisionEnvelope) {
    let guard = publish_slot().read().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = guard.as_ref() {
        f(envelope);
    }
}

pub(crate) fn request_supervision(
    placement: &NodeId,
    envelope: SupervisionEnvelope,
) -> Result<SupervisionEnvelope, TransportError> {
    let guard = request_slot().read().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = guard.as_ref() {
        f(placement, envelope)
    } else {
        Err(TransportError::RemoteUnreachable)
    }
}
