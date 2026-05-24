//! Supervision control-plane hooks (routed unicast, not federated broadcast).

use crate::protocol::{
    encode_cluster_frame, ClusterFrame, SupervisionEnvelope, SupervisionEvent,
};
use crate::supervision_validate::{validate_envelope, validate_reply};
use crate::TransportError;
use std::sync::Arc;

/// Handle an inbound supervision envelope. Return `Some(reply)` for correlated requests.
pub trait SupervisionInbound: Send + Sync {
    fn handle(&self, envelope: SupervisionEnvelope) -> Result<Option<SupervisionEnvelope>, TransportError>;
}

pub(crate) type SupervisionApplyFn =
    Arc<dyn Fn(SupervisionEnvelope) -> Result<Option<SupervisionEnvelope>, TransportError> + Send + Sync>;

struct FnInbound(SupervisionApplyFn);

impl SupervisionInbound for FnInbound {
    fn handle(&self, envelope: SupervisionEnvelope) -> Result<Option<SupervisionEnvelope>, TransportError> {
        (self.0)(envelope)
    }
}

#[derive(Clone)]
pub struct SupervisionHooks {
    pub inbound: Option<Arc<dyn SupervisionInbound>>,
}

impl SupervisionHooks {
    pub fn none() -> Self {
        Self { inbound: None }
    }

    pub fn from_fn(f: SupervisionApplyFn) -> Self {
        Self {
            inbound: Some(Arc::new(FnInbound(f))),
        }
    }
}

/// Stub handler for integration tests: rejects spawn, ignores fire-and-forget events.
pub fn stub_supervision_hooks() -> SupervisionHooks {
    SupervisionHooks::from_fn(Arc::new(|envelope| {
        validate_envelope(&envelope)?;
        match envelope.event {
            SupervisionEvent::SpawnRequest { .. } => Ok(Some(SupervisionEnvelope {
                correlation_id: envelope.correlation_id,
                event: SupervisionEvent::SpawnErr {
                    error: "supervision broker not running".into(),
                },
            })),
            _ => Ok(None),
        }
    }))
}

pub fn encode_supervision_frame(envelope: &SupervisionEnvelope) -> Result<Vec<u8>, TransportError> {
    encode_cluster_frame(&ClusterFrame::Supervision(envelope.clone())).map_err(TransportError::from)
}

/// Encode a supervision reply body (raw envelope, not `ClusterFrame`).
pub fn encode_supervision(envelope: &SupervisionEnvelope) -> Result<Vec<u8>, TransportError> {
    postcard::to_allocvec(envelope).map_err(|e| TransportError::Protocol(e.to_string()))
}

/// Decode a supervision reply body (raw envelope, not `ClusterFrame`).
pub fn decode_supervision(bytes: &[u8]) -> Result<SupervisionEnvelope, TransportError> {
    postcard::from_bytes(bytes).map_err(|e| TransportError::Protocol(e.to_string()))
}

pub fn apply_supervision(
    hooks: &SupervisionHooks,
    envelope: SupervisionEnvelope,
) -> Result<Option<SupervisionEnvelope>, TransportError> {
    validate_envelope(&envelope)?;
    if let Some(inbound) = &hooks.inbound {
        inbound.handle(envelope)
    } else {
        Ok(None)
    }
}

/// Validate and decode a correlated supervision reply against the request envelope.
pub fn decode_supervision_reply(
    request: &SupervisionEnvelope,
    bytes: &[u8],
) -> Result<SupervisionEnvelope, TransportError> {
    let reply = decode_supervision(bytes)?;
    validate_reply(request, &reply)?;
    Ok(reply)
}
