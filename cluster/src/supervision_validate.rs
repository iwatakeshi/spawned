//! Validation rules for supervision envelopes on the wire.

use crate::protocol::{
    RemoteSpawnSpec, SupervisionEnvelope, SupervisionEvent, MAX_REMOTE_SPAWN_INIT_BYTES,
};
use crate::TransportError;

/// Whether this event kind requires a non-zero correlation id.
pub fn requires_correlation(event: &SupervisionEvent) -> bool {
    matches!(
        event,
        SupervisionEvent::SpawnRequest { .. }
            | SupervisionEvent::SpawnOk { .. }
            | SupervisionEvent::SpawnErr { .. }
    )
}

fn validate_spawn_spec(spec: &RemoteSpawnSpec) -> Result<(), TransportError> {
    if let RemoteSpawnSpec::Worker { init, .. } = spec {
        if init.len() > MAX_REMOTE_SPAWN_INIT_BYTES {
            return Err(TransportError::Protocol(format!(
                "remote spawn init too large: {} bytes (max {MAX_REMOTE_SPAWN_INIT_BYTES})",
                init.len()
            )));
        }
    }
    Ok(())
}

/// Validate correlation id rules and payload size limits for an envelope.
pub fn validate_envelope(envelope: &SupervisionEnvelope) -> Result<(), TransportError> {
    let needs_correlation = requires_correlation(&envelope.event);
    if needs_correlation && envelope.correlation_id == 0 {
        return Err(TransportError::Protocol(format!(
            "supervision event {:?} requires non-zero correlation_id",
            envelope.event
        )));
    }
    if !needs_correlation && envelope.correlation_id != 0 {
        return Err(TransportError::Protocol(format!(
            "supervision event {:?} must use correlation_id 0",
            envelope.event
        )));
    }
    if let SupervisionEvent::SpawnRequest { spec, .. } = &envelope.event {
        validate_spawn_spec(spec)?;
    }
    Ok(())
}

/// Validate a correlated reply against its request.
pub fn validate_reply(
    request: &SupervisionEnvelope,
    reply: &SupervisionEnvelope,
) -> Result<(), TransportError> {
    validate_envelope(reply)?;
    if reply.correlation_id != request.correlation_id {
        return Err(TransportError::Protocol(format!(
            "supervision correlation mismatch: expected {}, got {}",
            request.correlation_id, reply.correlation_id
        )));
    }
    match (&request.event, &reply.event) {
        (SupervisionEvent::SpawnRequest { .. }, SupervisionEvent::SpawnOk { .. })
        | (SupervisionEvent::SpawnRequest { .. }, SupervisionEvent::SpawnErr { .. }) => Ok(()),
        _ => Err(TransportError::Protocol(format!(
            "invalid supervision reply {:?} for request {:?}",
            reply.event, request.event
        ))),
    }
}
