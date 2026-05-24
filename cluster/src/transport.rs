use crate::TransportError;
use spawned_wire::WireEnvelope;

/// Pluggable transport for cross-node actor messaging.
pub trait Transport: Send + Sync {
    /// Fire-and-forget delivery of an envelope to a remote node.
    fn send_envelope(&self, envelope: WireEnvelope) -> Result<(), TransportError>;

    /// Request/response delivery. Returns the response payload bytes.
    fn request_envelope(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError>;
}

/// Default stub until Phase 8c wires a real transport.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailableTransport;

impl Transport for UnavailableTransport {
    fn send_envelope(&self, _envelope: WireEnvelope) -> Result<(), TransportError> {
        Err(TransportError::RemoteUnreachable)
    }

    fn request_envelope(&self, _envelope: WireEnvelope) -> Result<Vec<u8>, TransportError> {
        Err(TransportError::RemoteUnreachable)
    }
}
