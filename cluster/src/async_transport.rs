use crate::TransportError;
use async_trait::async_trait;
use spawned_wire::WireEnvelope;

/// Async pluggable transport for cross-node actor messaging.
#[async_trait]
pub trait AsyncTransport: Send + Sync {
    /// Fire-and-forget delivery of an envelope to a remote node.
    async fn send_envelope(&self, envelope: WireEnvelope) -> Result<(), TransportError>;

    /// Request/response delivery. Returns the response payload bytes.
    async fn request_envelope(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError>;
}
