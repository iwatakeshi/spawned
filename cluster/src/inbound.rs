use crate::TransportError;
use spawned_wire::WireEnvelope;

/// Handles inbound wire envelopes on a cluster node.
pub trait InboundDispatch: Send + Sync {
    /// Process an envelope. Returns reply payload bytes for correlated requests.
    fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError>;
}
