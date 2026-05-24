/// Errors from cluster transport and routing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// No transport is configured or the remote node is unreachable.
    #[error("remote actor is unreachable")]
    RemoteUnreachable,
    /// Wire encoding or decoding failed.
    #[error("wire error: {0}")]
    Wire(#[from] spawned_wire::WireError),
}
