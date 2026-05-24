use crate::{AsyncTransport, Transport, TransportError, UnavailableTransport};
use spawned_wire::WireEnvelope;
use std::sync::{Arc, OnceLock};

static DEFAULT_ROUTER: OnceLock<Arc<ClusterRouter>> = OnceLock::new();

/// Routes envelopes to the configured [`Transport`].
///
/// Local dispatch is handled by `RemoteActorRef` in `spawned-concurrency`;
/// this router covers the remote path only.
pub struct ClusterRouter {
    transport: Arc<dyn Transport>,
    async_transport: Option<Arc<dyn AsyncTransport>>,
}

impl ClusterRouter {
    /// Create a router with an explicit transport implementation.
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self {
            transport,
            async_transport: None,
        }
    }

    /// Create a router with sync and async transport backends.
    pub fn with_async(
        transport: Arc<dyn Transport>,
        async_transport: Option<Arc<dyn AsyncTransport>>,
    ) -> Self {
        Self {
            transport,
            async_transport,
        }
    }

    /// Router using [`UnavailableTransport`] (returns [`TransportError::RemoteUnreachable`]).
    pub fn unavailable() -> Self {
        Self::new(Arc::new(UnavailableTransport))
    }

    /// Returns the process-global router, initializing with [`UnavailableTransport`] if needed.
    pub fn global() -> Arc<Self> {
        DEFAULT_ROUTER
            .get_or_init(|| Arc::new(Self::unavailable()))
            .clone()
    }

    /// Install the process-global router. Returns `false` if already initialized.
    pub fn set_global(router: Arc<Self>) -> bool {
        DEFAULT_ROUTER.set(router).is_ok()
    }

    /// Send a fire-and-forget envelope to a remote node.
    pub fn send_remote(&self, envelope: WireEnvelope) -> Result<(), TransportError> {
        self.transport.send_envelope(envelope)
    }

    /// Send a request envelope and return the response payload.
    pub fn request_remote(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError> {
        self.transport.request_envelope(envelope)
    }

    /// Async request/response delivery.
    pub async fn request_remote_async(
        &self,
        envelope: WireEnvelope,
    ) -> Result<Vec<u8>, TransportError> {
        if let Some(async_transport) = &self.async_transport {
            async_transport.request_envelope(envelope).await
        } else {
            let transport = self.transport.clone();
            tokio::task::spawn_blocking(move || transport.request_envelope(envelope))
                .await
                .map_err(|_| TransportError::RemoteUnreachable)?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spawned_address::{ActorAddress, ActorId};
    use spawned_wire::{RemoteMessage, WireEnvelope};

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Ping(u32);

    impl RemoteMessage for Ping {
        const REMOTE_ID: &'static str = "spawned.test.Ping/v1";
    }

    #[test]
    fn unavailable_transport_returns_unreachable() {
        let router = ClusterRouter::unavailable();
        let envelope = WireEnvelope::fire_and_forget(
            ActorAddress::on("remote@host".into(), ActorId::from_raw(1)),
            &Ping(1),
        )
        .unwrap();
        assert!(matches!(
            router.send_remote(envelope),
            Err(TransportError::RemoteUnreachable)
        ));
    }

    #[tokio::test]
    async fn async_request_falls_back_to_spawn_blocking() {
        let router = ClusterRouter::unavailable();
        let envelope = WireEnvelope::request(
            ActorAddress::on("remote@host".into(), ActorId::from_raw(1)),
            &Ping(1),
            1,
        )
        .unwrap();
        assert!(matches!(
            router.request_remote_async(envelope).await,
            Err(TransportError::RemoteUnreachable)
        ));
    }
}
