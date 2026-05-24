use crate::error::ActorError;
use crate::message::Message;
use crate::RemoteMessage;
use spawned_address::ActorAddress;
use spawned_cluster::ClusterRouter;
use spawned_wire::WireEnvelope;
use std::sync::Arc;

/// Local recipient handle for tasks or threads mode.
enum LocalRecipient<M: Message> {
    Tasks(crate::tasks::Recipient<M>),
    Threads(crate::threads::Recipient<M>),
}

impl<M: Message> LocalRecipient<M> {
    fn send(&self, msg: M) -> Result<(), ActorError> {
        match self {
            Self::Tasks(r) => r.send(msg),
            Self::Threads(r) => r.send(msg),
        }
    }

    fn request_raw(&self, msg: M) -> Result<RequestRx<M>, ActorError> {
        match self {
            Self::Tasks(r) => Ok(RequestRx::Tasks(r.request_raw(msg)?)),
            Self::Threads(r) => Ok(RequestRx::Threads(r.request_raw(msg)?)),
        }
    }
}

pub enum RequestRx<M: Message> {
    Tasks(spawned_rt::tasks::oneshot::Receiver<M::Result>),
    Threads(spawned_rt::threads::oneshot::Receiver<M::Result>),
}

impl<M: Message> RequestRx<M> {
    async fn recv(self) -> Result<M::Result, ActorError> {
        match self {
            Self::Tasks(rx) => rx.await.map_err(|_| ActorError::ActorStopped),
            Self::Threads(rx) => rx.recv().map_err(|_| ActorError::ActorStopped),
        }
    }
}

/// Address-aware reference that routes to a local `Recipient` or remote transport.
pub struct RemoteActorRef<M: Message> {
    address: ActorAddress,
    local: Option<LocalRecipient<M>>,
    router: Arc<ClusterRouter>,
}

impl<M: Message> Clone for RemoteActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            local: match &self.local {
                Some(LocalRecipient::Tasks(r)) => Some(LocalRecipient::Tasks(r.clone())),
                Some(LocalRecipient::Threads(r)) => Some(LocalRecipient::Threads(r.clone())),
                None => None,
            },
            router: self.router.clone(),
        }
    }
}

impl<M: Message> RemoteActorRef<M> {
    /// Local actor on the tasks runtime.
    pub fn local_tasks(address: ActorAddress, recipient: crate::tasks::Recipient<M>) -> Self {
        Self {
            address,
            local: Some(LocalRecipient::Tasks(recipient)),
            router: ClusterRouter::global(),
        }
    }

    /// Local actor on the threads runtime.
    pub fn local_threads(address: ActorAddress, recipient: crate::threads::Recipient<M>) -> Self {
        Self {
            address,
            local: Some(LocalRecipient::Threads(recipient)),
            router: ClusterRouter::global(),
        }
    }

    /// Remote actor (no local recipient); uses the given router.
    pub fn remote(address: ActorAddress, router: Arc<ClusterRouter>) -> Self {
        Self {
            address,
            local: None,
            router,
        }
    }

    /// Remote actor using the process-global router.
    pub fn remote_global(address: ActorAddress) -> Self {
        Self::remote(address, ClusterRouter::global())
    }

    /// Target address.
    pub fn address(&self) -> &ActorAddress {
        &self.address
    }

    /// Send a message, routing locally or remotely.
    pub fn send(&self, msg: M) -> Result<(), ActorError>
    where
        M: RemoteMessage,
    {
        if self.address.is_local() {
            if let Some(local) = self.local.as_ref() {
                return local.send(msg);
            }
        }

        let envelope = WireEnvelope::fire_and_forget(self.address.clone(), &msg)?;
        self.router
            .send_remote(envelope)
            .map_err(map_transport_error)
    }

    /// Raw request channel, routing locally or remotely.
    pub fn request_raw(&self, msg: M) -> Result<RemoteRequest<M>, ActorError>
    where
        M: RemoteMessage,
    {
        if self.address.is_local() {
            if let Some(local) = self.local.as_ref() {
                let rx = local.request_raw(msg)?;
                return Ok(RemoteRequest::Local(rx));
            }
        }

        let correlation_id = next_correlation_id();
        let envelope = WireEnvelope::request(self.address.clone(), &msg, correlation_id)?;
        let payload = self
            .router
            .request_remote(envelope)
            .map_err(map_transport_error)?;
        Ok(RemoteRequest::Remote(payload))
    }
}

/// Opaque request handle for local or remote replies.
pub enum RemoteRequest<M: Message> {
    Local(RequestRx<M>),
    Remote(Vec<u8>),
}

impl<M: Message> RemoteRequest<M> {
    /// Wait for the reply (async; threads mode uses blocking recv internally).
    pub async fn recv(self) -> Result<M::Result, ActorError>
    where
        M::Result: for<'de> serde::Deserialize<'de>,
    {
        match self {
            Self::Local(rx) => rx.recv().await,
            Self::Remote(payload) => spawned_wire::decode_reply(&payload).map_err(|e| {
                tracing::error!("wire reply decode error: {e}");
                ActorError::RemoteUnreachable
            }),
        }
    }
}

fn map_transport_error(err: spawned_cluster::TransportError) -> ActorError {
    match err {
        spawned_cluster::TransportError::RemoteUnreachable => ActorError::RemoteUnreachable,
        spawned_cluster::TransportError::Wire(e) => {
            tracing::error!("wire error during remote send: {e}");
            ActorError::RemoteUnreachable
        }
        spawned_cluster::TransportError::Io(e) => {
            tracing::error!("transport io error: {e}");
            ActorError::RemoteUnreachable
        }
        spawned_cluster::TransportError::Protocol(e) => {
            tracing::error!("transport protocol error: {e}");
            ActorError::RemoteUnreachable
        }
    }
}

static CORRELATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_correlation_id() -> u64 {
    CORRELATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_message;
    use serde::{Deserialize, Serialize};
    use spawned_address::ActorId;

    #[derive(Serialize, Deserialize)]
    #[remote_message]
    struct Ping {
        n: u32,
    }

    impl Message for Ping {
        type Result = ();
    }

    #[test]
    fn remote_send_hits_unreachable_transport() {
        let addr = ActorAddress::on("peer@host".into(), ActorId::from_raw(9));
        let remote = RemoteActorRef::<Ping>::remote_global(addr);
        assert!(matches!(
            remote.send(Ping { n: 1 }),
            Err(ActorError::RemoteUnreachable)
        ));
    }
}
