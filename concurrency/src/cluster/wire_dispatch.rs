//! Wire dispatch helpers for inbound cluster envelopes.

use crate::message::Message;
use crate::RemoteMessage;
use spawned_address::ActorAddress;
use spawned_cluster::InboundDispatch;
use spawned_cluster::TransportError;
use spawned_wire::{decode_payload, encode_reply, WireEnvelope};
use std::sync::Arc;
use std::time::Duration;

/// Tasks-runtime inbound dispatch for a single remote message type.
pub struct TasksWireDispatch<M: Message + RemoteMessage> {
    address: ActorAddress,
    recipient: crate::tasks::Recipient<M>,
    runtime: spawned_rt::tasks::Handle,
}

impl<M: Message + RemoteMessage> TasksWireDispatch<M> {
    pub fn new(
        address: ActorAddress,
        recipient: crate::tasks::Recipient<M>,
        runtime: spawned_rt::tasks::Handle,
    ) -> Self {
        Self {
            address,
            recipient,
            runtime,
        }
    }
}

impl<M> InboundDispatch for TasksWireDispatch<M>
where
    M: Message + RemoteMessage,
    M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
{
    fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError> {
        if envelope.to != self.address {
            return Err(TransportError::Protocol(format!(
                "unexpected target address {}",
                envelope.to
            )));
        }

        let msg = decode_payload::<M>(&envelope)?;
        if envelope.correlation_id == 0 {
            self.recipient
                .send(msg)
                .map_err(|_| TransportError::RemoteUnreachable)?;
            return Ok(None);
        }

        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let recipient = self.recipient.clone();
        self.runtime.spawn(async move {
            let result =
                crate::tasks::request(&*recipient, msg, Duration::from_secs(5)).await;
            let _ = tx.send(result);
        });
        let result = rx
            .recv()
            .map_err(|_| TransportError::RemoteUnreachable)?
            .map_err(|_| TransportError::RemoteUnreachable)?;
        Ok(Some(encode_reply(&result)?))
    }
}

/// Build an [`InboundDispatch`] for a tasks [`Recipient`](crate::tasks::Recipient).
pub fn tasks_wire_dispatch<M>(
    address: ActorAddress,
    recipient: crate::tasks::Recipient<M>,
) -> Arc<dyn InboundDispatch>
where
    M: Message + RemoteMessage,
    M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
{
    let runtime = spawned_rt::tasks::Handle::current();
    Arc::new(TasksWireDispatch::new(address, recipient, runtime))
}

/// Threads-runtime inbound dispatch for a single remote message type.
pub struct ThreadsWireDispatch<M: Message + RemoteMessage> {
    address: ActorAddress,
    recipient: crate::threads::Recipient<M>,
}

impl<M: Message + RemoteMessage> ThreadsWireDispatch<M> {
    pub fn new(address: ActorAddress, recipient: crate::threads::Recipient<M>) -> Self {
        Self { address, recipient }
    }
}

impl<M> InboundDispatch for ThreadsWireDispatch<M>
where
    M: Message + RemoteMessage,
    M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
{
    fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError> {
        if envelope.to != self.address {
            return Err(TransportError::Protocol(format!(
                "unexpected target address {}",
                envelope.to
            )));
        }

        let msg = decode_payload::<M>(&envelope)?;
        if envelope.correlation_id == 0 {
            self.recipient
                .send(msg)
                .map_err(|_| TransportError::RemoteUnreachable)?;
            return Ok(None);
        }

        let rx = self
            .recipient
            .request_raw(msg)
            .map_err(|_| TransportError::RemoteUnreachable)?;
        let result = rx
            .recv()
            .map_err(|_| TransportError::RemoteUnreachable)?;
        Ok(Some(encode_reply(&result)?))
    }
}

/// Build an [`InboundDispatch`] for a threads [`Recipient`](crate::threads::Recipient).
pub fn threads_wire_dispatch<M>(
    address: ActorAddress,
    recipient: crate::threads::Recipient<M>,
) -> Arc<dyn InboundDispatch>
where
    M: Message + RemoteMessage,
    M::Result: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
{
    Arc::new(ThreadsWireDispatch::new(address, recipient))
}
