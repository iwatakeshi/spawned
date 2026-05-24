use crate::TransportError;
use spawned_address::ActorAddress;
use spawned_wire::{WireEnvelope, WireError};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Routes inbound envelopes to per-address wire handlers.
#[derive(Default)]
pub struct AddressDispatch {
    routes: RwLock<HashMap<ActorAddress, Vec<Arc<dyn crate::InboundDispatch>>>>,
}

impl AddressDispatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an inbound handler for an actor address.
    pub fn register(&self, address: ActorAddress, handler: Arc<dyn crate::InboundDispatch>) {
        let mut routes = self.routes.write().unwrap_or_else(|p| p.into_inner());
        routes.entry(address).or_default().push(handler);
    }
}

impl crate::InboundDispatch for AddressDispatch {
    fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError> {
        let routes = self.routes.read().unwrap_or_else(|p| p.into_inner());
        let handlers = routes.get(&envelope.to).ok_or_else(|| {
            TransportError::Protocol(format!("no handler for {}", envelope.to))
        })?;

        let mut last_wire = None;
        for handler in handlers {
            match handler.dispatch(envelope.clone()) {
                Ok(result) => return Ok(result),
                Err(err @ TransportError::Wire(WireError::MessageIdMismatch { .. })) => {
                    last_wire = Some(err);
                }
                Err(TransportError::Protocol(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        Err(last_wire.unwrap_or_else(|| {
            TransportError::Protocol(format!(
                "no handler accepted message {} for {}",
                envelope.remote_msg_id, envelope.to
            ))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InboundDispatch;
    use spawned_address::{ActorId, NodeId};
    use spawned_wire::RemoteMessage;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct A(u32);

    impl RemoteMessage for A {
        const REMOTE_ID: &'static str = "spawned.test.A/v1";
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct B(u32);

    impl RemoteMessage for B {
        const REMOTE_ID: &'static str = "spawned.test.B/v1";
    }

    struct TypedDispatch<M: RemoteMessage> {
        address: ActorAddress,
        _marker: std::marker::PhantomData<M>,
    }

    impl<M: RemoteMessage + serde::de::DeserializeOwned> InboundDispatch for TypedDispatch<M> {
        fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError> {
            if envelope.to != self.address {
                return Err(TransportError::Protocol("address mismatch".into()));
            }
            let _msg: M = spawned_wire::decode_payload(&envelope)?;
            Ok(None)
        }
    }

    #[test]
    fn routes_by_address_and_message_id() {
        let addr = ActorAddress::on(NodeId::new("n@host"), ActorId::from_raw(1));
        let router = AddressDispatch::new();
        router.register(
            addr.clone(),
            Arc::new(TypedDispatch::<A> {
                address: addr.clone(),
                _marker: std::marker::PhantomData,
            }),
        );
        router.register(
            addr.clone(),
            Arc::new(TypedDispatch::<B> {
                address: addr.clone(),
                _marker: std::marker::PhantomData,
            }),
        );

        let envelope = WireEnvelope::fire_and_forget(addr.clone(), &A(1)).unwrap();
        assert!(router.dispatch(envelope).unwrap().is_none());

        let envelope = WireEnvelope::fire_and_forget(addr, &B(2)).unwrap();
        assert!(router.dispatch(envelope).unwrap().is_none());
    }
}
