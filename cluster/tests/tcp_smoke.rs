use spawned_address::NodeId;
use spawned_cluster::{
    InboundDispatch, TcpClusterListener, TcpTransport, Transport, TransportError,
};
use spawned_wire::{RemoteMessage, WireEnvelope};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
struct Echo(u32);

impl RemoteMessage for Echo {
    const REMOTE_ID: &'static str = "spawned.test.Echo/v1";
}

struct EchoDispatch;

impl InboundDispatch for EchoDispatch {
    fn dispatch(&self, envelope: WireEnvelope) -> Result<Option<Vec<u8>>, TransportError> {
        let echo: Echo = spawned_wire::decode_payload(&envelope)?;
        Ok(Some(
            spawned_wire::encode_reply(&(echo.0 + 1))?,
        ))
    }
}

#[test]
fn tcp_transport_request_without_actors() {
    let dispatch = Arc::new(EchoDispatch);
    let listener = TcpClusterListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        NodeId::new("node_a@127.0.0.1"),
        dispatch,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let mut peers = HashMap::new();
    peers.insert(NodeId::new("node_a@127.0.0.1"), listen_addr);
    let transport = TcpTransport::new(NodeId::new("node_b@127.0.0.1"), peers);

    let envelope = WireEnvelope::request(
        spawned_address::ActorAddress::on(
            NodeId::new("node_a@127.0.0.1"),
            spawned_address::ActorId::from_raw(1),
        ),
        &Echo(41),
        99,
    )
    .unwrap();

    let payload = transport.request_envelope(envelope).unwrap();
    let n: u32 = spawned_wire::decode_reply(&payload).unwrap();
    assert_eq!(n, 42);

    listener.shutdown();
}
