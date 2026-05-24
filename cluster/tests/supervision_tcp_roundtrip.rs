//! Two-node TCP supervision RPC roundtrip (stub broker).

use spawned_address::{ActorAddress, ActorId, NodeId};
use spawned_cluster::{
    stub_supervision_hooks, AddressDispatch, ControlPlaneHooks, RemoteSpawnSpec,
    SupervisionEnvelope, SupervisionEvent, TcpClusterListener, TcpTransport,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn supervision_tcp_spawn_request_stub_reply() {
    let _ = tracing_subscriber::fmt::try_init();

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", "sup_server@127.0.0.1");
    }

    let server_node = NodeId::new("sup_server@127.0.0.1");
    let client_node = NodeId::new("sup_client@127.0.0.1");

    let dispatch = Arc::new(AddressDispatch::new());
    let control = ControlPlaneHooks::none().with_supervision(stub_supervision_hooks());
    let listener = TcpClusterListener::bind_with_control_plane(
        "127.0.0.1:0".parse().unwrap(),
        server_node.clone(),
        dispatch,
        control,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let mut peers = HashMap::new();
    peers.insert(server_node.clone(), listen_addr);
    let transport = TcpTransport::new(client_node.clone(), peers);

    let request = SupervisionEnvelope {
        correlation_id: 1,
        event: SupervisionEvent::SpawnRequest {
            parent: ActorAddress::local(ActorId::from_raw(1)),
            placement: server_node.clone(),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "counter".into(),
                init: vec![1],
            },
            link: true,
        },
    };

    let reply = tokio::task::spawn_blocking(move || {
        transport.request_supervision(&server_node, request)
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(reply.correlation_id, 1);
    assert!(matches!(reply.event, SupervisionEvent::SpawnErr { .. }));
    if let SupervisionEvent::SpawnErr { error } = reply.event {
        assert_eq!(error, "supervision broker not running");
    }

    listener.shutdown();
}

#[test]
fn v2_client_rejected_by_v3_listener() {
    use spawned_cluster::{read_frame, write_frame, encode_handshake, Handshake, PROTOCOL_VERSION};

    let server_node = NodeId::new("v3_server@127.0.0.1");
    let dispatch = Arc::new(AddressDispatch::new());
    let listener = TcpClusterListener::bind_with_control_plane(
        "127.0.0.1:0".parse().unwrap(),
        server_node,
        dispatch,
        ControlPlaneHooks::none(),
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let mut stream =
        std::net::TcpStream::connect(listen_addr).expect("connect to v3 listener");
    let hs = encode_handshake(&Handshake {
        version: 2,
        node: NodeId::new("v2_client@127.0.0.1"),
    })
    .unwrap();
    write_frame(&mut stream, &hs).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        read_frame(&mut stream).is_err(),
        "v3 listener should reject v2 handshake without sending ack"
    );
    assert_eq!(PROTOCOL_VERSION, 3);

    listener.shutdown();
}
