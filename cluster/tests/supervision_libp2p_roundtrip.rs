//! Two-node libp2p supervision RPC roundtrip (stub broker).

#![cfg(feature = "libp2p")]

use spawned_address::{ActorAddress, ActorId, NodeId};
use spawned_cluster::{
    identity, stub_supervision_hooks, AddressDispatch, ControlPlaneHooks, Libp2pCluster,
    Libp2pPeer, Multiaddr, PeerId, RemoteSpawnSpec, SupervisionEnvelope, SupervisionEvent,
    LIBP2P_CLUSTER_PROTOCOL,
};
use std::sync::Arc;
use std::time::Duration;

fn peer_multiaddr(port: u16, peer_id: PeerId) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer_id}")
        .parse()
        .expect("valid multiaddr")
}

#[tokio::test]
async fn supervision_libp2p_spawn_request_stub_reply() {
    let _ = tracing_subscriber::fmt::try_init();

    assert_eq!(LIBP2P_CLUSTER_PROTOCOL.as_ref(), "/spawned/cluster/3");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", "sup_server@127.0.0.1");
    }

    let server_key = identity::Keypair::generate_ed25519();
    let server_peer = server_key.public().to_peer_id();
    let server_port = Libp2pCluster::ephemeral_tcp_port().unwrap();
    let server_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{server_port}").parse().unwrap();
    let server_node = NodeId::new("sup_server@127.0.0.1");

    let server_control = ControlPlaneHooks::none().with_supervision(stub_supervision_hooks());
    let server = Libp2pCluster::start(
        server_key,
        server_node.clone(),
        server_listen,
        Vec::new(),
        Arc::new(AddressDispatch::new()),
        server_control,
    )
    .unwrap();

    let client_key = identity::Keypair::generate_ed25519();
    let client_node = NodeId::new("sup_client@127.0.0.1");
    let client_port = Libp2pCluster::ephemeral_tcp_port().unwrap();
    let client_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{client_port}").parse().unwrap();

    let peers = vec![Libp2pPeer {
        node: server_node.clone(),
        peer_id: server_peer,
        addr: peer_multiaddr(server_port, server_peer),
    }];

    let client = Libp2pCluster::start(
        client_key,
        client_node,
        client_listen,
        peers,
        Arc::new(AddressDispatch::new()),
        ControlPlaneHooks::none(),
    )
    .unwrap();

    client.sync_peers().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let request = SupervisionEnvelope {
        correlation_id: 42,
        event: SupervisionEvent::SpawnRequest {
            parent: ActorAddress::local(ActorId::from_raw(1)),
            placement: server_node.clone(),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "counter".into(),
                init: vec![1],
            },
            link: false,
        },
    };

    let reply = client
        .request_supervision_from(&server_node, request)
        .unwrap();

    assert_eq!(reply.correlation_id, 42);
    assert!(matches!(reply.event, SupervisionEvent::SpawnErr { .. }));
    if let SupervisionEvent::SpawnErr { error } = reply.event {
        assert_eq!(error, "supervision broker not running");
    }

    server.shutdown();
    client.shutdown();
}
