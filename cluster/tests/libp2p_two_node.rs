//! Two-node libp2p integration test (single process, loopback).

#![cfg(feature = "libp2p")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    identity, AsyncTransport, ClusterRouter, ControlPlaneHooks, Libp2pCluster, Libp2pPeer,
    Multiaddr, PeerId,
};
use spawned_concurrency::cluster::{
    apply_remote_event, apply_remote_pg_event, local_pg_snapshot, local_snapshot,
};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{remote_message, tasks_wire_dispatch, RemoteActorRef};
use std::sync::Arc;
use std::time::Duration;

struct EchoActor;

impl Actor for EchoActor {}

#[derive(serde::Serialize, serde::Deserialize)]
#[remote_message]
struct Echo {
    n: u32,
}

impl Message for Echo {
    type Result = u32;
}

impl Handler<Echo> for EchoActor {
    async fn handle(&mut self, msg: Echo, _ctx: &Context<Self>) -> u32 {
        msg.n + 1
    }
}

fn peer_multiaddr(port: u16, peer_id: PeerId) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer_id}")
        .parse()
        .expect("valid multiaddr")
}

#[tokio::test]
async fn libp2p_two_node_request_roundtrip() {
    let _ = tracing_subscriber::fmt::try_init();

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", "node_a@127.0.0.1");
    }

    let server_key = identity::Keypair::generate_ed25519();
    let server_peer = server_key.public().to_peer_id();
    let server_port = Libp2pCluster::ephemeral_tcp_port().unwrap();
    let server_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{server_port}").parse().unwrap();

    let actor = EchoActor.start();
    let address = ActorAddress::local(actor.id());
    let dispatch = tasks_wire_dispatch(address.clone(), actor.recipient());

    let control = ControlPlaneHooks::none();
    let server = Libp2pCluster::start(
        server_key,
        NodeId::new("node_a@127.0.0.1"),
        server_listen,
        Vec::new(),
        dispatch,
        control,
    )
    .unwrap();

    let client_key = identity::Keypair::generate_ed25519();
    let client_node = NodeId::new("node_b@127.0.0.1");
    let client_port = Libp2pCluster::ephemeral_tcp_port().unwrap();
    let client_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{client_port}").parse().unwrap();

    let server_addr = peer_multiaddr(server_port, server_peer);
    let peers = vec![Libp2pPeer {
        node: NodeId::new("node_a@127.0.0.1"),
        peer_id: server_peer,
        addr: server_addr,
    }];

    let control = ControlPlaneHooks::federated(
        Arc::new(|event| apply_remote_event(event)),
        Arc::new(local_snapshot),
        Arc::new(|event| apply_remote_pg_event(event)),
        Arc::new(local_pg_snapshot),
    );

    let client = Arc::new(
        Libp2pCluster::start(
            client_key,
            client_node,
            client_listen,
            peers,
            Arc::new(spawned_cluster::AddressDispatch::new()),
            control,
        )
        .unwrap(),
    );

    client.sync_peers().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let transport: Arc<dyn spawned_cluster::Transport> = client.clone();
    let async_transport: Arc<dyn AsyncTransport> = client;
    let router = Arc::new(ClusterRouter::with_async(
        transport,
        Some(async_transport),
    ));

    let remote = RemoteActorRef::<Echo>::remote(address, router);
    let n = remote.request_async(Echo { n: 41 }).await.unwrap();
    assert_eq!(n, 42);

    server.shutdown();
}
