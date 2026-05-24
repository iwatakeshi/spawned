//! Two-node libp2p integration test via [`NodeBuilder`] (single process, loopback).

#![cfg(feature = "cluster-libp2p")]

use spawned_address::ActorAddress;
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{identity, remote_message, Node, RemoteActorRef};
use spawned_cluster::{Libp2pCluster, Multiaddr, PeerId};
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
async fn node_libp2p_two_node_request_roundtrip() {
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

    let server_node = Node::builder()
        .name("node_a@127.0.0.1")
        .transport_libp2p(Some(server_key))
        .listen_libp2p(server_listen)
        .build()
        .unwrap();
    server_node.register_tasks_wire(address.clone(), actor.recipient());

    let client_key = identity::Keypair::generate_ed25519();
    let client_port = Libp2pCluster::ephemeral_tcp_port().unwrap();
    let client_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{client_port}").parse().unwrap();
    let server_addr = peer_multiaddr(server_port, server_peer);

    let client_node = Node::builder()
        .transport_libp2p(Some(client_key))
        .listen_libp2p(client_listen)
        .libp2p_peer("node_a@127.0.0.1", server_peer, server_addr)
        .build()
        .unwrap();

    client_node.sync_registry().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let remote = RemoteActorRef::<Echo>::remote(address, client_node.router());
    let n = remote.request_async(Echo { n: 41 }).await.unwrap();
    assert_eq!(n, 42);
}
