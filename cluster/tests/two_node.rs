//! Two-node TCP integration test (single process, loopback).

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{ClusterRouter, TcpClusterListener, TcpTransport};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{remote_message, tasks_wire_dispatch, RemoteActorRef};
use std::collections::HashMap;
use std::sync::Arc;

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

#[tokio::test]
async fn tcp_two_node_request_roundtrip() {
    let _ = tracing_subscriber::fmt::try_init();

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", "node_a@127.0.0.1");
    }

    let actor = EchoActor.start();
    let address = ActorAddress::local(actor.id());
    let dispatch = tasks_wire_dispatch(address.clone(), actor.recipient());

    let listener = TcpClusterListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        NodeId::new("node_a@127.0.0.1"),
        dispatch,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let mut peers = HashMap::new();
    peers.insert(NodeId::new("node_a@127.0.0.1"), listen_addr);
    let transport = Arc::new(TcpTransport::new(
        NodeId::new("node_b@127.0.0.1"),
        peers,
    ));
    let router = Arc::new(ClusterRouter::new(transport));

    let remote = RemoteActorRef::<Echo>::remote(address, router);
    let reply = tokio::task::spawn_blocking(move || remote.request_raw(Echo { n: 41 }))
        .await
        .unwrap()
        .unwrap();
    let n = reply.recv().await.unwrap();
    assert_eq!(n, 42);

    listener.shutdown();
}

#[tokio::test]
async fn tcp_two_node_fire_and_forget() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let hits = Arc::new(AtomicU32::new(0));

    struct HitActor {
        hits: Arc<AtomicU32>,
    }

    impl Actor for HitActor {}

    #[derive(serde::Serialize, serde::Deserialize)]
    #[remote_message]
    struct Hit;

    impl Message for Hit {
        type Result = ();
    }

    impl Handler<Hit> for HitActor {
        async fn handle(&mut self, _msg: Hit, _ctx: &Context<Self>) {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", "node_a@127.0.0.1");
    }

    let actor = HitActor {
        hits: hits.clone(),
    }
    .start();
    let address = ActorAddress::local(actor.id());
    let dispatch = tasks_wire_dispatch(address.clone(), actor.recipient());

    let listener = TcpClusterListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        NodeId::new("node_a@127.0.0.1"),
        dispatch,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let mut peers = HashMap::new();
    peers.insert(NodeId::new("node_a@127.0.0.1"), listen_addr);
    let transport = Arc::new(TcpTransport::new(
        NodeId::new("node_b@127.0.0.1"),
        peers,
    ));
    let router = Arc::new(ClusterRouter::new(transport));

    let remote = RemoteActorRef::<Hit>::remote(address, router);
    remote.send(Hit).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(hits.load(Ordering::Relaxed), 1);

    listener.shutdown();
}
