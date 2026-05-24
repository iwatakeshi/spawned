//! Two-node federated registry integration test.

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{ClusterRouter, TcpTransport};
use spawned_concurrency::cluster::{apply_remote_event, install_registry_sync, local_snapshot};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{
    lookup_address, register_named, remote_message, Node, RemoteActorRef,
};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
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

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn federated_registry_name_lookup_and_remote_request() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_addr = free_port();
    let server_name = NodeId::new("registry_server@127.0.0.1");
    let client_name = NodeId::new("registry_client@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", server_name.as_str());
    }

    let server_node = Node::builder()
        .name(server_name.clone())
        .listen(server_addr)
        .build()
        .unwrap();

    let actor = EchoActor.start();
    let address = ActorAddress::local(actor.id());
    register_named("echo", actor.child_handle()).unwrap();
    server_node.register_tasks_wire(address.clone(), actor.recipient());

    let listen_addr = server_node.listen_addr().unwrap();

    let mut peers = HashMap::new();
    peers.insert(server_name.clone(), listen_addr);
    let tcp = Arc::new(
        TcpTransport::new(client_name.clone(), peers).with_registry_hooks(
            Arc::new(|event| apply_remote_event(event)),
            Arc::new(local_snapshot),
        ),
    );
    install_registry_sync({
        let tcp = tcp.clone();
        move |event| {
            let _ = tcp.broadcast_registry(event);
        }
    });
    tcp.sync_peers().unwrap();

    let router = Arc::new(ClusterRouter::new(tcp));

    let remote_addr = lookup_address("echo").expect("federated name lookup");
    assert_eq!(remote_addr, address);
    assert_eq!(remote_addr.node, server_name);

    let remote = RemoteActorRef::<Echo>::remote(remote_addr, router);
    let reply = tokio::task::spawn_blocking(move || remote.request_raw(Echo { n: 41 }))
        .await
        .unwrap()
        .unwrap();
    let n = reply.recv().await.unwrap();
    assert_eq!(n, 42);

    server_node.shutdown();
}
