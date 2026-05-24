//! Two-node federated pg integration test.

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{ClusterRouter, ControlPlaneHooks, TcpTransport};
use spawned_concurrency::cluster::{
    apply_remote_event, apply_remote_pg_event, install_pg_sync, install_registry_sync,
    local_pg_snapshot, local_snapshot,
};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{pg as tasks_pg, Actor, ActorStart, Context, Handler};
use spawned_concurrency::{member_addresses_scoped, remote_message, Node};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

struct Counter {
    value: u32,
}

impl Actor for Counter {}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
#[remote_message]
struct Tick;

impl Message for Tick {
    type Result = u32;
}

impl Handler<Tick> for Counter {
    async fn handle(&mut self, _msg: Tick, _ctx: &Context<Self>) -> u32 {
        self.value += 1;
        self.value
    }
}

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn federated_pg_membership_and_remote_cast_call() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_addr = free_port();
    let server_name = NodeId::new("pg_server@127.0.0.1");
    let client_name = NodeId::new("pg_client@127.0.0.1");
    let group = "workers";

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", server_name.as_str());
    }

    let server_node = Node::builder()
        .name(server_name.clone())
        .listen(server_addr)
        .build()
        .unwrap();

    let counter = Counter { value: 0 }.start();
    let address = ActorAddress::local(counter.id());
    server_node.register_tasks_wire(address.clone(), counter.recipient());
    tasks_pg::join(group, &counter);

    let listen_addr = server_node.listen_addr().unwrap();

    let mut peers = HashMap::new();
    peers.insert(server_name.clone(), listen_addr);
    let control = ControlPlaneHooks::federated(
        Arc::new(|event| apply_remote_event(event)),
        Arc::new(local_snapshot),
        Arc::new(|event| apply_remote_pg_event(event)),
        Arc::new(local_pg_snapshot),
    );
    let tcp = Arc::new(
        TcpTransport::new(client_name.clone(), peers).with_control_plane_hooks(control),
    );
    install_registry_sync({
        let tcp = tcp.clone();
        move |event| {
            let _ = tcp.broadcast_registry(event);
        }
    });
    install_pg_sync({
        let tcp = tcp.clone();
        move |event| {
            let _ = tcp.broadcast_pg(event);
        }
    });
    tcp.sync_peers().unwrap();

    let _router = Arc::new(ClusterRouter::new(tcp));

    let members = member_addresses_scoped("default", group);
    assert_eq!(members.len(), 1);
    assert_eq!(members[0], address);
    assert_eq!(members[0].node, server_name);

    let cast = tasks_pg::cast_federated::<Counter, _>(group, Tick);
    assert_eq!(cast.delivered, 1);
    assert!(cast.failed.is_empty());

    let call = tasks_pg::call_federated::<Counter, _>(group, Tick).await;
    assert_eq!(call.ok.len(), 1);
    assert_eq!(call.ok[0].1, 2);
    assert!(call.failed.is_empty());

    server_node.shutdown();
}
