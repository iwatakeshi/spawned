//! Three-node federated process group integration test.

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{AddressDispatch, ControlPlaneHooks, TcpClusterListener};
use spawned_concurrency::cluster::{
    apply_remote_event, apply_remote_pg_event, local_snapshot,
};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{pg as tasks_pg, Actor, ActorStart, Context, Handler};
use spawned_concurrency::{member_addresses_scoped, remote_message, tasks_wire_dispatch, Node};
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

fn pg_worker_hooks(member: spawned_cluster::PgMemberEntry) -> ControlPlaneHooks {
    ControlPlaneHooks::federated(
        Arc::new(|event| apply_remote_event(event)),
        Arc::new(local_snapshot),
        Arc::new(|event| apply_remote_pg_event(event)),
        Arc::new(move || vec![member.clone()]),
    )
}

struct PgWorker {
    address: ActorAddress,
    listener: TcpClusterListener,
    listen_addr: SocketAddr,
}

fn start_pg_worker(name: NodeId, listen: SocketAddr, group: &str) -> PgWorker {
    let counter = Counter { value: 0 }.start();
    let address = ActorAddress::on(name.clone(), counter.id());
    let member = spawned_cluster::PgMemberEntry {
        scope: "default".into(),
        group: group.into(),
        address: address.clone(),
    };

    let dispatch = Arc::new(AddressDispatch::new());
    dispatch.register(
        address.clone(),
        tasks_wire_dispatch(address.clone(), counter.recipient()),
    );

    let listener = TcpClusterListener::bind_with_control_plane(
        listen,
        name,
        dispatch,
        pg_worker_hooks(member),
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    PgWorker {
        address,
        listener,
        listen_addr,
    }
}

#[tokio::test]
async fn federated_pg_three_node_membership_and_remote_cast_call() {
    let _ = tracing_subscriber::fmt::try_init();

    let node_a = NodeId::new("pg_a@127.0.0.1");
    let node_b = NodeId::new("pg_b@127.0.0.1");
    let node_c = NodeId::new("pg_c@127.0.0.1");
    let group = "workers";

    let worker_a = start_pg_worker(node_a.clone(), free_port(), group);
    let worker_b = start_pg_worker(node_b.clone(), free_port(), group);

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", node_c.as_str());
    }

    // Join before Node::builder installs the pg publish hook (avoids blocking broadcast).
    let local_counter = Counter { value: 0 }.start();
    let local_address = ActorAddress::local(local_counter.id());
    tasks_pg::join(group, &local_counter);

    let client = Node::builder()
        .name(node_c.clone())
        .peer(node_a.clone(), worker_a.listen_addr)
        .peer(node_b.clone(), worker_b.listen_addr)
        .build()
        .unwrap();
    client.register_tasks_wire(local_address.clone(), local_counter.recipient());
    client.sync_registry().unwrap();

    let members = member_addresses_scoped("default", group);
    assert_eq!(members.len(), 3);
    assert!(members.contains(&worker_a.address));
    assert!(members.contains(&worker_b.address));
    assert!(members.contains(&local_address));

    let cast = tasks_pg::cast_federated::<Counter, _>(group, Tick);
    assert_eq!(cast.delivered, 3, "cast failed: {:?}", cast.failed);
    assert!(cast.failed.is_empty());

    client.shutdown();
    worker_a.listener.shutdown();
    worker_b.listener.shutdown();
}
