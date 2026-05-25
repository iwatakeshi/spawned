//! Three-node federated registry integration tests.
//!
//! Single-process tests use per-listener registry snapshot hooks so two service
//! nodes can coexist with distinct [`NodeId`]s. Registry lookup requires a direct
//! peer connection to the owning node (transitive relay is not supported).

use spawned_address::{ActorAddress, NodeId};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{
    lookup_address, remote_message, tasks_wire_dispatch, Node, RemoteActorRef,
};
use spawned_cluster::{AddressDispatch, RegistryHooks, TcpClusterListener};
use spawned_concurrency::cluster::apply_remote_event;
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

fn registry_hooks(
    snapshot: Arc<dyn Fn() -> Vec<(String, ActorAddress)> + Send + Sync>,
) -> RegistryHooks {
    RegistryHooks::from_fns(
        Arc::new(|event| apply_remote_event(event)),
        snapshot,
    )
}

struct RegistryWorker {
    address: ActorAddress,
    listener: TcpClusterListener,
    listen_addr: SocketAddr,
}

fn start_registry_worker(
    name: NodeId,
    listen: SocketAddr,
    registered_name: &str,
) -> RegistryWorker {
    let actor = EchoActor.start();
    let address = ActorAddress::on(name.clone(), actor.id());
    let registered_name = registered_name.to_string();
    let snapshot_name = registered_name.clone();
    let snapshot_address = address.clone();

    let dispatch = Arc::new(AddressDispatch::new());
    dispatch.register(
        address.clone(),
        tasks_wire_dispatch(address.clone(), actor.recipient()),
    );

    let listener = TcpClusterListener::bind_with_registry(
        listen,
        name,
        dispatch,
        registry_hooks(Arc::new(move || {
            vec![(snapshot_name.clone(), snapshot_address.clone())]
        })),
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    RegistryWorker {
        address,
        listener,
        listen_addr,
    }
}

#[tokio::test]
async fn federated_registry_three_node_mesh_lookup_and_remote_request() {
    let _ = tracing_subscriber::fmt::try_init();

    let node_a = NodeId::new("registry_a@127.0.0.1");
    let node_b = NodeId::new("registry_b@127.0.0.1");
    let node_c = NodeId::new("registry_c@127.0.0.1");

    let worker_a = start_registry_worker(node_a.clone(), free_port(), "echo_a");
    let worker_b = start_registry_worker(node_b.clone(), free_port(), "echo_b");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", node_c.as_str());
    }

    let client = Node::builder()
        .name(node_c.clone())
        .peer(node_a.clone(), worker_a.listen_addr)
        .peer(node_b.clone(), worker_b.listen_addr)
        .build()
        .unwrap();
    client.sync_registry().unwrap();

    let addr_a = lookup_address("echo_a").expect("echo_a visible on client");
    assert_eq!(addr_a, worker_a.address);
    assert_eq!(addr_a.node, node_a);

    let addr_b = lookup_address("echo_b").expect("echo_b visible on client");
    assert_eq!(addr_b, worker_b.address);
    assert_eq!(addr_b.node, node_b);

    let router = client.router();

    let remote_a = RemoteActorRef::<Echo>::remote(addr_a, router.clone());
    let reply_a = tokio::task::spawn_blocking(move || remote_a.request_raw(Echo { n: 10 }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply_a.recv().await.unwrap(), 11);

    let remote_b = RemoteActorRef::<Echo>::remote(addr_b, router);
    let reply_b = tokio::task::spawn_blocking(move || remote_b.request_raw(Echo { n: 20 }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply_b.recv().await.unwrap(), 21);

    client.shutdown();
    worker_a.listener.shutdown();
    worker_b.listener.shutdown();
}
