//! Cross-node link propagation: trap actor on A, worker on B.

#![cfg(feature = "cluster")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, RemoteSpawnSpec, SupervisionHooks, TcpClusterListener,
};
use spawned_concurrency::cluster::{install_tasks_runtime, register_supervision_actor, start_supervision_broker};
use spawned_concurrency::link::Exit;
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{
    register_remote_worker, Actor, ActorStart, Context, DynamicSupervisor, DynamicSupervisorApi,
    Handler,
};
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};

struct Counter {
    value: u32,
}

impl Actor for Counter {}

#[derive(serde::Serialize, serde::Deserialize)]
struct CounterInit {
    start: u32,
}

struct TrapObserver {
    exits: Arc<Mutex<Vec<Exit>>>,
}

struct LinkRemote(ActorAddress);
impl Message for LinkRemote {
    type Result = ();
}

struct GetExits;
impl Message for GetExits {
    type Result = Vec<Exit>;
}

impl Actor for TrapObserver {
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
        register_supervision_actor(ctx.actor_address(), ctx.child_handle()).unwrap();
    }

    async fn exit_received(&mut self, exit: Exit, _ctx: &Context<Self>) {
        self.exits.lock().unwrap().push(exit);
    }
}

impl Handler<LinkRemote> for TrapObserver {
    async fn handle(&mut self, msg: LinkRemote, ctx: &Context<Self>) {
        ctx.link_address(&msg.0);
    }
}

impl Handler<GetExits> for TrapObserver {
    async fn handle(&mut self, _msg: GetExits, _ctx: &Context<Self>) -> Vec<Exit> {
        self.exits.lock().unwrap().clone()
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn remote_link_delivers_exit_on_worker_stop() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_addr = free_port();
    let server_name = NodeId::new("worker@127.0.0.1");
    let client_name = NodeId::new("trap@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", client_name.as_str());
    }

    register_remote_worker::<Counter, CounterInit>("test.Counter/v1", |init| Counter {
        value: init.start,
    })
    .unwrap();

    install_tasks_runtime(spawned_rt::tasks::Handle::current());

    let (_broker, broker_inner) = start_supervision_broker(server_name.clone());
    let control = ControlPlaneHooks::none().with_supervision(SupervisionHooks::from_fn({
        let inner = broker_inner.clone();
        Arc::new(move |envelope| inner.apply(envelope))
    }));
    let listener = TcpClusterListener::bind_with_control_plane(
        server_addr,
        server_name.clone(),
        Arc::new(AddressDispatch::new()),
        control,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let client_node = Node::builder()
        .name(client_name.clone())
        .peer(server_name.clone(), listen_addr)
        .build()
        .unwrap();

    let trap = TrapObserver {
        exits: Arc::new(Mutex::new(Vec::new())),
    }
    .start();

    let sup = DynamicSupervisor::builder().start();

    let init = postcard::to_allocvec(&CounterInit { start: 1 }).unwrap();
    let remote = sup
        .start_child_remote(
            RemoteSpawnSpec::Worker {
                worker_type: "test.Counter/v1".into(),
                init,
            },
            server_name.clone(),
        )
        .await
        .unwrap()
        .unwrap();

    let worker_addr = remote.address().clone();
    trap.request(LinkRemote(worker_addr.clone())).await.unwrap();

    remote.stop().unwrap();

    for _ in 0..50 {
        let exits = trap.request(GetExits).await.unwrap();
        if !exits.is_empty() {
            assert_eq!(exits.len(), 1);
            assert_eq!(exits[0].from, worker_addr);
            assert!(matches!(exits[0].reason, spawned_concurrency::ExitReason::Normal));
            trap.context().stop();
            trap.join().await;
            sup.child_handle().stop();
            sup.join().await;
            listener.shutdown();
            client_node.shutdown();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    panic!("trap actor did not receive Exit after remote worker stop");
}
