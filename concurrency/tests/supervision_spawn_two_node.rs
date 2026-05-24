//! Dynamic supervisor remote spawn: supervisor on A, worker on B.

#![cfg(feature = "cluster")]

use spawned_address::NodeId;
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, RemoteSpawnSpec, SupervisionHooks, TcpClusterListener,
};
use spawned_concurrency::cluster::{install_tasks_runtime, start_supervision_broker};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{
    register_remote_worker, Actor, Context, DynamicSupervisor, DynamicSupervisorApi, Handler,
};
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static WORKER_EXITED: AtomicBool = AtomicBool::new(false);

struct Counter {
    value: u32,
}

impl Actor for Counter {
    async fn stopped(&mut self, _ctx: &Context<Self>) {
        WORKER_EXITED.store(true, Ordering::SeqCst);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CounterInit {
    start: u32,
}

#[derive(Clone, Copy)]
struct Get;

impl Message for Get {
    type Result = u32;
}

impl Handler<Get> for Counter {
    async fn handle(&mut self, _msg: Get, _ctx: &Context<Self>) -> u32 {
        self.value
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn dynamic_supervisor_spawns_remote_worker() {
    let _ = tracing_subscriber::fmt::try_init();

    WORKER_EXITED.store(false, Ordering::SeqCst);

    let server_addr = free_port();
    let server_name = NodeId::new("worker@127.0.0.1");
    let client_name = NodeId::new("sup@127.0.0.1");

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

    let sup = DynamicSupervisor::builder().start();
    let client_node = Node::builder()
        .name(client_name.clone())
        .peer(server_name.clone(), listen_addr)
        .build()
        .unwrap();

    let init = postcard::to_allocvec(&CounterInit { start: 7 }).unwrap();
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

    assert_eq!(remote.address().node, server_name);
    assert_eq!(sup.count_children().await.unwrap(), 1);

    remote.shutdown().unwrap();

    for _ in 0..50 {
        if WORKER_EXITED.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(WORKER_EXITED.load(Ordering::SeqCst));

    sup.child_handle().stop();
    sup.join().await;
    listener.shutdown();
    client_node.shutdown();
}
