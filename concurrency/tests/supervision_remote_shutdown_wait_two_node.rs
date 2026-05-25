//! Static supervisor `stopped()` blocks until remote ChildExit (Phase 13).

#![cfg(feature = "cluster")]

use spawned_address::NodeId;
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, SupervisionHooks, TcpClusterListener,
};
use spawned_concurrency::cluster::{install_tasks_runtime, start_supervision_broker};
use spawned_concurrency::tasks::{register_remote_worker, Actor, Context, ChildSpec, Supervisor};
use spawned_concurrency::{RestartType, SupervisorStrategy};
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

static REMOTE_STARTED: AtomicU32 = AtomicU32::new(0);
static REMOTE_STOPPED: AtomicBool = AtomicBool::new(false);

struct RemoteWorker;

impl Actor for RemoteWorker {
    async fn started(&mut self, _ctx: &Context<Self>) {
        REMOTE_STARTED.fetch_add(1, Ordering::SeqCst);
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        REMOTE_STOPPED.store(true, Ordering::SeqCst);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RemoteInit;

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn static_supervisor_stopped_waits_for_remote_child_exit() {
    let _ = tracing_subscriber::fmt::try_init();

    REMOTE_STARTED.store(0, Ordering::SeqCst);
    REMOTE_STOPPED.store(false, Ordering::SeqCst);

    let server_addr = free_port();
    let server_name = NodeId::new("worker@127.0.0.1");
    let client_name = NodeId::new("sup@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", client_name.as_str());
    }

    register_remote_worker::<RemoteWorker, RemoteInit>("test.RemoteShutdown/v1", |_| RemoteWorker)
        .unwrap();

    install_tasks_runtime(spawned_rt::tasks::Handle::current());

    let (_broker, _broker_inner) = start_supervision_broker(server_name.clone());
    let control = ControlPlaneHooks::none().with_supervision(SupervisionHooks::from_fn({
        let inner = _broker_inner.clone();
        std::sync::Arc::new(move |envelope| inner.apply(envelope))
    }));
    let listener = TcpClusterListener::bind_with_control_plane(
        server_addr,
        server_name.clone(),
        std::sync::Arc::new(AddressDispatch::new()),
        control,
    )
    .unwrap();
    let listen_addr = listener.local_addr();

    let client_node = Node::builder()
        .name(client_name.clone())
        .peer(server_name.clone(), listen_addr)
        .build()
        .unwrap();

    let sup = Supervisor::builder()
        .strategy(SupervisorStrategy::OneForOne)
        .child(ChildSpec::remote_worker(
            "remote",
            "test.RemoteShutdown/v1",
            RemoteInit,
            server_name.clone(),
            RestartType::Permanent,
        ))
        .start();

    for _ in 0..50 {
        if REMOTE_STARTED.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(REMOTE_STARTED.load(Ordering::SeqCst), 1);

    let join = tokio::spawn(async move {
        sup.child_handle().stop();
        sup.join().await;
    });

    for _ in 0..100 {
        if join.is_finished() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    assert!(
        REMOTE_STOPPED.load(Ordering::SeqCst),
        "remote child must exit before supervisor join completes"
    );
    join.await.unwrap();

    listener.shutdown();
    client_node.shutdown();
}
