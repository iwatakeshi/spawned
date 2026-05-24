//! Cross-node monitor propagation: observer on A, worker on B.

#![cfg(feature = "cluster")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, RemoteSpawnSpec, SupervisionHooks, TcpClusterListener,
};
use spawned_concurrency::cluster::{install_tasks_runtime, register_supervision_monitor_owner, start_supervision_broker};
use spawned_concurrency::message::Message;
use spawned_concurrency::monitor::{Down, MonitorRef};
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

struct Observer {
    downs: Arc<Mutex<Vec<Down>>>,
}

struct MonitorRemote(ActorAddress);
impl Message for MonitorRemote {
    type Result = MonitorRef;
}

struct GetDowns;
impl Message for GetDowns {
    type Result = Vec<Down>;
}

impl Actor for Observer {
    async fn started(&mut self, ctx: &Context<Self>) {
        register_supervision_monitor_owner(
            ctx.actor_address(),
            Arc::new({
                let ctx = ctx.clone();
                move |down| ctx.send(down)
            }),
        )
        .unwrap();
    }
}

impl Handler<MonitorRemote> for Observer {
    async fn handle(&mut self, msg: MonitorRemote, ctx: &Context<Self>) -> MonitorRef {
        ctx.monitor_address(&msg.0)
    }
}

impl Handler<Down> for Observer {
    async fn handle(&mut self, msg: Down, _ctx: &Context<Self>) {
        self.downs.lock().unwrap().push(msg);
    }
}

impl Handler<GetDowns> for Observer {
    async fn handle(&mut self, _msg: GetDowns, _ctx: &Context<Self>) -> Vec<Down> {
        self.downs.lock().unwrap().clone()
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn remote_monitor_delivers_down_on_worker_stop() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_addr = free_port();
    let server_name = NodeId::new("worker@127.0.0.1");
    let client_name = NodeId::new("observer@127.0.0.1");

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

    let observer = Observer {
        downs: Arc::new(Mutex::new(Vec::new())),
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
    let monitor_ref = observer
        .request(MonitorRemote(worker_addr.clone()))
        .await
        .unwrap();

    remote.stop().unwrap();

    for _ in 0..50 {
        let downs = observer.request(GetDowns).await.unwrap();
        if !downs.is_empty() {
            assert_eq!(downs.len(), 1);
            assert_eq!(downs[0].monitor_ref, monitor_ref);
            assert!(matches!(downs[0].reason, spawned_concurrency::ExitReason::Normal));
            observer.context().stop();
            observer.join().await;
            sup.child_handle().stop();
            sup.join().await;
            listener.shutdown();
            client_node.shutdown();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    panic!("observer did not receive Down after remote worker stop");
}
