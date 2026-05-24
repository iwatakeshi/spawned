//! Static supervisor remote_worker: supervisor on A, worker on B.

#![cfg(feature = "cluster")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, SupervisionEnvelope, SupervisionEvent, SupervisionHooks,
    SupervisionSignal, TcpClusterListener,
};
use spawned_concurrency::child_handle::ActorId;
use spawned_concurrency::cluster::{install_tasks_runtime, start_supervision_broker};
use spawned_concurrency::tasks::{register_remote_worker, Actor, Context, ChildSpec, Supervisor};
use spawned_concurrency::{RestartType, SupervisorStrategy};
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

static START_COUNT: AtomicU32 = AtomicU32::new(0);
static REMOTE_ACTOR_ID: Mutex<Option<ActorId>> = Mutex::new(None);

struct Counter {
    value: u32,
}

impl Actor for Counter {
    async fn started(&mut self, ctx: &Context<Self>) {
        START_COUNT.fetch_add(1, Ordering::SeqCst);
        *REMOTE_ACTOR_ID.lock().unwrap_or_else(|p| p.into_inner()) = Some(ctx.id());
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CounterInit {
    start: u32,
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn static_supervisor_remote_worker_restarts_on_exit() {
    let _ = tracing_subscriber::fmt::try_init();

    START_COUNT.store(0, Ordering::SeqCst);
    *REMOTE_ACTOR_ID.lock().unwrap_or_else(|p| p.into_inner()) = None;

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

    let client_node = Node::builder()
        .name(client_name.clone())
        .peer(server_name.clone(), listen_addr)
        .build()
        .unwrap();

    let sup = Supervisor::builder()
        .strategy(SupervisorStrategy::OneForOne)
        .child(ChildSpec::remote_worker(
            "counter",
            "test.Counter/v1",
            CounterInit { start: 1 },
            server_name.clone(),
            RestartType::Permanent,
        ))
        .start();

    for _ in 0..50 {
        if START_COUNT.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(START_COUNT.load(Ordering::SeqCst), 1);

    let first_id = REMOTE_ACTOR_ID
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .expect("remote worker id");

    broker_inner
        .apply(SupervisionEnvelope {
            correlation_id: 0,
            event: SupervisionEvent::Signal {
                target: ActorAddress::on(server_name.clone(), first_id),
                signal: SupervisionSignal::Stop,
            },
        })
        .unwrap();

    for _ in 0..50 {
        if START_COUNT.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(START_COUNT.load(Ordering::SeqCst), 2);

    let second_id = REMOTE_ACTOR_ID
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .expect("remote worker id after restart");
    assert_ne!(second_id, first_id);

    sup.child_handle().stop();
    sup.join().await;
    listener.shutdown();
    client_node.shutdown();
}
