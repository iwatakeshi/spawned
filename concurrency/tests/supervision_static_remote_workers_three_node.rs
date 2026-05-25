//! Static supervisor with remote workers on two worker nodes (three-node topology).

#![cfg(feature = "cluster")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, SupervisionEnvelope, SupervisionEvent, SupervisionHooks,
    SupervisionSignal, TcpClusterListener,
};
use spawned_concurrency::child_handle::ActorId;
use spawned_concurrency::cluster::{
    install_tasks_runtime, start_supervision_broker, SupervisionBrokerInner,
};
use spawned_concurrency::tasks::{register_remote_worker, Actor, Context, ChildSpec, Supervisor};
use spawned_concurrency::{RestartType, SupervisorStrategy};
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

static WORKER_A_STARTS: AtomicU32 = AtomicU32::new(0);
static WORKER_B_STARTS: AtomicU32 = AtomicU32::new(0);
static WORKER_A_ID: Mutex<Option<ActorId>> = Mutex::new(None);
static WORKER_B_ID: Mutex<Option<ActorId>> = Mutex::new(None);

struct Counter {
    #[allow(dead_code)]
    value: u32,
    worker: u8,
}

impl Actor for Counter {
    async fn started(&mut self, ctx: &Context<Self>) {
        match self.worker {
            1 => {
                WORKER_A_STARTS.fetch_add(1, Ordering::SeqCst);
                *WORKER_A_ID.lock().unwrap_or_else(|p| p.into_inner()) = Some(ctx.id());
            }
            2 => {
                WORKER_B_STARTS.fetch_add(1, Ordering::SeqCst);
                *WORKER_B_ID.lock().unwrap_or_else(|p| p.into_inner()) = Some(ctx.id());
            }
            _ => panic!("unexpected worker tag"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CounterInit {
    start: u32,
    worker: u8,
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

struct SupervisionWorker {
    broker_inner: Arc<SupervisionBrokerInner>,
    listener: TcpClusterListener,
    listen_addr: SocketAddr,
}

fn start_supervision_worker(name: NodeId, listen: SocketAddr) -> SupervisionWorker {
    let (_broker, broker_inner) = start_supervision_broker(name.clone());
    let control = ControlPlaneHooks::none().with_supervision(SupervisionHooks::from_fn({
        let inner = broker_inner.clone();
        Arc::new(move |envelope| inner.apply(envelope))
    }));
    let listener = TcpClusterListener::bind_with_control_plane(
        listen,
        name,
        Arc::new(AddressDispatch::new()),
        control,
    )
    .unwrap();

    SupervisionWorker {
        broker_inner,
        listen_addr: listener.local_addr(),
        listener,
    }
}

#[tokio::test]
async fn static_supervisor_remote_workers_on_two_nodes_start() {
    let _ = tracing_subscriber::fmt::try_init();

    WORKER_A_STARTS.store(0, Ordering::SeqCst);
    WORKER_B_STARTS.store(0, Ordering::SeqCst);
    *WORKER_A_ID.lock().unwrap_or_else(|p| p.into_inner()) = None;
    *WORKER_B_ID.lock().unwrap_or_else(|p| p.into_inner()) = None;

    let node_a = NodeId::new("worker_a@127.0.0.1");
    let node_b = NodeId::new("worker_b@127.0.0.1");
    let node_c = NodeId::new("sup@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", node_c.as_str());
    }

    register_remote_worker::<Counter, CounterInit>("test.Counter/v1", |init| Counter {
        value: init.start,
        worker: init.worker,
    })
    .unwrap();

    install_tasks_runtime(spawned_rt::tasks::Handle::current());

    let worker_a = start_supervision_worker(node_a.clone(), free_port());
    let worker_b = start_supervision_worker(node_b.clone(), free_port());

    let client = Node::builder()
        .name(node_c.clone())
        .peer(node_a.clone(), worker_a.listen_addr)
        .peer(node_b.clone(), worker_b.listen_addr)
        .build()
        .unwrap();

    let sup = Supervisor::builder()
        .strategy(SupervisorStrategy::OneForOne)
        .child(ChildSpec::remote_worker(
            "counter_a",
            "test.Counter/v1",
            CounterInit {
                start: 1,
                worker: 1,
            },
            node_a.clone(),
            RestartType::Permanent,
        ))
        .child(ChildSpec::remote_worker(
            "counter_b",
            "test.Counter/v1",
            CounterInit {
                start: 10,
                worker: 2,
            },
            node_b.clone(),
            RestartType::Permanent,
        ))
        .start();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if WORKER_A_STARTS.load(Ordering::SeqCst) >= 1
            && WORKER_B_STARTS.load(Ordering::SeqCst) >= 1
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "both remote workers should start within 2s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let worker_a_id = WORKER_A_ID
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .expect("worker A id");

    worker_a
        .broker_inner
        .apply(SupervisionEnvelope {
            correlation_id: 0,
            event: SupervisionEvent::Signal {
                target: ActorAddress::on(node_a.clone(), worker_a_id),
                signal: SupervisionSignal::Stop,
            },
        })
        .unwrap();

    let restart_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if WORKER_A_STARTS.load(Ordering::SeqCst) >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < restart_deadline,
            "worker A should restart after stop signal"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(WORKER_B_STARTS.load(Ordering::SeqCst), 1, "worker B unaffected");

    sup.child_handle().stop();
    sup.join().await;
    worker_a.listener.shutdown();
    worker_b.listener.shutdown();
    client.shutdown();
}
