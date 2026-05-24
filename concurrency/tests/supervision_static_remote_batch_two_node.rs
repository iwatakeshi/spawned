//! Static supervisor OneForAll with local + remote children across two nodes.

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

static REMOTE_STARTS: AtomicU32 = AtomicU32::new(0);
static REMOTE_ACTOR_ID: Mutex<Option<ActorId>> = Mutex::new(None);

struct RemoteCounter {
    value: u32,
}

impl Actor for RemoteCounter {
    async fn started(&mut self, ctx: &Context<Self>) {
        REMOTE_STARTS.fetch_add(1, Ordering::SeqCst);
        *REMOTE_ACTOR_ID.lock().unwrap_or_else(|p| p.into_inner()) = Some(ctx.id());
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RemoteInit {
    start: u32,
}

struct LocalCounter {
    starts: Arc<AtomicU32>,
}

impl Actor for LocalCounter {
    async fn started(&mut self, _ctx: &Context<Self>) {
        self.starts.fetch_add(1, Ordering::SeqCst);
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn static_supervisor_one_for_all_restarts_local_and_remote() {
    let _ = tracing_subscriber::fmt::try_init();

    REMOTE_STARTS.store(0, Ordering::SeqCst);
    *REMOTE_ACTOR_ID.lock().unwrap_or_else(|p| p.into_inner()) = None;
    let local_starts = Arc::new(AtomicU32::new(0));

    let server_addr = free_port();
    let server_name = NodeId::new("worker@127.0.0.1");
    let client_name = NodeId::new("sup@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", client_name.as_str());
    }

    register_remote_worker::<RemoteCounter, RemoteInit>("test.Remote/v1", |init| RemoteCounter {
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

    let local_starts_for_spec = local_starts.clone();
    let sup = Supervisor::builder()
        .strategy(SupervisorStrategy::OneForAll)
        .child(
            ChildSpec::worker(
                "local",
                move || LocalCounter {
                    starts: local_starts_for_spec.clone(),
                },
                RestartType::Permanent,
            ),
        )
        .child(ChildSpec::remote_worker(
            "remote",
            "test.Remote/v1",
            RemoteInit { start: 0 },
            server_name.clone(),
            RestartType::Permanent,
        ))
        .start();

    for _ in 0..50 {
        if REMOTE_STARTS.load(Ordering::SeqCst) >= 1 && local_starts.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(REMOTE_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(local_starts.load(Ordering::SeqCst), 1);

    let remote_id = REMOTE_ACTOR_ID
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .expect("remote worker id");

    broker_inner
        .apply(SupervisionEnvelope {
            correlation_id: 0,
            event: SupervisionEvent::Signal {
                target: ActorAddress::on(server_name.clone(), remote_id),
                signal: SupervisionSignal::Stop,
            },
        })
        .unwrap();

    for _ in 0..100 {
        if REMOTE_STARTS.load(Ordering::SeqCst) >= 2 && local_starts.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        REMOTE_STARTS.load(Ordering::SeqCst),
        2,
        "remote child should restart after OneForAll batch"
    );
    assert_eq!(
        local_starts.load(Ordering::SeqCst),
        2,
        "local child should restart after OneForAll batch"
    );

    sup.child_handle().stop();
    sup.join().await;
    listener.shutdown();
    client_node.shutdown();
}
