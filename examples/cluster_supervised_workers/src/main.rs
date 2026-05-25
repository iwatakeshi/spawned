//! Static supervisor with local + remote worker children across two nodes.
//!
//! Run two processes:
//!
//! ```text
//! # terminal 1 — remote worker placement node
//! cargo run -p cluster_supervised_workers -- worker \
//!   --name worker@127.0.0.1 --listen 127.0.0.1:9201
//!
//! # terminal 2 — supervisor node (local + remote children)
//! cargo run -p cluster_supervised_workers -- supervisor \
//!   --name sup@127.0.0.1 \
//!   --peer worker@127.0.0.1=127.0.0.1:9201
//! ```

use spawned_address::NodeId;
use spawned_cluster::{
    AddressDispatch, ControlPlaneHooks, SupervisionHooks, TcpClusterListener,
};
use spawned_concurrency::cluster::{install_tasks_runtime, start_supervision_broker};
use spawned_concurrency::tasks::{
    register_remote_worker, Actor, ChildSpec, Context, Supervisor,
};
use spawned_concurrency::{Application, RestartType, SupervisorStrategy};
use spawned_rt::tasks as rt;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WORKER_TYPE: &str = "spawned.cluster_demo.Worker/v1";

static REMOTE_STARTS: AtomicU32 = AtomicU32::new(0);

struct RemoteWorker {
    id: u32,
}

impl Actor for RemoteWorker {
    async fn started(&mut self, ctx: &Context<Self>) {
        let n = REMOTE_STARTS.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            remote_id = self.id,
            generation = n,
            actor = %ctx.id(),
            "remote worker started on placement node"
        );
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        tracing::info!(remote_id = self.id, "remote worker stopped");
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RemoteInit {
    id: u32,
}

struct LocalWorker {
    name: String,
    starts: Arc<AtomicU32>,
}

impl Actor for LocalWorker {
    async fn started(&mut self, ctx: &Context<Self>) {
        let n = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            name = %self.name,
            generation = n,
            actor = %ctx.id(),
            "local worker started on supervisor node"
        );
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        tracing::info!(name = %self.name, "local worker stopped");
    }
}

fn usage() -> ! {
    eprintln!(
        "Usage:\n  \
         cluster_supervised_workers worker --name NAME --listen HOST:PORT\n  \
         cluster_supervised_workers supervisor --name NAME --peer NAME=HOST:PORT\n"
    );
    std::process::exit(1);
}

fn parse_node(s: &str) -> NodeId {
    NodeId::new(s)
}

fn parse_peer(s: &str) -> (NodeId, SocketAddr) {
    let (name, addr) = s.split_once('=').unwrap_or_else(|| {
        eprintln!("invalid --peer {s:?}, expected NAME=HOST:PORT");
        std::process::exit(1);
    });
    (
        parse_node(name),
        addr.parse().unwrap_or_else(|_| {
            eprintln!("invalid peer address {addr:?}");
            std::process::exit(1);
        }),
    )
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| usage());
    let mut name = None;
    let mut listen = None;
    let mut peers = Vec::new();

    let mut arg = args.next();
    while let Some(token) = arg {
        match token.as_str() {
            "--name" => name = Some(parse_node(&args.next().unwrap_or_else(|| usage()))),
            "--listen" => {
                listen = Some(
                    args.next()
                        .unwrap_or_else(|| usage())
                        .parse::<SocketAddr>()
                        .unwrap_or_else(|_| {
                            eprintln!("invalid --listen address");
                            std::process::exit(1);
                        }),
                );
            }
            "--peer" => peers.push(parse_peer(&args.next().unwrap_or_else(|| usage()))),
            other => {
                eprintln!("unknown argument {other}");
                usage();
            }
        }
        arg = args.next();
    }

    rt::run(async {
        match mode.as_str() {
            "worker" => run_worker(name, listen).await,
            "supervisor" => run_supervisor(name, peers).await,
            _ => usage(),
        }
    });
}

async fn run_worker(name: Option<NodeId>, listen: Option<SocketAddr>) {
    let listen = listen.unwrap_or_else(|| {
        eprintln!("worker mode requires --listen HOST:PORT");
        std::process::exit(1);
    });
    let worker_name = name.unwrap_or_else(|| NodeId::new("worker@127.0.0.1"));

    register_remote_worker::<RemoteWorker, RemoteInit>(WORKER_TYPE, |init| RemoteWorker {
        id: init.id,
    })
    .expect("register remote worker");

    install_tasks_runtime(spawned_rt::tasks::Handle::current());

    let (_broker, broker_inner) = start_supervision_broker(worker_name.clone());
    let control = ControlPlaneHooks::none().with_supervision(SupervisionHooks::from_fn({
        let inner = broker_inner.clone();
        Arc::new(move |envelope| inner.apply(envelope))
    }));
    let listener = TcpClusterListener::bind_with_control_plane(
        listen,
        worker_name.clone(),
        Arc::new(AddressDispatch::new()),
        control,
    )
    .expect("bind worker listener");

    println!("=== Worker placement node ===");
    println!("Node:        {worker_name}");
    println!("Listen:      {}", listener.local_addr());
    println!("Worker type: {WORKER_TYPE}");
    println!("Waiting for remote spawn requests (Ctrl+C to stop)...\n");

    rt::wait_shutdown_signal().await;
    listener.shutdown();
}

async fn run_supervisor(name: Option<NodeId>, peers: Vec<(NodeId, SocketAddr)>) {
    if peers.len() != 1 {
        eprintln!("supervisor mode requires exactly one --peer (worker placement node)");
        std::process::exit(1);
    }
    let (worker_node, worker_addr) = peers.into_iter().next().unwrap();

    REMOTE_STARTS.store(0, Ordering::SeqCst);
    let local_starts = Arc::new(AtomicU32::new(0));

    let mut builder = Application::builder().peer(worker_node.clone(), worker_addr);
    if let Some(name) = name {
        builder = builder.name(name);
    }

    let local_starts_for_spec = local_starts.clone();
    let worker_node_for_spec = worker_node.clone();

    let app = builder
        .start(move |_ctx| {
            let local_starts = local_starts.clone();
            let local_starts_for_spec = local_starts_for_spec.clone();
            let worker_node_for_spec = worker_node_for_spec.clone();
            async move {
                println!("=== Supervisor node ===");
                println!("Worker node: {worker_node_for_spec}");
                println!("Starting static supervisor (local + remote_worker)...\n");

                let sup = Supervisor::builder()
                    .strategy(SupervisorStrategy::OneForOne)
                    .child(ChildSpec::worker(
                        "local",
                        move || LocalWorker {
                            name: "local".into(),
                            starts: local_starts_for_spec.clone(),
                        },
                        RestartType::Permanent,
                    ))
                    .child(ChildSpec::remote_worker(
                        "remote",
                        WORKER_TYPE,
                        RemoteInit { id: 1 },
                        worker_node_for_spec,
                        RestartType::Permanent,
                    ))
                    .start();

                for _ in 0..100 {
                    if REMOTE_STARTS.load(Ordering::SeqCst) >= 1
                        && local_starts.load(Ordering::SeqCst) >= 1
                    {
                        break;
                    }
                    rt::sleep(Duration::from_millis(20)).await;
                }

                println!(
                    "Both children running — local starts: {}, remote starts: {}",
                    local_starts.load(Ordering::SeqCst),
                    REMOTE_STARTS.load(Ordering::SeqCst),
                );
                println!("Supervisor will restart children on abnormal exit.");
                println!("Press Ctrl+C to shut down.\n");

                Ok(vec![sup.child_handle()])
            }
        })
        .await
        .expect("start supervisor application");

    app.run().await;
}
