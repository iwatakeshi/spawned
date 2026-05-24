//! Cross-node ping/pong using the Phase 8d [`Node`] bootstrap API.
//!
//! Run two processes:
//!
//! ```text
//! # terminal 1 — pong server
//! cargo run -p cluster_ping_pong -- pong --name pong@127.0.0.1 --listen 127.0.0.1:9101
//!
//! # terminal 2 — ping client (use the address printed by pong)
//! cargo run -p cluster_ping_pong -- ping \
//!   --name ping@127.0.0.1 \
//!   --peer pong@127.0.0.1=127.0.0.1:9101 \
//!   --target 'pong@127.0.0.1#ActorId(2)'
//! ```

use spawned_address::{ActorAddress, ActorId, NodeId};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{remote_message, Node, RemoteActorRef};
use spawned_rt::tasks as rt;
use std::env;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize)]
#[remote_message]
struct Ping {
    n: u32,
}

impl Message for Ping {
    type Result = Pong;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
struct Pong {
    n: u32,
}

struct PongActor;

impl Actor for PongActor {}

impl Handler<Ping> for PongActor {
    async fn handle(&mut self, msg: Ping, _ctx: &Context<Self>) -> Pong {
        tracing::info!("received Ping({})", msg.n);
        Pong { n: msg.n + 1 }
    }
}

fn usage() -> ! {
    eprintln!(
        "Usage:\n  \
         cluster_ping_pong pong --name NAME --listen HOST:PORT\n  \
         cluster_ping_pong ping --name NAME --peer NAME=HOST:PORT --target ADDRESS\n\n\
         Target format: node@host#ActorId(N) (printed by the pong process)."
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

fn parse_target(s: &str) -> ActorAddress {
    let (node, id_part) = s.split_once('#').unwrap_or_else(|| {
        eprintln!("invalid --target {s:?}, expected node@host#ActorId(N)");
        std::process::exit(1);
    });
    let id_str = id_part
        .strip_prefix("ActorId(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or_else(|| {
            eprintln!("invalid actor id in target {s:?}");
            std::process::exit(1);
        });
    let raw: u64 = id_str.parse().unwrap_or_else(|_| {
        eprintln!("invalid actor id number in target {s:?}");
        std::process::exit(1);
    });
    ActorAddress::on(parse_node(node), ActorId::from_raw(raw))
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
    let mut target = None;

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
            "--target" => target = Some(parse_target(&args.next().unwrap_or_else(|| usage()))),
            other => {
                eprintln!("unknown argument {other}");
                usage();
            }
        }
        arg = args.next();
    }

    rt::run(async {
        match mode.as_str() {
            "pong" => run_pong(name, listen).await,
            "ping" => run_ping(name, peers, target).await,
            _ => usage(),
        }
    });
}

async fn run_pong(name: Option<NodeId>, listen: Option<SocketAddr>) {
    let listen = listen.unwrap_or_else(|| {
        eprintln!("pong mode requires --listen HOST:PORT");
        std::process::exit(1);
    });

    let mut builder = Node::builder().listen(listen);
    if let Some(name) = name {
        builder = builder.name(name);
    }
    let node = builder.build().expect("start pong node");

    let actor = PongActor.start();
    let address = ActorAddress::local(actor.id());
    node.register_tasks_wire(address.clone(), actor.recipient());

    println!("=== Pong node ===");
    println!("Local node: {}", node.local_node());
    println!("Listen:     {}", node.listen_addr().unwrap());
    println!("Target:     {address}");
    println!("Waiting for remote pings (Ctrl+C to stop)...\n");

    actor.join().await;
    node.shutdown();
}

async fn run_ping(
    name: Option<NodeId>,
    peers: Vec<(NodeId, SocketAddr)>,
    target: Option<ActorAddress>,
) {
    let target = target.unwrap_or_else(|| {
        eprintln!("ping mode requires --target node@host#ActorId(N)");
        std::process::exit(1);
    });
    if peers.is_empty() {
        eprintln!("ping mode requires at least one --peer NAME=HOST:PORT");
        std::process::exit(1);
    }

    let mut builder = Node::builder();
    if let Some(name) = name {
        builder = builder.name(name);
    }
    for (peer, addr) in peers {
        builder = builder.peer(peer, addr);
    }
    let node = builder.build().expect("start ping node");

    rt::sleep(Duration::from_millis(200)).await;

    let remote = RemoteActorRef::<Ping>::remote(target.clone(), node.router());
    println!("=== Ping node ===");
    println!("Local node: {}", node.local_node());
    println!("Target:     {target}\n");

    for n in 1..=5 {
        let remote = remote.clone();
        let reply = rt::spawn_blocking(move || remote.request_raw(Ping { n }))
            .await
            .expect("spawn_blocking")
            .expect("remote request");
        let pong = reply.recv().await.expect("decode reply");
        println!("Ping({n}) -> Pong({})", pong.n);
        assert_eq!(pong.n, n + 1);
        rt::sleep(Duration::from_millis(100)).await;
    }

    println!("\nDone.");
    node.shutdown();
}
