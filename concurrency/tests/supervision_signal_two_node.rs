//! Cross-node supervision signal: remote Shutdown via SupervisionBroker.

#![cfg(feature = "cluster")]

use spawned_address::{ActorAddress, NodeId};
use spawned_cluster::{
    ControlPlaneHooks, SupervisionEnvelope, SupervisionEvent, SupervisionSignal, TcpTransport,
};
use spawned_concurrency::cluster::{
    start_supervision_broker, SupervisionHooks, SupervisionBrokerInner,
};
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_concurrency::{ExitReason, Node};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

struct Target;

impl Actor for Target {}

#[derive(Clone, Copy)]
struct Ping;

impl Message for Ping {
    type Result = ();
}

impl Handler<Ping> for Target {
    async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) {}
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn remote_shutdown_signal_via_broker() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_addr = free_port();
    let server_name = NodeId::new("sup_server@127.0.0.1");
    let client_name = NodeId::new("sup_client@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", server_name.as_str());
    }

    let server_node = Node::builder()
        .name(server_name.clone())
        .listen(server_addr)
        .build()
        .unwrap();

    let actor = Target.start();
    let address = ActorAddress::local(actor.id());
    server_node
        .register_supervision(address.clone(), actor.child_handle())
        .unwrap();

    let listen_addr = server_node.listen_addr().unwrap();

    let mut peers = HashMap::new();
    peers.insert(server_name.clone(), listen_addr);

    let (_broker, broker_inner): (_, Arc<SupervisionBrokerInner>) =
        start_supervision_broker(client_name.clone());
    let control = ControlPlaneHooks::none().with_supervision(SupervisionHooks::from_fn({
        let inner = broker_inner.clone();
        Arc::new(move |envelope| inner.apply(envelope))
    }));

    let client = Arc::new(
        TcpTransport::new(client_name.clone(), peers).with_control_plane_hooks(control),
    );
    client.sync_peers().unwrap();

    client
        .send_supervision(
            &server_name,
            SupervisionEnvelope {
                correlation_id: 0,
                event: SupervisionEvent::Signal {
                    target: address.clone(),
                    signal: SupervisionSignal::Shutdown,
                },
            },
        )
        .unwrap();

    assert_eq!(
        actor.child_handle().wait_exit_async().await,
        ExitReason::Shutdown
    );

    server_node.shutdown();
}
