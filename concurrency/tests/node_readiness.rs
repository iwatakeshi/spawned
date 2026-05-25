//! Node readiness after cluster bootstrap.

#![cfg(feature = "cluster")]

use spawned_address::NodeId;
use spawned_concurrency::Node;
use std::net::{SocketAddr, TcpListener};

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test]
async fn node_with_listen_and_broker_is_ready() {
    let addr = free_port();
    let name = NodeId::new("ready@127.0.0.1");

    unsafe {
        std::env::set_var("SPAWNED_NODE_NAME", name.as_str());
    }

    let node = Node::builder().name(name).listen(addr).build().unwrap();

    for _ in 0..50 {
        if node.is_ready() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let readiness = node.readiness();
    assert!(readiness.router_installed);
    assert!(readiness.supervision_enabled);
    assert!(readiness.listen_configured);
    assert!(readiness.listener_active);
    assert!(readiness.broker_alive);
    assert!(readiness.is_ready());

    node.shutdown();
}
