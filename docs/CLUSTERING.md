# Clustering Guide

Spawned is evolving toward OTP-style distribution with Kameo-inspired ergonomics. **When Kameo and OTP conflict, prefer OTP semantics** — GenServer lifecycle, supervision trees, links/monitors, pg, and registry remain the architectural north star.

## North star

| Prefer (OTP) | Over (Kameo) |
|--------------|--------------|
| pg for broadcast/dispatch | Separate PubSub actor types |
| Local supervision trees | Remote restart in MVP |
| `request` / `send` + `Handler<M>` | Type-erased global PID dispatch |
| `simple_one_for_one` + pg routing | Dedicated ActorPool actor model |

## Phase 8a: addressing and wire format

Foundation crates (no network):

- **`spawned-address`** — `NodeId`, `ActorAddress`, `ActorId`, `local_node()`
- **`spawned-wire`** — `WireEnvelope`, `RemoteActor`, `RemoteMessage`, postcard codec

### Node identity

Set the local node name before starting actors (default: `spawned@localhost`):

```bash
export SPAWNED_NODE_NAME=worker@10.0.0.5
```

```rust
use spawned_concurrency::{local_node, ActorAddress, ActorId};

let node = local_node();
let addr = ActorAddress::local(actor_ref.id());
assert!(addr.is_local());
```

### Remote-capable types

Mark types that may cross the wire (requires `Serialize` + `Deserialize` on messages):

```rust
use serde::{Deserialize, Serialize};
use spawned_concurrency::{remote_actor, remote_message};

#[remote_actor]
pub struct Worker;

#[derive(Serialize, Deserialize)]
#[remote_message]
pub struct Ping {
    pub n: u32,
}
```

Stable ids are generated as `spawned.{TypeName}/v1`.

## Phase 8b: router + named registry

Enable with `spawned-concurrency` feature `cluster`:

```toml
spawned-concurrency = { version = "...", features = ["cluster"] }
```

- **`spawned-cluster`** — `ClusterRouter`, `Transport`, `UnavailableTransport` stub
- **`RemoteActorRef<M>`** — local `Recipient` when attached, else transport
- **Named registry** — `register_named`, `lookup_address`, `lookup_handle`, `unregister_named`

## Phase 8c (current): TCP transport

- **Length-framed TCP** — u32 big-endian + postcard payload; handshake (`PROTOCOL_VERSION`, `NodeId`)
- **`TcpTransport`** — client-side `Transport` with peer `SocketAddr` map and connection pooling
- **`TcpClusterListener`** — accept loop + `InboundDispatch` for inbound envelopes
- **`tasks_wire_dispatch` / `threads_wire_dispatch`** — bridge wire envelopes to local `Recipient`s
- Integration tests: `cluster/tests/tcp_smoke.rs`, `cluster/tests/two_node.rs`

Remote `request_raw` uses blocking I/O — call from async code via `tokio::task::spawn_blocking` until Phase 8d adds async transport wiring.

```rust
use spawned_concurrency::{
    TcpClusterListener, TcpTransport, ClusterRouter, tasks_wire_dispatch, RemoteActorRef,
};
use std::collections::HashMap;
use std::sync::Arc;

let dispatch = tasks_wire_dispatch(address, actor.recipient());
let listener = TcpClusterListener::bind(listen_addr, local_node, dispatch)?;

let mut peers = HashMap::new();
peers.insert(remote_node, listener.local_addr());
let router = Arc::new(ClusterRouter::new(Arc::new(TcpTransport::new(local_node, peers))));

let remote = RemoteActorRef::<Ping>::remote(target_address, router);
remote.send(Ping { n: 1 })?;
```

### Clustering checklist (every feature PR)

1. **Address, not local id** — Public handles that may be grouped or looked up use `ActorAddress`.
2. **Serializable boundary** — Cross-node messages implement `RemoteMessage`. Control plane (`Exit`, stop, OS signals) stays local.
3. **Registry names are global** — Named registration implies cluster-wide uniqueness (federation in Phase 10).
4. **pg members are addresses** — Internal pg keys use `ActorAddress`; local join fills in `local_node()`.
5. **Supervision stays local-first** — Restart/stop/kill target local mailboxes until remote supervision is designed.
6. **Threads mode** — Address/wire types are sync-safe; remote I/O may delegate to a cluster runtime (tasks MVP first).

## Roadmap (summary)

| Phase | Focus |
|-------|--------|
| **8a** | Address + wire + pg refactor |
| **8b** | `ClusterRouter`, `RemoteActorRef`, registry hooks |
| **8c** | TCP transport + two-node test (this document) |
| **8d** | `Node` / `Application` bootstrap |
| **9** | Cluster-safe parity: backoff, pg scopes, pools, unified ChildSpec |
| **10** | Federated registry, distributed pg, libp2p transport |

See [ROADMAP.md](ROADMAP.md) for full detail.
