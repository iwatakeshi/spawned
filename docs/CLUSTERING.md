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

## Phase 8d (current): Node bootstrap

`NodeBuilder` is the standard entry point for cluster-aware apps (requires `cluster` feature):

```rust
use spawned_concurrency::Node;
use std::net::SocketAddr;

let node = Node::builder()
    .name("worker@10.0.0.5")
    .listen("0.0.0.0:9000".parse::<SocketAddr>()?)
    .peer("peer@10.0.0.2", "10.0.0.2:9000".parse()?)
    .shutdown_on_signal(&[sup.child_handle()])
    .build()?;

node.register_tasks_wire(ActorAddress::local(actor.id()), actor.recipient());
```

- Installs the process-global [`ClusterRouter`] with [`TcpTransport`] when peers are configured
- Starts [`TcpClusterListener`] when `listen` is set; routes via [`AddressDispatch`]
- Spawns the OS signal dispatcher and registers shutdown handles when provided
- Example: `examples/cluster_ping_pong` (two-terminal demo)

For signal-only bootstrap (no cluster TCP), omit `listen` / `peer` — see `examples/http_workers`.

### Phase 10.1: Federated registry

When a [`Node`] has `listen` and/or `peer` configured, `register_named` replicates across the cluster:

```rust
use spawned_concurrency::{register_named, lookup_address, Node, RemoteActorRef};

// Server
register_named("pong", actor.child_handle())?;
node.register_tasks_wire(ActorAddress::local(actor.id()), actor.recipient());

// Client (after Node::sync_registry())
let addr = lookup_address("pong").expect("federated name");
let remote = RemoteActorRef::<Ping>::remote(addr, node.router());
```

- Control plane: `RegistryEvent` (`Register`, `Unregister`, `Snapshot`) over TCP
- Actor data plane unchanged: `ClusterFrame::Actor(WireEnvelope)`
- `lookup_handle` remains local-only; use `lookup_address` + `RemoteActorRef` for remote actors
- Example: `examples/cluster_ping_pong` (ping discovers pong by name)

## Phase 8c: TCP transport

- **Length-framed TCP** — u32 big-endian + postcard payload; handshake (`PROTOCOL_VERSION = 3`, `NodeId`)
- **`TcpTransport`** — client-side `Transport` with peer `SocketAddr` map and connection pooling
- **`TcpClusterListener`** — accept loop + `InboundDispatch` for inbound envelopes
- **`tasks_wire_dispatch` / `threads_wire_dispatch`** — bridge wire envelopes to local `Recipient`s
- Integration tests: `cluster/tests/tcp_smoke.rs`, `cluster/tests/two_node.rs`

Remote `request_raw` uses blocking I/O — use `request_async` in async handlers (Phase 11.2), or `spawn_blocking` for legacy sync paths.

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

## Phase 10.3: libp2p transport

Optional feature on `spawned-cluster` — same `ClusterFrame` wire format as TCP, carried over libp2p request-response (`/spawned/cluster/3`).

```toml
spawned-cluster = { version = "...", features = ["libp2p"] }
```

```rust
use libp2p::{identity, Multiaddr, PeerId};
use spawned_cluster::{ClusterRouter, ControlPlaneHooks, Libp2pCluster, Libp2pPeer};
use std::sync::Arc;

let keypair = identity::Keypair::generate_ed25519();
let listen: Multiaddr = "/ip4/0.0.0.0/tcp/9000".parse()?;
let peers = vec![Libp2pPeer {
    node: remote_node,
    peer_id: remote_peer_id,
    addr: format!("/ip4/10.0.0.2/tcp/9000/p2p/{remote_peer_id}").parse()?,
}];

let cluster = Libp2pCluster::start(
    keypair,
    local_node,
    listen,
    peers,
    dispatch,
    ControlPlaneHooks::federated(/* registry + pg hooks */),
)?;

cluster.sync_peers()?;
let router = Arc::new(ClusterRouter::new(Arc::new(cluster)));
```

- Static peer map (like TCP `HashMap<NodeId, SocketAddr>`): each peer needs `NodeId`, `PeerId`, and dialable `Multiaddr`
- Background OS thread runs the libp2p swarm
- Control-plane snapshots exchange on connect; actor replies are `WireReply` bytes (not `ClusterFrame`)
- Integration test: `cargo test -p spawned-cluster --features libp2p --test libp2p_two_node`

### NodeBuilder libp2p bootstrap (Phase 11.1)

Enable with `cluster-libp2p` on `spawned-concurrency`:

```toml
spawned-concurrency = { version = "...", features = ["cluster-libp2p"] }
```

```rust
use spawned_concurrency::{identity, Node, RemoteActorRef};

let server_key = identity::Keypair::generate_ed25519();
let server_peer = server_key.public().to_peer_id();

let node = Node::builder()
    .name("worker@10.0.0.5")
    .transport_libp2p(Some(server_key))
    .listen_libp2p("/ip4/0.0.0.0/tcp/9000".parse()?)
    .libp2p_peer("peer@10.0.0.2", remote_peer_id, remote_multiaddr)
    .build()?;

node.register_tasks_wire(address, actor.recipient());
node.sync_registry()?;
```

TCP and libp2p listen/peer options are mutually exclusive on a single node.

## Phase 11.2: Async remote requests

Prefer `RemoteActorRef::request_async` in async code — it uses native async I/O for libp2p and offloads TCP to a blocking pool without stalling the runtime:

```rust
let remote = RemoteActorRef::<Ping>::remote(address, node.router());
let pong = remote.request_async(Ping { n: 1 }).await?;
```

Sync `request_raw` remains for threads mode and legacy call sites.

## Phase 12.1: Supervision control plane (wire only)

Supervision events use a **routed** control plane — unicast to a target node, not federated broadcast like registry/pg.

- `ClusterFrame::Supervision(SupervisionEnvelope)` on TCP and libp2p
- `SupervisionEnvelope { correlation_id, event }` — non-zero id for RPC (`SpawnRequest` → `SpawnOk`/`SpawnErr`); zero for fire-and-forget (`ChildExit`, `Monitor`, `Link`, …)
- Correlated replies are raw `SupervisionEnvelope` bytes (not `WireReply`)
- Connect handshake stays two frames (registry + pg snapshots); no supervision snapshot on connect
- Wire protocol version **3** (`PROTOCOL_VERSION` / `/spawned/cluster/3`); mixed v2/v3 clusters fail at handshake
- `TcpTransport::send_supervision` / `request_supervision`; `Libp2pCluster::send_supervision_to` / `request_supervision_from`
- `ControlPlaneHooks::with_supervision` installs inbound handler; `stub_supervision_hooks()` for tests
- `install_supervision_sync` publish hook stub in `spawned-concurrency` (broker wiring in Phase 12.2)
- Max remote worker init payload: 64 KiB (`MAX_REMOTE_SPAWN_INIT_BYTES`)

Integration tests: `cluster/tests/supervision_protocol.rs`, `supervision_tcp_roundtrip.rs`, `supervision_libp2p_roundtrip.rs`.

### Phase 12.2: SupervisionBroker (shipped)

Each cluster node runs a **SupervisionBroker** actor that handles inbound supervision wire events:

- `NodeBuilder` starts the broker when listen/peer is configured and wires `ControlPlaneHooks::with_supervision`
- `install_supervision_sync` routes outbound envelopes unicast via `TcpTransport::send_supervision` / `Libp2pCluster::send_supervision_to`
- `Node::register_supervision(address, child_handle)` registers local actors for inbound `Signal` delivery

Integration test: `concurrency/tests/supervision_signal_two_node.rs` (remote `Signal::Shutdown`).

### Phase 12.3: Remote spawn (shipped)

Remote child spawn closes the loop: a supervisor on node A can start a worker on node B and receive a [`RemoteChildHandle`](../../concurrency/src/cluster/remote_spawn.rs).

**Registries** (process-global, separate tasks/threads tables):

```rust
register_remote_worker::<Counter, CounterInit>("spawned.Counter/v1", |init| Counter { .. })?;
register_remote_spec("api_worker", || ChildSpec::worker("api", || ApiServer::new(), RestartType::Permanent))?;
```

**Broker inbound `SpawnRequest`:**

1. Validate `placement == local`
2. Resolve worker type or named spec from registry
3. Start linked to broker (`link = true`) or unlinked
4. Auto-register `ChildHandle` for remote signals; record `(child → parent)` for Phase 12.4

**Client API:**

- `DynamicSupervisor::start_child_remote(spec, placement)` → `RemoteChildHandle`
- `RemoteChildHandle::shutdown` / `stop` / `kill` via supervision publish
- `install_supervision_request` + `request_spawn` for correlated spawn RPC
- `install_tasks_runtime(Handle::current())` when using a manual TCP listener (worker node without `Node::builder`)

Interim linking: `link = true` links the child to the **placement node's broker**; true cross-node link arrives in Phase 12.6.

Integration test: `concurrency/tests/supervision_spawn_two_node.rs`.

### Clustering checklist (every feature PR)

1. **Address, not local id** — Public handles that may be grouped or looked up use `ActorAddress`.
2. **Serializable boundary** — Cross-node messages implement `RemoteMessage`. Control plane (`Exit`, stop, OS signals) stays local.
3. **Registry names are global** — Named registration implies cluster-wide uniqueness (federation in Phase 10).
4. **pg members are addresses** — Internal pg keys use `ActorAddress`; local join fills in `local_node()`.
5. **Supervision signals** — Register local actors with `Node::register_supervision` for remote stop/shutdown/kill; remote spawn via `register_remote_worker` / `DynamicSupervisor::start_child_remote` (Phase 12.3).
6. **Threads mode** — Address/wire types are sync-safe; remote I/O may delegate to a cluster runtime (tasks MVP first).

## Roadmap (summary)

| Phase | Focus |
|-------|--------|
| **8a** | Address + wire + pg refactor |
| **8b** | `ClusterRouter`, `RemoteActorRef`, registry hooks |
| **8c** | TCP transport + two-node test |
| **8d** | `Node` bootstrap (this document) |
| **9** | Cluster-safe parity: backoff, pg scopes, pools, unified ChildSpec |
| **10.1** | Federated registry (this document) |
| **10.2** | Distributed pg (this document) |
| **10.3** | libp2p transport (this document) |
| **11.1** | NodeBuilder libp2p bootstrap (this document) |
| **11.2** | Async remote requests (this document) |
| **12.1** | Supervision control plane protocol (this document) |
| **12.2** | SupervisionBroker + Node wiring (this document) |
| **12.3** | Remote spawn registry + DynamicSupervisor API (this document) |

See [ROADMAP.md](ROADMAP.md) for full detail.
