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

## Phase 8b (current): router + named registry

Enable with `spawned-concurrency` feature `cluster`:

```toml
spawned-concurrency = { version = "...", features = ["cluster"] }
```

- **`spawned-cluster`** — [`ClusterRouter`], [`Transport`] trait, [`UnavailableTransport`] stub
- **`RemoteActorRef<M>`** — routes by [`ActorAddress::is_local()`]: local `Recipient` or remote transport
- **Named registry** — `register_named`, `lookup_address`, `unregister_named` (cluster-wide names, local handles)

Remote send/request returns [`ActorError::RemoteUnreachable`] until Phase 8c wires TCP.

```rust
use spawned_concurrency::{
    register_named, lookup_address, RemoteActorRef, ClusterRouter,
};

register_named("worker", child_handle)?;
let addr = lookup_address("worker").unwrap();
let remote = RemoteActorRef::<Ping>::local_tasks(addr, recipient);
remote.send(Ping { n: 1 })?; // local path

let remote_only = RemoteActorRef::<Ping>::remote_global(
    ActorAddress::on("peer@host".into(), actor_id),
);
assert!(remote_only.send(Ping { n: 1 }).is_err()); // stub transport
```

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
| **8b** | `ClusterRouter`, `RemoteActorRef`, registry hooks (this document) |
| **8c** | Pluggable transport, TCP MVP, two-node test |
| **8d** | `Node` / `Application` bootstrap |
| **9** | Cluster-safe parity: backoff, pg scopes, pools, unified ChildSpec |
| **10** | Federated registry, distributed pg, libp2p transport |

See [ROADMAP.md](ROADMAP.md) for full detail.
