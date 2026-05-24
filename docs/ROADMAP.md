# Spawned Roadmap

**Last updated:** after Phase 10.1 (federated registry).

For API details, see the [API Guide](API-GUIDE.md). For supervision patterns, see [Supervision Guide](SUPERVISION.md). For framework comparison research, see [design/FRAMEWORK_COMPARISON.md](design/FRAMEWORK_COMPARISON.md).

## Phase 1: Core Actor Framework — ✅ v0.4

- `Actor` trait with `started()` / `stopped()` lifecycle
- `ActorRef<A>` for communication (`request()` and `send()`)
- Dual execution modes (async tasks / sync threads)
- Timers (`send_after`, `send_interval`)
- Stream processing
- Signal handling via `send_message_on()`

## Phase 2: Type-Safe Multi-Message API — ✅ v0.5

Solved the two critical API issues ([#144](https://github.com/lambdaclass/spawned/issues/144), [#145](https://github.com/lambdaclass/spawned/issues/145)):

- `Handler<M>` pattern — per-message type safety
- `Recipient<M>` and protocol `XRef` — type-erased handles for bidirectional actors
- `#[protocol]` and `#[actor]` macros
- Named registry — global actor lookup by name

## Phase 3: Supervision Trees — ✅

Core supervision is shipped including dynamic supervisors.

### 3a. Exit Reasons — ✅ [PR #163](https://github.com/lambdaclass/spawned/pull/163)

- `ExitReason` enum with `is_abnormal()`
- `ActorRef::wait_exit()` and `exit_reason()`

### 3b. ChildHandle and ActorId — ✅ [PR #164](https://github.com/lambdaclass/spawned/pull/164)

- `ActorId` — unique per-actor identity
- `ChildHandle` — type-erased stop, wait, liveness, exit reason

### 3c. Monitors — ✅ [PR #165](https://github.com/lambdaclass/spawned/pull/165)

- `ctx.monitor(child_handle)` → `MonitorRef`
- `ctx.demonitor(monitor_ref)`
- `Down` message via `Handler<Down>`

### 3d. Links and Trap Exit — ✅ [PR #166](https://github.com/lambdaclass/spawned/pull/166)

Closes [#131](https://github.com/lambdaclass/spawned/issues/131) (monitor half was #165).

- Bidirectional `ctx.link()` / `ctx.unlink()`
- `ctx.trap_exit(true)` — receive `Exit` via `Actor::exit_received` instead of dying
- `start_linked(parent_ctx)` on `ActorStart`
- Transitive propagation through non-trapping middle actors
- `ExitReason::Kill` is untrappable

### 3e. Child Specs and Supervisor — ✅

Closes [#132](https://github.com/lambdaclass/spawned/issues/132) and [#133](https://github.com/lambdaclass/spawned/issues/133) (MVP).

**Shipped:**

- **Child specs** — `RestartType`, `ShutdownType`, `ChildType`, `RestartIntensity`, `should_restart()`
- **Supervisor actor** — `Supervisor::builder()` with `ChildSpec::worker()` / `ChildSpec::supervisor()`
- **Restart strategies** — `OneForOne`, `OneForAll`, `RestForOne` (shared `SupervisorLogic`)
- **Meltdown protection** — restart intensity window (`max_restarts` within `Duration`)
- **`ChildHandle::shutdown()` / `kill()`** — produce `ExitReason::Shutdown` / `Kill`
- **`ShutdownType::Timeout` escalation** — `shutdown()` then wait; escalate to `kill()` on timeout (OTP default 5s for workers)
- **Dynamic supervisor** ([#134](https://github.com/lambdaclass/spawned/issues/134)) — `DynamicSupervisor` for runtime OneForOne child pools
- **Examples** — [`supervised_workers`](../examples/supervised_workers), [`dynamic_workers`](../examples/dynamic_workers)

**Deferred from supervision MVP:**

| Item | Notes |
|------|-------|
| **OTP `Application` / root supervisor** | ✅ Shipped in Phase 9.2 (`Application` wrapper); nested supervisor-as-child upgrades still deferred |
| **Supervisor-as-child hot code upgrade** | No built-in code reload or child spec migration |
| **Interruptible shutdown** | `kill()` does not preempt an in-flight handler or `stopped()`; escalation waits for the actor to return to its loop |
| **Dynamic supervisor strategies** | `DynamicSupervisor` is **OneForOne only** (Erlang `simple_one_for_one`); no OneForAll / RestForOne at runtime |
| **Unified `ChildSpec` type** | ✅ Shipped in Phase 9.5 — shared inner spec for static + dynamic supervisors |
| **Supervised process groups** | ✅ Shipped in Phase 9.7 — `ChildSpec::with_pg_group` / `with_pg_group_scoped` |

### Other Phase 3 work

- **Threads shutdown perf** — ✅ [PR #168](https://github.com/lambdaclass/spawned/pull/168), closes [#157](https://github.com/lambdaclass/spawned/issues/157): poison-pill `Shutdown` replaces 100ms `recv_timeout` polling in threads mode

## Phase 4: Process Groups — ✅ (local MVP)

Erlang/Ractor-style named actor sets for broadcast and dispatch on a **single node**.

**Shipped:**

- **`spawned_concurrency::pg`** — `join`, `leave`, `get_members`, `which_groups` via `ChildHandle`
- **`tasks::pg` / `threads::pg`** — typed `join`, `leave`, `members` for `ActorRef<A>` dispatch
- **Auto-leave on exit** — actors removed from all groups when they stop
- **Refcounted joins** — multiple joins require matching `leave` calls
- **Integration tests** — [`pg_integration.rs`](../concurrency/tests/pg_integration.rs) covers tasks and threads (`cargo test -p spawned-concurrency --test pg_integration`)
- **Example** — [`pg_workers`](../examples/pg_workers)

**Deferred from process groups MVP:**

| Item | Notes |
|------|-------|
| **Group monitors** | No `monitor` / `demonitor` for membership change notifications (Ractor-style) |
| **Distributed pg** | ✅ Shipped in Phase 10.2 — `PgEvent` control plane, `member_addresses`, `cast_federated` / `call_federated` |
| **Federated registry** | ✅ Shipped in Phase 10.1 — `register_named` replicates via TCP control plane |
| **Supervisor integration** | ✅ Auto-join via `ChildSpec::with_pg_group` (Phase 9.7); `ActorPool` applies pool group by default |

## Phase 5: Documentation & Polish — ongoing

- API Guide, migration guide, 18 examples
- [Supervision guide](SUPERVISION.md) — static/dynamic supervisors, restart/shutdown, meltdown
- Doc tests in crate READMEs ([#137](https://github.com/lambdaclass/spawned/issues/137)) — README examples tested via `cargo test --doc`
- Process groups API reference in [API-GUIDE](API-GUIDE.md#process-groups)

## Phase 6: Production Hardening — ✅

### 6a. Unified mailbox channel — ✅

Single `MailboxItem` enum (`Message`, `Exit`, `Shutdown`) for both tasks and threads mode actor loops.

### 6b. Mailbox buffer strategies — ✅

Configurable backpressure for user messages only; system items (`Exit`, `Shutdown`) bypass limits.

**Shipped:**

- **`MailboxConfig`** — `unbounded()` (default), `bounded(n)` (fail-fast), `bounded_blocking(n)` (block senders)
- **`BackpressureMode`** — `FailFast` | `Block`
- **`ActorError::MailboxFull`** — returned when a bounded mailbox is full in fail-fast mode
- **Start API** — `start_with_mailbox(config)` (tasks + threads); `start_with_backend_and_mailbox(backend, config)` (tasks)
- **Counter-based limits** — depth tracked on dequeue; no change to underlying unbounded mpsc channels
- **API reference** — [Mailbox configuration](API-GUIDE.md#mailbox-configuration)

### 6c. Production mailboxes — ✅

Wire bounded mailboxes into supervision and observability for load-safe dispatch.

**Shipped:**

- **`ActorRef::mailbox_depth()` / `mailbox_capacity()`** — runtime queue observability
- **`ChildSpec::with_mailbox(config)`** — static + dynamic supervisors (tasks + threads)
- **`start_linked_with_mailbox`** — linked child startup with mailbox policy
- **Examples** — [`mailbox_backpressure`](../examples/mailbox_backpressure), [`http_workers`](../examples/http_workers)

### 6d. Priority system dequeue — ✅

Split each actor mailbox into separate user and system channels so `Exit` and `Shutdown` are dequeued before queued user messages (complements 6b send-bypass).

**Shipped:**

- **Dual-channel mailbox** — user messages and system items (`Exit`, `Shutdown`) on separate internal channels (tasks + threads)
- **Priority receive** — tasks use biased `select!`; threads use crossbeam `try_recv` + `select!`
- **No public API change** — priority is always on

### 6e. Signal priority shutdown — ✅ (Phase 7)

Full four-tier mailbox priority: **Signal > Stop > Supervision > Message**.

**Shipped:**

- **Four internal channels** — `signal`, `stop`, `supervision`, `user` (replaces unified system channel from 6d)
- **OS signals** — Ctrl+C + SIGTERM via `spawned_rt::OsSignal` and `wait_shutdown_signal()` (tasks + threads)
- **Registration API** — `ActorRef::shutdown_on_signal()`, `ChildHandle::shutdown_on_signal()`, `register_shutdown_on_signal()`, `spawn_shutdown_signal_dispatcher()` (per mode)
- **Signal → graceful shutdown** — OS signals map to `ExitReason::Shutdown` without a user-message round trip
- **Examples** — [`http_workers`](../examples/http_workers), [`signal_test`](../examples/signal_test)

**Deferred from production mailboxes:**

| Item | Notes |
|------|-------|
| **Dropping / sliding buffers** | Only fixed capacity with fail-fast or block |
| **Configurable FIFO mode** | `MailboxConfig::fifo()` to disable system priority |

## Phase 8: Clustering foundations — shipped

**North star:** OTP over Kameo — see [CLUSTERING.md](CLUSTERING.md).

### 8a. Address + wire format — shipped

- **`spawned-address`** — `NodeId`, `ActorAddress`, `ActorId`, `local_node()` (`SPAWNED_NODE_NAME`)
- **`spawned-wire`** — `WireEnvelope`, `RemoteActor`, `RemoteMessage`, postcard codec
- **`#[remote_actor]` / `#[remote_message]`** macros with stable `spawned.{Type}/v1` ids
- **pg internal keys** — `ActorAddress` (local node + actor id); public API unchanged

### 8b. Router + named registry — shipped

- **`spawned-cluster`** — `ClusterRouter`, `Transport`, `UnavailableTransport` stub
- **`RemoteActorRef<M>`** — locality-aware send/request (`cluster` feature on `spawned-concurrency`)
- **Named registry** — `register_named` / `lookup_address` / `lookup_handle` / `unregister_named`

### 8c. TCP transport MVP — shipped

- Length-framed TCP + node handshake
- `TcpTransport`, `TcpClusterListener`, `InboundDispatch`
- `tasks_wire_dispatch` / `threads_wire_dispatch` for inbound actor delivery
- Two-node integration tests in `spawned-cluster`

### 8d. Node bootstrap — shipped

- `Node::builder()` — name, listen, peers, `shutdown_on_signal`
- `AddressDispatch` for multi-actor inbound routing
- `examples/cluster_ping_pong` two-node demo
- `examples/http_workers` migrated to signal-only `Node` bootstrap

### 9–10 (planned)

### 9.1. Restart backoff — shipped

- `RestartBackoff` on `ChildSpec`: `None` (default), `Fixed(Duration)`, `Exponential { base, max }`
- `.with_backoff(...)` on static and dynamic child specs (tasks + threads)
- Per-child consecutive attempt tracking in `SupervisorLogic`; resets on non-restarting exit

### 9.2. Application wrapper — shipped

- `Application::builder().start(async |ctx| ...)` — startup callback + OS signal shutdown
- Optional cluster `Node` via `.name()` / `.listen()` / `.peer()` when `cluster` feature enabled
- `Application::run()` / `run_blocking()` await root `ChildHandle` exit
- `examples/http_workers` migrated to `Application` API

### 9.3. Default bounded worker mailbox — shipped

- `ChildSpec::worker()` defaults to `MailboxConfig::default_worker()` (capacity 64, fail-fast)
- Nested supervisors remain unbounded; override with `.with_mailbox(...)` or `.with_mailbox(MailboxConfig::unbounded())`

### 9.4. pg scopes + broadcast helpers — shipped

- Scoped membership: `join_scoped` / `leave_scoped` / `members_scoped` / `which_groups_scoped` / `which_scopes`
- Default scope `"default"` preserves backward-compatible unscoped APIs
- Typed broadcast: `cast` / `call` (+ `_scoped` variants) returning `PgSendReport` / `PgCallReport`
- `PgError::NotJoined` includes scope name

### 9.5. Unified ChildSpec — shipped

- Single `spawned_concurrency::child_spec::ChildSpec` inner type for static and dynamic supervisors
- `tasks::ChildSpec` / `threads::ChildSpec` newtypes with `worker()` / `supervisor()` constructors (same API as before)
- Handle-based child linking via `ChildHandle::link` and `ActorStart::start_linked_to_handle`
- `dynamic_supervisor::ChildSpec` re-exports the runtime `ChildSpec` (backward compatible)

### 9.6. Actor pool pattern — shipped

- `PoolStrategy` (`RoundRobin`, `LeastLoaded`) and `PoolDispatcher` for pg-based routed dispatch
- `tasks::pool::dispatch` / `call_one` (+ threads sync variants)
- `ActorPool::builder(group).start(count, spec_for)` wraps `DynamicSupervisor` + pg group
- `examples/http_workers` migrated from manual round-robin to `ActorPool`

### 9.7. Supervisor ↔ pg auto-join — shipped

- `ChildSpec::with_pg_group(group)` / `with_pg_group_scoped(scope, group)` — typed pg join at child start
- Works for static and dynamic supervisors (tasks + threads)
- `ActorPool::start` auto-applies the pool group when the spec has no pg membership
- Restarts re-join via the same start path

### 10.1. Federated registry — shipped

- `RegistryEvent` control plane on TCP (`ClusterFrame::Registry` vs `ClusterFrame::Actor`)
- `register_named` / `unregister_named` replicate to peers; snapshot sync on connect
- `Node::sync_registry()` exchanges snapshots with configured peers
- `lookup_address(name)` returns local or federated remote [`ActorAddress`]
- `examples/cluster_ping_pong` uses name-based discovery (`register_named("pong")`)
- Integration test: `cluster/tests/registry_two_node.rs`
- Wire protocol version bumped to **2**

### 10.2. Distributed pg — shipped

- `PgEvent` control plane on TCP (`ClusterFrame::Pg`) alongside registry snapshots
- Local `join` / `leave` publish to peers; remote memberships stored in a federated map
- `member_addresses` / `member_addresses_scoped` — cluster-wide membership (local + remote)
- `cast_federated` / `call_federated` — broadcast to local and remote members via `RemoteActorRef`
- `ActorPool::dispatch_federated` — pool routing includes remote members (depth 0)
- `NodeBuilder` wires `ControlPlaneHooks` for registry + pg replication
- Integration test: `cluster/tests/pg_two_node.rs`

### 10.3. libp2p transport — shipped

- Optional `libp2p` feature on `spawned-cluster` (`Libp2pCluster`, `Libp2pPeer`)
- Same `ClusterFrame` protocol as TCP over libp2p request-response (`/spawned/cluster/3`)
- Static peer map: `NodeId` → `PeerId` + `Multiaddr` (Erlang-style node names)
- Background swarm thread; sync `Transport` methods via command channel
- Control-plane snapshots on connect (registry + pg); deferred sends until peer connected
- Integration test: `cluster/tests/libp2p_two_node.rs` (requires `--features libp2p`)

| Phase | Focus |
|-------|--------|
| **10.3** | libp2p transport — shipped |

### 11.1. NodeBuilder libp2p — shipped

- `cluster-libp2p` feature on `spawned-concurrency` wires `Libp2pCluster` into `NodeBuilder` / `Application`
- `transport_libp2p`, `listen_libp2p`, `libp2p_peer` mirror TCP `listen` / `peer`
- Federated registry + pg hooks installed automatically (same as TCP)
- Integration test: `concurrency/tests/node_libp2p_two_node.rs`

### 11.2. Async remote requests — shipped

- `AsyncTransport` trait + `ClusterRouter::request_remote_async`
- Native oneshot async path in `Libp2pCluster`; `TcpAsyncTransport` wraps blocking TCP
- `RemoteActorRef::request_async` — prefer in async handlers over `request_raw` + `spawn_blocking`
- `NodeBuilder` installs async transport alongside sync `Transport` for both TCP and libp2p

| Phase | Focus |
|-------|--------|
| **11.2** | Async remote requests — shipped |

### 12.1. Supervision control plane protocol — shipped

- `ClusterFrame::Supervision(SupervisionEnvelope)` — routed unicast (not federated broadcast)
- Wire types: `SupervisionEvent`, `RemoteSpawnSpec`, `WireExitReason`, `SupervisionSignal`
- `SupervisionEnvelope` correlation id for RPC vs fire-and-forget
- Correlated replies: raw `SupervisionEnvelope` bytes (not `WireReply`)
- `ControlPlaneHooks::with_supervision`; `stub_supervision_hooks()` for integration tests
- TCP: `send_supervision` / `request_supervision`; libp2p: `send_supervision_to` / `request_supervision_from`
- Wire protocol version bumped to **3** (`/spawned/cluster/3`)
- No connect-time supervision snapshot; no `SupervisionBroker` yet (Phase 12.2)
- `install_supervision_sync` publish hook stub in `spawned-concurrency`
- Integration tests: `cluster/tests/supervision_protocol.rs`, `supervision_tcp_roundtrip.rs`, `supervision_libp2p_roundtrip.rs`

| Phase | Focus |
|-------|--------|
| **12.1** | Supervision protocol + control plane hooks — shipped |

### 12.2. SupervisionBroker + Node wiring — shipped

- `SupervisionBroker` actor + `SupervisionBrokerInner` per cluster node (tasks runtime)
- `NodeBuilder` starts broker; wires inbound `SupervisionHooks` and `install_supervision_sync` publish routing
- `Node::register_supervision` — register local `ChildHandle` for inbound remote signals
- Inbound `Signal` (Stop / Shutdown / Kill) delivered to registered local actors
- Integration test: `concurrency/tests/supervision_signal_two_node.rs`

| Phase | Focus |
|-------|--------|
| **12.2** | SupervisionBroker + Node wiring — shipped |

### 12.3. Remote spawn (registry + named specs) — shipped

- Process-global remote worker and named-spec registries (`register_remote_worker`, `register_remote_spec`) for tasks and threads
- `SupervisionBroker` handles inbound `SpawnRequest`: registry dispatch, linked/unlinked start, auto-register, parent map for Phase 12.4
- `install_supervision_request` + `request_spawn` correlated RPC; `RemoteChildHandle` lifecycle via supervision signals
- `DynamicSupervisor::start_child_remote` (tasks + threads); `Context::actor_address()` for spawn parent
- `install_tasks_runtime` — dispatch worker starts from TCP listener threads onto the app runtime
- Integration test: `concurrency/tests/supervision_spawn_two_node.rs`

Deferred to **12.5+**: cross-node monitor/link, static supervisor `ChildSpec::placement` (12.7).

| Phase | Focus |
|-------|--------|
| **12.3** | Remote spawn registry + DynamicSupervisor API — shipped |

### 12.4. ChildExit propagation + remote restart — shipped

- `Exit.from` migrated to `ActorAddress` (cluster-aware child identity)
- `SupervisionBroker` emits `ChildExit` on linked remote-spawn child death; inbound delivery via `apply_child_exit`
- `register_supervision_actor` — supervisors auto-register for ChildExit delivery
- `DynamicSupervisor` stores remote spawn metadata and re-spawns via `request_spawn` on restart
- Integration test: `concurrency/tests/supervision_exit_two_node.rs`

| Phase | Focus |
|-------|--------|
| **12.4** | ChildExit propagation + remote restart — shipped |

## Future Considerations

| Feature | Notes |
|---------|-------|
| pg scopes and group monitors | Scopes ✅ (Phase 9.4); group monitors still deferred |
| Distributed process groups | Requires clustering first |
| Priority message channels | ✅ Shipped in Phase 7 (Signal > Stop > Supervision > Message) |
| State machines (`gen_statem`) | Protocol implementations |
| Backoff strategies | ✅ Shipped in Phase 9.1 (`RestartBackoff` on `ChildSpec`) |
| Persistence / event sourcing | Akka Persistence pattern |
| Clustering / distribution | Phase 8–11 shipped — see [CLUSTERING.md](CLUSTERING.md) |
| Built-in observability | Message latency (mailbox depth shipped in 6c) |
| Custom runtime | Purpose-built actor runtime |

## What's still missing for OTP parity

| Feature | Status |
|---------|--------|
| Supervisor actor + child specs | ✅ Shipped |
| Restart strategies (OneForOne/All/RestForOne) | ✅ Shipped (static supervisor) |
| Meltdown protection | ✅ Shipped |
| Dynamic supervisor (OneForOne pools) | ✅ Shipped ([#134](https://github.com/lambdaclass/spawned/issues/134)) |
| Process groups (local) | ✅ Shipped (MVP) |
| Process groups (distributed) | ✅ Shipped in Phase 10.2 |
| Distributed actors | ❌ Not started |

## References

- [PR #153](https://github.com/lambdaclass/spawned/pull/153): v0.5 implementation
- [PR #154](https://github.com/lambdaclass/spawned/pull/154): Design research
- [PR #163–#166](https://github.com/lambdaclass/spawned/pulls?q=is%3Apr+is%3Amerged): Supervision building blocks
- [PR #168](https://github.com/lambdaclass/spawned/pull/168): Threads shutdown perf
