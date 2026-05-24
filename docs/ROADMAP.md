# Spawned Roadmap

**Last updated:** after clustering foundations (Phase 8a).

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
| **Exponential backoff** | Restarts are immediate; no built-in delay between attempts |
| **OTP `Application` / root supervisor** | No single top-level application wrapper; compose supervisors manually |
| **Supervisor-as-child hot code upgrade** | No built-in code reload or child spec migration |
| **Interruptible shutdown** | `kill()` does not preempt an in-flight handler or `stopped()`; escalation waits for the actor to return to its loop |
| **Dynamic supervisor strategies** | `DynamicSupervisor` is **OneForOne only** (Erlang `simple_one_for_one`); no OneForAll / RestForOne at runtime |
| **Unified `ChildSpec` type** | Static and dynamic supervisors use separate `ChildSpec` types (`tasks::ChildSpec` vs `tasks::dynamic_supervisor::ChildSpec`) |
| **Supervised process groups** | No automatic pg membership when starting children; join groups explicitly in `started()` |

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
| **Scopes** | Erlang `pg` overlay networks (`join(scope, group, pid)`); only a default scope exists today |
| **Group monitors** | No `monitor` / `demonitor` for membership change notifications (Ractor-style) |
| **Distributed pg** | No cross-node membership; `get_local_members` is identical to `get_members` on one node |
| **Built-in broadcast/call** | No `pg_cast` / `pg_call` helpers; iterate `members()` and send yourself |
| **Supervisor integration** | Dynamic/static supervisors do not auto-join children to groups |

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
| **Default bounded mailbox for workers** | `ChildSpec::worker()` still defaults to unbounded; opt in via `.with_mailbox()` |
| **Dropping / sliding buffers** | Only fixed capacity with fail-fast or block |
| **Configurable FIFO mode** | `MailboxConfig::fifo()` to disable system priority |

## Phase 8: Clustering foundations — in progress

**North star:** OTP over Kameo — see [CLUSTERING.md](CLUSTERING.md).

### 8a. Address + wire format — in progress

**Shipped / in flight:**

- **`spawned-address`** — `NodeId`, `ActorAddress`, `ActorId`, `local_node()` (`SPAWNED_NODE_NAME`)
- **`spawned-wire`** — `WireEnvelope`, `RemoteActor`, `RemoteMessage`, postcard codec
- **`#[remote_actor]` / `#[remote_message]`** macros with stable `spawned.{Type}/v1` ids
- **pg internal keys** — `ActorAddress` (local node + actor id); public API unchanged

**Non-goals (8a):** network transport, remote send, distributed registry

### 8b–10 (planned)

| Phase | Focus |
|-------|--------|
| **8b** | `ClusterRouter`, `RemoteActorRef`, registry address integration |
| **8c** | Pluggable transport, TCP MVP, two-node integration test |
| **8d** | `Node` / `Application` bootstrap (Kameo `bootstrap` parity, OTP-shaped) |
| **9** | Cluster-safe Kameo parity: backoff, pg scopes, pools, unified ChildSpec |
| **10** | Federated registry, distributed pg, libp2p transport |

## Future Considerations

| Feature | Notes |
|---------|-------|
| pg scopes and group monitors | Deferred from local pg MVP (see above) |
| Distributed process groups | Requires clustering first |
| Priority message channels | ✅ Shipped in Phase 7 (Signal > Stop > Supervision > Message) |
| State machines (`gen_statem`) | Protocol implementations |
| Backoff strategies | Built into supervision |
| Persistence / event sourcing | Akka Persistence pattern |
| Clustering / distribution | Phase 8 in progress — see [CLUSTERING.md](CLUSTERING.md) |
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
| Process groups (distributed) | ❌ Deferred |
| Distributed actors | ❌ Not started |

## References

- [PR #153](https://github.com/lambdaclass/spawned/pull/153): v0.5 implementation
- [PR #154](https://github.com/lambdaclass/spawned/pull/154): Design research
- [PR #163–#166](https://github.com/lambdaclass/spawned/pulls?q=is%3Apr+is%3Amerged): Supervision building blocks
- [PR #168](https://github.com/lambdaclass/spawned/pull/168): Threads shutdown perf
