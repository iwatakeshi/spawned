# Spawned API Guide

Complete reference for the spawned actor framework. For a quick introduction, see the [README](../README.md).

## Table of Contents

- [Actor Lifecycle](#actor-lifecycle)
- [Context](#context)
- [ActorRef](#actorref)
- [Backend Selection (tasks mode)](#backend-selection-tasks-mode)
- [Mailbox configuration](#mailbox-configuration)
- [Timers](#timers)
- [send\_message\_on](#send_message_on)
- [spawn\_listener](#spawn_listener)
- [Type Erasure: Recipient and Receiver](#type-erasure-recipient-and-receiver)
- [Registry](#registry)
- [ChildHandle and ActorId](#childhandle-and-actorid)
- [Monitors](#monitors)
- [Links and trap\_exit](#links-and-trap_exit)
- [Child Specs and Supervisor](#child-specs-and-supervisor)
- [Response\<T\>](#responset)
- [Message Trait](#message-trait)
- [Error Handling](#error-handling)
- [spawned-rt](#spawned-rt)

---

## Actor Lifecycle

Every actor goes through three phases:

1. **`started()`** — called once before the actor begins processing messages. Use this for initialization: starting timers, registering with the registry, etc.
2. **Message loop** — the actor processes messages one at a time via `Handler<M>` implementations.
3. **`stopped()`** — called once after the message loop exits. Use this for cleanup.

```rust
#[actor(protocol = MyProtocol)]
impl MyActor {
    #[started]
    async fn started(&mut self, ctx: &Context<Self>) {
        // Start a periodic timer
        send_interval(Duration::from_secs(10), ctx.clone(), Tick);
    }

    #[stopped]
    async fn stopped(&mut self, _ctx: &Context<Self>) {
        tracing::info!("actor shutting down");
    }

    #[send_handler]
    async fn handle_tick(&mut self, _msg: Tick, _ctx: &Context<Self>) {
        // periodic work
    }
}
```

### Panic Recovery

All three phases are wrapped in `catch_unwind`:

- **Panic in `started()`** — the actor stops immediately. No messages are processed, `stopped()` is not called.
- **Panic in a handler** — the current message is lost. The actor stops and `stopped()` is called.
- **Panic in `stopped()`** — the panic is logged but `join()` still returns normally.

In all cases, `join()` will eventually return and subsequent `send()`/`request()` calls return `Err(ActorStopped)`.

### Stopping an Actor

Call `ctx.stop()` from inside a handler or lifecycle hook. This cancels the actor's internal token, causing the message loop to exit after the current handler finishes.

From outside, there is no direct stop method on `ActorRef`. Design your protocol with an explicit shutdown message, or use `ctx.stop()` from within the actor.

---

## Context

`Context<A>` is the handle passed to every handler and lifecycle hook. It provides access to the actor's own mailbox and lifecycle controls.

| Method | Description |
|--------|-------------|
| `ctx.stop()` | Signal the actor to stop after the current handler finishes |
| `ctx.send(msg)` | Send a fire-and-forget message to this actor (self-send) |
| `ctx.request(msg)` | Send a request and wait for the reply (tasks: async, threads: blocking) |
| `ctx.request_with_timeout(msg, duration)` | Send a request with a custom timeout |
| `ctx.request_raw(msg)` | Send a request and get a raw oneshot receiver |
| `ctx.recipient::<M>()` | Get a type-erased `Recipient<M>` for this actor |
| `ctx.actor_ref()` | Get an `ActorRef<A>` from the context |

`Context::from_ref(&actor_ref)` creates a context from an `ActorRef`, useful for setting up timers or stream listeners from outside the actor.

### Self-scheduling example

```rust
#[send_handler]
async fn handle_tick(&mut self, _msg: Tick, ctx: &Context<Self>) {
    self.do_work();
    // Schedule the next tick
    send_after(Duration::from_secs(5), ctx.clone(), Tick);
}
```

---

## ActorRef

`ActorRef<A>` is the external handle to a running actor. Cloneable, `Send + Sync`.

| Method | Description |
|--------|-------------|
| `actor_ref.send(msg)` | Fire-and-forget. Returns `Result<(), ActorError>` |
| `actor_ref.request(msg)` | Request with default 5s timeout. Tasks: `.await`, Threads: blocking |
| `actor_ref.request_with_timeout(msg, duration)` | Request with custom timeout |
| `actor_ref.request_raw(msg)` | Returns a raw oneshot receiver |
| `actor_ref.recipient::<M>()` | Get a type-erased `Recipient<M>` |
| `actor_ref.context()` | Get a `Context<A>` (for timer setup, etc.) |
| `actor_ref.mailbox_depth()` | Queued user message count (0 when unbounded or empty) |
| `actor_ref.mailbox_capacity()` | Configured limit, or `None` when unbounded |
| `actor_ref.join()` | Wait until the actor has fully stopped (tasks: `.await`, threads: blocking) |

### Starting an actor

```rust
use spawned_concurrency::tasks::ActorStart as _;

let actor_ref = MyActor::new().start();

// tasks mode: choose a backend
let actor_ref = MyActor::new().start_with_backend(Backend::Thread);

// bounded mailbox (fail-fast or blocking backpressure)
let actor_ref = MyActor::new().start_with_mailbox(MailboxConfig::bounded(100));
```

See [Mailbox configuration](#mailbox-configuration) for backpressure modes and system-message bypass.

---

## Backend Selection (tasks mode)

The `Backend` enum controls where the actor's message loop runs. Only available in `tasks` mode.

| Backend | When to use |
|---------|-------------|
| `Backend::Async` (default) | Standard async actors. Runs on the tokio runtime. Handlers must not block. |
| `Backend::Blocking` | Actors that do blocking I/O (file system, synchronous HTTP). Runs on tokio's blocking thread pool. |
| `Backend::Thread` | CPU-bound work or full isolation. Runs on a dedicated OS thread with its own tokio runtime. |

```rust
// Default — async on tokio
let a = MyActor::new().start();

// Blocking I/O
let b = MyActor::new().start_with_backend(Backend::Blocking);

// Dedicated thread
let c = MyActor::new().start_with_backend(Backend::Thread);
```

`Backend::Async` will emit a tracing warning (debug builds only) if a poll takes longer than 10ms — use `Backend::Blocking` or `Backend::Thread` for slow work.

---

## Mailbox configuration

By default, actors accept an unlimited number of queued user messages. For load-sensitive workloads (HTTP dispatch, worker pools), you can cap the mailbox depth and choose how senders behave when the limit is reached.

```rust
use spawned_concurrency::{BackpressureMode, MailboxConfig};
use spawned_concurrency::tasks::ActorStart as _;

// Default — unbounded (same as .start())
let a = MyActor::new().start_with_mailbox(MailboxConfig::unbounded());

// Bounded, fail fast when full
let b = MyActor::new().start_with_mailbox(MailboxConfig::bounded(100));

// Bounded, block senders until space is available
let c = MyActor::new().start_with_mailbox(MailboxConfig::bounded_blocking(100));

// tasks mode: combine backend + mailbox
let d = MyActor::new().start_with_backend_and_mailbox(
    Backend::Thread,
    MailboxConfig::bounded(50),
);
```

| Config | Behavior when at capacity |
|--------|---------------------------|
| `MailboxConfig::unbounded()` | No limit (default for `.start()`) |
| `MailboxConfig::bounded(n)` | `send()` / `request()` return `Err(ActorError::MailboxFull)` |
| `MailboxConfig::bounded_blocking(n)` | `send()` / `request()` wait until a queued message is dequeued |

**Depth semantics:** The counter tracks **queued** user messages only. When the actor dequeues a message (before the handler runs), depth decreases and blocked senders are woken. A message currently being handled does not count toward the limit.

**System messages bypass limits:** `Exit` (link propagation), stop items (cancellation), and OS signals always enqueue immediately, even when the user mailbox is full. Supervised actors started via `.start()` remain unbounded by default.

**Priority dequeue:** When multiple internal channels have pending items, the actor processes them in order: **Signal → Stop → Supervision (Exit) → user messages**. Link-propagated exits and cancellation therefore reach a trapping supervisor before queued user messages — even under backlog. Stop beats supervision when both are queued. User messages among themselves remain FIFO.

**Block mode in tasks mode:** Sync `send()` from within an async runtime uses `block_in_place` internally to wait for capacity. Prefer calling from a blocking thread or dedicated task when possible.

**Observability:** `actor_ref.mailbox_depth()` returns the current queued depth; `actor_ref.mailbox_capacity()` returns `Some(n)` for bounded mailboxes or `None` when unbounded.

---

## Shutdown signals

Register actors for OS shutdown (Ctrl+C / SIGTERM). Signals use the highest-priority mailbox channel and exit with [`ExitReason::Shutdown`](error/enum.ExitReason.html) — they are **not** delivered as user messages.

**Tasks mode:**

```rust
use spawned_concurrency::tasks::spawn_shutdown_signal_dispatcher;
use spawned_concurrency::{register_shutdown_on_signal, ChildHandle};

spawn_shutdown_signal_dispatcher();

let sup = MySupervisor::start();
let _guards = register_shutdown_on_signal(&[sup.child_handle()]);

// Or per-actor:
let _guard = worker.shutdown_on_signal();
```

**Threads mode** — same API with `spawned_concurrency::threads::spawn_shutdown_signal_dispatcher`.

The dispatcher listens once per process (`wait_shutdown_signal()` in `spawned_rt`) and fans out to all registered actors. [`SignalGuard`](shutdown_signal/struct.SignalGuard.html) deregisters on drop.

**Do not use [`send_message_on`](#send_message_on) for OS shutdown** under load — `Shutdown` sent that way is a user message and can sit behind a deep mailbox backlog. Use `shutdown_on_signal()` or `register_shutdown_on_signal()` instead. Keep `send_message_on` for non-OS events (timers, I/O completion).

---

## Timers

Timers send messages to actors after a delay or at regular intervals.

### `send_after`

Sends a single message after a delay.

```rust
use spawned_concurrency::tasks::{send_after, Context};

// Inside a handler or started()
let timer = send_after(Duration::from_secs(5), ctx.clone(), MyMessage);

// Cancel before it fires
timer.cancellation_token.cancel();
```

### `send_interval`

Sends a message repeatedly at a fixed interval. The message type must implement `Clone`.

```rust
use spawned_concurrency::tasks::send_interval;

let timer = send_interval(Duration::from_secs(1), ctx.clone(), Tick);

// Stop the interval
timer.cancellation_token.cancel();
```

### `TimerHandle`

Both functions return a `TimerHandle` with two public fields:

- `join_handle` — the spawned task/thread handle
- `cancellation_token` — cancel the timer before it fires (or stop an interval)

Timers are automatically cancelled when the actor stops.

---

## send_message_on

Sends a message to an actor when an external event completes. Messages delivered this way use the **user** mailbox channel — they do **not** bypass backpressure or priority dequeue. For OS shutdown (Ctrl+C / SIGTERM), use [`shutdown_on_signal()`](#shutdown-signals) instead.

**Tasks mode** — takes a `Future`:

```rust
use spawned_concurrency::tasks::send_message_on;

// Timer / I/O completion — appropriate use of send_message_on
send_message_on(ctx, fetch_data(), DataReady);

// OS shutdown — prefer shutdown_on_signal() (see Shutdown signals section)
// send_message_on(ctx, rt::ctrl_c(), Shutdown);  // not recommended under load
```

**Threads mode** — takes a `FnOnce()` closure:

```rust
use spawned_concurrency::threads::send_message_on;

// Send Shutdown when the closure returns (blocking call)
send_message_on(ctx, rt::ctrl_c(), Shutdown);
```

If the actor stops before the event completes, the message is not sent.

---

## spawn_listener

Forwards items from a stream (tasks) or iterator (threads) to an actor as messages.

**Tasks mode** — takes an async `Stream`:

```rust
use spawned_concurrency::tasks::spawn_listener;

let stream = ReceiverStream::new(rx);
let handle = spawn_listener(ctx, stream);
```

**Threads mode** — takes an `IntoIterator`:

```rust
use spawned_concurrency::threads::spawn_listener;

let items = vec![Push { value: 1 }, Push { value: 2 }];
let handle = spawn_listener(ctx, items);
```

The listener stops when:
- The stream/iterator is exhausted
- The actor stops (cancellation token is triggered)
- Sending to the actor's mailbox fails

---

## Type Erasure: Recipient and Receiver

When you need to send a specific message type to an actor without knowing its concrete type, use `Recipient<M>`.

```rust
pub type Recipient<M> = Arc<dyn Receiver<M>>;
```

`Receiver<M>` is the object-safe trait that provides `send()` and `request_raw()` for a single message type.

### Getting a Recipient

```rust
let recipient: Recipient<Notify> = actor_ref.recipient();
// or from inside a handler:
let recipient: Recipient<Notify> = ctx.recipient();
```

### Using a Recipient

```rust
// Fire-and-forget
recipient.send(Notify { text: "hello".into() })?;

// Request with timeout (tasks mode)
let result = request(&*recipient, GetCount, Duration::from_secs(5)).await?;

// Request with timeout (threads mode)
let result = request(&*recipient, GetCount, Duration::from_secs(5))?;
```

### When to use

- Passing actor references to other actors without exposing the concrete type
- Storing heterogeneous actor references in collections
- Cross-module boundaries where you want to depend on a message type, not an actor type

Note: For most cases, protocol-generated `XRef` types (e.g., `NameServerRef = Arc<dyn NameServerProtocol>`) are a better fit since they expose the full protocol interface. `Recipient<M>` is the escape hatch for single-message type erasure.

---

## Registry

Global name-based registry for discovering actors at runtime. Stores any `Clone + Send + Sync + 'static` value.

```rust
use spawned_concurrency::registry;
```

| Function | Description |
|----------|-------------|
| `registry::register(name, value)` | Register a value by name. Returns `Err(AlreadyRegistered)` if the name is taken. |
| `registry::whereis::<T>(name)` | Look up a value by name. Returns `None` if not found or wrong type. |
| `registry::unregister(name)` | Remove a registration. |
| `registry::registered()` | List all registered names. |

### Example

```rust
// Register a protocol reference
let ns_ref = ns.to_name_server_ref();
registry::register("name_server", ns_ref)?;

// Look it up elsewhere
let ns: Option<NameServerRef> = registry::whereis("name_server");
if let Some(ns) = ns {
    let result = ns.find("Joe".into()).await.unwrap();
}
```

The registry uses `Any`-based downcasting, so `whereis` returns `None` if the stored type doesn't match the requested type.

**Registry vs process groups:** The registry maps one name → one value. Process groups map one name → many actors. Use the registry for singleton services; use [`pg`](#process-groups) for pools you want to broadcast to or pick from at runtime.

---

## Process Groups

Erlang-style named actor sets for broadcast and dispatch. Single-node MVP; see [deferred items](#deferred-not-in-mvp-1) below.

Both runtimes expose the same typed API — import from `tasks::pg` or `threads::pg`:

```rust
use spawned_concurrency::pg;                      // ChildHandle membership (shared)
use spawned_concurrency::tasks::pg as tasks_pg; // async ActorRef dispatch
// use spawned_concurrency::threads::pg as threads_pg;  // blocking ActorRef dispatch
```

### Untyped membership (`spawned_concurrency::pg`)

| Function | Description |
|----------|-------------|
| `pg::join(group, handle)` | Join a `ChildHandle` (refcounted) |
| `pg::leave(group, id)` | Leave once; returns `PgError::NotJoined` if absent |
| `pg::get_members(group)` | All live members as `ChildHandle` |
| `pg::get_local_members(group)` | Same as `get_members` on a single node |
| `pg::which_groups()` | Names of non-empty groups |

### Typed dispatch (`tasks::pg` or `threads::pg`)

Same functions in both modules; only the underlying `ActorRef` mode differs.

| Function | Description |
|----------|-------------|
| `pg::join(group, &actor_ref)` | Join for later message dispatch |
| `pg::leave(group, id)` | Decrement membership |
| `pg::members::<A>(group)` | Live `ActorRef<A>` members |
| `pg::local_members::<A>(group)` | Same as `members` on a single node |

### Example (tasks mode)

```rust
use spawned_concurrency::tasks::{pg, Actor, Context, Handler, ActorStart as _};
use spawned_concurrency::message::Message;

struct Ping;
impl Message for Ping { type Result = (); }

struct Worker;
impl Actor for Worker {
    async fn started(&mut self, ctx: &Context<Self>) {
        pg::join("pool", &ctx.actor_ref());
    }
}
impl Handler<Ping> for Worker {
    async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) { /* ... */ }
}

// Broadcast
for w in pg::members::<Worker>("pool") {
    w.send(Ping)?;
}
```

### Example (threads mode)

Same pattern with blocking handlers — import `spawned_concurrency::threads::pg` instead:

```rust
use spawned_concurrency::threads::{pg, Actor, Context, Handler, ActorStart as _};

impl Actor for Worker {
    fn started(&mut self, ctx: &Context<Self>) {
        pg::join("pool", &ctx.actor_ref());
    }
}
impl Handler<Ping> for Worker {
    fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) { /* ... */ }
}

for w in pg::members::<Worker>("pool") {
    w.send(Ping)?;
}
```

Actors are **automatically removed** from all groups when they exit. `DynamicSupervisor` optional registry names are separate from pg — join explicitly if needed.

See [`pg_workers`](../examples/pg_workers) (tasks mode demo).

### Testing

Integration tests in [`concurrency/tests/pg_integration.rs`](../concurrency/tests/pg_integration.rs) cover both runtimes (`mod tasks` and `mod threads`): join, broadcast, refcounted leave, auto-leave on exit, and `ChildHandle` membership.

Run: `cargo test -p spawned-concurrency --test pg_integration`

#### Deferred (not in MVP)

| Item | Notes |
|------|-------|
| **Scopes** | Erlang overlay-network scopes; only a default scope today |
| **Group monitors** | No notifications when membership changes |
| **Distributed pg** | Cross-node membership requires clustering (not started) |
| **Built-in broadcast/call** | No framework-level `pg_cast`; iterate `members()` yourself |
| **Supervisor auto-join** | Starting a child does not add it to a process group |

---

## ChildHandle and ActorId

`ActorId` uniquely identifies a running actor. `ChildHandle` is a type-erased handle for lifecycle operations — stop, wait, poll exit reason — without knowing the actor's message protocol.

```rust
let worker = Worker::new().start();
let handle: ChildHandle = worker.child_handle();

handle.stop();
let reason = handle.wait_exit_async().await;  // tasks mode
```

| Method | Description |
|--------|-------------|
| `handle.stop()` | Graceful stop; actor exits with `ExitReason::Normal` after `stopped()` |
| `handle.shutdown()` | Supervisor-ordered shutdown; exits with `ExitReason::Shutdown` |
| `handle.kill()` | Brutal stop; skips `stopped()`, exits with `ExitReason::Kill` |
| `handle.is_alive()` | Returns `true` while the actor is running |
| `handle.exit_reason()` | Poll exit reason; `None` if still running |
| `handle.wait_exit_async()` | Async wait (both modes; threads mode uses `spawn_blocking`) |
| `handle.wait_exit_blocking()` | Block until exit (see docs for tokio runtime constraints) |
| `handle.wait_exit_*_with_timeout(d)` | Timed wait; returns `None` on timeout |

Use `Vec<ChildHandle>` to manage heterogeneous actors. See the [`exit_reason`](../examples/exit_reason) example for monitors and manual linking.

---

## Monitors

Unidirectional death observation. When a monitored actor stops, a `Down` message is delivered to the monitoring actor's mailbox.

```rust
impl Handler<Down> for Watcher {
    async fn handle(&mut self, msg: Down, _ctx: &Context<Self>) {
        tracing::info!("actor died: {} ({})", msg.monitor_ref, msg.reason);
    }
}

let monitor_ref = ctx.monitor(&target.child_handle());
ctx.demonitor(monitor_ref);  // cancel before target dies
```

---

## Links and trap_exit

Bidirectional fate-sharing. When a linked actor dies abnormally, the peer receives an exit signal — either cancelling it or delivering an `Exit` message if trapping.

Supervisors use links (not monitors) and trap exits so child deaths arrive as messages instead of killing the supervisor. See [Child Specs and Supervisor](#child-specs-and-supervisor) for the built-in supervisor.

```rust
impl Actor for MySupervisor {
    async fn exit_received(&mut self, exit: Exit, ctx: &Context<Self>) {
        tracing::info!("child {} died: {}", exit.from, exit.reason);
        // manual restart logic — or use Supervisor::builder() instead
    }
}

ctx.trap_exit(true);
let child = Worker::new().start_linked(&ctx);
```

| API | Description |
|-----|-------------|
| `ctx.link(&child_handle)` | Bidirectional link (idempotent) |
| `ctx.unlink(&child_handle)` | Remove link |
| `ctx.trap_exit(true)` | Convert exit signals to `Exit` via `exit_received` |
| `actor.start_linked(&parent_ctx)` | Spawn and link atomically |

`ExitReason::Kill` is untrappable. Normal exits are not propagated to non-trapping peers.

---

## Child Specs and Supervisor

Built-in Erlang-style supervision. See the [Supervision Guide](SUPERVISION.md) for patterns and pitfalls; this section is the API reference.

Shared policy types live in the crate root; `ChildSpec` and `Supervisor` are mode-specific (`tasks::` or `threads::`).

### Shared types

```rust
use spawned_concurrency::{
    RestartType, ShutdownType, ChildType, RestartIntensity, SupervisorStrategy,
    should_restart, shutdown_child_async, shutdown_child_blocking, DEFAULT_WORKER_SHUTDOWN,
};
```

| Type | Description |
|------|-------------|
| `RestartType::Permanent` | Restart on any exit except `ExitReason::Shutdown` |
| `RestartType::Transient` | Restart only on abnormal exit (`is_abnormal()`) |
| `RestartType::Temporary` | Never restart |
| `ShutdownType::Infinity` | Wait for `stopped()` during supervisor shutdown |
| `ShutdownType::Timeout(d)` | Grace period, then escalate to `kill()` |
| `ShutdownType::BrutalKill` | Immediate `kill()` on shutdown |
| `RestartIntensity` | `{ max_restarts, within }` — meltdown if exceeded |
| `SupervisorStrategy` | `OneForOne`, `OneForAll`, `RestForOne` |

**OTP defaults:** `ChildSpec::worker()` uses `DEFAULT_WORKER_SHUTDOWN` (`Timeout(5s)`). Nested supervisors default to `ShutdownType::Infinity`. Override with `.with_shutdown(...)` when a child needs longer cleanup.

**Mailbox limits:** Supervised children default to unbounded mailboxes. Use `.with_mailbox(MailboxConfig::bounded(n))` on `ChildSpec` (static or dynamic) to cap worker queue depth. Restarts inherit the spec's mailbox config. See [Mailbox configuration](#mailbox-configuration).

### Shutdown orchestration

Shared helpers apply a `ShutdownType` and block until the child exits:

```rust
use spawned_concurrency::{shutdown_child_async, shutdown_child_blocking, ShutdownType};

// Blocking (threads supervisors, batch terminate in threads mode)
let reason = shutdown_child_blocking(&handle, ShutdownType::Timeout(Duration::from_secs(5)));

// Async (tasks supervisors)
let reason = shutdown_child_async(&handle, ShutdownType::Timeout(Duration::from_secs(5))).await;
```

For `Timeout`, the flow is: `shutdown()` → timed wait → on timeout, log a warning and `kill()` → wait for exit. Supervisors use these helpers in `stopped()` and during OneForAll / RestForOne batch termination.

**Note:** `kill()` skips `stopped()` but does not interrupt an in-flight message handler or `stopped()` callback. Escalation takes effect once the actor returns to its message loop; until then the supervisor continues waiting for the child to exit.

### ChildSpec

Describes how to start and supervise one child. The start closure receives the supervisor's `Context` and must call `start_linked`:

```rust
use spawned_concurrency::tasks::{ChildSpec, Supervisor, ActorStart as _};
use spawned_concurrency::{
    RestartType, RestartIntensity, ShutdownType, SupervisorStrategy,
};
use std::time::Duration;

let spec = ChildSpec::worker("worker1", || Worker::new(), RestartType::Permanent);
// Default shutdown is Timeout(5s); use Infinity for long stopped() cleanup:
let spec = spec.with_shutdown(ShutdownType::Infinity);
// Bounded mailbox for load-sensitive workers:
let spec = spec.with_mailbox(MailboxConfig::bounded(100));

// Nested supervisor:
let nested = ChildSpec::supervisor("sup", || inner_supervisor(), RestartType::Permanent);
```

### Supervisor builder

```rust
let sup = Supervisor::builder()
    .strategy(SupervisorStrategy::OneForOne)
    .intensity(RestartIntensity {
        max_restarts: 3,
        within: Duration::from_secs(5),
    })
    .child(ChildSpec::worker("alpha", || Worker::new("alpha"), RestartType::Permanent))
    .child(ChildSpec::worker("beta", || Worker::new("beta"), RestartType::Transient))
    .start();  // returns ActorRef<Supervisor>
```

The supervisor enables `trap_exit(true)` in `started()`, links each child via `start_linked`, and handles `exit_received` internally:

- **OneForOne** — restart only the dead child (if policy and intensity allow)
- **OneForAll** — shut down all children, then restart all
- **RestForOne** — shut down the dead child and all children started after it, then restart those
- **Meltdown** — supervisor stops abnormally when restart intensity is exceeded

On supervisor shutdown, children are stopped in reverse start order using each spec's `ShutdownType`. Batch termination (OneForAll / RestForOne) waits for each child to exit before restarting survivors.

See the [`supervised_workers`](../examples/supervised_workers) example, [`dynamic_workers`](../examples/dynamic_workers), and the [Supervision Guide](SUPERVISION.md). Manual linking is shown in [`exit_reason`](../examples/exit_reason) Scenario 9.

**Deferred from static supervisor MVP:** exponential backoff, OTP `Application` wrapper, interruptible shutdown. See [ROADMAP](ROADMAP.md).

### DynamicSupervisor

For runtime child pools (Erlang `simple_one_for_one` style). Fixed **OneForOne** strategy; children are started via messages after the supervisor boots.

Use when the child set is not known at build time (connection handlers, job workers). For a fixed tree known at startup, use static [`Supervisor::builder()`](#supervisor-builder) instead.

```rust
use spawned_concurrency::tasks::{
    dynamic_supervisor::ChildSpec, DynamicSupervisor, DynamicSupervisorApi,
};
use spawned_concurrency::{RestartIntensity, RestartType};

let sup = DynamicSupervisor::builder()
    .max_children(100)   // optional
    .intensity(RestartIntensity {
        max_restarts: 5,
        within: Duration::from_secs(10),
    })
    .start();

let handle = sup
    .start_child(
        ChildSpec::worker("conn", || ConnectionHandler::new(), RestartType::Temporary),
        Some("conn-1".into()),  // optional registry name
    )
    .await?
    .unwrap();

assert_eq!(sup.count_children().await?, 1);
sup.terminate_child(handle.id()).await?;  // intentional remove — no restart
```

| API | Description |
|-----|-------------|
| `start_child(spec, reg_name)` | Start a child; returns `ChildHandle`. Instance id is `{spec.id}#{n}`. |
| `terminate_child(actor_id)` | Graceful shutdown and remove from supervision (no restart) |
| `count_children()` | Number of alive children |
| `which_children()` | List `DynamicChildInfo` (id, actor_id, restart/shutdown policy) |

`DynamicSupervisorError` covers `MaxChildrenExceeded`, `ChildNotFound`, `DuplicateChildId`, and registry failures.

See [`dynamic_workers`](../examples/dynamic_workers).

#### Deferred (not in MVP)

| Item | Notes |
|------|-------|
| **Restart strategies** | OneForOne only; no OneForAll / RestForOne for runtime pools |
| **Unified ChildSpec** | Uses `dynamic_supervisor::ChildSpec`, separate from static supervisor's `ChildSpec` |
| **Backoff** | Immediate restart on crash; no exponential delay |
| **Process group integration** | Children are not auto-joined to pg; call `pg::join` in `started()` |

## Response\<T\>

`Response<T>` is the return type for protocol request methods. It works in both execution modes:

- **Tasks mode** — wraps a oneshot receiver. Use `.await` to get `Result<T, ActorError>`.
- **Threads mode** — wraps a pre-computed result. Use `.unwrap()` or `.expect()` directly.

### Methods (sync, for threads mode)

| Method | Description |
|--------|-------------|
| `.unwrap()` | Extract the value, panic on error |
| `.expect(msg)` | Extract the value, panic with custom message on error |
| `.is_ok()` | Returns `true` if the response contains `Ok` |
| `.is_err()` | Returns `true` if the response contains `Err` |
| `.map(f)` | Transform the inner value if `Ok` |

### Async usage (tasks mode)

```rust
// .await returns Result<T, ActorError>
let result = ns.find("Joe".into()).await.unwrap();
```

### Sync usage (threads mode)

```rust
// .unwrap() extracts the value directly
let result = ns.find("Joe".into()).unwrap();
```

`Response::ready(result)` creates a pre-computed response — this is what the `#[protocol]` macro generates for threads-mode blanket impls.

---

## Message Trait

The `Message` trait defines a message type and its expected reply type:

```rust
pub trait Message: Send + 'static {
    type Result: Send + 'static;
}
```

You rarely need to implement this manually — `#[protocol]` generates message structs that implement `Message` automatically. For cases where you need a standalone message without a protocol:

```rust
struct Ping;
impl Message for Ping {
    type Result = ();
}

struct GetCount;
impl Message for GetCount {
    type Result = u64;
}
```

---

## Error Handling

### Communication errors (`ActorError`)

```rust
pub enum ActorError {
    ActorStopped,
    RequestTimeout,
    MailboxFull,   // bounded mailbox at capacity (FailFast mode)
}
```

### Exit reasons (`ExitReason`)

```rust
pub enum ExitReason {
    Normal,           // ctx.stop() or clean channel closure
    Shutdown,         // ChildHandle::shutdown() or supervisor-ordered stop
    Panic(String),    // panic in started(), handler, or stopped()
    Kill,             // ChildHandle::kill() or external abort
}
```

Use `reason.is_abnormal()` for restart policy decisions. `should_restart(restart_type, &reason)` encodes Erlang-style permanent/transient/temporary rules.

Supervisors are built in — see [Child Specs and Supervisor](#child-specs-and-supervisor).

- `send()` / `request()` return `Err(ActorStopped)` when the actor has stopped
- All lifecycle phases are wrapped in `catch_unwind`

---

## spawned-rt

`spawned-rt` provides runtime abstractions used by `spawned-concurrency`. Users import it for `run()`, `sleep()`, and signal handling.

### tasks module (`spawned_rt::tasks`)

Wraps tokio primitives:

| Item | Description |
|------|-------------|
| `run(future)` | Create a tokio runtime, initialize tracing, and block on the future |
| `block_on(future)` | Block on a future using the current tokio runtime handle |
| `spawn(future)` | Spawn an async task |
| `spawn_blocking(f)` | Spawn a blocking closure on tokio's blocking pool |
| `sleep(duration)` | Async sleep |
| `timeout(duration, future)` | Wrap a future with a timeout |
| `Runtime` | Tokio runtime (re-export) |
| `JoinHandle` | Handle to a spawned task |
| `CancellationToken` | Cooperative cancellation (from tokio-util) |
| `mpsc` | Multi-producer, single-consumer channel |
| `oneshot` | Single-use channel |
| `watch` | Watch channel for broadcasting state changes |
| `ctrl_c()` | Returns a future that resolves on Ctrl+C (backward compatible) |
| `wait_shutdown_signal()` | Returns a future that resolves on Ctrl+C or SIGTERM (`OsSignal`) |
| `OsSignal` | `CtrlC` or `Terminate` (SIGTERM on Unix) |

### threads module (`spawned_rt::threads`)

Wraps standard library primitives:

| Item | Description |
|------|-------------|
| `run(f)` | Initialize tracing and call `f()` |
| `block_on(future)` | Create a temporary tokio runtime and block on a future |
| `spawn(f)` | Spawn an OS thread |
| `sleep(duration)` | Block the current thread |
| `JoinHandle` | Handle to a spawned thread |
| `CancellationToken` | Cooperative cancellation with callback support via `on_cancel()` |
| `mpsc` | Multi-producer, single-consumer channel (wraps `std::sync::mpsc`) |
| `oneshot` | Single-use channel (wraps `std::sync::mpsc`) |
| `ctrl_c()` | Returns a closure that blocks until Ctrl+C. Supports multiple subscribers. |
| `wait_shutdown_signal()` | Blocks until Ctrl+C or SIGTERM; returns `OsSignal` |
| `shutdown_signal_listener()` | Returns a closure for subscriber-style listening (returns `OsSignal`) |

### Choosing tasks vs threads

Use **`tasks`** when you need async I/O, high actor counts (thousands), or integration with async libraries.

Use **`threads`** when you want simplicity, no async runtime, or CPU-bound actors that benefit from dedicated OS threads. Each actor gets its own thread, so this mode works best with a moderate number of actors.

Both modes provide the same `Actor`, `Handler<M>`, `ActorRef<A>`, and `Context<A>` types. Switching requires changing imports and adding/removing `async`/`.await` on handlers and lifecycle hooks.
