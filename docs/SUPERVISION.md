# Supervision Guide

Erlang-style fault tolerance for Spawned actors. This guide explains *when* and *how* to use supervision; see the [API Guide](API-GUIDE.md) for type and method reference.

## Overview

Spawned supervision separates **policy** (how to restart and shut down) from **mechanism** (links, exit signals, child handles). Built-in supervisors use [`SupervisorLogic`](../concurrency/src/supervisor/mod.rs) for restart decisions; you configure behavior through [`ChildSpec`](../concurrency/src/tasks/supervisor.rs) and [`RestartIntensity`](../concurrency/src/child_spec.rs).

```text
Client ──► Supervisor actor ──links──► Worker actors
              │
              └── exit_received(Exit) ──► restart / meltdown / ignore
```

Supervisors enable `trap_exit(true)` and link children via `start_linked`. Child deaths arrive as [`Exit`](../concurrency/src/link.rs) messages instead of killing the supervisor.

## Choosing a supervision model

| Model | Use when | Crate API |
|-------|----------|-----------|
| **Static supervisor** | Fixed tree known at startup (most services) | `Supervisor::builder()` |
| **Dynamic supervisor** | Runtime pools (connections, jobs, sessions) | `DynamicSupervisor::builder()` |
| **Manual linking** | Custom restart logic, learning, or legacy code | `trap_exit` + `exit_received` |

**Static vs dynamic:** If you call `.child(...)` in a builder before `.start()`, use static `Supervisor`. If children appear and disappear during operation, use `DynamicSupervisor` (OneForOne only, Erlang `simple_one_for_one` style).

**Nested trees:** A static supervisor can supervise another supervisor via `ChildSpec::supervisor(...)`. Nested supervisors default to `ShutdownType::Infinity` so the subtree has time to shut down.

## Child specs

A child spec describes how to **start**, **restart**, and **shut down** one child.

### RestartType

| Value | Restarts when |
|-------|----------------|
| `Permanent` | Any exit except supervisor-ordered `ExitReason::Shutdown` |
| `Transient` | Abnormal exit only (`ExitReason::is_abnormal()`) |
| `Temporary` | Never |

`ExitReason::Shutdown` is produced by `ChildHandle::shutdown()` during graceful supervisor shutdown — permanent children do **not** restart on that path.

### ShutdownType

| Value | Behavior |
|-------|----------|
| `Timeout(d)` | `shutdown()` → wait up to `d` → escalate to `kill()` (OTP worker default: 5s) |
| `Infinity` | Wait indefinitely for `stopped()` |
| `BrutalKill` | Immediate `kill()`, skips `stopped()` |

Workers default to `Timeout(5s)` via `DEFAULT_WORKER_SHUTDOWN`. Use `.with_shutdown(ShutdownType::Infinity)` when `stopped()` cleanup can exceed five seconds.

**Escalation caveat:** `kill()` sets `skip_stopped` but does not interrupt an in-flight message handler or `stopped()` callback. The supervisor keeps waiting until the actor returns to its message loop.

For load-sensitive workers, `ChildSpec::worker()` defaults to a bounded fail-fast mailbox (`MailboxConfig::default_worker()`, capacity 64). Override with `.with_mailbox(...)` — e.g. `.with_mailbox(MailboxConfig::unbounded())` or a custom capacity. Restarts inherit the spec's mailbox config.

**Exit delivery priority:** Link-propagated `Exit` messages bypass user mailbox backpressure (Phase 6b) and are dequeued before queued user messages. Stop/cancellation beats supervision exits when both are queued. OS signals (Ctrl+C / SIGTERM) use the highest-priority channel (Phase 7). Supervisors with `trap_exit(true)` see child deaths promptly even under load.

### Root supervisor and OS signals

Register the top-level supervisor (and any peers that should stop on Ctrl+C / SIGTERM):

```rust
use spawned_concurrency::tasks::spawn_shutdown_signal_dispatcher;
use spawned_concurrency::register_shutdown_on_signal;

spawn_shutdown_signal_dispatcher();
let _guards = register_shutdown_on_signal(&[sup.child_handle()]);
sup.join().await;
```

Registered actors receive a priority shutdown signal (`ExitReason::Shutdown`) without manual `ctrl_c()` orchestration. See [`http_workers`](../examples/http_workers) for a full example.

```rust
use spawned_concurrency::MailboxConfig;

ChildSpec::worker("api", || ApiServer::new(), RestartType::Permanent)
    .with_mailbox(MailboxConfig::bounded(128)) // override default 64
```

### Start closure

Child specs must start children **linked** to the supervisor:

```rust
ChildSpec::worker("worker", || MyWorker::new(), RestartType::Permanent)
// internally: start_linked_with_mailbox(supervisor_ctx, spec.mailbox).child_handle()
```

## Static Supervisor

### Builder

```rust
use spawned_concurrency::tasks::{ChildSpec, Supervisor};
use spawned_concurrency::{
    RestartIntensity, RestartType, ShutdownType, SupervisorStrategy,
};
use std::time::Duration;

let sup = Supervisor::builder()
    .strategy(SupervisorStrategy::OneForOne)
    .intensity(RestartIntensity {
        max_restarts: 5,
        within: Duration::from_secs(10),
    })
    .child(ChildSpec::worker("api", || ApiServer::new(), RestartType::Permanent))
    .child(ChildSpec::worker("cache", || Cache::new(), RestartType::Transient))
    .start();
```

### Restart strategies

| Strategy | On child death |
|----------|----------------|
| **OneForOne** | Restart only the dead child |
| **OneForAll** | Shut down all children, then restart all |
| **RestForOne** | Shut down the dead child and all children started **after** it, then restart those |

Batch strategies wait for each child to exit (using its `ShutdownType`) before restarting survivors.

### Meltdown

Restart attempts are counted in a sliding window (`max_restarts` within `Duration`). When exceeded, the supervisor logs an error and stops abnormally — same idea as Erlang's restart intensity.

Tune intensity for crash-looping dependencies: too low causes unnecessary meltdown; too high delays detection of systemic failure.

### Graceful supervisor shutdown

When the supervisor stops, `stopped()` runs with `suppress_restarts` set. Children are shut down in **reverse start order** using each spec's `ShutdownType`. Permanent children receive `ExitReason::Shutdown` and are not restarted.

**Deferred from static supervisor MVP:** exponential backoff between restarts, OTP-style `Application` wrapper, and interruptible shutdown (see [ROADMAP](ROADMAP.md)).

## DynamicSupervisor

For homogeneous pools started at runtime:

```rust
use spawned_concurrency::tasks::{
    dynamic_supervisor::ChildSpec, DynamicSupervisor, DynamicSupervisorApi,
};
use spawned_concurrency::{RestartIntensity, RestartType};

let sup = DynamicSupervisor::builder()
    .max_children(100)
    .intensity(RestartIntensity {
        max_restarts: 10,
        within: Duration::from_secs(60),
    })
    .start();

let handle = sup
    .start_child(
        ChildSpec::worker("conn", || Connection::new(), RestartType::Temporary),
        Some("conn-42".into()), // optional registry name
    )
    .await?
    .unwrap();
```

| API | Purpose |
|-----|---------|
| `start_child(spec, reg_name)` | Start and supervise; instance id is `{template_id}#{n}` |
| `terminate_child(actor_id)` | Intentional remove — **no restart**, even for `Permanent` |
| `count_children()` | Alive child count |
| `which_children()` | List ids, actor ids, policies |

`terminate_child` removes the child from supervision before shutdown so crash/restart logic does not run.

### Deferred (not in MVP)

| Item | Workaround |
|------|------------|
| **OneForAll / RestForOne** | Use static `Supervisor` for batch restart trees; dynamic supervisor is OneForOne only |
| **Separate `ChildSpec` type** | Import from `dynamic_supervisor::ChildSpec`, not the static supervisor module |
| **Auto pg membership** | Call `tasks::pg::join` or `threads::pg::join` in the child's `started()` if the pool should be discoverable |
| **Backoff between restarts** | Sleep in `started()` or wrap restarts in application logic |

## Process groups

Named sets of actors for broadcast and dispatch — complementary to supervision, not a replacement.

```rust
use spawned_concurrency::tasks::{pg, ActorStart as _};
// or: use spawned_concurrency::threads::{pg, ActorStart as _};

// In Worker::started()
pg::join("handlers", &ctx.actor_ref());

// Broadcast to all live members
for worker in pg::members::<Worker>("handlers") {
    worker.send(Ping)?;
}
```

Actors auto-leave all groups on exit. See [API Guide — Process Groups](API-GUIDE.md#process-groups) (tasks and threads examples), [`pg_workers`](../examples/pg_workers), and integration tests in [`pg_integration.rs`](../concurrency/tests/pg_integration.rs).

**Deferred:** scopes, group membership monitors, distributed pg, built-in cast/call helpers. See [ROADMAP](ROADMAP.md).

## ChildHandle lifecycle

| Method | Exit reason | Runs `stopped()`? |
|--------|-------------|-------------------|
| `stop()` | `Normal` | Yes |
| `shutdown()` | `Shutdown` | Yes |
| `kill()` | `Kill` | No |

Supervisors use `shutdown()` / `kill()` via [`shutdown_child_async`](../concurrency/src/child_spec.rs) / `shutdown_child_blocking`.

## Links, monitors, and supervision

| Mechanism | Direction | Supervisor use |
|-----------|-----------|----------------|
| **Link** | Bidirectional | Supervisors link children; `trap_exit` converts exit to messages |
| **Monitor** | Unidirectional | Observers that should not affect child fate (metrics, logging) |

Use **links** inside supervision trees. Use **monitors** when an actor watches another without participating in fate-sharing.

## Exit reasons and restart decisions

| `ExitReason` | Abnormal? | Permanent restart? |
|--------------|-----------|-------------------|
| `Normal` | No | No |
| `Shutdown` | No | No |
| `Panic(_)` | Yes | Yes |
| `Kill` | Yes | Yes |

Use `should_restart(restart_type, &reason)` to preview policy outside a supervisor.

## Registry integration

`DynamicSupervisor::start_child` accepts an optional registry name. On success the child handle is registered globally; on terminate or final exit it is unregistered. Static supervisors do not auto-register — register manually if needed.

## Examples

| Example | Demonstrates |
|---------|--------------|
| [`supervised_workers`](../examples/supervised_workers) | Static supervisor, OneForOne, restart policies |
| [`dynamic_workers`](../examples/dynamic_workers) | Runtime pool, terminate, crash restart |
| [`pg_workers`](../examples/pg_workers) | Process group join, broadcast, auto-leave on exit |
| [`exit_reason`](../examples/exit_reason) Scenario 9 | Manual `trap_exit` + linking (pre-supervisor pattern) |

## Common pitfalls

1. **Long `stopped()` without `Infinity`** — default 5s worker timeout escalates to kill.
2. **Supervisor + `Timeout` shutdown** — warned at spec build time; nested tree may not finish.
3. **Using `stop()` instead of `shutdown()`** — produces `Normal`, not `Shutdown`; restart policy differs for some setups.
4. **Blocking in handlers** — shutdown waits for the current handler to finish before `stopped()` runs.
5. **Mixing static and dynamic** — use static supervisor for the service tree; add a `DynamicSupervisor` child for the pool if both are needed.

## Further reading

- [API Guide — Child Specs and Supervisor](API-GUIDE.md#child-specs-and-supervisor)
- [ROADMAP](ROADMAP.md)
- [Framework comparison — Supervision](design/FRAMEWORK_COMPARISON.md)
