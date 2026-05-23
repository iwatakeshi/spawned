# Spawned Roadmap

**Last updated:** after merging PRs #166 (links), #168 (threads shutdown perf), and #162 (this doc).

For API details, see the [API Guide](API-GUIDE.md). For framework comparison research, see [design/FRAMEWORK_COMPARISON.md](design/FRAMEWORK_COMPARISON.md).

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

## Phase 3: Supervision Trees — in progress

Target: **v1.0.0**. Building blocks are largely in place; supervisor actor and restart policies remain.

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

### 3e. Child Specs and Supervisor — next

- **Child specs** ([#132](https://github.com/lambdaclass/spawned/issues/132)) — restart type, shutdown type
- **Supervisor actor** ([#133](https://github.com/lambdaclass/spawned/issues/133)) — OneForOne, OneForAll, RestForOne
- **Meltdown protection** — restart intensity limits
- **Dynamic supervisor** ([#134](https://github.com/lambdaclass/spawned/issues/134)) — stretch goal

### Other Phase 3 work

- **Threads shutdown perf** — ✅ [PR #168](https://github.com/lambdaclass/spawned/pull/168), closes [#157](https://github.com/lambdaclass/spawned/issues/157): poison-pill `Shutdown` replaces 100ms `recv_timeout` polling in threads mode

## Phase 4: Documentation & Polish — ongoing

- API Guide, migration guide, 15 examples
- Supervision guide (blocked on 3e)
- Doc tests in crate READMEs ([#137](https://github.com/lambdaclass/spawned/issues/137))

## Future Considerations (post-v1.0)

| Feature | Notes |
|---------|-------|
| Process groups (pg) | Erlang-style actor grouping |
| Priority message channels | Signal > Stop > Supervision > Message |
| State machines (`gen_statem`) | Protocol implementations |
| Backoff strategies | Built into supervision |
| Persistence / event sourcing | Akka Persistence pattern |
| Clustering / distribution | `ractor_cluster` equivalent |
| Built-in observability | Mailbox depth, message latency |
| Custom runtime | Purpose-built actor runtime |

## What's still missing for OTP parity

| Feature | Status |
|---------|--------|
| Supervisor actor + child specs | ❌ Not started |
| Restart strategies | ❌ Not started |
| Meltdown protection | ❌ Not started |
| Process groups | ❌ Not started |
| Distributed actors | ❌ Not started |

## References

- [PR #153](https://github.com/lambdaclass/spawned/pull/153): v0.5 implementation
- [PR #154](https://github.com/lambdaclass/spawned/pull/154): Design research
- [PR #163–#166](https://github.com/lambdaclass/spawned/pulls?q=is%3Apr+is%3Amerged): Supervision building blocks
- [PR #168](https://github.com/lambdaclass/spawned/pull/168): Threads shutdown perf
