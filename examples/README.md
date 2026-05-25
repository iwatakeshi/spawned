# Spawned examples

| Example | Mode | What it demonstrates |
|---------|------|----------------------|
| [`name_server`](name_server) | tasks | Basic protocol + actor macros |
| [`bank`](bank) | tasks | Typed per-handler errors |
| [`bank_threads`](bank_threads) | threads | Same bank API, thread-based |
| [`chat_room`](chat_room) | tasks | Multi-actor via protocol `XRef` type erasure |
| [`chat_room_threads`](chat_room_threads) | threads | Chat room, thread-based |
| [`ping_pong`](ping_pong) | tasks | Bidirectional actor communication |
| [`ping_pong_threads`](ping_pong_threads) | threads | Ping/pong, thread-based |
| [`service_discovery`](service_discovery) | tasks | Global registry |
| [`exit_reason`](exit_reason) | tasks | ExitReason, ChildHandle, monitors, links |
| [`supervised_workers`](supervised_workers) | tasks | ChildSpec, Supervisor, OneForOne restart |
| [`dynamic_workers`](dynamic_workers) | tasks | DynamicSupervisor, runtime child pool |
| [`pg_workers`](pg_workers) | tasks | Process groups — join, broadcast, auto-leave (`threads::pg` covered in [`pg_integration`](../concurrency/tests/pg_integration.rs)) |
| [`signal_test`](signal_test) | tasks | Timers + `send_message_on` |
| [`signal_test_threads`](signal_test_threads) | threads | Timers + signals, thread-based |
| [`updater`](updater) | tasks | Periodic HTTP via `send_interval` |
| [`updater_threads`](updater_threads) | threads | Updater, thread-based |
| [`blocking_genserver`](blocking_genserver) | tasks | Backend comparison |
| [`busy_genserver_warning`](busy_genserver_warning) | tasks | Blocking-operation warning |
| [`mailbox_backpressure`](mailbox_backpressure) | tasks | Bounded mailboxes — fail-fast, block, system bypass |
| [`http_workers`](http_workers) | tasks | Axum dispatch to bounded worker pool — 503 on overload |
| [`cluster_ping_pong`](cluster_ping_pong) | tasks + cluster | Cross-node RPC via `Node` + `RemoteActorRef` |
| [`cluster_supervised_workers`](cluster_supervised_workers) | tasks + cluster | Static supervisor with local + remote worker children |

See [docs/ROADMAP.md](../docs/ROADMAP.md) and [docs/PRODUCTION_READINESS.md](../docs/PRODUCTION_READINESS.md) for shipped features and production path.
