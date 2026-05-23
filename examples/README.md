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
| [`signal_test`](signal_test) | tasks | Timers + `send_message_on` |
| [`signal_test_threads`](signal_test_threads) | threads | Timers + signals, thread-based |
| [`updater`](updater) | tasks | Periodic HTTP via `send_interval` |
| [`updater_threads`](updater_threads) | threads | Updater, thread-based |
| [`blocking_genserver`](blocking_genserver) | tasks | Backend comparison |
| [`busy_genserver_warning`](busy_genserver_warning) | tasks | Blocking-operation warning |

See [docs/ROADMAP.md](../docs/ROADMAP.md) for feature status.
