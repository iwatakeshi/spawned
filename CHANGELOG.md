# Changelog

All notable cluster protocol and supervision changes are documented here. For library API history, see git tags and release notes.

## Cluster protocol

### v3 (current)

- **Supervision control plane** — `SpawnRequest` / `SpawnReply`, `ChildExit`, `SupervisionSignal` (Stop, Shutdown, Kill), monitor/link propagation
- **`Demonitor.target`** — wire event includes monitored actor address (breaking change within v3 adopters must upgrade peers together)
- **Static remote placement** — declarative `ChildSpec::remote_worker` / `remote_named` on static supervisors (Phase 12.7)
- **Remote shutdown wait** — supervisors block on `ChildExit` for remote terminate/shutdown paths; spawn retry on transport errors (Phase 13)

Handshake: `PROTOCOL_VERSION = 3`, libp2p protocol id `/spawned/cluster/3`. Mixed-version clusters fail at handshake.

### v2

- Federated **named registry** and **process group (pg)** sync
- `ActorAddress` routing foundation
- No supervision control plane (remote spawn/supervision added in v3)

### v1

- Initial TCP cluster framing and address types (superseded by v2 registry work)

## Upgrade guidance

| From | To | Action |
|------|-----|--------|
| v2 | v3 | Upgrade all nodes together; enable `cluster` feature; install supervision broker via `Node::builder()` or manual `start_supervision_broker` |
| Pre-12.5 v3 | 12.5+ v3 | Upgrade all nodes together for `Demonitor.target` wire shape |
| 12.7 v3 | 13+ v3 | Rolling upgrade safe — Phase 13 is additive (shutdown wait + spawn retry); no wire change |

Breaking changes **always** bump `PROTOCOL_VERSION` and the libp2p protocol id. See [CLUSTERING.md — Protocol stability](docs/CLUSTERING.md#protocol-stability).
