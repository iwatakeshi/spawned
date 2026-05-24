//! Control-plane replication hooks (registry + pg).

use crate::pg_sync::{apply_pg_event, PgHooks};
use crate::protocol::{PgEvent, RegistryEvent};
use crate::registry::{apply_registry_event, RegistryHooks};

#[derive(Clone)]
pub struct ControlPlaneHooks {
    pub registry: RegistryHooks,
    pub pg: PgHooks,
}

impl ControlPlaneHooks {
    pub fn none() -> Self {
        Self {
            registry: RegistryHooks::none(),
            pg: PgHooks::none(),
        }
    }

    pub fn federated(
        registry_apply: crate::registry::RegistryApplyFn,
        registry_snapshot: crate::registry::RegistrySnapshotFn,
        pg_apply: crate::pg_sync::PgApplyFn,
        pg_snapshot: crate::pg_sync::PgSnapshotFn,
    ) -> Self {
        Self {
            registry: RegistryHooks::from_fns(registry_apply, registry_snapshot),
            pg: PgHooks::from_fns(pg_apply, pg_snapshot),
        }
    }
}

pub(crate) fn send_control_plane_snapshots(
    stream: &mut std::net::TcpStream,
    hooks: &ControlPlaneHooks,
) -> Result<(), crate::TransportError> {
    let registry_event = if let Some(snapshot) = hooks.registry.snapshot.as_ref() {
        RegistryEvent::Snapshot {
            entries: snapshot.local_entries(),
        }
    } else {
        RegistryEvent::Snapshot {
            entries: Vec::new(),
        }
    };
    let registry_bytes = crate::registry::encode_registry_event(&registry_event)?;
    crate::frame::write_frame(&mut *stream, &registry_bytes)?;

    let pg_event = if let Some(snapshot) = hooks.pg.snapshot.as_ref() {
        PgEvent::Snapshot {
            entries: snapshot.local_entries(),
        }
    } else {
        PgEvent::Snapshot {
            entries: Vec::new(),
        }
    };
    let pg_bytes = crate::pg_sync::encode_pg_event(&pg_event)?;
    crate::frame::write_frame(&mut *stream, &pg_bytes)?;
    Ok(())
}

pub(crate) fn apply_control_plane_snapshots(
    stream: &mut std::net::TcpStream,
    hooks: &ControlPlaneHooks,
) -> Result<(), crate::TransportError> {
    use crate::frame::read_frame;
    use crate::protocol::{decode_cluster_frame, ClusterFrame, PgEvent, RegistryEvent};

    let registry_frame = read_frame(&mut *stream)?;
    if let Ok(ClusterFrame::Registry(RegistryEvent::Snapshot { entries })) =
        decode_cluster_frame(&registry_frame)
    {
        if hooks.registry.inbound.is_some() {
            for (name, address) in entries {
                apply_registry_event(
                    &hooks.registry,
                    RegistryEvent::Register { name, address },
                )?;
            }
        }
    }

    let pg_frame = read_frame(&mut *stream)?;
    if let Ok(ClusterFrame::Pg(PgEvent::Snapshot { entries })) = decode_cluster_frame(&pg_frame) {
        if hooks.pg.inbound.is_some() {
            for entry in entries {
                apply_pg_event(
                    &hooks.pg,
                    PgEvent::Join {
                        scope: entry.scope,
                        group: entry.group,
                        address: entry.address,
                    },
                )?;
            }
        }
    }
    Ok(())
}
