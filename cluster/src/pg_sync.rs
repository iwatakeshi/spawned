//! Process group replication hooks for TCP transport.

use crate::protocol::{encode_cluster_frame, ClusterFrame, PgEvent, PgMemberEntry};
use crate::TransportError;
use std::sync::Arc;

/// Apply an inbound pg event from a remote peer.
pub trait PgInbound: Send + Sync {
    fn apply(&self, event: PgEvent) -> Result<(), TransportError>;
}

/// Snapshot locally-owned pg memberships for peer sync.
pub trait PgSnapshot: Send + Sync {
    fn local_entries(&self) -> Vec<PgMemberEntry>;
}

pub(crate) type PgApplyFn = Arc<dyn Fn(PgEvent) -> Result<(), TransportError> + Send + Sync>;
pub(crate) type PgSnapshotFn = Arc<dyn Fn() -> Vec<PgMemberEntry> + Send + Sync>;

struct FnInbound(FnApply);
type FnApply = Arc<dyn Fn(PgEvent) -> Result<(), TransportError> + Send + Sync>;

impl PgInbound for FnInbound {
    fn apply(&self, event: PgEvent) -> Result<(), TransportError> {
        (self.0)(event)
    }
}

struct FnSnapshot(FnSnap);
type FnSnap = Arc<dyn Fn() -> Vec<PgMemberEntry> + Send + Sync>;

impl PgSnapshot for FnSnapshot {
    fn local_entries(&self) -> Vec<PgMemberEntry> {
        (self.0)()
    }
}

#[derive(Clone)]
pub struct PgHooks {
    pub inbound: Option<Arc<dyn PgInbound>>,
    pub snapshot: Option<Arc<dyn PgSnapshot>>,
}

impl PgHooks {
    pub fn none() -> Self {
        Self {
            inbound: None,
            snapshot: None,
        }
    }

    pub fn from_fns(apply: PgApplyFn, snapshot: PgSnapshotFn) -> Self {
        Self {
            inbound: Some(Arc::new(FnInbound(apply))),
            snapshot: Some(Arc::new(FnSnapshot(snapshot))),
        }
    }
}

pub(crate) fn encode_pg_event(event: &PgEvent) -> Result<Vec<u8>, TransportError> {
    encode_cluster_frame(&ClusterFrame::Pg(event.clone())).map_err(TransportError::from)
}

pub(crate) fn apply_pg_event(hooks: &PgHooks, event: PgEvent) -> Result<(), TransportError> {
    if let Some(inbound) = &hooks.inbound {
        inbound.apply(event)
    } else {
        Ok(())
    }
}
