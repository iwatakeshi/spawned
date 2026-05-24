//! Registry replication hooks for TCP transport.

use crate::protocol::{encode_cluster_frame, ClusterFrame, RegistryEvent};
use crate::TransportError;
use spawned_address::ActorAddress;
use std::sync::Arc;

/// Apply an inbound registry event from a remote peer.
pub trait RegistryInbound: Send + Sync {
    fn apply(&self, event: RegistryEvent) -> Result<(), TransportError>;
}

/// Snapshot locally-owned registry entries for peer sync.
pub trait RegistrySnapshot: Send + Sync {
    fn local_entries(&self) -> Vec<(String, ActorAddress)>;
}

pub(crate) type RegistryApplyFn = Arc<dyn Fn(RegistryEvent) -> Result<(), TransportError> + Send + Sync>;
pub(crate) type RegistrySnapshotFn = Arc<dyn Fn() -> Vec<(String, ActorAddress)> + Send + Sync>;

struct FnInbound(FnApply);
type FnApply = Arc<dyn Fn(RegistryEvent) -> Result<(), TransportError> + Send + Sync>;

impl RegistryInbound for FnInbound {
    fn apply(&self, event: RegistryEvent) -> Result<(), TransportError> {
        (self.0)(event)
    }
}

struct FnSnapshot(FnSnap);
type FnSnap = Arc<dyn Fn() -> Vec<(String, ActorAddress)> + Send + Sync>;

impl RegistrySnapshot for FnSnapshot {
    fn local_entries(&self) -> Vec<(String, ActorAddress)> {
        (self.0)()
    }
}

pub struct RegistryHooks {
    pub inbound: Option<Arc<dyn RegistryInbound>>,
    pub snapshot: Option<Arc<dyn RegistrySnapshot>>,
}

impl Clone for RegistryHooks {
    fn clone(&self) -> Self {
        Self {
            inbound: self.inbound.clone(),
            snapshot: self.snapshot.clone(),
        }
    }
}

impl RegistryHooks {
    pub fn none() -> Self {
        Self {
            inbound: None,
            snapshot: None,
        }
    }

    pub fn from_fns(apply: RegistryApplyFn, snapshot: RegistrySnapshotFn) -> Self {
        Self {
            inbound: Some(Arc::new(FnInbound(apply))),
            snapshot: Some(Arc::new(FnSnapshot(snapshot))),
        }
    }
}

pub(crate) fn encode_registry_event(event: &RegistryEvent) -> Result<Vec<u8>, TransportError> {
    encode_cluster_frame(&ClusterFrame::Registry(event.clone())).map_err(TransportError::from)
}

pub(crate) fn apply_registry_event(
    hooks: &RegistryHooks,
    event: RegistryEvent,
) -> Result<(), TransportError> {
    if let Some(inbound) = &hooks.inbound {
        inbound.apply(event)
    } else {
        Ok(())
    }
}
