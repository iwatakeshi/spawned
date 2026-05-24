//! Route supervision envelopes to target nodes (unicast, not broadcast).

use spawned_address::NodeId;
use spawned_cluster::{SupervisionEnvelope, SupervisionEvent, TransportError};

/// Node that should receive this supervision event.
pub fn route_node(event: &SupervisionEvent) -> Option<NodeId> {
    match event {
        SupervisionEvent::SpawnRequest { placement, .. } => Some(placement.clone()),
        SupervisionEvent::Signal { target, .. } => Some(target.node.clone()),
        SupervisionEvent::ChildExit { parent, .. } => Some(parent.node.clone()),
        SupervisionEvent::Down { owner, .. } => Some(owner.node.clone()),
        SupervisionEvent::Monitor { target, .. } => Some(target.node.clone()),
        SupervisionEvent::Demonitor { target, .. } => Some(target.node.clone()),
        SupervisionEvent::Link { .. } | SupervisionEvent::Unlink { .. } => None,
        SupervisionEvent::SpawnOk { .. } | SupervisionEvent::SpawnErr { .. } => None,
    }
}

/// Publish an envelope locally or send it to a remote node.
pub fn publish_routed(
    local: &NodeId,
    envelope: SupervisionEnvelope,
    apply_local: impl FnOnce(SupervisionEnvelope) -> Result<(), TransportError>,
    send_remote: impl FnOnce(NodeId, SupervisionEnvelope) -> Result<(), TransportError>,
) -> Result<(), TransportError> {
    let Some(node) = route_node(&envelope.event) else {
        return Ok(());
    };
    if &node == local {
        apply_local(envelope)?;
    } else {
        send_remote(node, envelope)?;
    }
    Ok(())
}
