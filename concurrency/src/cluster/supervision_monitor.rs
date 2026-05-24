//! Remote monitor wire helpers and client publish API.

use crate::error::ExitReason;
use crate::monitor::{Down, MonitorRef};
use spawned_address::{local_node, ActorAddress};
use spawned_cluster::{SupervisionEnvelope, SupervisionEvent};

pub(crate) type SendDownFn =
    std::sync::Arc<dyn Fn(Down) -> Result<(), crate::error::ActorError> + Send + Sync>;

pub fn monitor_envelope(owner: ActorAddress, target: ActorAddress, monitor_ref: MonitorRef) -> SupervisionEnvelope {
    SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::Monitor {
            owner,
            target,
            monitor_ref: monitor_ref.raw(),
        },
    }
}

pub fn demonitor_envelope(
    owner: ActorAddress,
    target: ActorAddress,
    monitor_ref: MonitorRef,
) -> SupervisionEnvelope {
    SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::Demonitor {
            owner,
            target,
            monitor_ref: monitor_ref.raw(),
        },
    }
}

pub fn down_envelope(
    owner: ActorAddress,
    monitor_ref: MonitorRef,
    child: ActorAddress,
    reason: &ExitReason,
) -> SupervisionEnvelope {
    SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::Down {
            owner,
            monitor_ref: monitor_ref.raw(),
            child,
            reason: super::supervision_exit::exit_reason_to_wire(reason),
        },
    }
}

/// Publish a remote monitor install request to the target's node.
pub fn publish_monitor(owner: ActorAddress, target: ActorAddress, monitor_ref: MonitorRef) {
    super::supervision_sync::publish_supervision(monitor_envelope(owner, target, monitor_ref));
}

/// Publish a remote monitor cancel request to the target's node.
pub fn publish_demonitor(owner: ActorAddress, target: ActorAddress, monitor_ref: MonitorRef) {
    super::supervision_sync::publish_supervision(demonitor_envelope(owner, target, monitor_ref));
}

/// Publish a monitor-down event to the owner's node.
pub fn publish_down(
    owner: ActorAddress,
    monitor_ref: MonitorRef,
    child: ActorAddress,
    reason: &ExitReason,
) {
    super::supervision_sync::publish_supervision(down_envelope(
        owner, monitor_ref, child, reason,
    ));
}

pub fn is_local_address(address: &ActorAddress) -> bool {
    address.node == local_node()
}

pub(crate) fn local_supervision_handle(
    actor_id: crate::child_handle::ActorId,
) -> Option<crate::child_handle::ChildHandle> {
    super::supervision_broker::local_handle(actor_id)
}

/// Register a local actor to receive inbound remote [`Down`] messages.
pub fn register_supervision_monitor_owner(
    address: ActorAddress,
    send_down: SendDownFn,
) -> Result<(), spawned_cluster::TransportError> {
    super::supervision_broker::register_down_owner(address, send_down)
}

