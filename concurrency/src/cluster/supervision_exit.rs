//! Wire ↔ runtime conversion for supervision exit events.

use crate::error::ExitReason;
use spawned_address::ActorAddress;
use spawned_cluster::WireExitReason;

pub fn exit_reason_to_wire(reason: &ExitReason) -> WireExitReason {
    match reason {
        ExitReason::Normal => WireExitReason::Normal,
        ExitReason::Shutdown => WireExitReason::Shutdown,
        ExitReason::Panic(msg) => WireExitReason::Panic(msg.clone()),
        ExitReason::Kill => WireExitReason::Kill,
    }
}

pub fn wire_to_exit_reason(reason: WireExitReason) -> ExitReason {
    match reason {
        WireExitReason::Normal => ExitReason::Normal,
        WireExitReason::Shutdown => ExitReason::Shutdown,
        WireExitReason::Panic(msg) => ExitReason::Panic(msg),
        WireExitReason::Kill => ExitReason::Kill,
    }
}

pub fn child_exit_envelope(
    child: ActorAddress,
    parent: ActorAddress,
    reason: &ExitReason,
) -> spawned_cluster::SupervisionEnvelope {
    spawned_cluster::SupervisionEnvelope {
        correlation_id: 0,
        event: spawned_cluster::SupervisionEvent::ChildExit {
            child,
            parent,
            reason: exit_reason_to_wire(reason),
        },
    }
}
