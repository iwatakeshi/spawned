//! Remote link wire helpers and client publish API.

use crate::child_handle::ActorId;
use crate::cluster::supervision_exit::child_exit_envelope;
use crate::error::ExitReason;
use spawned_address::ActorAddress;
use spawned_cluster::{SupervisionEnvelope, SupervisionEvent};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub(crate) type RemoteLinkTable = Arc<Mutex<HashSet<ActorAddress>>>;

pub fn link_envelope(a: ActorAddress, b: ActorAddress) -> SupervisionEnvelope {
    SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::Link { a, b },
    }
}

pub fn unlink_envelope(a: ActorAddress, b: ActorAddress) -> SupervisionEnvelope {
    SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::Unlink { a, b },
    }
}

/// Publish a remote link install request to the target's node (`b`).
pub fn publish_link(a: ActorAddress, b: ActorAddress) {
    super::supervision_sync::publish_supervision(link_envelope(a, b));
}

/// Publish a remote link cancel request to the target's node (`b`).
pub fn publish_unlink(a: ActorAddress, b: ActorAddress) {
    super::supervision_sync::publish_supervision(unlink_envelope(a, b));
}

/// Notify remote link peers when a local actor exits.
pub fn propagate_remote_link_exits(
    actor_id: ActorId,
    remote_links: &RemoteLinkTable,
    reason: &ExitReason,
) {
    let child = ActorAddress::local(actor_id);
    let peers: Vec<ActorAddress> = remote_links
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .cloned()
        .collect();
    for peer in peers {
        super::supervision_sync::publish_supervision(child_exit_envelope(
            child.clone(),
            peer,
            reason,
        ));
    }
}
