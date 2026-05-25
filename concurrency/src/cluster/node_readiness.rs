//! Cluster node readiness for orchestrator health probes (Phase 14).

/// Snapshot of cluster node readiness components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeReadiness {
    /// Global router is installed (always true after `Node::build`).
    pub router_installed: bool,
    /// TCP listener or libp2p listen addrs are active.
    pub listener_active: bool,
    /// Supervision broker was started for this node.
    pub supervision_enabled: bool,
    /// Supervision broker actor is alive.
    pub broker_alive: bool,
    /// Number of configured remote peers.
    pub peers_configured: usize,
    /// Whether a listen address was configured at build time.
    pub listen_configured: bool,
}

impl NodeReadiness {
    /// Returns true when required components for this node configuration are healthy.
    ///
    /// - When supervision is enabled, the broker actor must be alive.
    /// - When listen was configured, an active listener must exist.
    pub fn is_ready(&self) -> bool {
        let broker_ok = !self.supervision_enabled || self.broker_alive;
        let listener_ok = !self.listen_configured || self.listener_active;
        broker_ok && listener_ok
    }
}
