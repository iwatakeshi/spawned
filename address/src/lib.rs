//! Location-aware actor addressing for local and clustered deployments.
//!
//! [`ActorAddress`] pairs a [`NodeId`] with a local [`ActorId`], similar to
//! Erlang's `{Pid, Node}` tuple. Use [`local_node`] and [`ActorAddress::local`]
//! for single-node code paths today; cluster transport fills in remote nodes later.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
static LOCAL_NODE: OnceLock<NodeId> = OnceLock::new();

/// Unique identity for an actor instance on a single node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorId(u64);

impl ActorId {
    /// Allocate a fresh actor id on this node.
    pub fn next() -> Self {
        Self(NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// Construct from a known numeric id (e.g. after decoding from the wire).
    pub const fn from_raw(id: u64) -> Self {
        Self(id)
    }

    /// Raw numeric component.
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActorId({})", self.0)
    }
}

/// Erlang-style node name, e.g. `worker@127.0.0.1`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(String);

impl NodeId {
    /// Create a node identifier from a name string.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// View the underlying node name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Alias for [`NodeId`] — OTP documentation uses "node name" interchangeably.
pub type NodeName = NodeId;

/// Whether an address refers to this node or a remote peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locality {
    Local,
    Remote(NodeId),
}

/// Global actor address: node + local actor id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorAddress {
    pub node: NodeId,
    pub actor_id: ActorId,
}

impl ActorAddress {
    /// Address of an actor on this node.
    pub fn local(actor_id: ActorId) -> Self {
        Self {
            node: local_node(),
            actor_id,
        }
    }

    /// Address on an explicit node.
    pub fn on(node: NodeId, actor_id: ActorId) -> Self {
        Self { node, actor_id }
    }

    /// Returns whether this address targets the current node.
    pub fn is_local(&self) -> bool {
        self.node == local_node()
    }

    /// Classify as local or remote relative to this process.
    pub fn locality(&self) -> Locality {
        if self.is_local() {
            Locality::Local
        } else {
            Locality::Remote(self.node.clone())
        }
    }
}

impl std::fmt::Display for ActorAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.node, self.actor_id)
    }
}

/// Returns this process's node id.
///
/// Initialized from `SPAWNED_NODE_NAME` on first call, defaulting to
/// `spawned@localhost`.
pub fn local_node() -> NodeId {
    LOCAL_NODE
        .get_or_init(|| {
            std::env::var("SPAWNED_NODE_NAME")
                .map(NodeId::new)
                .unwrap_or_else(|_| NodeId::new("spawned@localhost"))
        })
        .clone()
}

/// Override the local node id (tests and bootstrap). Must run before any actors start.
pub fn set_local_node_for_test(node: NodeId) {
    let _ = LOCAL_NODE.set(node);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_unique() {
        let a = ActorId::next();
        let b = ActorId::next();
        assert_ne!(a, b);
    }

    #[test]
    fn local_address_is_local() {
        let id = ActorId::next();
        let addr = ActorAddress::local(id);
        assert!(addr.is_local());
        assert_eq!(addr.locality(), Locality::Local);
        assert_eq!(addr.actor_id, id);
    }

    #[test]
    fn remote_address_is_not_local() {
        let remote = NodeId::new("peer@10.0.0.2");
        let addr = ActorAddress::on(remote.clone(), ActorId::from_raw(1));
        assert!(!addr.is_local());
        assert_eq!(addr.locality(), Locality::Remote(remote));
    }

    #[test]
    fn display_format() {
        let addr = ActorAddress::on(NodeId::new("n@host"), ActorId::from_raw(42));
        assert_eq!(addr.to_string(), "n@host#ActorId(42)");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_roundtrip() {
        let addr = ActorAddress::on(NodeId::new("n@host"), ActorId::from_raw(7));
        let json = serde_json::to_string(&addr).unwrap();
        let back: ActorAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, back);
    }
}
