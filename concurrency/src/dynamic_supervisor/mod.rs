use crate::child_handle::ActorId;
use crate::child_spec::{RestartType, ShutdownType};

/// Errors returned by dynamic supervisor operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DynamicSupervisorError {
    #[error("maximum number of children exceeded")]
    MaxChildrenExceeded,
    #[error("child not found: {0}")]
    ChildNotFound(ActorId),
    #[error("child id already registered: {0}")]
    DuplicateChildId(String),
    #[error("supervisor is stopping")]
    SupervisorStopping,
    #[error("registry error: {0}")]
    Registry(String),
    #[error("remote spawn error: {0}")]
    RemoteSpawn(String),
}

/// Runtime metadata for a dynamically supervised child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicChildInfo {
    pub id: String,
    pub actor_id: ActorId,
    pub alive: bool,
    pub restart: RestartType,
    pub shutdown: ShutdownType,
}

/// Build a unique instance id from a template id and monotonic counter.
pub(crate) fn instance_id(template_id: &str, counter: u64) -> String {
    format!("{template_id}#{counter}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_includes_template_and_counter() {
        assert_eq!(instance_id("worker", 3), "worker#3");
    }
}
