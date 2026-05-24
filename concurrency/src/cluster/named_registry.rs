//! Named actor registration with cluster-ready [`ActorAddress`] lookup.

use crate::child_handle::{ActorId, ChildHandle};
use spawned_address::ActorAddress;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Errors from named actor registration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NamedRegistryError {
    /// A name is already registered.
    #[error("name '{0}' is already registered")]
    AlreadyRegistered(String),
}

struct NamedStore {
    addresses: HashMap<String, ActorAddress>,
    handles: HashMap<String, ChildHandle>,
}

impl NamedStore {
    fn new() -> Self {
        Self {
            addresses: HashMap::new(),
            handles: HashMap::new(),
        }
    }
}

fn store() -> &'static RwLock<NamedStore> {
    static STORE: OnceLock<RwLock<NamedStore>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(NamedStore::new()))
}

/// Register a local actor under a cluster-wide name.
///
/// Stores both the [`ActorAddress`] and a clone of the [`ChildHandle`] for local dispatch.
pub fn register_named(name: impl AsRef<str>, handle: ChildHandle) -> Result<(), NamedRegistryError> {
    use std::collections::hash_map::Entry;
    let name = name.as_ref().to_string();
    let address = ActorAddress::local(handle.id());
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    match store.addresses.entry(name.clone()) {
        Entry::Occupied(_) => Err(NamedRegistryError::AlreadyRegistered(name)),
        Entry::Vacant(v) => {
            v.insert(address);
            store.handles.insert(name, handle);
            Ok(())
        }
    }
}

/// Look up a registered actor's [`ActorAddress`].
pub fn lookup_address(name: impl AsRef<str>) -> Option<ActorAddress> {
    store()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .addresses
        .get(name.as_ref())
        .cloned()
}

/// Look up a registered local [`ChildHandle`].
pub fn lookup_handle(name: impl AsRef<str>) -> Option<ChildHandle> {
    store()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .handles
        .get(name.as_ref())
        .cloned()
}

/// Remove a named registration.
pub fn unregister_named(name: impl AsRef<str>) {
    let name = name.as_ref();
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());
    store.addresses.remove(name);
    store.handles.remove(name);
}

/// Remove all names pointing at a local actor id (called on actor exit).
pub(crate) fn remove_by_actor_id(id: ActorId) {
    let address = ActorAddress::local(id);
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());
    store.addresses.retain(|_, addr| addr != &address);
    store.handles.retain(|_, handle| handle.id() != id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exit_request::{new_requested_exit_reason, new_skip_stopped_flag};
    use crate::link::{new_link_table, new_linked_exit_reason, new_trap_exit_flag};
    use std::sync::Arc;
    use std::sync::{Condvar, Mutex};

    fn dummy_handle() -> ChildHandle {
        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let no_op_send_exit: crate::link::SendExitFn = Arc::new(|_| Ok(()));
        let no_op_send_signal: crate::child_handle::SendSignalFn = Arc::new(|_| Ok(()));
        ChildHandle::from_threads(
            ActorId::next(),
            Arc::new(|| {}),
            completion,
            new_trap_exit_flag(),
            new_link_table(),
            new_linked_exit_reason(),
            no_op_send_exit,
            no_op_send_signal,
            new_requested_exit_reason(),
            new_skip_stopped_flag(),
        )
    }

    fn unique_name(prefix: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}_{}", N.fetch_add(1, Ordering::Relaxed))
    }

    #[test]
    fn register_and_lookup_address() {
        let name = unique_name("named");
        let handle = dummy_handle();
        let id = handle.id();
        register_named(&name, handle).unwrap();

        let addr = lookup_address(&name).unwrap();
        assert_eq!(addr.actor_id, id);
        assert!(addr.is_local());
        assert!(lookup_handle(&name).is_some());
    }

    #[test]
    fn duplicate_name_fails() {
        let name = unique_name("dup");
        register_named(&name, dummy_handle()).unwrap();
        assert!(matches!(
            register_named(&name, dummy_handle()),
            Err(NamedRegistryError::AlreadyRegistered(_))
        ));
    }

    #[test]
    fn unregister_removes_entry() {
        let name = unique_name("unreg");
        register_named(&name, dummy_handle()).unwrap();
        unregister_named(&name);
        assert!(lookup_address(&name).is_none());
    }
}
