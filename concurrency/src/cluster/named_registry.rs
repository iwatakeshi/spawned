//! Named actor registration with cluster-ready [`ActorAddress`] lookup.
//!
//! Local actors store a [`ChildHandle`]; remote entries (Phase 10.1 federation)
//! store only [`ActorAddress`] and replicate via registry control-plane events.

use crate::child_handle::{ActorId, ChildHandle};
use spawned_address::ActorAddress;
use spawned_cluster::RegistryEvent;
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
/// Replicates a [`RegistryEvent::Register`] to peers when federation is enabled.
pub fn register_named(name: impl AsRef<str>, handle: ChildHandle) -> Result<(), NamedRegistryError> {
    use std::collections::hash_map::Entry;
    let name = name.as_ref().to_string();
    let address = ActorAddress::local(handle.id());
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    match store.addresses.entry(name.clone()) {
        Entry::Occupied(_) => Err(NamedRegistryError::AlreadyRegistered(name)),
        Entry::Vacant(v) => {
            v.insert(address.clone());
            store.handles.insert(name.clone(), handle);
            super::registry_sync::publish(RegistryEvent::Register { name, address });
            Ok(())
        }
    }
}

/// Look up a registered actor's [`ActorAddress`] (local or federated remote).
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

/// Remove a named registration (local only).
pub fn unregister_named(name: impl AsRef<str>) {
    let name = name.as_ref();
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());
    if let Some(address) = store.addresses.remove(name) {
        store.handles.remove(name);
        super::registry_sync::publish(RegistryEvent::Unregister {
            name: name.to_string(),
            address,
        });
    }
}

/// Remove all names pointing at a local actor id (called on actor exit).
pub(crate) fn remove_by_actor_id(id: ActorId) {
    let address = ActorAddress::local(id);
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());
    let names: Vec<_> = store
        .handles
        .iter()
        .filter(|(_, h)| h.id() == id)
        .map(|(n, _)| n.clone())
        .collect();
    for name in names {
        store.addresses.remove(&name);
        store.handles.remove(&name);
        super::registry_sync::publish(RegistryEvent::Unregister {
            name,
            address: address.clone(),
        });
    }
}

/// Locally-owned `(name, address)` pairs for registry snapshot sync.
pub fn local_snapshot() -> Vec<(String, ActorAddress)> {
    let store = store().read().unwrap_or_else(|p| p.into_inner());
    store
        .handles
        .keys()
        .filter_map(|name| {
            store
                .addresses
                .get(name)
                .map(|addr| (name.clone(), addr.clone()))
        })
        .collect()
}

/// Apply an inbound registry event from a remote peer.
pub fn apply_remote_event(event: RegistryEvent) -> Result<(), spawned_cluster::TransportError> {
    match event {
        RegistryEvent::Register { name, address } => {
            let mut store = store().write().unwrap_or_else(|p| p.into_inner());
            if store.handles.contains_key(&name) {
                tracing::debug!(%name, "ignoring remote register — owned locally");
                return Ok(());
            }
            if let Some(existing) = store.addresses.get(&name) {
                if existing != &address {
                    tracing::warn!(
                        %name,
                        ?existing,
                        ?address,
                        "registry conflict — keeping existing entry"
                    );
                    return Ok(());
                }
            }
            store.addresses.insert(name, address);
            Ok(())
        }
        RegistryEvent::Unregister { name, address } => {
            let mut store = store().write().unwrap_or_else(|p| p.into_inner());
            if store.handles.contains_key(&name) {
                return Ok(());
            }
            if store.addresses.get(&name) == Some(&address) {
                store.addresses.remove(&name);
            }
            Ok(())
        }
        RegistryEvent::Snapshot { entries } => {
            for (name, address) in entries {
                let _ = apply_remote_event(RegistryEvent::Register { name, address });
            }
            Ok(())
        }
    }
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

    #[test]
    fn remote_register_is_lookupable() {
        let name = unique_name("remote");
        let addr = ActorAddress::on("peer@host".into(), ActorId::from_raw(42));
        apply_remote_event(RegistryEvent::Register {
            name: name.clone(),
            address: addr.clone(),
        })
        .unwrap();
        assert_eq!(lookup_address(&name), Some(addr));
        assert!(lookup_handle(&name).is_none());
    }
}
