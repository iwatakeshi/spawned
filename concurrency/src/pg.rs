//! Erlang-style process groups — named sets of actors for broadcast and dispatch.
//!
//! Groups are many-to-many: an actor may join multiple groups, and a group may
//! contain many actors. Non-existent groups are empty. Groups are created on
//! first join and removed when empty.
//!
//! Actors are automatically removed from all groups when they exit.
//!
//! For typed dispatch (sending messages to group members), use [`crate::tasks::pg`]
//! or [`crate::threads::pg`] depending on your runtime.

use crate::child_handle::{ActorId, ChildHandle};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};

/// Errors from process group operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PgError {
    /// The actor was not a member of the group (or the group did not exist).
    #[error("actor {0} is not a member of group '{1}'")]
    NotJoined(ActorId, String),
}

struct Member {
    handle: ChildHandle,
    joins: u32,
}

struct PgStore {
    groups: HashMap<String, HashMap<ActorId, Member>>,
    index: HashMap<ActorId, HashSet<String>>,
    typed: HashMap<(String, TypeId), Box<dyn TypedBucket + Send + Sync>>,
}

impl PgStore {
    fn typed_join<T: Clone + Send + Sync + 'static>(&mut self, group: &str, id: ActorId, value: T) {
        let key = (group.to_string(), TypeId::of::<T>());
        let bucket = self
            .typed
            .entry(key)
            .or_insert_with(|| Box::new(TypedMembers::<T>::default()));
        bucket
            .as_any_mut()
            .downcast_mut::<TypedMembers<T>>()
            .expect("typed pg bucket type mismatch")
            .insert(id, value);
    }

    fn typed_members<T: Clone + Send + Sync + 'static>(&self, group: &str) -> Vec<T> {
        let key = (group.to_string(), TypeId::of::<T>());
        self.typed
            .get(&key)
            .and_then(|bucket| {
                bucket
                    .as_any()
                    .downcast_ref::<TypedMembers<T>>()
                    .map(TypedMembers::values)
            })
            .unwrap_or_default()
    }

    fn remove_actor(&mut self, id: ActorId) {
        if let Some(groups) = self.index.remove(&id) {
            for group in groups {
                if let Some(members) = self.groups.get_mut(&group) {
                    members.remove(&id);
                    if members.is_empty() {
                        self.groups.remove(&group);
                    }
                }
            }
        }
        for bucket in self.typed.values_mut() {
            bucket.remove(id);
        }
        self.typed.retain(|_, bucket| !bucket.is_empty());
    }
}

fn store() -> &'static RwLock<PgStore> {
    static STORE: OnceLock<RwLock<PgStore>> = OnceLock::new();
    STORE.get_or_init(|| {
        RwLock::new(PgStore {
            groups: HashMap::new(),
            index: HashMap::new(),
            typed: HashMap::new(),
        })
    })
}

trait TypedBucket: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn remove(&mut self, id: ActorId);
    fn is_empty(&self) -> bool;
}

struct TypedMembers<T> {
    members: HashMap<ActorId, T>,
}

impl<T> Default for TypedMembers<T> {
    fn default() -> Self {
        Self {
            members: HashMap::new(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> TypedMembers<T> {
    fn insert(&mut self, id: ActorId, value: T) {
        self.members.insert(id, value);
    }

    fn values(&self) -> Vec<T> {
        self.members.values().cloned().collect()
    }
}

impl<T: Clone + Send + Sync + 'static> TypedBucket for TypedMembers<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn remove(&mut self, id: ActorId) {
        self.members.remove(&id);
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// Join an actor to a group.
///
/// Joins are refcounted: each call must be paired with [`leave`]. Groups are
/// created automatically on first join.
pub fn join(group: impl AsRef<str>, handle: ChildHandle) {
    let group = group.as_ref().to_string();
    let id = handle.id();
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let members = store.groups.entry(group.clone()).or_default();
    members
        .entry(id)
        .and_modify(|member| member.joins += 1)
        .or_insert(Member { handle, joins: 1 });

    store.index.entry(id).or_default().insert(group);
}

/// Leave a group once (decrement join count).
pub fn leave(group: impl AsRef<str>, id: ActorId) -> Result<(), PgError> {
    let group = group.as_ref();
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let Some(members) = store.groups.get_mut(group) else {
        return Err(PgError::NotJoined(id, group.to_string()));
    };

    let Some(member) = members.get_mut(&id) else {
        return Err(PgError::NotJoined(id, group.to_string()));
    };

    member.joins -= 1;
    if member.joins == 0 {
        members.remove(&id);
        if members.is_empty() {
            store.groups.remove(group);
        }
        if let Some(groups) = store.index.get_mut(&id) {
            groups.remove(group);
            if groups.is_empty() {
                store.index.remove(&id);
            }
        }
        for (key, bucket) in store.typed.iter_mut() {
            if key.0 == group {
                bucket.remove(id);
            }
        }
        store.typed.retain(|_, bucket| !bucket.is_empty());
    }

    Ok(())
}

/// Returns all members of a group (including dead actors until pruned).
///
/// Dead actors are removed lazily on read.
pub fn get_members(group: impl AsRef<str>) -> Vec<ChildHandle> {
    get_local_members(group)
}

/// Returns all members of a group on the local node.
///
/// Identical to [`get_members`] in single-node deployments.
pub fn get_local_members(group: impl AsRef<str>) -> Vec<ChildHandle> {
    let group = group.as_ref();
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let Some(members) = store.groups.get_mut(group) else {
        return Vec::new();
    };

    members.retain(|_, member| member.handle.is_alive());
    if members.is_empty() {
        store.groups.remove(group);
        store
            .index
            .values_mut()
            .for_each(|groups| _ = groups.remove(group));
        store.index.retain(|_, groups| !groups.is_empty());
        store.typed.retain(|(group_name, _), bucket| {
            if group_name == group {
                false
            } else {
                !bucket.is_empty()
            }
        });
        return Vec::new();
    }

    members
        .values()
        .map(|member| member.handle.clone())
        .collect()
}

/// Returns the names of all non-empty groups.
pub fn which_groups() -> Vec<String> {
    let store = store().read().unwrap_or_else(|p| p.into_inner());
    let mut names: Vec<_> = store.groups.keys().cloned().collect();
    names.sort();
    names
}

pub(crate) fn typed_join<T: Clone + Send + Sync + 'static>(
    group: impl AsRef<str>,
    id: ActorId,
    value: T,
) {
    store()
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .typed_join(group.as_ref(), id, value);
}

pub(crate) fn typed_members<T: Clone + Send + Sync + 'static>(group: impl AsRef<str>) -> Vec<T> {
    store()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .typed_members(group.as_ref())
}

/// Remove an actor from all groups. Called automatically when an actor exits.
pub(crate) fn remove_actor(id: ActorId) {
    store()
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .remove_actor(id);
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
        ChildHandle::from_threads(
            ActorId::next(),
            Arc::new(|| {}),
            completion,
            new_trap_exit_flag(),
            new_link_table(),
            new_linked_exit_reason(),
            no_op_send_exit,
            new_requested_exit_reason(),
            new_skip_stopped_flag(),
        )
    }

    fn unique_group(prefix: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}_{}", N.fetch_add(1, Ordering::Relaxed))
    }

    #[test]
    fn join_and_get_members() {
        let group = unique_group("pg_join");
        let h1 = dummy_handle();
        let h2 = dummy_handle();
        join(&group, h1.clone());
        join(&group, h2.clone());

        let members = get_members(&group);
        assert_eq!(members.len(), 2);
        assert!(members.iter().any(|h| h.id() == h1.id()));
        assert!(members.iter().any(|h| h.id() == h2.id()));
    }

    #[test]
    fn leave_decrements_refcount() {
        let group = unique_group("pg_refcount");
        let handle = dummy_handle();
        join(&group, handle.clone());
        join(&group, handle.clone());

        leave(&group, handle.id()).unwrap();
        assert_eq!(get_members(&group).len(), 1);

        leave(&group, handle.id()).unwrap();
        assert!(get_members(&group).is_empty());
        assert!(!which_groups().contains(&group));
    }

    #[test]
    fn leave_not_joined_returns_error() {
        let group = unique_group("pg_not_joined");
        let handle = dummy_handle();
        let err = leave(&group, handle.id()).unwrap_err();
        assert_eq!(err, PgError::NotJoined(handle.id(), group));
    }

    #[test]
    fn remove_actor_clears_all_groups() {
        let g1 = unique_group("pg_rm1");
        let g2 = unique_group("pg_rm2");
        let handle = dummy_handle();
        join(&g1, handle.clone());
        join(&g2, handle.clone());

        remove_actor(handle.id());

        assert!(get_members(&g1).is_empty());
        assert!(get_members(&g2).is_empty());
    }

    #[test]
    fn typed_members_roundtrip() {
        let group = unique_group("pg_typed");
        typed_join(&group, ActorId::next(), 42u32);
        typed_join(&group, ActorId::next(), 99u32);
        let values: Vec<u32> = typed_members(&group);
        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&99));
    }

    #[test]
    fn which_groups_lists_active_groups() {
        let g1 = unique_group("pg_which1");
        let g2 = unique_group("pg_which2");
        join(&g1, dummy_handle());
        join(&g2, dummy_handle());
        let groups = which_groups();
        assert!(groups.contains(&g1));
        assert!(groups.contains(&g2));
    }
}
