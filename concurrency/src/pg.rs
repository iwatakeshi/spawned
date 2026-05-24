//! Erlang-style process groups — named sets of actors for broadcast and dispatch.
//!
//! Groups are scoped (overlay networks) and many-to-many: an actor may join
//! multiple groups, and a group may contain many actors. Non-existent groups are
//! empty. Groups are created on first join and removed when empty.
//!
//! Actors are automatically removed from all groups when they exit.
//!
//! Internal member keys use [`ActorAddress`] (local node + [`ActorId`]) so the
//! store is cluster-ready; public APIs still accept [`ActorId`] on this node.
//!
//! For typed dispatch (sending messages to group members), use [`crate::tasks::pg`]
//! or [`crate::threads::pg`] depending on your runtime.

use crate::child_handle::{ActorId, ChildHandle};
use crate::error::ActorError;
use spawned_address::ActorAddress;
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};

/// Default process group scope (backward-compatible with unscoped MVP APIs).
pub const DEFAULT_SCOPE: &str = "default";

/// Errors from process group operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PgError {
    /// The actor was not a member of the group (or the group did not exist).
    #[error("actor {0} is not a member of group '{1}' in scope '{2}'")]
    NotJoined(ActorId, String, String),
}

/// Result of a fire-and-forget broadcast ([`crate::tasks::pg::cast`]).
#[derive(Debug, Default)]
pub struct PgSendReport {
    pub delivered: usize,
    pub failed: Vec<(ActorId, ActorError)>,
}

/// Result of a request broadcast ([`crate::tasks::pg::call`]).
#[derive(Debug)]
pub struct PgCallReport<T> {
    pub ok: Vec<(ActorId, T)>,
    pub failed: Vec<(ActorId, ActorError)>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct GroupKey {
    scope: String,
    group: String,
}

impl GroupKey {
    fn new(scope: impl AsRef<str>, group: impl AsRef<str>) -> Self {
        Self {
            scope: scope.as_ref().to_string(),
            group: group.as_ref().to_string(),
        }
    }
}

struct Member {
    handle: ChildHandle,
    joins: u32,
}

struct RemoteMember {
    joins: u32,
}

struct PgStore {
    groups: HashMap<GroupKey, HashMap<ActorAddress, Member>>,
    remote: HashMap<GroupKey, HashMap<ActorAddress, RemoteMember>>,
    index: HashMap<ActorAddress, HashSet<GroupKey>>,
    typed: HashMap<(GroupKey, TypeId), Box<dyn TypedBucket + Send + Sync>>,
}

fn local_address(id: ActorId) -> ActorAddress {
    ActorAddress::local(id)
}

impl PgStore {
    fn typed_join<T: Clone + Send + Sync + 'static>(
        &mut self,
        key: &GroupKey,
        address: ActorAddress,
        value: T,
    ) {
        let bucket_key = (key.clone(), TypeId::of::<T>());
        let bucket = self
            .typed
            .entry(bucket_key)
            .or_insert_with(|| Box::new(TypedMembers::<T>::default()));
        bucket
            .as_any_mut()
            .downcast_mut::<TypedMembers<T>>()
            .expect("typed pg bucket type mismatch")
            .insert(address, value);
    }

    fn typed_members<T: Clone + Send + Sync + 'static>(&self, key: &GroupKey) -> Vec<T> {
        let bucket_key = (key.clone(), TypeId::of::<T>());
        self.typed
            .get(&bucket_key)
            .and_then(|bucket| {
                bucket
                    .as_any()
                    .downcast_ref::<TypedMembers<T>>()
                    .map(TypedMembers::values)
            })
            .unwrap_or_default()
    }

    fn remove_actor(&mut self, address: ActorAddress) {
        if let Some(groups) = self.index.remove(&address) {
            for key in groups {
                if let Some(members) = self.groups.get_mut(&key) {
                    members.remove(&address);
                }
                if let Some(remote) = self.remote.get_mut(&key) {
                    remote.remove(&address);
                }
                self.remove_group_if_empty(&key);
            }
        }
        for bucket in self.typed.values_mut() {
            bucket.remove(&address);
        }
        self.typed.retain(|_, bucket| !bucket.is_empty());
    }

    fn remove_group_if_empty(&mut self, key: &GroupKey) {
        let local_empty = self.groups.get(key).is_none_or(|m| m.is_empty());
        let remote_empty = self.remote.get(key).is_none_or(|m| m.is_empty());
        if local_empty && remote_empty {
            self.groups.remove(key);
            self.remote.remove(key);
            self.index
                .values_mut()
                .for_each(|groups| _ = groups.remove(key));
            self.index.retain(|_, groups| !groups.is_empty());
            self.typed.retain(|(group_key, _), bucket| {
                if group_key == key {
                    false
                } else {
                    !bucket.is_empty()
                }
            });
        } else if local_empty {
            self.groups.remove(key);
        } else if remote_empty {
            self.remote.remove(key);
        }
    }

    fn remove_remote_if_empty(&mut self, key: &GroupKey) {
        if self.remote.get(key).is_some_and(|m| m.is_empty()) {
            self.remote.remove(key);
        }
        self.remove_group_if_empty(key);
    }
}

fn store() -> &'static RwLock<PgStore> {
    static STORE: OnceLock<RwLock<PgStore>> = OnceLock::new();
    STORE.get_or_init(|| {
        RwLock::new(PgStore {
            groups: HashMap::new(),
            remote: HashMap::new(),
            index: HashMap::new(),
            typed: HashMap::new(),
        })
    })
}

trait TypedBucket: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn remove(&mut self, address: &ActorAddress);
    fn is_empty(&self) -> bool;
}

struct TypedMembers<T> {
    members: HashMap<ActorAddress, T>,
}

impl<T> Default for TypedMembers<T> {
    fn default() -> Self {
        Self {
            members: HashMap::new(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> TypedMembers<T> {
    fn insert(&mut self, address: ActorAddress, value: T) {
        self.members.insert(address, value);
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

    fn remove(&mut self, address: &ActorAddress) {
        self.members.remove(address);
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// Join an actor to a group in the default scope.
///
/// Joins are refcounted: each call must be paired with [`leave`]. Groups are
/// created automatically on first join.
pub fn join(group: impl AsRef<str>, handle: ChildHandle) {
    join_scoped(DEFAULT_SCOPE, group, handle);
}

/// Join an actor to a scoped group.
pub fn join_scoped(scope: impl AsRef<str>, group: impl AsRef<str>, handle: ChildHandle) {
    let key = GroupKey::new(scope, group);
    let address = local_address(handle.id());
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let members = store.groups.entry(key.clone()).or_default();
    members
        .entry(address.clone())
        .and_modify(|member| member.joins += 1)
        .or_insert(Member { handle, joins: 1 });

    store.index.entry(address.clone()).or_default().insert(key.clone());

    #[cfg(feature = "cluster")]
    crate::cluster::publish_pg_event(spawned_cluster::PgEvent::Join {
        scope: key.scope.clone(),
        group: key.group.clone(),
        address,
    });
}

/// Leave a group once in the default scope (decrement join count).
pub fn leave(group: impl AsRef<str>, id: ActorId) -> Result<(), PgError> {
    leave_scoped(DEFAULT_SCOPE, group, id)
}

/// Leave a scoped group once (decrement join count).
pub fn leave_scoped(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    id: ActorId,
) -> Result<(), PgError> {
    let key = GroupKey::new(scope.as_ref(), group.as_ref());
    let scope_name = key.scope.clone();
    let group_name = key.group.clone();
    let address = local_address(id);
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let Some(members) = store.groups.get_mut(&key) else {
        return Err(PgError::NotJoined(id, group_name, scope_name));
    };

    let Some(member) = members.get_mut(&address) else {
        return Err(PgError::NotJoined(id, group_name, scope_name));
    };

    member.joins -= 1;
    if member.joins == 0 {
        members.remove(&address);
        store.remove_group_if_empty(&key);
        if let Some(groups) = store.index.get_mut(&address) {
            groups.remove(&key);
            if groups.is_empty() {
                store.index.remove(&address);
            }
        }
        for (bucket_key, bucket) in store.typed.iter_mut() {
            if bucket_key.0 == key {
                bucket.remove(&address);
            }
        }
        store.typed.retain(|_, bucket| !bucket.is_empty());

        #[cfg(feature = "cluster")]
        crate::cluster::publish_pg_event(spawned_cluster::PgEvent::Leave {
            scope: scope_name.clone(),
            group: group_name.clone(),
            address,
        });
    }

    Ok(())
}

/// Returns all member addresses in a scoped group (local + federated remote).
pub fn member_addresses_scoped(scope: impl AsRef<str>, group: impl AsRef<str>) -> Vec<ActorAddress> {
    let key = GroupKey::new(scope, group);
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    if let Some(members) = store.groups.get_mut(&key) {
        members.retain(|_, member| member.handle.is_alive());
        if members.is_empty() {
            store.groups.remove(&key);
        }
    }

    let mut out = Vec::new();
    if let Some(members) = store.groups.get(&key) {
        out.extend(members.keys().cloned());
    }
    if let Some(remote) = store.remote.get(&key) {
        out.extend(remote.keys().cloned());
    }
    store.remove_group_if_empty(&key);
    out
}

/// Returns all member addresses in the default scope (local + federated remote).
pub fn member_addresses(group: impl AsRef<str>) -> Vec<ActorAddress> {
    member_addresses_scoped(DEFAULT_SCOPE, group)
}

/// Returns all members of a group in the default scope (including dead actors until pruned).
pub fn get_members(group: impl AsRef<str>) -> Vec<ChildHandle> {
    get_members_scoped(DEFAULT_SCOPE, group)
}

/// Returns all members of a scoped group (including dead actors until pruned).
pub fn get_members_scoped(scope: impl AsRef<str>, group: impl AsRef<str>) -> Vec<ChildHandle> {
    get_local_members_scoped(scope, group)
}

/// Returns all members of a group on the local node in the default scope.
///
/// Identical to [`get_members`] in single-node deployments.
pub fn get_local_members(group: impl AsRef<str>) -> Vec<ChildHandle> {
    get_local_members_scoped(DEFAULT_SCOPE, group)
}

/// Returns all members of a scoped group on the local node.
///
/// Identical to [`get_members_scoped`] in single-node deployments.
pub fn get_local_members_scoped(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
) -> Vec<ChildHandle> {
    let key = GroupKey::new(scope, group);
    let mut store = store().write().unwrap_or_else(|p| p.into_inner());

    let Some(members) = store.groups.get_mut(&key) else {
        return Vec::new();
    };

    members.retain(|_, member| member.handle.is_alive());
    if members.is_empty() {
        store.remove_group_if_empty(&key);
        return Vec::new();
    }

    members
        .values()
        .map(|member| member.handle.clone())
        .collect()
}

/// Returns the names of all non-empty groups in the default scope.
pub fn which_groups() -> Vec<String> {
    which_groups_scoped(DEFAULT_SCOPE)
}

/// Returns the names of all non-empty groups in a scope.
pub fn which_groups_scoped(scope: impl AsRef<str>) -> Vec<String> {
    let scope = scope.as_ref();
    let store = store().read().unwrap_or_else(|p| p.into_inner());
    let mut names: Vec<_> = store
        .groups
        .keys()
        .filter(|key| key.scope == scope)
        .map(|key| key.group.clone())
        .collect();
    names.sort();
    names
}

/// Returns the names of all scopes that contain at least one group.
pub fn which_scopes() -> Vec<String> {
    let store = store().read().unwrap_or_else(|p| p.into_inner());
    let mut names: HashSet<_> = store.groups.keys().map(|key| key.scope.clone()).collect();
    let mut scopes: Vec<_> = names.drain().collect();
    scopes.sort();
    scopes
}

pub(crate) fn typed_join<T: Clone + Send + Sync + 'static>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
    id: ActorId,
    value: T,
) {
    let key = GroupKey::new(scope, group);
    store()
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .typed_join(&key, local_address(id), value);
}

pub(crate) fn typed_members<T: Clone + Send + Sync + 'static>(
    scope: impl AsRef<str>,
    group: impl AsRef<str>,
) -> Vec<T> {
    let key = GroupKey::new(scope, group);
    store()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .typed_members(&key)
}

/// Remove an actor from all groups. Called automatically when an actor exits.
pub(crate) fn remove_actor(id: ActorId) {
    let address = local_address(id);
    #[cfg(feature = "cluster")]
    let leaves: Vec<(String, String)> = {
        let store = store().read().unwrap_or_else(|p| p.into_inner());
        store
            .index
            .get(&address)
            .into_iter()
            .flat_map(|groups| groups.iter())
            .map(|key| (key.scope.clone(), key.group.clone()))
            .collect()
    };

    store()
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .remove_actor(address.clone());

    #[cfg(feature = "cluster")]
    {
        for (scope, group) in leaves {
            crate::cluster::publish_pg_event(spawned_cluster::PgEvent::Leave {
                scope,
                group,
                address: address.clone(),
            });
        }
        crate::cluster::remove_named_by_actor_id(id);
    }

    #[cfg(not(feature = "cluster"))]
    let _ = id;
}

/// Locally-owned pg memberships for federated snapshot sync.
#[cfg(feature = "cluster")]
pub fn local_pg_snapshot() -> Vec<spawned_cluster::PgMemberEntry> {
    let store = store().read().unwrap_or_else(|p| p.into_inner());
    store
        .groups
        .iter()
        .flat_map(|(key, members)| {
            members.keys().map(|address| spawned_cluster::PgMemberEntry {
                scope: key.scope.clone(),
                group: key.group.clone(),
                address: address.clone(),
            })
        })
        .collect()
}

/// Apply an inbound pg event from a remote peer.
#[cfg(feature = "cluster")]
pub fn apply_remote_pg_event(
    event: spawned_cluster::PgEvent,
) -> Result<(), spawned_cluster::TransportError> {
    use spawned_cluster::PgEvent;

    match event {
        PgEvent::Join {
            scope,
            group,
            address,
        } => {
            if address.is_local() {
                tracing::debug!(
                    scope = %scope,
                    group = %group,
                    ?address,
                    "ignoring remote pg join — owned locally"
                );
                return Ok(());
            }
            let key = GroupKey::new(&scope, &group);
            let mut store = store().write().unwrap_or_else(|p| p.into_inner());
            store
                .remote
                .entry(key)
                .or_default()
                .entry(address)
                .and_modify(|member| member.joins += 1)
                .or_insert(RemoteMember { joins: 1 });
            Ok(())
        }
        PgEvent::Leave {
            scope,
            group,
            address,
        } => {
            if address.is_local() {
                return Ok(());
            }
            let key = GroupKey::new(&scope, &group);
            let mut store = store().write().unwrap_or_else(|p| p.into_inner());
            let Some(members) = store.remote.get_mut(&key) else {
                return Ok(());
            };
            let Some(member) = members.get_mut(&address) else {
                return Ok(());
            };
            member.joins = member.joins.saturating_sub(1);
            if member.joins == 0 {
                members.remove(&address);
            }
            store.remove_remote_if_empty(&key);
            Ok(())
        }
        PgEvent::Snapshot { entries } => {
            for entry in entries {
                apply_remote_pg_event(PgEvent::Join {
                    scope: entry.scope,
                    group: entry.group,
                    address: entry.address,
                })?;
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
    use spawned_address::ActorAddress;
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
    fn scoped_groups_are_isolated() {
        let group = unique_group("pg_scope_iso");
        let scope_a = unique_group("scope_a");
        let scope_b = unique_group("scope_b");
        let h1 = dummy_handle();
        let h2 = dummy_handle();

        join_scoped(&scope_a, &group, h1.clone());
        join_scoped(&scope_b, &group, h2.clone());

        assert_eq!(get_members_scoped(&scope_a, &group).len(), 1);
        assert_eq!(get_members_scoped(&scope_b, &group).len(), 1);
        assert!(get_members(&group).is_empty());
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
        assert_eq!(
            err,
            PgError::NotJoined(handle.id(), group, DEFAULT_SCOPE.to_string())
        );
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
        typed_join(DEFAULT_SCOPE, &group, ActorId::next(), 42u32);
        typed_join(DEFAULT_SCOPE, &group, ActorId::next(), 99u32);
        let values: Vec<u32> = typed_members(DEFAULT_SCOPE, &group);
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

    #[test]
    fn which_scopes_lists_active_scopes() {
        let scope = unique_group("pg_scope_list");
        let group = unique_group("pg_grp");
        join_scoped(&scope, &group, dummy_handle());
        assert!(which_scopes().contains(&scope));
    }

    #[test]
    fn member_key_matches_local_address() {
        let group = unique_group("pg_addr");
        let handle = dummy_handle();
        let id = handle.id();
        join(&group, handle);

        let store = store().read().unwrap_or_else(|p| p.into_inner());
        let addr = ActorAddress::local(id);
        let key = GroupKey::new(DEFAULT_SCOPE, &group);
        assert_eq!(addr, local_address(id));
        assert!(store.groups.get(&key).unwrap().contains_key(&addr));
    }
}
