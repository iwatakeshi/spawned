//! Threads-runtime [`ChildSpec`] newtype with `worker` / `supervisor` constructors.

use std::sync::Arc;

use crate::child_handle::ChildHandle;
use crate::child_spec::{
    ChildSpec as InnerChildSpec, ChildType, PgMembership, RestartBackoff, RestartType,
    ShutdownType, DEFAULT_WORKER_SHUTDOWN,
};
use crate::mailbox::MailboxConfig;

use super::pg;
use super::ActorStart;

/// Child specification for threads-mode supervisors (static and dynamic).
#[derive(Clone)]
pub struct ChildSpec(pub(crate) InnerChildSpec);

impl ChildSpec {
    pub fn worker<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self(InnerChildSpec {
            id: id.into(),
            start: Arc::new(
                move |parent: &ChildHandle, mailbox: MailboxConfig, pg_membership: Option<&PgMembership>| {
                    let actor_ref = start().start_linked_to_handle(parent, mailbox);
                    if let Some(pg) = pg_membership {
                        pg::join_scoped(&pg.scope, &pg.group, &actor_ref);
                    }
                    actor_ref.child_handle()
                },
            ),
            restart,
            shutdown: DEFAULT_WORKER_SHUTDOWN,
            child_type: ChildType::Worker,
            mailbox: MailboxConfig::default_worker(),
            backoff: RestartBackoff::default(),
            pg_membership: None,
            #[cfg(feature = "cluster")]
            placement: crate::cluster::Placement::Local,
            #[cfg(feature = "cluster")]
            remote: None,
        })
    }

    pub fn supervisor<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self(InnerChildSpec {
            id: id.into(),
            start: Arc::new(
                move |parent: &ChildHandle, mailbox: MailboxConfig, pg_membership: Option<&PgMembership>| {
                    let actor_ref = start().start_linked_to_handle(parent, mailbox);
                    if let Some(pg) = pg_membership {
                        pg::join_scoped(&pg.scope, &pg.group, &actor_ref);
                    }
                    actor_ref.child_handle()
                },
            ),
            restart,
            shutdown: ShutdownType::Infinity,
            child_type: ChildType::Supervisor,
            mailbox: MailboxConfig::unbounded(),
            backoff: RestartBackoff::default(),
            pg_membership: None,
            #[cfg(feature = "cluster")]
            placement: crate::cluster::Placement::Local,
            #[cfg(feature = "cluster")]
            remote: None,
        })
    }

    /// Create a remote child spec backed by a registered named spec on the placement node.
    #[cfg(feature = "cluster")]
    pub fn remote_named(
        id: impl Into<String>,
        spec_name: impl Into<String>,
        placement: spawned_address::NodeId,
        restart: RestartType,
    ) -> Self {
        use crate::child_spec::{unreachable_start, RemoteChildSpec};
        Self(InnerChildSpec {
            id: id.into(),
            start: unreachable_start(),
            restart,
            shutdown: DEFAULT_WORKER_SHUTDOWN,
            child_type: ChildType::Worker,
            mailbox: MailboxConfig::default_worker(),
            backoff: RestartBackoff::default(),
            pg_membership: None,
            placement: crate::cluster::Placement::Remote(placement),
            remote: Some(RemoteChildSpec::Named {
                spec_name: spec_name.into(),
            }),
        })
    }

    /// Create a remote child spec backed by a registered worker type on the placement node.
    #[cfg(feature = "cluster")]
    pub fn remote_worker(
        id: impl Into<String>,
        worker_type: impl Into<String>,
        init: impl serde::Serialize,
        placement: spawned_address::NodeId,
        restart: RestartType,
    ) -> Self {
        use crate::child_spec::{unreachable_start, RemoteChildSpec};
        let init = postcard::to_allocvec(&init).expect("remote worker init must serialize");
        Self(InnerChildSpec {
            id: id.into(),
            start: unreachable_start(),
            restart,
            shutdown: DEFAULT_WORKER_SHUTDOWN,
            child_type: ChildType::Worker,
            mailbox: MailboxConfig::default_worker(),
            backoff: RestartBackoff::default(),
            pg_membership: None,
            placement: crate::cluster::Placement::Remote(placement),
            remote: Some(RemoteChildSpec::Worker {
                worker_type: worker_type.into(),
                init,
            }),
        })
    }

    #[cfg(feature = "cluster")]
    pub fn with_placement(self, placement: crate::cluster::Placement) -> Self {
        Self(self.0.with_placement(placement))
    }

    pub fn with_shutdown(self, shutdown: ShutdownType) -> Self {
        Self(self.0.with_shutdown(shutdown))
    }

    pub fn with_mailbox(self, mailbox: MailboxConfig) -> Self {
        Self(self.0.with_mailbox(mailbox))
    }

    pub fn with_backoff(self, backoff: RestartBackoff) -> Self {
        Self(self.0.with_backoff(backoff))
    }

    pub fn with_pg_group(self, group: impl Into<String>) -> Self {
        Self(self.0.with_pg_group(group))
    }

    pub fn with_pg_group_scoped(self, scope: impl Into<String>, group: impl Into<String>) -> Self {
        Self(self.0.with_pg_group_scoped(scope, group))
    }
}

impl From<ChildSpec> for InnerChildSpec {
    fn from(spec: ChildSpec) -> Self {
        spec.0
    }
}

impl From<InnerChildSpec> for ChildSpec {
    fn from(spec: InnerChildSpec) -> Self {
        Self(spec)
    }
}

impl std::ops::Deref for ChildSpec {
    type Target = InnerChildSpec;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
