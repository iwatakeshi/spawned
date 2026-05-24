//! Threads-runtime [`ChildSpec`] newtype with `worker` / `supervisor` constructors.

use std::sync::Arc;

use crate::child_handle::ChildHandle;
use crate::child_spec::{
    ChildSpec as InnerChildSpec, ChildType, RestartBackoff, RestartType, ShutdownType,
    DEFAULT_WORKER_SHUTDOWN,
};
use crate::mailbox::MailboxConfig;

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
            start: Arc::new(move |parent: &ChildHandle, mailbox: MailboxConfig| {
                start()
                    .start_linked_to_handle(parent, mailbox)
                    .child_handle()
            }),
            restart,
            shutdown: DEFAULT_WORKER_SHUTDOWN,
            child_type: ChildType::Worker,
            mailbox: MailboxConfig::default_worker(),
            backoff: RestartBackoff::default(),
        })
    }

    pub fn supervisor<A, F>(id: impl Into<String>, start: F, restart: RestartType) -> Self
    where
        A: ActorStart,
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self(InnerChildSpec {
            id: id.into(),
            start: Arc::new(move |parent: &ChildHandle, mailbox: MailboxConfig| {
                start()
                    .start_linked_to_handle(parent, mailbox)
                    .child_handle()
            }),
            restart,
            shutdown: ShutdownType::Infinity,
            child_type: ChildType::Supervisor,
            mailbox: MailboxConfig::unbounded(),
            backoff: RestartBackoff::default(),
        })
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
