use crate::child_handle::ChildHandle;
use crate::error::ExitReason;
use crate::mailbox::MailboxConfig;
use std::sync::Arc;
use std::time::Duration;

/// OTP default shutdown for worker children (5 seconds).
pub const DEFAULT_WORKER_SHUTDOWN: ShutdownType = ShutdownType::Timeout(Duration::from_secs(5));

/// Apply shutdown policy and block until the child has exited.
pub fn shutdown_child_blocking(handle: &ChildHandle, shutdown: ShutdownType) -> ExitReason {
    if let Some(reason) = handle.exit_reason() {
        return reason;
    }

    match shutdown {
        ShutdownType::BrutalKill => {
            handle.kill();
            handle.wait_exit_blocking()
        }
        ShutdownType::Infinity => {
            handle.shutdown();
            handle.wait_exit_blocking()
        }
        ShutdownType::Timeout(duration) => {
            handle.shutdown();
            if let Some(reason) = handle.wait_exit_blocking_with_timeout(duration) {
                return reason;
            }
            tracing::warn!(
                child = %handle.id(),
                ?duration,
                "child shutdown timed out — escalating to kill"
            );
            handle.kill();
            handle.wait_exit_blocking()
        }
    }
}

/// Apply shutdown policy and wait asynchronously until the child has exited.
pub async fn shutdown_child_async(handle: &ChildHandle, shutdown: ShutdownType) -> ExitReason {
    if let Some(reason) = handle.exit_reason() {
        return reason;
    }

    match shutdown {
        ShutdownType::BrutalKill => {
            handle.kill();
            handle.wait_exit_async().await
        }
        ShutdownType::Infinity => {
            handle.shutdown();
            handle.wait_exit_async().await
        }
        ShutdownType::Timeout(duration) => {
            handle.shutdown();
            if let Some(reason) = handle.wait_exit_async_with_timeout(duration).await {
                return reason;
            }
            tracing::warn!(
                child = %handle.id(),
                ?duration,
                "child shutdown timed out — escalating to kill"
            );
            handle.kill();
            handle.wait_exit_async().await
        }
    }
}

/// Warn when a nested supervisor uses a finite shutdown timeout (OTP race risk).
pub(crate) fn warn_supervisor_timeout(child_type: ChildType, shutdown: ShutdownType) {
    if matches!(child_type, ChildType::Supervisor) && matches!(shutdown, ShutdownType::Timeout(_)) {
        tracing::warn!(
            "ChildSpec with ChildType::Supervisor and ShutdownType::Timeout may terminate \
             the subtree before nested children finish shutting down"
        );
    }
}

/// How a supervised child is restarted when it exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartType {
    /// Restart on any exit except supervisor-initiated [`ExitReason::Shutdown`].
    Permanent,
    /// Restart only on abnormal exit ([`ExitReason::is_abnormal`]).
    Transient,
    /// Never restart.
    Temporary,
}

/// How a supervisor stops a child during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownType {
    /// Wait indefinitely for `stopped()` to complete.
    Infinity,
    /// Wait up to `Duration`, then escalate to kill.
    Timeout(Duration),
    /// Immediate kill — no `stopped()` callback.
    BrutalKill,
}

/// Whether a supervised child is a worker or nested supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildType {
    Worker,
    Supervisor,
}

/// Restart intensity limits — Erlang-style max restarts within a time window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartIntensity {
    pub max_restarts: u32,
    pub within: Duration,
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self {
            max_restarts: 3,
            within: Duration::from_secs(5),
        }
    }
}

/// Delay applied before restarting a child after an abnormal exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartBackoff {
    /// Restart immediately (default).
    None,
    /// Wait a fixed duration before each restart attempt.
    Fixed(Duration),
    /// Exponential delay: `base * 2^(attempt - 1)`, capped at `max`.
    Exponential { base: Duration, max: Duration },
}

impl Default for RestartBackoff {
    fn default() -> Self {
        Self::None
    }
}

impl RestartBackoff {
    /// Delay before restart attempt `attempt` (1-based).
    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        match self {
            Self::None => Duration::ZERO,
            Self::Fixed(delay) => delay,
            Self::Exponential { base, max } => {
                let shift = attempt.saturating_sub(1).min(20);
                base.saturating_mul(1u32 << shift).min(max)
            }
        }
    }
}

/// Returns `true` if a child with `restart` policy should be restarted after `reason`.
pub fn should_restart(restart: RestartType, reason: &ExitReason) -> bool {
    match restart {
        RestartType::Permanent => !matches!(reason, ExitReason::Shutdown),
        RestartType::Transient => reason.is_abnormal(),
        RestartType::Temporary => false,
    }
}

/// Type-erased child start function (parent handle + mailbox + optional pg → child handle).
pub type ChildStartFn =
    Arc<dyn Fn(&ChildHandle, MailboxConfig, Option<&PgMembership>) -> ChildHandle + Send + Sync>;

/// Process group to join when a supervised child starts (see [`ChildSpec::with_pg_group`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgMembership {
    pub scope: String,
    pub group: String,
}

/// Specification for a supervised child — shared by static and dynamic supervisors.
pub struct ChildSpec {
    pub id: String,
    pub restart: RestartType,
    pub shutdown: ShutdownType,
    pub child_type: ChildType,
    pub mailbox: MailboxConfig,
    pub backoff: RestartBackoff,
    pub pg_membership: Option<PgMembership>,
    pub(crate) start: ChildStartFn,
}

impl Clone for ChildSpec {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            start: self.start.clone(),
            restart: self.restart,
            shutdown: self.shutdown,
            child_type: self.child_type,
            mailbox: self.mailbox,
            backoff: self.backoff,
            pg_membership: self.pg_membership.clone(),
        }
    }
}

impl ChildSpec {
    /// Start a linked child under `parent` using this spec's mailbox policy.
    pub(crate) fn start_child(&self, parent: &ChildHandle) -> ChildHandle {
        (self.start)(parent, self.mailbox, self.pg_membership.as_ref())
    }

    /// Join the child to this process group (default scope) when it starts.
    ///
    /// Typed dispatch via [`crate::tasks::pg`] / [`crate::threads::pg`] works when the
    /// spec is built with [`crate::tasks::ChildSpec::worker`] or
    /// [`crate::tasks::ChildSpec::supervisor`].
    pub fn with_pg_group(mut self, group: impl Into<String>) -> Self {
        self.with_pg_group_scoped(crate::pg::DEFAULT_SCOPE, group)
    }

    /// Join the child to a scoped process group when it starts.
    pub fn with_pg_group_scoped(mut self, scope: impl Into<String>, group: impl Into<String>) -> Self {
        self.pg_membership = Some(PgMembership {
            scope: scope.into(),
            group: group.into(),
        });
        self
    }

    pub(crate) fn has_pg_membership(&self) -> bool {
        self.pg_membership.is_some()
    }

    pub fn with_shutdown(mut self, shutdown: ShutdownType) -> Self {
        warn_supervisor_timeout(self.child_type, shutdown);
        self.shutdown = shutdown;
        self
    }

    pub fn with_mailbox(mut self, mailbox: MailboxConfig) -> Self {
        self.mailbox = mailbox;
        self
    }

    pub fn with_backoff(mut self, backoff: RestartBackoff) -> Self {
        self.backoff = backoff;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exit_request::{new_requested_exit_reason, new_skip_stopped_flag};
    use crate::link::{new_link_table, new_linked_exit_reason, new_trap_exit_flag, SendExitFn};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;

    fn test_handle(
        cancel: Arc<dyn Fn() + Send + Sync>,
        completion: Arc<(Mutex<Option<ExitReason>>, Condvar)>,
    ) -> ChildHandle {
        let no_op_send_exit: SendExitFn = Arc::new(|_| Ok(()));
        let no_op_send_signal: crate::child_handle::SendSignalFn = Arc::new(|_| Ok(()));
        ChildHandle::from_threads(
            crate::child_handle::ActorId::next(),
            cancel,
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

    #[test]
    fn default_worker_shutdown_is_five_second_timeout() {
        assert_eq!(
            DEFAULT_WORKER_SHUTDOWN,
            ShutdownType::Timeout(Duration::from_secs(5))
        );
    }

    #[test]
    fn shutdown_child_blocking_infinity_waits_for_shutdown_reason() {
        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let token = spawned_rt::threads::CancellationToken::new();
        let cancel = Arc::new(move || token.cancel());
        let handle = test_handle(cancel, completion.clone());

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let (lock, cvar) = &*completion;
            let mut guard = lock.lock().unwrap();
            *guard = Some(ExitReason::Shutdown);
            cvar.notify_all();
        });

        assert_eq!(
            shutdown_child_blocking(&handle, ShutdownType::Infinity),
            ExitReason::Shutdown
        );
    }

    #[test]
    fn shutdown_child_blocking_timeout_escalates_to_kill() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let phase = Arc::new(AtomicUsize::new(0));
        let token = spawned_rt::threads::CancellationToken::new();
        let completion_for_cancel = completion.clone();
        let phase_for_cancel = phase.clone();
        let cancel = Arc::new(move || {
            token.cancel();
            let p = phase_for_cancel.fetch_add(1, Ordering::SeqCst);
            let completion = completion_for_cancel.clone();
            thread::spawn(move || {
                if p == 0 {
                    thread::sleep(Duration::from_millis(100));
                    let (lock, cvar) = &*completion;
                    let mut guard = lock.lock().unwrap();
                    *guard = Some(ExitReason::Shutdown);
                    cvar.notify_all();
                } else {
                    thread::sleep(Duration::from_millis(5));
                    let (lock, cvar) = &*completion;
                    let mut guard = lock.lock().unwrap();
                    *guard = Some(ExitReason::Kill);
                    cvar.notify_all();
                }
            });
        });
        let handle = test_handle(cancel, completion);

        let reason =
            shutdown_child_blocking(&handle, ShutdownType::Timeout(Duration::from_millis(20)));
        assert_eq!(reason, ExitReason::Kill);
    }

    #[test]
    fn shutdown_child_blocking_brutal_kill() {
        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let token = spawned_rt::threads::CancellationToken::new();
        let cancel = Arc::new(move || token.cancel());
        let handle = test_handle(cancel, completion.clone());

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let (lock, cvar) = &*completion;
            let mut guard = lock.lock().unwrap();
            *guard = Some(ExitReason::Kill);
            cvar.notify_all();
        });

        assert_eq!(
            shutdown_child_blocking(&handle, ShutdownType::BrutalKill),
            ExitReason::Kill
        );
    }

    #[test]
    fn permanent_restarts_on_normal_and_abnormal_but_not_shutdown() {
        assert!(should_restart(RestartType::Permanent, &ExitReason::Normal));
        assert!(should_restart(
            RestartType::Permanent,
            &ExitReason::Panic("x".into())
        ));
        assert!(!should_restart(
            RestartType::Permanent,
            &ExitReason::Shutdown
        ));
    }

    #[test]
    fn transient_restarts_only_on_abnormal() {
        assert!(!should_restart(RestartType::Transient, &ExitReason::Normal));
        assert!(!should_restart(
            RestartType::Transient,
            &ExitReason::Shutdown
        ));
        assert!(should_restart(
            RestartType::Transient,
            &ExitReason::Panic("x".into())
        ));
        assert!(should_restart(RestartType::Transient, &ExitReason::Kill));
    }

    #[test]
    fn temporary_never_restarts() {
        for reason in [
            ExitReason::Normal,
            ExitReason::Shutdown,
            ExitReason::Panic("x".into()),
            ExitReason::Kill,
        ] {
            assert!(!should_restart(RestartType::Temporary, &reason));
        }
    }

    #[test]
    fn backoff_none_is_zero() {
        assert_eq!(RestartBackoff::None.delay_for_attempt(1), Duration::ZERO);
    }

    #[test]
    fn backoff_fixed_is_constant() {
        let policy = RestartBackoff::Fixed(Duration::from_millis(50));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(50));
        assert_eq!(policy.delay_for_attempt(5), Duration::from_millis(50));
    }

    #[test]
    fn backoff_exponential_doubles_and_caps() {
        let policy = RestartBackoff::Exponential {
            base: Duration::from_millis(10),
            max: Duration::from_millis(100),
        };
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(10));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(20));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(40));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(80));
        assert_eq!(policy.delay_for_attempt(5), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(10), Duration::from_millis(100));
    }
}
