use crate::error::ExitReason;
use std::time::Duration;

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

/// Returns `true` if a child with `restart` policy should be restarted after `reason`.
pub fn should_restart(restart: RestartType, reason: &ExitReason) -> bool {
    match restart {
        RestartType::Permanent => !matches!(reason, ExitReason::Shutdown),
        RestartType::Transient => reason.is_abnormal(),
        RestartType::Temporary => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_restarts_on_normal_and_abnormal_but_not_shutdown() {
        assert!(should_restart(
            RestartType::Permanent,
            &ExitReason::Normal
        ));
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
        assert!(!should_restart(
            RestartType::Transient,
            &ExitReason::Normal
        ));
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
}
