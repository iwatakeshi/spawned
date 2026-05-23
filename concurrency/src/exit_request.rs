use crate::error::ExitReason;
use crate::link::LinkedExitReason;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

/// Slot set before cancellation to override the actor's final [`ExitReason`].
pub(crate) type RequestedExitReason = Arc<Mutex<Option<ExitReason>>>;

pub(crate) fn new_requested_exit_reason() -> RequestedExitReason {
    Arc::new(Mutex::new(None))
}

pub(crate) fn new_skip_stopped_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// Merge handler exit reason with requested and linked overrides.
pub(crate) fn resolve_exit_reason(
    exit_reason: ExitReason,
    requested: &RequestedExitReason,
    linked_reason: &LinkedExitReason,
) -> ExitReason {
    if let Some(reason) = requested.lock().unwrap_or_else(|p| p.into_inner()).take() {
        return reason;
    }
    if matches!(exit_reason, ExitReason::Normal) {
        if let Some(linked) = linked_reason
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            return linked;
        }
    }
    exit_reason
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::new_linked_exit_reason;

    #[test]
    fn requested_exit_takes_precedence() {
        let requested = new_requested_exit_reason();
        *requested.lock().unwrap() = Some(ExitReason::Shutdown);
        let linked = new_linked_exit_reason();
        assert_eq!(
            resolve_exit_reason(ExitReason::Normal, &requested, &linked),
            ExitReason::Shutdown
        );
    }

    #[test]
    fn linked_reason_used_when_normal_and_no_request() {
        let requested = new_requested_exit_reason();
        let linked = new_linked_exit_reason();
        *linked.lock().unwrap() = Some(ExitReason::Panic("boom".into()));
        assert!(matches!(
            resolve_exit_reason(ExitReason::Normal, &requested, &linked),
            ExitReason::Panic(_)
        ));
    }
}
