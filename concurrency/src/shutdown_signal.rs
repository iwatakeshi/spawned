use crate::error::ActorError;
use spawned_rt::OsSignal;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
static DISPATCHER_STARTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

type SendSignalFn = Arc<dyn Fn(OsSignal) -> Result<(), ActorError> + Send + Sync>;

struct Registry {
    next_id: AtomicU64,
    entries: Vec<(u64, SendSignalFn)>,
}

impl Registry {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            entries: Vec::new(),
        }
    }

    fn register(&mut self, send: SendSignalFn) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries.push((id, send));
        id
    }

    fn unregister(&mut self, id: u64) {
        self.entries.retain(|(entry_id, _)| *entry_id != id);
    }

    fn dispatch(&mut self, signal: OsSignal) {
        self.entries.retain(|(_, send)| send(signal).is_ok());
    }
}

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
}

/// Guard returned by [`register_shutdown_signal`]. Deregisters on drop.
pub struct SignalGuard(u64);

impl Drop for SignalGuard {
    fn drop(&mut self) {
        registry()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .unregister(self.0);
    }
}

/// Register a callback invoked when an OS shutdown signal is dispatched.
pub fn register_shutdown_signal(send: SendSignalFn) -> SignalGuard {
    let id = registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .register(send);
    SignalGuard(id)
}

/// Fan out an OS shutdown signal to all registered actors.
pub fn dispatch_shutdown_signal(signal: OsSignal) {
    registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .dispatch(signal);
}

/// Register multiple [`crate::ChildHandle`]s for OS shutdown signals.
pub fn register_shutdown_on_signal(handles: &[crate::ChildHandle]) -> Vec<SignalGuard> {
    handles
        .iter()
        .map(|handle| handle.shutdown_on_signal())
        .collect()
}

/// Ensure the global OS signal dispatcher is running (tasks mode).
pub fn spawn_shutdown_signal_dispatcher_tasks() {
    if DISPATCHER_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    spawned_rt::tasks::spawn(async {
        let signal = spawned_rt::tasks::wait_shutdown_signal().await;
        dispatch_shutdown_signal(signal);
    });
}

/// Ensure the global OS signal dispatcher is running (threads mode).
pub fn spawn_shutdown_signal_dispatcher_threads() {
    if DISPATCHER_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    spawned_rt::threads::spawn(|| {
        let signal = spawned_rt::threads::wait_shutdown_signal();
        dispatch_shutdown_signal(signal);
    });
}

pub(crate) fn make_tasks_send_signal(
    limits: Arc<crate::mailbox::MailboxLimits>,
    signal: spawned_rt::tasks::mpsc::Sender<crate::mailbox::SignalItem>,
) -> SendSignalFn {
    Arc::new(move |os_signal| limits.send_signal_tasks(&signal, os_signal))
}

pub(crate) fn make_threads_send_signal(
    limits: Arc<crate::mailbox::MailboxLimits>,
    signal: spawned_rt::threads::crossbeam_channel::Sender<crate::mailbox::SignalItem>,
) -> SendSignalFn {
    Arc::new(move |os_signal| limits.send_signal_threads(&signal, os_signal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn register_dispatch_and_deregister() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let guard = register_shutdown_signal(Arc::new(move |_| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));

        dispatch_shutdown_signal(OsSignal::CtrlC);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        drop(guard);
        dispatch_shutdown_signal(OsSignal::Terminate);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dead_sender_pruned_on_dispatch() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let _guard = register_shutdown_signal(Arc::new(move |_| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Err(ActorError::ActorStopped)
        }));

        dispatch_shutdown_signal(OsSignal::CtrlC);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let count_clone = count.clone();
        let guard = register_shutdown_signal(Arc::new(move |_| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        dispatch_shutdown_signal(OsSignal::Terminate);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        drop(guard);
    }
}
