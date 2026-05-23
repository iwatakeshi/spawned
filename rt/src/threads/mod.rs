//! IO-threads based module to support shared behavior with task based version.

pub mod mpsc;
pub mod oneshot;

pub use crossbeam_channel;

use crate::os_signal::OsSignal;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc as std_mpsc, Arc, Mutex, OnceLock,
};
pub use std::{
    future::Future,
    thread::{sleep, spawn, JoinHandle},
};

use crate::{tasks::Runtime, tracing::init_tracing};

/// Global list of shutdown signal subscribers (Ctrl+C and SIGTERM).
static SHUTDOWN_SIGNAL_SUBSCRIBERS: OnceLock<Mutex<Vec<std_mpsc::Sender<OsSignal>>>> =
    OnceLock::new();
static SHUTDOWN_SIGNAL_HANDLERS_REGISTERED: AtomicBool = AtomicBool::new(false);

fn register_shutdown_signal_handlers() {
    if SHUTDOWN_SIGNAL_HANDLERS_REGISTERED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    fn notify_subscribers(signal: OsSignal) {
        if let Some(subs) = SHUTDOWN_SIGNAL_SUBSCRIBERS.get() {
            let mut guard = subs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.retain(|tx| tx.send(signal).is_ok());
        }
    }

    ctrlc::set_handler(move || notify_subscribers(OsSignal::CtrlC))
        .expect("shutdown signal handler already set. Use shutdown_signal_listener() instead of ctrlc::set_handler()");

    #[cfg(unix)]
    {
        use signal_hook::consts::SIGTERM;
        let mut signals = signal_hook::iterator::Signals::new([SIGTERM])
            .expect("failed to register SIGTERM handler");
        std::thread::spawn(move || {
            for sig in &mut signals {
                if sig == SIGTERM {
                    notify_subscribers(OsSignal::Terminate);
                }
            }
        });
    }
}

fn subscribe_shutdown_signal() -> std_mpsc::Receiver<OsSignal> {
    register_shutdown_signal_handlers();
    let subscribers = SHUTDOWN_SIGNAL_SUBSCRIBERS.get_or_init(|| Mutex::new(Vec::new()));
    let (tx, rx) = std_mpsc::channel();
    subscribers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(tx);
    rx
}

/// Returns a closure that blocks until an OS shutdown signal is received.
pub fn shutdown_signal_listener() -> impl FnOnce() -> OsSignal + Send + 'static {
    let rx = subscribe_shutdown_signal();
    move || rx.recv().unwrap_or(OsSignal::CtrlC)
}

/// Returns a closure that blocks until Ctrl+C is received.
///
/// Multiple calls are supported — each returns a closure notified on Ctrl+C or SIGTERM.
pub fn ctrl_c() -> impl FnOnce() + Send + 'static {
    let listener = shutdown_signal_listener();
    move || {
        listener();
    }
}

/// Initialize tracing and run the given function.
pub fn run(f: fn()) {
    init_tracing();

    f()
}

/// Create a temporary tokio runtime and block on the given future.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let rt = Runtime::new().unwrap();
    rt.block_on(future)
}

/// Spawn blocking is the same as spawn for pure threaded usage.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn(f)
}

type CancelCallback = Box<dyn FnOnce() + Send>;

/// A token that can be used to signal cancellation.
///
/// Supports registering callbacks via `on_cancel()` that fire when
/// the token is cancelled, enabling efficient waiting patterns.
#[derive(Clone, Default)]
pub struct CancellationToken {
    is_cancelled: Arc<AtomicBool>,
    callbacks: Arc<Mutex<Vec<CancelCallback>>>,
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        CancellationToken {
            is_cancelled: Arc::new(false.into()),
            callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
        // Fire all registered callbacks
        let callbacks: Vec<_> = self
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        for cb in callbacks {
            cb();
        }
    }

    /// Register a callback to be invoked when this token is cancelled.
    /// If already cancelled, the callback fires immediately.
    ///
    /// This method is thread-safe: the callback is guaranteed to fire exactly
    /// once, either immediately (if already cancelled) or when `cancel()` is called.
    pub fn on_cancel(&self, callback: CancelCallback) {
        // Hold the lock while checking is_cancelled to avoid a race with cancel().
        // cancel() sets the flag BEFORE acquiring the lock, so if we see
        // is_cancelled=false while holding the lock, cancel() hasn't drained
        // callbacks yet and will drain ours after we release the lock.
        let mut callbacks = self.callbacks.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_cancelled() {
            drop(callbacks);
            callback();
        } else {
            callbacks.push(callback);
        }
    }
}

/// Wait for the next OS shutdown signal (blocking).
pub fn wait_shutdown_signal() -> OsSignal {
    shutdown_signal_listener()()
}
