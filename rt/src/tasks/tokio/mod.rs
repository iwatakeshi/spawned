//! Tokio.rs reexports to prevent tokio dependencies within external code
pub mod mpsc;
pub mod oneshot;
pub use tokio::sync::watch;

pub use crate::os_signal::OsSignal;
pub use tokio::{
    runtime::{Handle, Runtime, RuntimeFlavor},
    select,
    sync::Notify,
    task::{block_in_place, id as task_id, spawn, spawn_blocking, JoinHandle},
    time::{sleep, timeout},
};
pub use tokio_stream::wrappers::{BroadcastStream, UnboundedReceiverStream as ReceiverStream};
pub use tokio_util::sync::CancellationToken;

/// Returns a future that completes when Ctrl+C is received.
///
/// This is a thin wrapper around `tokio::signal::ctrl_c()` that panics on error.
pub async fn ctrl_c() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}

/// Wait for the next OS shutdown signal (Ctrl+C, and SIGTERM on Unix).
pub async fn wait_shutdown_signal() -> OsSignal {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        select! {
            result = tokio::signal::ctrl_c() => {
                result.expect("Failed to listen for Ctrl+C");
                OsSignal::CtrlC
            }
            _ = sigterm.recv() => OsSignal::Terminate,
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c().await;
        OsSignal::CtrlC
    }
}
