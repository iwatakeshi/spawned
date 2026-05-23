//! OS shutdown signals shared by tasks and threads runtimes.

/// An operating-system shutdown signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsSignal {
    /// Ctrl+C / SIGINT
    CtrlC,
    /// SIGTERM (Unix graceful shutdown)
    Terminate,
}
