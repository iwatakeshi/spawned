#![doc = include_str!("../README.md")]

//! # Modules
//!
//! - [`tasks`] — async runtime backed by tokio: `run()`, `spawn()`, `CancellationToken`,
//!   `mpsc`, `oneshot`, `watch`, `timeout`, `sleep`
//! - [`threads`] — blocking runtime using OS threads: `CancellationToken`,
//!   `mpsc`, `oneshot`, `sleep`

mod os_signal;
pub mod tasks;
pub mod threads;
mod tracing;

pub use os_signal::OsSignal;
