#![doc = include_str!("../README.md")]

//! # Modules
//!
//! - [`tasks`] — async runtime backed by tokio: `run()`, `spawn()`, `CancellationToken`,
//!   `mpsc`, `oneshot`, `watch`, `timeout`, `sleep`
//! - [`threads`] — blocking runtime using OS threads: `CancellationToken`,
//!   `mpsc`, `oneshot`, `sleep`

pub mod tasks;
pub mod threads;
mod tracing;
