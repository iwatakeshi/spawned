#![doc = include_str!("../README.md")]

//! # Modules
//!
//! - [`tasks`] — async actor runtime (requires tokio)
//! - [`threads`] — blocking actor runtime (native OS threads)
//! - [`registry`] — global name-based actor registry
//! - [`pg`] — Erlang-style process groups for broadcast and dispatch
//! - [`response`] — `Response<T>` unified wrapper for request-response
//! - [`error`] — `ActorError` and `ExitReason` types
//! - [`message`] — `Message` trait for defining message types
//! - [`child_handle`] — `ChildHandle` and `ActorId` for type-erased actor management
//! - [`child_spec`] — restart/shutdown policy types for supervision
//! - [`supervisor`] — shared `SupervisorLogic` and `SupervisorStrategy`
//! - [`dynamic_supervisor`] — shared errors and types for runtime supervision
//! - [`monitor`] — `MonitorRef` and `Down` for unidirectional death observation
//! - [`link`] — `Exit` and bidirectional links with `trap_exit` semantics
//!
//! # Choosing `tasks` vs `threads`
//!
//! Both modules provide identical `Actor`, `Handler<M>`, `ActorRef<A>`, and
//! `Context<A>` types. Use `tasks` when you need async I/O or high actor counts.
//! Use `threads` for CPU-bound work or when you want to avoid an async runtime.
//! Switching requires changing imports and adding/removing `async`/`.await`.
//!
//! # Advanced
//!
//! - [`message::Message`] trait for manual message definitions without `#[protocol]`
//! - `Recipient<M>` (`Arc<dyn Receiver<M>>`) for type-erased per-message references
//! - [`tasks::Backend`] enum for choosing async runtime, blocking pool, or OS thread
//! - [Supervision Guide](https://github.com/lambdaclass/spawned/blob/main/docs/SUPERVISION.md)

pub mod child_handle;
pub mod child_spec;
pub mod dynamic_supervisor;
pub mod error;
pub(crate) mod exit_request;
pub mod link;
pub(crate) mod mailbox;
pub mod message;
pub mod monitor;
pub mod pg;
pub mod registry;
pub mod response;
pub mod supervisor;
pub mod tasks;
pub mod threads;

pub use child_handle::{ActorId, ChildHandle};
pub use child_spec::{
    should_restart, shutdown_child_async, shutdown_child_blocking, ChildType, RestartIntensity,
    RestartType, ShutdownType, DEFAULT_WORKER_SHUTDOWN,
};
pub use dynamic_supervisor::{DynamicChildInfo, DynamicSupervisorError};
pub use error::{ActorError, ExitReason};
pub use link::Exit;
pub use monitor::{Down, MonitorRef};
pub use pg::PgError;
pub use response::Response;
pub use spawned_macros::{actor, protocol};
pub use supervisor::SupervisorStrategy;
