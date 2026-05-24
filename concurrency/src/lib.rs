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
pub mod shutdown_signal;
pub mod supervisor;
pub mod tasks;
pub mod threads;

#[cfg(feature = "cluster")]
pub mod cluster;

pub use child_handle::{ActorId, ChildHandle};
pub use spawned_address::{ActorAddress, Locality, NodeId, NodeName, local_node};
pub use spawned_wire::{RemoteActor, RemoteMessage, WireEnvelope, WireError};
pub use child_spec::{
    should_restart, shutdown_child_async, shutdown_child_blocking, ChildType, RestartIntensity,
    RestartType, ShutdownType, DEFAULT_WORKER_SHUTDOWN,
};
pub use dynamic_supervisor::{DynamicChildInfo, DynamicSupervisorError};
pub use error::{ActorError, ExitReason};
pub use link::Exit;
pub use mailbox::{BackpressureMode, MailboxConfig};
pub use monitor::{Down, MonitorRef};
pub use pg::PgError;
pub use response::Response;
pub use shutdown_signal::{
    register_shutdown_on_signal, spawn_shutdown_signal_dispatcher_tasks,
    spawn_shutdown_signal_dispatcher_threads, SignalGuard,
};
pub use spawned_macros::{actor, protocol, remote_actor, remote_message};
pub use spawned_rt::OsSignal;
pub use supervisor::SupervisorStrategy;

#[cfg(feature = "cluster")]
pub use cluster::{
    lookup_address, lookup_handle, register_named, unregister_named, tasks_wire_dispatch,
    threads_wire_dispatch, ClusterRouter, InboundDispatch, NamedRegistryError, Node, NodeBuilder,
    NodeError, RemoteActorRef, RemoteRequest, TcpClusterListener, TcpTransport, Transport,
    TransportError, UnavailableTransport, WireReply, PROTOCOL_VERSION,
};

#[cfg(test)]
mod remote_macro_tests {
    use super::{RemoteActor, RemoteMessage};
    use crate::{remote_actor, remote_message};
    use serde::{Deserialize, Serialize};

    #[remote_actor]
    struct DemoActor;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    #[remote_message]
    struct DemoMsg {
        x: u32,
    }

    #[test]
    fn remote_macro_generates_stable_ids() {
        assert_eq!(DemoActor::REMOTE_ID, "spawned.DemoActor/v1");
        assert_eq!(DemoMsg::REMOTE_ID, "spawned.DemoMsg/v1");
    }

    #[test]
    fn remote_message_roundtrips_via_wire() {
        use crate::WireEnvelope;
        let envelope = WireEnvelope::fire_and_forget(
            spawned_address::ActorAddress::local(spawned_address::ActorId::from_raw(1)),
            &DemoMsg { x: 7 },
        )
        .unwrap();
        let msg: DemoMsg = spawned_wire::decode_payload(&envelope).unwrap();
        assert_eq!(msg, DemoMsg { x: 7 });
    }
}
