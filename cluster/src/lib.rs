//! Cluster routing and pluggable transport for Spawned.
//!
//! Phase 8b provides [`ClusterRouter`] and [`Transport`]; the default
//! [`UnavailableTransport`] returns [`TransportError::RemoteUnreachable`] until
//! a real transport is wired in Phase 8c. Local dispatch lives in
//! `spawned-concurrency` behind the `cluster` feature.

mod error;
mod router;
mod transport;

pub use error::TransportError;
pub use router::ClusterRouter;
pub use transport::{Transport, UnavailableTransport};
