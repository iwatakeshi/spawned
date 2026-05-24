//! Cluster routing and pluggable transport for Spawned.
//!
//! Phase 8b provides [`ClusterRouter`] and [`Transport`]; Phase 8c adds
//! [`TcpTransport`] and [`TcpClusterListener`] for length-framed TCP.

mod error;
mod frame;
mod inbound;
mod protocol;
mod router;
mod transport;
mod tcp;

pub use error::TransportError;
pub use inbound::InboundDispatch;
pub use protocol::{Handshake, WireReply, PROTOCOL_VERSION};
pub use router::ClusterRouter;
pub use tcp::{TcpClusterListener, TcpTransport};
pub use transport::{Transport, UnavailableTransport};
