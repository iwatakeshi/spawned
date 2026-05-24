//! Cluster routing and pluggable transport for Spawned.
//!
//! Phase 8b provides [`ClusterRouter`] and [`Transport`]; Phase 8c adds
//! [`TcpTransport`] and [`TcpClusterListener`] for length-framed TCP.

mod dispatch;
mod error;
mod frame;
mod inbound;
mod protocol;
mod registry;
mod router;
mod transport;
mod tcp;

pub use dispatch::AddressDispatch;
pub use error::TransportError;
pub use inbound::InboundDispatch;
pub use protocol::{ClusterFrame, Handshake, RegistryEvent, WireReply, PROTOCOL_VERSION};
pub use registry::{RegistryHooks, RegistryInbound, RegistrySnapshot};
pub use router::ClusterRouter;
pub use tcp::{TcpClusterListener, TcpTransport};
pub use transport::{Transport, UnavailableTransport};
