use crate::frame::{read_frame, write_frame};
use crate::protocol::{
    decode_handshake, decode_reply_frame, encode_handshake, Handshake, PROTOCOL_VERSION,
};
use crate::{Transport, TransportError};
use spawned_address::NodeId;
use spawned_wire::{encode_envelope, WireEnvelope};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;
use std::time::Duration;

/// TCP transport with peer socket map and connection pooling.
pub struct TcpTransport {
    local_node: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
    connections: Mutex<HashMap<NodeId, TcpStream>>,
}

impl TcpTransport {
    /// Create a transport that dials peers by [`NodeId`].
    pub fn new(local_node: NodeId, peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            local_node,
            peers,
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Known peer nodes and their socket addresses.
    pub fn peer_nodes(&self) -> impl Iterator<Item = (&NodeId, &SocketAddr)> {
        self.peers.iter()
    }

    fn connect(&self, node: &NodeId) -> Result<TcpStream, TransportError> {
        let addr = self
            .peers
            .get(node)
            .ok_or_else(|| TransportError::RemoteUnreachable)?;
        let mut stream = TcpStream::connect(addr).map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let hs = encode_handshake(&Handshake::local(self.local_node.clone()))?;
        write_frame(&mut stream, &hs)?;

        let ack_frame = read_frame(&mut stream)?;
        let ack = decode_handshake(&ack_frame).map_err(TransportError::from)?;
        if ack.version != PROTOCOL_VERSION {
            return Err(TransportError::Protocol(format!(
                "unsupported protocol version {}",
                ack.version
            )));
        }

        Ok(stream)
    }

    fn with_connection<R>(
        &self,
        node: &NodeId,
        f: impl FnOnce(&mut TcpStream) -> Result<R, TransportError>,
    ) -> Result<R, TransportError> {
        let mut guard = self
            .connections
            .lock()
            .map_err(|_| TransportError::RemoteUnreachable)?;

        if !guard.contains_key(node) {
            let stream = self.connect(node)?;
            guard.insert(node.clone(), stream);
        }

        let stream = guard
            .get_mut(node)
            .ok_or(TransportError::RemoteUnreachable)?;

        match f(stream) {
            Ok(result) => Ok(result),
            Err(err) => {
                guard.remove(node);
                Err(err)
            }
        }
    }

    fn send_on_stream(stream: &mut TcpStream, envelope: WireEnvelope) -> Result<(), TransportError> {
        let bytes = encode_envelope(&envelope)?;
        write_frame(&mut *stream, &bytes)
    }

    fn request_on_stream(
        stream: &mut TcpStream,
        envelope: WireEnvelope,
    ) -> Result<Vec<u8>, TransportError> {
        let correlation_id = envelope.correlation_id;
        if correlation_id == 0 {
            return Err(TransportError::Protocol(
                "request envelope requires non-zero correlation id".into(),
            ));
        }
        let bytes = encode_envelope(&envelope)?;
        write_frame(&mut *stream, &bytes)?;
        let reply_frame = read_frame(&mut *stream)?;
        let reply = decode_reply_frame(&reply_frame).map_err(TransportError::from)?;
        if reply.correlation_id != correlation_id {
            return Err(TransportError::Protocol(format!(
                "correlation mismatch: expected {correlation_id}, got {}",
                reply.correlation_id
            )));
        }
        Ok(reply.payload)
    }
}

impl Transport for TcpTransport {
    fn send_envelope(&self, envelope: WireEnvelope) -> Result<(), TransportError> {
        if envelope.correlation_id != 0 {
            return Err(TransportError::Protocol(
                "fire-and-forget envelope must use correlation_id 0".into(),
            ));
        }
        let node = envelope.to.node.clone();
        self.with_connection(&node, |stream| Self::send_on_stream(stream, envelope))
    }

    fn request_envelope(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError> {
        let node = envelope.to.node.clone();
        self.with_connection(&node, |stream| Self::request_on_stream(stream, envelope))
    }
}
