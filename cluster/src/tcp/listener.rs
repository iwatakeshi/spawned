use crate::control::{send_control_plane_snapshots, ControlPlaneHooks};
use crate::frame::{read_frame, write_frame};
use crate::pg_sync::apply_pg_event;
use crate::protocol::{
    decode_cluster_frame, decode_handshake, encode_handshake, encode_reply, ClusterFrame,
    Handshake, WireReply, PROTOCOL_VERSION,
};
use crate::registry::{apply_registry_event, RegistryHooks};
use crate::{InboundDispatch, TransportError};
use spawned_address::NodeId;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// TCP cluster listener that dispatches inbound envelopes and control-plane events.
pub struct TcpClusterListener {
    local_addr: SocketAddr,
    local_node: NodeId,
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl TcpClusterListener {
    /// Bind and spawn a background thread to accept cluster connections.
    pub fn bind(
        addr: SocketAddr,
        local_node: NodeId,
        dispatch: Arc<dyn InboundDispatch>,
    ) -> Result<Self, TransportError> {
        Self::bind_with_control_plane(addr, local_node, dispatch, ControlPlaneHooks::none())
    }

    /// Bind with registry replication hooks (pg hooks disabled).
    pub fn bind_with_registry(
        addr: SocketAddr,
        local_node: NodeId,
        dispatch: Arc<dyn InboundDispatch>,
        registry: RegistryHooks,
    ) -> Result<Self, TransportError> {
        Self::bind_with_control_plane(
            addr,
            local_node,
            dispatch,
            ControlPlaneHooks {
                registry,
                pg: crate::pg_sync::PgHooks::none(),
            },
        )
    }

    /// Bind with registry and pg replication hooks.
    pub fn bind_with_control_plane(
        addr: SocketAddr,
        local_node: NodeId,
        dispatch: Arc<dyn InboundDispatch>,
        control: ControlPlaneHooks,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr).map_err(|e| TransportError::Io(e.to_string()))?;
        let local_addr = listener.local_addr().map_err(|e| TransportError::Io(e.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_thread = shutdown.clone();
        let local = local_node.clone();

        let join = thread::Builder::new()
            .name("spawned-tcp-cluster".into())
            .spawn(move || {
                accept_loop(listener, local, dispatch, control, shutdown_thread);
            })
            .map_err(|e| TransportError::Io(e.to_string()))?;

        Ok(Self {
            local_addr,
            local_node,
            shutdown,
            join: Some(join),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn local_node(&self) -> &NodeId {
        &self.local_node
    }

    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for TcpClusterListener {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn accept_loop(
    listener: TcpListener,
    local_node: NodeId,
    dispatch: Arc<dyn InboundDispatch>,
    control: ControlPlaneHooks,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let dispatch = dispatch.clone();
                let local = local_node.clone();
                let control = control.clone();
                thread::spawn(move || {
                    if let Err(err) = serve_connection(stream, local, dispatch, control) {
                        tracing::debug!("cluster connection closed: {err}");
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(_err) if shutdown.load(Ordering::Relaxed) => break,
            Err(err) => {
                tracing::warn!("cluster accept error: {err}");
            }
        }
    }
}

fn serve_connection(
    mut stream: TcpStream,
    local_node: NodeId,
    dispatch: Arc<dyn InboundDispatch>,
    control: ControlPlaneHooks,
) -> Result<(), TransportError> {
    stream
        .set_nonblocking(false)
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let peer = read_handshake(&mut stream)?;
    if peer.version != PROTOCOL_VERSION {
        return Err(TransportError::Protocol(format!(
            "unsupported protocol version {}",
            peer.version
        )));
    }

    let ack = encode_handshake(&Handshake::local(local_node))?;
    write_frame(&mut stream, &ack)?;

    send_control_plane_snapshots(&mut stream, &control)?;

    loop {
        let frame = read_frame(&mut stream)?;
        match decode_cluster_frame(&frame).map_err(TransportError::from)? {
            ClusterFrame::Registry(event) => {
                apply_registry_event(&control.registry, event)?;
            }
            ClusterFrame::Pg(event) => {
                apply_pg_event(&control.pg, event)?;
            }
            ClusterFrame::Actor(envelope) => {
                let correlation_id = envelope.correlation_id;
                let reply = dispatch.dispatch(envelope)?;
                if correlation_id != 0 {
                    let wire_reply = WireReply {
                        correlation_id,
                        payload: reply.unwrap_or_default(),
                    };
                    let bytes = encode_reply(&wire_reply)?;
                    write_frame(&mut stream, &bytes)?;
                }
            }
        }
    }
}

fn read_handshake(stream: &mut impl Read) -> Result<Handshake, TransportError> {
    let frame = read_frame(stream)?;
    decode_handshake(&frame).map_err(TransportError::from)
}
