//! libp2p cluster runtime: swarm, request-response, and control-plane sync.

mod codec;

use crate::control::ControlPlaneHooks;
use crate::pg_sync::encode_pg_event;
use crate::protocol::{
    decode_cluster_frame, decode_reply_frame, encode_cluster_frame, encode_reply, ClusterFrame,
    RegistryEvent, SupervisionEnvelope, WireReply,
};
use crate::registry::encode_registry_event;
use crate::supervision_sync::{
    apply_supervision, decode_supervision_reply, encode_supervision, encode_supervision_frame,
};
use crate::supervision_validate::validate_envelope;
use crate::{AsyncTransport, InboundDispatch, Transport, TransportError};
use codec::ClusterCodec;
use futures::StreamExt;
use libp2p::request_response::{
    Behaviour as RequestResponse, Config as RequestConfig, Message, OutboundRequestId,
    ProtocolSupport,
};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{identity, noise, tcp, yamux, Multiaddr, PeerId, SwarmBuilder};
use spawned_address::NodeId;
use spawned_wire::WireEnvelope;
use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Static peer entry: Erlang-style node name mapped to libp2p identity.
#[derive(Debug, Clone)]
pub struct Libp2pPeer {
    pub node: NodeId,
    pub peer_id: PeerId,
    pub addr: Multiaddr,
}

enum RequestReply {
    Sync(std::sync::mpsc::Sender<Result<Vec<u8>, TransportError>>),
    Async(tokio::sync::oneshot::Sender<Result<Vec<u8>, TransportError>>),
}

enum VoidReply {
    Sync(std::sync::mpsc::Sender<Result<(), TransportError>>),
    Async(tokio::sync::oneshot::Sender<Result<(), TransportError>>),
}

enum PendingReply {
    Sync(std::sync::mpsc::Sender<Result<Vec<u8>, TransportError>>),
    Async(tokio::sync::oneshot::Sender<Result<Vec<u8>, TransportError>>),
}

fn complete_pending(tx: PendingReply, result: Result<Vec<u8>, TransportError>) {
    match tx {
        PendingReply::Sync(tx) => {
            let _ = tx.send(result);
        }
        PendingReply::Async(tx) => {
            let _ = tx.send(result);
        }
    }
}

fn complete_void(tx: VoidReply, result: Result<(), TransportError>) {
    match tx {
        VoidReply::Sync(tx) => {
            let _ = tx.send(result);
        }
        VoidReply::Async(tx) => {
            let _ = tx.send(result);
        }
    }
}

enum RuntimeCommand {
    Send {
        peer_id: PeerId,
        frame: Vec<u8>,
        expect_reply: bool,
        respond_to: RequestReply,
    },
    Broadcast {
        frame: Vec<u8>,
        respond_to: VoidReply,
    },
    SyncPeer {
        peer_id: PeerId,
        respond_to: VoidReply,
    },
}

struct DeferredSend {
    peer_id: PeerId,
    frame: Vec<u8>,
    expect_reply: bool,
    respond_to: RequestReply,
}

struct SharedState {
    pending: HashMap<OutboundRequestId, PendingReply>,
    peer_to_node: HashMap<PeerId, NodeId>,
    node_to_peer: HashMap<NodeId, PeerId>,
    listen_addrs: Vec<Multiaddr>,
}

/// libp2p cluster node: background swarm + [`Transport`] implementation.
pub struct Libp2pCluster {
    local_peer_id: PeerId,
    local_node: NodeId,
    cmd_tx: Sender<RuntimeCommand>,
    shutdown: Arc<AtomicBool>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    inner: Arc<Mutex<SharedState>>,
}

impl Libp2pCluster {
    /// Start a libp2p cluster listener and outbound transport.
    pub fn start(
        keypair: identity::Keypair,
        local_node: NodeId,
        listen_addr: Multiaddr,
        peers: Vec<Libp2pPeer>,
        dispatch: Arc<dyn InboundDispatch>,
        control: ControlPlaneHooks,
    ) -> Result<Self, TransportError> {
        let local_peer_id = keypair.public().to_peer_id();
        let mut peer_to_node = HashMap::new();
        let mut node_to_peer = HashMap::new();
        for peer in &peers {
            peer_to_node.insert(peer.peer_id, peer.node.clone());
            node_to_peer.insert(peer.node.clone(), peer.peer_id);
        }

        let shared = Arc::new(Mutex::new(SharedState {
            pending: HashMap::new(),
            peer_to_node,
            node_to_peer,
            listen_addrs: Vec::new(),
        }));

        let (cmd_tx, cmd_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_thread = shutdown.clone();
        let shared_thread = shared.clone();

        let join = thread::Builder::new()
            .name("spawned-libp2p-cluster".into())
            .spawn(move || {
                if let Err(err) = run_swarm(
                    keypair,
                    listen_addr,
                    peers,
                    dispatch,
                    control,
                    cmd_rx,
                    shared_thread,
                    shutdown_thread,
                ) {
                    tracing::warn!("libp2p cluster stopped: {err}");
                }
            })
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let join = Arc::new(Mutex::new(Some(join)));

        for _ in 0..50 {
            if !shared
                .lock()
                .map_err(|_| TransportError::RemoteUnreachable)?
                .listen_addrs
                .is_empty()
            {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        Ok(Self {
            local_peer_id,
            local_node,
            cmd_tx,
            shutdown,
            join,
            inner: shared,
        })
    }

    /// Convenience: bind an ephemeral TCP port and return its port number.
    pub fn ephemeral_tcp_port() -> Result<u16, TransportError> {
        let listener =
            TcpListener::bind("127.0.0.1:0").map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(listener
            .local_addr()
            .map_err(|e| TransportError::Io(e.to_string()))?
            .port())
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub fn local_node(&self) -> &NodeId {
        &self.local_node
    }

    pub fn listen_addrs(&self) -> Vec<Multiaddr> {
        self.inner
            .lock()
            .map(|guard| guard.listen_addrs.clone())
            .unwrap_or_default()
    }

    pub fn with_control_plane_hooks(self, control: ControlPlaneHooks) -> Self {
        let _ = control;
        self
    }

    pub fn broadcast_registry(&self, event: RegistryEvent) -> Result<(), TransportError> {
        let bytes = encode_registry_event(&event)?;
        self.broadcast_frame(bytes)
    }

    pub fn broadcast_pg(&self, event: crate::protocol::PgEvent) -> Result<(), TransportError> {
        let bytes = encode_pg_event(&event)?;
        self.broadcast_frame(bytes)
    }

    pub fn sync_peers(&self) -> Result<(), TransportError> {
        let peer_ids: Vec<PeerId> = self
            .inner
            .lock()
            .map_err(|_| TransportError::RemoteUnreachable)?
            .node_to_peer
            .values()
            .copied()
            .collect();
        for peer_id in peer_ids {
            self.sync_peer(peer_id)?;
        }
        Ok(())
    }

    /// Send a fire-and-forget supervision envelope to a specific node.
    pub fn send_supervision_to(
        &self,
        node: &NodeId,
        envelope: SupervisionEnvelope,
    ) -> Result<(), TransportError> {
        validate_envelope(&envelope)?;
        if envelope.correlation_id != 0 {
            return Err(TransportError::Protocol(
                "send_supervision requires correlation_id 0".into(),
            ));
        }
        let bytes = encode_supervision_frame(&envelope)?;
        self.send_frame(node, bytes, false)?;
        Ok(())
    }

    /// Send a correlated supervision request and read the reply envelope.
    pub fn request_supervision_from(
        &self,
        node: &NodeId,
        envelope: SupervisionEnvelope,
    ) -> Result<SupervisionEnvelope, TransportError> {
        validate_envelope(&envelope)?;
        let bytes = encode_supervision_frame(&envelope)?;
        let reply_bytes = self.send_frame(node, bytes, true)?;
        if reply_bytes.is_empty() {
            return Err(TransportError::Protocol(
                "supervision request received empty reply".into(),
            ));
        }
        decode_supervision_reply(&envelope, &reply_bytes)
    }

    fn broadcast_frame(&self, frame: Vec<u8>) -> Result<(), TransportError> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cmd_tx
            .send(RuntimeCommand::Broadcast {
                frame,
                respond_to: VoidReply::Sync(tx),
            })
            .map_err(|_| TransportError::RemoteUnreachable)?;
        rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| TransportError::RemoteUnreachable)?
    }

    fn sync_peer(&self, peer_id: PeerId) -> Result<(), TransportError> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cmd_tx
            .send(RuntimeCommand::SyncPeer {
                peer_id,
                respond_to: VoidReply::Sync(tx),
            })
            .map_err(|_| TransportError::RemoteUnreachable)?;
        rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| TransportError::RemoteUnreachable)?
    }

    fn send_frame(
        &self,
        node: &NodeId,
        frame: Vec<u8>,
        expect_reply: bool,
    ) -> Result<Vec<u8>, TransportError> {
        let peer_id = self
            .inner
            .lock()
            .map_err(|_| TransportError::RemoteUnreachable)?
            .node_to_peer
            .get(node)
            .copied()
            .ok_or(TransportError::RemoteUnreachable)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cmd_tx
            .send(RuntimeCommand::Send {
                peer_id,
                frame,
                expect_reply,
                respond_to: RequestReply::Sync(tx),
            })
            .map_err(|_| TransportError::RemoteUnreachable)?;
        rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| TransportError::RemoteUnreachable)?
    }

    async fn send_frame_async(
        &self,
        node: &NodeId,
        frame: Vec<u8>,
        expect_reply: bool,
    ) -> Result<Vec<u8>, TransportError> {
        let peer_id = self
            .inner
            .lock()
            .map_err(|_| TransportError::RemoteUnreachable)?
            .node_to_peer
            .get(node)
            .copied()
            .ok_or(TransportError::RemoteUnreachable)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(RuntimeCommand::Send {
                peer_id,
                frame,
                expect_reply,
                respond_to: RequestReply::Async(tx),
            })
            .map_err(|_| TransportError::RemoteUnreachable)?;
        rx.await.map_err(|_| TransportError::RemoteUnreachable)?
    }

    /// Signal the background swarm to stop.
    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Wait for the background swarm thread to exit.
    pub fn join_thread(&self) {
        if let Ok(mut guard) = self.join.lock() {
            if let Some(join) = guard.take() {
                let _ = join.join();
            }
        }
    }

    pub fn shutdown(self) {
        self.signal_shutdown();
        if let Ok(mut guard) = self.join.lock() {
            if let Some(join) = guard.take() {
                let _ = join.join();
            }
        }
    }
}

impl Drop for Libp2pCluster {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

impl Transport for Libp2pCluster {
    fn send_envelope(&self, envelope: WireEnvelope) -> Result<(), TransportError> {
        if envelope.correlation_id != 0 {
            return Err(TransportError::Protocol(
                "fire-and-forget envelope must use correlation_id 0".into(),
            ));
        }
        let node = envelope.to.node.clone();
        let bytes =
            encode_cluster_frame(&ClusterFrame::Actor(envelope)).map_err(TransportError::from)?;
        self.send_frame(&node, bytes, false)?;
        Ok(())
    }

    fn request_envelope(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError> {
        let correlation_id = envelope.correlation_id;
        if correlation_id == 0 {
            return Err(TransportError::Protocol(
                "request envelope requires non-zero correlation id".into(),
            ));
        }
        let node = envelope.to.node.clone();
        let bytes =
            encode_cluster_frame(&ClusterFrame::Actor(envelope)).map_err(TransportError::from)?;
        let reply_bytes = self.send_frame(&node, bytes, true)?;
        if reply_bytes.is_empty() {
            return Ok(Vec::new());
        }
        let reply = decode_reply_frame(&reply_bytes).map_err(TransportError::from)?;
        if reply.correlation_id != correlation_id {
            return Err(TransportError::Protocol(format!(
                "correlation mismatch: expected {correlation_id}, got {}",
                reply.correlation_id
            )));
        }
        Ok(reply.payload)
    }
}

#[async_trait::async_trait]
impl AsyncTransport for Libp2pCluster {
    async fn send_envelope(&self, envelope: WireEnvelope) -> Result<(), TransportError> {
        if envelope.correlation_id != 0 {
            return Err(TransportError::Protocol(
                "fire-and-forget envelope must use correlation_id 0".into(),
            ));
        }
        let node = envelope.to.node.clone();
        let bytes =
            encode_cluster_frame(&ClusterFrame::Actor(envelope)).map_err(TransportError::from)?;
        self.send_frame_async(&node, bytes, false).await?;
        Ok(())
    }

    async fn request_envelope(&self, envelope: WireEnvelope) -> Result<Vec<u8>, TransportError> {
        let correlation_id = envelope.correlation_id;
        if correlation_id == 0 {
            return Err(TransportError::Protocol(
                "request envelope requires non-zero correlation id".into(),
            ));
        }
        let node = envelope.to.node.clone();
        let bytes =
            encode_cluster_frame(&ClusterFrame::Actor(envelope)).map_err(TransportError::from)?;
        let reply_bytes = self.send_frame_async(&node, bytes, true).await?;
        if reply_bytes.is_empty() {
            return Ok(Vec::new());
        }
        let reply = decode_reply_frame(&reply_bytes).map_err(TransportError::from)?;
        if reply.correlation_id != correlation_id {
            return Err(TransportError::Protocol(format!(
                "correlation mismatch: expected {correlation_id}, got {}",
                reply.correlation_id
            )));
        }
        Ok(reply.payload)
    }
}

fn run_swarm(
    keypair: identity::Keypair,
    listen_addr: Multiaddr,
    peers: Vec<Libp2pPeer>,
    dispatch: Arc<dyn InboundDispatch>,
    control: ControlPlaneHooks,
    cmd_rx: Receiver<RuntimeCommand>,
    shared: Arc<Mutex<SharedState>>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), TransportError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| TransportError::Io(e.to_string()))?;

    rt.block_on(async move {
        run_swarm_async(
            keypair,
            listen_addr,
            peers,
            dispatch,
            control,
            cmd_rx,
            shared,
            shutdown,
        )
        .await
    })
}

async fn run_swarm_async(
    keypair: identity::Keypair,
    listen_addr: Multiaddr,
    peers: Vec<Libp2pPeer>,
    dispatch: Arc<dyn InboundDispatch>,
    control: ControlPlaneHooks,
    cmd_rx: Receiver<RuntimeCommand>,
    shared: Arc<Mutex<SharedState>>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), TransportError> {
    let rr = RequestResponse::with_codec(
        ClusterCodec,
        [(CLUSTER_PROTOCOL, ProtocolSupport::Full)],
        RequestConfig::default().with_request_timeout(Duration::from_secs(30)),
    );

    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| TransportError::Io(e.to_string()))?
        .with_behaviour(|_| rr)
        .map_err(|e| TransportError::Protocol(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

    swarm
        .listen_on(listen_addr)
        .map_err(|e| TransportError::Io(e.to_string()))?;

    let mut peer_addrs: HashMap<PeerId, Multiaddr> = HashMap::new();
    for peer in peers {
        peer_addrs.insert(peer.peer_id, peer.addr.clone());
        swarm.add_peer_address(peer.peer_id, peer.addr);
    }

    let mut synced_peers = HashSet::new();
    let mut deferred_sends: Vec<DeferredSend> = Vec::new();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        while let Ok(cmd) = cmd_rx.try_recv() {
            handle_command(
                &mut swarm,
                &control,
                &shared,
                &mut synced_peers,
                &peer_addrs,
                &mut deferred_sends,
                cmd,
            )?;
        }

        flush_deferred_sends(&mut swarm, &shared, &mut deferred_sends)?;

        tokio::select! {
            event = swarm.select_next_some() => {
                handle_swarm_event(
                    &mut swarm,
                    event,
                    &dispatch,
                    &control,
                    &shared,
                    &mut synced_peers,
                    &peer_addrs,
                    &mut deferred_sends,
                )?;
                flush_deferred_sends(&mut swarm, &shared, &mut deferred_sends)?;
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }

    Ok(())
}

fn flush_deferred_sends(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    shared: &Arc<Mutex<SharedState>>,
    deferred: &mut Vec<DeferredSend>,
) -> Result<(), TransportError> {
    let mut remaining = Vec::new();
    for send in deferred.drain(..) {
        if swarm.is_connected(&send.peer_id) {
            dispatch_send(swarm, shared, send)?;
        } else {
            remaining.push(send);
        }
    }
    *deferred = remaining;
    Ok(())
}

fn dispatch_send(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    shared: &Arc<Mutex<SharedState>>,
    send: DeferredSend,
) -> Result<(), TransportError> {
    let request_id = swarm
        .behaviour_mut()
        .send_request(&send.peer_id, send.frame);
    if send.expect_reply {
        shared
            .lock()
            .map_err(|_| TransportError::RemoteUnreachable)?
            .pending
            .insert(
                request_id,
                match send.respond_to {
                    RequestReply::Sync(tx) => PendingReply::Sync(tx),
                    RequestReply::Async(tx) => PendingReply::Async(tx),
                },
            );
    } else {
        match send.respond_to {
            RequestReply::Sync(tx) => {
                let _ = tx.send(Ok(Vec::new()));
            }
            RequestReply::Async(tx) => {
                let _ = tx.send(Ok(Vec::new()));
            }
        }
    }
    Ok(())
}

fn handle_command(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    control: &ControlPlaneHooks,
    shared: &Arc<Mutex<SharedState>>,
    synced_peers: &mut HashSet<PeerId>,
    peer_addrs: &HashMap<PeerId, Multiaddr>,
    deferred_sends: &mut Vec<DeferredSend>,
    cmd: RuntimeCommand,
) -> Result<(), TransportError> {
    match cmd {
        RuntimeCommand::Send {
            peer_id,
            frame,
            expect_reply,
            respond_to,
        } => {
            ensure_peer_dialed(swarm, peer_id, peer_addrs)?;
            let send = DeferredSend {
                peer_id,
                frame,
                expect_reply,
                respond_to,
            };
            if swarm.is_connected(&peer_id) {
                dispatch_send(swarm, shared, send)?;
            } else {
                deferred_sends.push(send);
            }
        }
        RuntimeCommand::Broadcast { frame, respond_to } => {
            let peer_ids: Vec<PeerId> = shared
                .lock()
                .map_err(|_| TransportError::RemoteUnreachable)?
                .peer_to_node
                .keys()
                .copied()
                .collect();
            for peer_id in peer_ids {
                ensure_peer_dialed(swarm, peer_id, peer_addrs)?;
                let _ = swarm.behaviour_mut().send_request(&peer_id, frame.clone());
            }
            let _ = complete_void(respond_to, Ok(()));
        }
        RuntimeCommand::SyncPeer {
            peer_id,
            respond_to,
        } => {
            ensure_peer_dialed(swarm, peer_id, peer_addrs)?;
            send_control_snapshots_to_peer(swarm, control, peer_id)?;
            synced_peers.insert(peer_id);
            complete_void(respond_to, Ok(()));
        }
    }
    Ok(())
}

fn ensure_peer_dialed(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    peer_id: PeerId,
    peer_addrs: &HashMap<PeerId, Multiaddr>,
) -> Result<(), TransportError> {
    if swarm.is_connected(&peer_id) {
        return Ok(());
    }
    if let Some(addr) = peer_addrs.get(&peer_id) {
        swarm
            .dial(addr.clone())
            .map_err(|e| TransportError::Io(e.to_string()))?;
    }
    Ok(())
}

fn snapshot_frames(control: &ControlPlaneHooks) -> Result<Vec<Vec<u8>>, TransportError> {
    let registry_event = if let Some(snapshot) = control.registry.snapshot.as_ref() {
        RegistryEvent::Snapshot {
            entries: snapshot.local_entries(),
        }
    } else {
        RegistryEvent::Snapshot {
            entries: Vec::new(),
        }
    };
    let pg_event = if let Some(snapshot) = control.pg.snapshot.as_ref() {
        crate::protocol::PgEvent::Snapshot {
            entries: snapshot.local_entries(),
        }
    } else {
        crate::protocol::PgEvent::Snapshot {
            entries: Vec::new(),
        }
    };
    Ok(vec![
        encode_registry_event(&registry_event)?,
        encode_pg_event(&pg_event)?,
    ])
}

fn send_control_snapshots_to_peer(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    control: &ControlPlaneHooks,
    peer_id: PeerId,
) -> Result<(), TransportError> {
    for frame in snapshot_frames(control)? {
        let _ = swarm.behaviour_mut().send_request(&peer_id, frame);
    }
    Ok(())
}

fn handle_swarm_event(
    swarm: &mut Swarm<RequestResponse<ClusterCodec>>,
    event: SwarmEvent<libp2p::request_response::Event<Vec<u8>, Vec<u8>>>,
    dispatch: &Arc<dyn InboundDispatch>,
    control: &ControlPlaneHooks,
    shared: &Arc<Mutex<SharedState>>,
    synced_peers: &mut HashSet<PeerId>,
    peer_addrs: &HashMap<PeerId, Multiaddr>,
    deferred_sends: &mut Vec<DeferredSend>,
) -> Result<(), TransportError> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            if let Ok(mut guard) = shared.lock() {
                guard.listen_addrs.push(address);
            }
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            if !synced_peers.contains(&peer_id) {
                send_control_snapshots_to_peer(swarm, control, peer_id)?;
                synced_peers.insert(peer_id);
            }
            let _ = peer_addrs;
            let _ = deferred_sends;
        }
        SwarmEvent::Behaviour(libp2p::request_response::Event::Message {
            message, ..
        }) => match message {
            Message::Request {
                request, channel, ..
            } => {
                let response = handle_inbound_frame(&request, dispatch, control)?;
                let _ = swarm.behaviour_mut().send_response(channel, response);
            }
            Message::Response {
                request_id,
                response,
                ..
            } => {
                if let Ok(mut guard) = shared.lock() {
                    if let Some(tx) = guard.pending.remove(&request_id) {
                        complete_pending(tx, Ok(response));
                    } else {
                        drop(guard);
                        apply_inbound_frame(&response, control)?;
                    }
                }
            }
        },
        SwarmEvent::OutgoingConnectionError { peer_id, .. } => {
            if let Some(peer_id) = peer_id {
                let _ = peer_id;
                let _ = peer_addrs;
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_inbound_frame(frame: &[u8], control: &ControlPlaneHooks) -> Result<(), TransportError> {
    if frame.is_empty() {
        return Ok(());
    }
    match decode_cluster_frame(frame).map_err(TransportError::from)? {
        ClusterFrame::Registry(event) => {
            crate::registry::apply_registry_event(&control.registry, event)
        }
        ClusterFrame::Pg(event) => crate::pg_sync::apply_pg_event(&control.pg, event),
        ClusterFrame::Supervision(envelope) => {
            let _ = apply_supervision(&control.supervision, envelope)?;
            Ok(())
        }
        ClusterFrame::Actor(_) => Ok(()),
    }
}

fn handle_inbound_frame(
    frame: &[u8],
    dispatch: &Arc<dyn InboundDispatch>,
    control: &ControlPlaneHooks,
) -> Result<Vec<u8>, TransportError> {
    match decode_cluster_frame(frame).map_err(TransportError::from)? {
        ClusterFrame::Registry(event) => {
            crate::registry::apply_registry_event(&control.registry, event)?;
            Ok(Vec::new())
        }
        ClusterFrame::Pg(event) => {
            crate::pg_sync::apply_pg_event(&control.pg, event)?;
            Ok(Vec::new())
        }
        ClusterFrame::Supervision(envelope) => {
            if let Some(reply) = apply_supervision(&control.supervision, envelope)? {
                encode_supervision(&reply).map_err(TransportError::from)
            } else {
                Ok(Vec::new())
            }
        }
        ClusterFrame::Actor(envelope) => {
            let correlation_id = envelope.correlation_id;
            let reply = dispatch.dispatch(envelope)?;
            if correlation_id == 0 {
                Ok(Vec::new())
            } else {
                let wire_reply = WireReply {
                    correlation_id,
                    payload: reply.unwrap_or_default(),
                };
                encode_reply(&wire_reply).map_err(TransportError::from)
            }
        }
    }
}

pub use codec::CLUSTER_PROTOCOL;