use super::{
    deserialize, serialize, MessageId, ResponsePayload, RpcMessage, RpcRequest, RpcResponse,
};
use crate::error::{OctopiiError, Result};
use crate::transport::{PeerConnection, QuicTransport};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::time::{timeout, Duration};

/// Callback for handling incoming requests
pub type RequestHandler = Arc<dyn Fn(RpcRequest) -> ResponsePayload + Send + Sync>;

/// RPC handler that manages request/response correlation
pub struct RpcHandler {
    transport: Arc<QuicTransport>,
    next_id: AtomicU64,
    pending_requests: Arc<RwLock<HashMap<MessageId, oneshot::Sender<RpcResponse>>>>,
    request_handler: Arc<RwLock<Option<RequestHandler>>>,
    peer_receivers: Arc<Mutex<HashSet<SocketAddr>>>,
}

impl RpcHandler {
    /// Create a new RPC handler
    pub fn new(transport: Arc<QuicTransport>) -> Self {
        Self {
            transport: Arc::clone(&transport),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_handler: Arc::new(RwLock::new(None)),
            peer_receivers: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Set the request handler callback
    pub async fn set_request_handler<F>(&self, handler: F)
    where
        F: Fn(RpcRequest) -> ResponsePayload + Send + Sync + 'static,
    {
        let mut h = self.request_handler.write().await;
        *h = Some(Arc::new(handler));
    }

    /// Send a request and wait for response
    pub async fn request(
        self: &Arc<Self>,
        addr: SocketAddr,
        payload: super::RequestPayload,
        timeout_duration: Duration,
    ) -> Result<RpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = RpcMessage::new_request(id, payload.clone());

        tracing::debug!(
            "RPC request {} to {}: {:?}",
            id,
            addr,
            match &payload {
                super::RequestPayload::OpenRaft { kind, .. } => format!("OpenRaft({})", kind),
                _ => "Other".to_string(),
            }
        );

        let (tx, rx) = oneshot::channel();

        // Register pending request
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(id, tx);
        }

        // Serialize and send (ensure receive loop for this peer)
        let data = serialize(&request)?;

        tracing::debug!("RPC request {}: connecting to {}", id, addr);
        let peer = match self.transport.connect(addr).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("RPC request {}: failed to connect to {}: {}", id, addr, e);
                let mut pending = self.pending_requests.write().await;
                pending.remove(&id);
                return Err(e);
            }
        };

        tracing::debug!("RPC request {}: ensuring peer receiver for {}", id, addr);
        self.ensure_peer_receiver(addr, Arc::clone(&peer)).await;

        tracing::debug!("RPC request {}: sending data to {}", id, addr);
        timeout(timeout_duration, peer.send(data))
            .await
            .map_err(|_| {
                tracing::error!("RPC request {}: send timeout to {}", id, addr);
                OctopiiError::Rpc("Request send timeout".to_string())
            })?
            .map_err(|e| {
                tracing::error!(
                    "RPC request {}: transport send failed to {}: {}",
                    id,
                    addr,
                    e
                );
                OctopiiError::Rpc(format!("Transport send failed: {}", e))
            })?;

        tracing::debug!("RPC request {}: waiting for response from {}", id, addr);
        // Wait for response with timeout
        match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => {
                tracing::debug!("RPC request {}: received response from {}", id, addr);
                Ok(response)
            }
            Ok(Err(_)) => {
                tracing::error!("RPC request {}: response channel closed for {}", id, addr);
                Err(OctopiiError::Rpc("Response channel closed".to_string()))
            }
            Err(_) => {
                // Timeout - clean up pending request
                tracing::error!(
                    "RPC request {}: timeout waiting for response from {}",
                    id,
                    addr
                );
                let mut pending = self.pending_requests.write().await;
                pending.remove(&id);
                Err(OctopiiError::Rpc("Request timeout".to_string()))
            }
        }
    }

    /// Send a one-way message (no response expected)
    pub async fn send_one_way(
        self: &Arc<Self>,
        addr: SocketAddr,
        message: super::OneWayMessage,
    ) -> Result<()> {
        let msg = RpcMessage::new_one_way(message);
        let data = serialize(&msg)?;
        let peer = self.transport.connect(addr).await?;
        self.ensure_peer_receiver(addr, Arc::clone(&peer)).await;

        peer.send(data).await.map_err(|e| {
            OctopiiError::Rpc(format!("Failed to send one-way message to {}: {}", addr, e))
        })
    }

    /// Notify the handler of an incoming message
    ///
    /// This method handles the message inline to avoid spawning tasks,
    /// which prevents context switching starvation with Quinn QUIC.
    ///
    /// The peer parameter is used to send responses back on the same connection.
    pub async fn notify_message(
        &self,
        addr: SocketAddr,
        msg: RpcMessage,
        peer: Option<Arc<PeerConnection>>,
    ) {
        tracing::debug!(
            "RPC notify_message from {}: {:?}",
            addr,
            match &msg {
                RpcMessage::Request(req) => format!("Request(id={})", req.id),
                RpcMessage::Response(resp) => format!("Response(id={})", resp.id),
                RpcMessage::OneWay(_) => "OneWay".to_string(),
            }
        );

        match msg {
            RpcMessage::Request(req) => {
                self.handle_request(addr, req, peer).await;
            }
            RpcMessage::Response(resp) => {
                tracing::debug!("RPC handler: received response {}", resp.id);
                self.handle_response(resp).await;
            }
            RpcMessage::OneWay(_) => {
                // One-way messages would be handled by application logic
                tracing::debug!("Received one-way message from {}", addr);
            }
        }
    }

    /// Handle an incoming request
    async fn handle_request(
        &self,
        addr: SocketAddr,
        req: RpcRequest,
        peer: Option<Arc<PeerConnection>>,
    ) {
        let handler = self.request_handler.read().await;

        let response_payload = match handler.as_ref() {
            Some(h) => h(req.clone()),
            None => ResponsePayload::Error {
                message: "No request handler registered".to_string(),
            },
        };

        let response = RpcMessage::new_response(req.id, response_payload);

        // Send response back on the same connection if peer is provided,
        // otherwise fall back to creating a new connection
        if let Ok(data) = serialize(&response) {
            if let Some(peer) = peer {
                if let Err(e) = peer.send(data).await {
                    tracing::error!("Failed to send response via peer: {}", e);
                }
            } else {
                if let Err(e) = self.transport.send(addr, data).await {
                    tracing::error!("Failed to send response to {}: {}", addr, e);
                }
            }
        }
    }

    /// Handle an incoming response
    async fn handle_response(&self, resp: RpcResponse) {
        let mut pending = self.pending_requests.write().await;
        if let Some(tx) = pending.remove(&resp.id) {
            let _ = tx.send(resp);
        }
    }

    async fn ensure_peer_receiver(self: &Arc<Self>, addr: SocketAddr, peer: Arc<PeerConnection>) {
        let mut receivers = self.peer_receivers.lock().await;
        if receivers.contains(&addr) {
            return;
        }
        receivers.insert(addr);
        drop(receivers);

        let rpc = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match peer.recv().await {
                    Ok(Some(data)) => match deserialize::<RpcMessage>(&data) {
                        Ok(msg) => {
                            rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to deserialize RPC message from {}: {}",
                                addr,
                                e
                            );
                        }
                    },
                    Ok(None) => {
                        tracing::info!("Peer {} closed connection", addr);
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Peer {} recv error: {}", addr, e);
                        break;
                    }
                }
            }

            let mut receivers = rpc.peer_receivers.lock().await;
            receivers.remove(&addr);
        });
    }
}

// RPC tests are in tests/rpc_test.rs due to bidirectional communication requirements
