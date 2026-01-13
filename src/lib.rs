#![forbid(unsafe_code)]

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{pin_mut, select, FutureExt, StreamExt};
use itertools::Itertools;
use local_delivery::LocalDelivery;
use message_tracker::MessageTracker;
use num::traits::NumOps;
use num::Num;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub mod local_delivery;
pub(crate) mod message_tracker;

#[cfg(feature = "testing")]
pub mod testing;

const OUTBOUND_POLL: Duration = Duration::from_millis(200);
const INBOUND_POLL: Duration = Duration::from_millis(200);

/// Platform-agnostic async sleep function
/// - Native: Uses tokio::time::sleep
/// - WASM: Uses wasmtimer::tokio::sleep
#[cfg(not(target_arch = "wasm32"))]
async fn platform_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
async fn platform_sleep(duration: Duration) {
    wasmtimer::tokio::sleep(duration).await;
}

/// Platform-agnostic async timeout function
/// - Native: Uses tokio::time::timeout
/// - WASM: Uses wasmtimer::tokio::timeout
#[cfg(not(target_arch = "wasm32"))]
async fn platform_timeout<F, T>(
    duration: Duration,
    future: F,
) -> Result<T, tokio::time::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(duration, future).await
}

#[cfg(target_arch = "wasm32")]
async fn platform_timeout<F, T>(
    duration: Duration,
    future: F,
) -> Result<T, wasmtimer::tokio::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    wasmtimer::tokio::timeout(duration, future).await
}

/// Platform-agnostic async spawn function
/// - Native: Uses tokio::spawn (requires Send + 'static)
/// - WASM: Uses wasm_bindgen_futures::spawn_local (single-threaded, no Send required)
#[cfg(not(target_arch = "wasm32"))]
fn platform_spawn<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    drop(tokio::spawn(future));
}

#[cfg(target_arch = "wasm32")]
fn platform_spawn<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

/// Platform-agnostic timestamp function (seconds since UNIX epoch)
/// - Native: Uses std::time::UNIX_EPOCH.elapsed()
/// - WASM: Uses js_sys::Date::now()
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn platform_timestamp_secs() -> u64 {
    std::time::UNIX_EPOCH.elapsed().unwrap().as_secs()
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn platform_timestamp_secs() -> u64 {
    // js_sys::Date::now() returns milliseconds since epoch
    (js_sys::Date::now() / 1000.0) as u64
}

#[async_trait]
pub trait MessageMetadata: Debug + Send + Sync + 'static {
    type PeerId: Default
        + Display
        + Debug
        + Hash
        + Eq
        + Copy
        + Ord
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;
    type MessageId: Num
        + NumOps
        + Eq
        + Default
        + PartialEq
        + Display
        + Debug
        + Hash
        + Ord
        + PartialOrd
        + Copy
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    type Contents: Send + Sync + 'static;

    fn source_id(&self) -> Self::PeerId;
    fn destination_id(&self) -> Self::PeerId;
    fn message_id(&self) -> Self::MessageId;
    fn contents(&self) -> &Self::Contents;
    fn construct_from_parts(
        source_id: Self::PeerId,
        destination_id: Self::PeerId,
        message_id: Self::MessageId,
        contents: impl Into<Self::Contents>,
    ) -> Self;
}

#[async_trait]
pub trait UnderlyingSessionTransport {
    type Message: MessageMetadata + Send + Sync + 'static;

    async fn next_message(&self) -> Option<Payload<Self::Message>>;
    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>>;
    async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId>;
    fn local_id(&self) -> <Self::Message as MessageMetadata>::PeerId;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Payload<M: MessageMetadata> {
    Ack {
        from_id: M::PeerId,
        to_id: M::PeerId,
        message_id: M::MessageId,
    },
    Message(M),
    Poll {
        from_id: M::PeerId,
        to_id: M::PeerId,
        /// The last message ID I received from you (for resync detection).
        /// If you sent me a message but I never received it, this will be None or
        /// lower than what you think you sent. You should then resend the missing message.
        last_received_from_peer: Option<M::MessageId>,
    },
}

impl<M: MessageMetadata> Payload<M> {
    pub fn source_id(&self) -> M::PeerId {
        match self {
            Payload::Ack { from_id, .. } => *from_id,
            Payload::Message(msg) => msg.source_id(),
            Payload::Poll { from_id, .. } => *from_id,
        }
    }
    pub fn destination_id(&self) -> M::PeerId {
        match self {
            Payload::Ack { to_id, .. } => *to_id,
            Payload::Message(msg) => msg.destination_id(),
            Payload::Poll { to_id, .. } => *to_id,
        }
    }

    pub fn message_id(&self) -> Option<M::MessageId> {
        match self {
            Payload::Ack { message_id, .. } => Some(*message_id),
            Payload::Message(msg) => Some(msg.message_id()),
            Payload::Poll { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum NetworkError<T> {
    SendFailed { reason: String, message: T },
    ConnectionError(String),
    BackendError(BackendError<T>),
    ShutdownFailed(String),
    SystemShutdown,
}

#[derive(Debug)]
pub enum BackendError<T> {
    StorageError(String),
    SendFailed { reason: String, message: T },
    NotFound,
}

#[derive(Debug, Copy, Clone)]
pub enum DeliveryError {
    NoReceiver,
    ChannelClosed,
    BadInput,
}

// Modified Backend trait to handle both outbound and inbound messages
#[async_trait]
#[auto_impl::auto_impl(&, Arc, Box)]
/// Each local client that uses a backend needs to have a different backend in order
/// to not have collisions in the message tracker
pub trait Backend<M: MessageMetadata>: Send + Sync {
    async fn store_outbound(&self, message: M) -> Result<(), BackendError<M>>;
    async fn store_inbound(&self, message: M) -> Result<(), BackendError<M>>;
    async fn clear_message_inbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError<M>>;
    async fn clear_message_outbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError<M>>;
    async fn get_pending_outbound(&self) -> Result<Vec<M>, BackendError<M>>;
    async fn get_pending_inbound(&self) -> Result<Vec<M>, BackendError<M>>;
    // Simple K/V interface for tracker state
    async fn store_value(&self, key: &str, value: &[u8]) -> Result<(), BackendError<M>>;
    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<M>>;

    /// Load multiple values in a single batched operation.
    /// Default implementation calls load_value() sequentially for backwards compatibility.
    /// Implementations should override this with a batched network call to avoid
    /// sequential await blocking (especially important in WASM where async yields
    /// can block the JavaScript event loop).
    async fn load_values_batched(
        &self,
        keys: &[&str],
    ) -> Result<Vec<Option<Vec<u8>>>, BackendError<M>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.load_value(key).await?);
        }
        Ok(results)
    }
}

const MAX_MAP_SIZE: usize = 1000;

/// Max consecutive blocks before clearing stale state for a peer
const MAX_CONSECUTIVE_BLOCKS: u32 = 10;

pub struct ILM<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: UnderlyingSessionTransport<Message = M> + Send + Sync + 'static,
{
    backend: Arc<B>,
    local_delivery: Arc<Mutex<Option<L>>>,
    network: Arc<N>,
    is_running: Arc<AtomicBool>,
    is_shutting_down: Arc<AtomicBool>,
    tracker: Arc<MessageTracker<M, B>>,
    poll_inbound_tx: citadel_io::tokio::sync::mpsc::UnboundedSender<()>,
    poll_outbound_tx: citadel_io::tokio::sync::mpsc::UnboundedSender<()>,
    known_peers: Arc<Mutex<Vec<M::PeerId>>>,
    /// Tracks consecutive block counts per peer for fallback clearing
    blocked_count: Arc<DashMap<M::PeerId, u32>>,
}

impl<M, B, L, N> Drop for ILM<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: UnderlyingSessionTransport<Message = M> + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if Arc::strong_count(&self.is_running) == 1 {
            let _ = self.poll_outbound_tx.send(());
        }
    }
}

impl<M, B, L, N> ILM<M, B, L, N>
where
    M: MessageMetadata + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    B: Backend<M> + Send + Sync + 'static,
    L: LocalDelivery<M> + Send + Sync + 'static,
    N: UnderlyingSessionTransport<Message = M> + Send + Sync + 'static,
{
    pub async fn new(backend: B, local_delivery: L, network: N) -> Result<Self, BackendError<M>> {
        let (poll_inbound_tx, poll_inbound_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel();
        let (poll_outbound_tx, poll_outbound_rx) =
            citadel_io::tokio::sync::mpsc::unbounded_channel();

        let backend = Arc::new(backend);
        let this = Self {
            backend: backend.clone(),
            local_delivery: Arc::new(Mutex::new(Some(local_delivery))),
            network: Arc::new(network),
            is_running: Arc::new(AtomicBool::new(true)),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            tracker: Arc::new(MessageTracker::new(backend).await?),
            poll_inbound_tx,
            poll_outbound_tx,
            known_peers: Arc::new(Mutex::new(Vec::new())),
            blocked_count: Arc::new(DashMap::new()),
        };

        this.spawn_background_tasks(poll_inbound_rx, poll_outbound_rx);

        Ok(this)
    }

    fn clone_internal(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            local_delivery: self.local_delivery.clone(),
            network: self.network.clone(),
            is_running: self.is_running.clone(),
            is_shutting_down: self.is_shutting_down.clone(),
            tracker: self.tracker.clone(),
            poll_inbound_tx: self.poll_inbound_tx.clone(),
            poll_outbound_tx: self.poll_outbound_tx.clone(),
            known_peers: self.known_peers.clone(),
            blocked_count: self.blocked_count.clone(),
        }
    }

    fn spawn_background_tasks(
        &self,
        mut poll_inbound_rx: citadel_io::tokio::sync::mpsc::UnboundedReceiver<()>,
        mut poll_outbound_rx: citadel_io::tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        // Spawn outbound processing task
        let this = self.clone_internal();

        let background_task = async move {
            let this = &this;

            let outbound_handle = async move {
                loop {
                    if !this.can_run() {
                        break;
                    }

                    // Use futures::select! for WASM compatibility instead of tokio::select!
                    let recv_fut = poll_outbound_rx.recv().fuse();
                    let sleep_fut = platform_sleep(OUTBOUND_POLL).fuse();
                    pin_mut!(recv_fut, sleep_fut);

                    select! {
                        res0 = recv_fut => {
                            if res0.is_none() {
                                log::warn!(target: "ism", "Poll outbound channel closed");
                                return;
                            }
                        },
                        _ = sleep_fut => {},
                    }

                    this.process_outbound().await;
                }
            };

            // Spawn inbound processing task
            let inbound_handle = async move {
                loop {
                    if !this.can_run() {
                        break;
                    }

                    // Use futures::select! for WASM compatibility instead of tokio::select!
                    let recv_fut = poll_inbound_rx.recv().fuse();
                    let sleep_fut = platform_sleep(INBOUND_POLL).fuse();
                    pin_mut!(recv_fut, sleep_fut);

                    // futures::select_biased! equivalent: list priority branches first
                    select! {
                        res0 = recv_fut => {
                            if res0.is_none() {
                                log::warn!(target: "ism", "Poll inbound channel closed");
                            }
                        },
                        _ = sleep_fut => {},
                    }

                    this.process_inbound().await;
                }
            };

            // Spawn network listener task
            let network_io_handle = async move {
                loop {
                    if !this.can_run() {
                        break;
                    }

                    this.process_next_network_message().await;
                }
            };

            // Spawn task that periodically polls for connected peers to help establish intersession recovery
            let peer_polling_handle = async move {
                loop {
                    if !this.can_run() {
                        break;
                    }

                    this.poll_peers().await;

                    platform_sleep(Duration::from_secs(5)).await;
                }
            };

            // Use futures::select! for WASM compatibility instead of tokio::select!
            let outbound_handle_fused = outbound_handle.fuse();
            let inbound_handle_fused = inbound_handle.fuse();
            let network_io_handle_fused = network_io_handle.fuse();
            let peer_polling_handle_fused = peer_polling_handle.fuse();
            pin_mut!(
                outbound_handle_fused,
                inbound_handle_fused,
                network_io_handle_fused,
                peer_polling_handle_fused
            );
            select! {
                _ = outbound_handle_fused => {
                    log::error!(target: "ism", "Outbound processing task prematurely ended");
                },
                _ = inbound_handle_fused => {
                    log::error!(target: "ism", "Inbound processing task prematurely ended");
                },
                _ = network_io_handle_fused => {
                    log::error!(target: "ism", "Network IO task prematurely ended");
                },
                _ = peer_polling_handle_fused => {
                    log::error!(target: "ism", "Peer polling task prematurely ended");
                },
            }

            if let Err(err) = this.tracker.sync_backend().await {
                log::error!(target: "ism", "Failed to sync tracker state to backend on shutdown hook: {err:?}");
            }

            log::warn!(target: "ism", "Message system has shut down");

            this.toggle_off();
            drop(this.local_delivery.lock().await.take());
        };

        // Spawn a task that selects all three handles, and on any of them finishing, it will
        // set the atomic bool to false
        platform_spawn(background_task);
    }

    async fn poll_peers(&self) {
        let connected_peers_now = self.get_connected_peers().await;
        let mut current_peers_lock = self.known_peers.lock().await;
        let connected_peers_previous = current_peers_lock
            .iter()
            .copied()
            .sorted()
            .collect::<Vec<_>>();
        if connected_peers_now != connected_peers_previous {
            log::info!(target: "ism", "Connected peers changed to {connected_peers_now:?}, sending poll for refresh in state");

            // Now, send a poll to each new connected peer with resync state
            for peer_id in connected_peers_now
                .iter()
                .filter(|id| !connected_peers_previous.contains(id))
            {
                // Include what we last received FROM this peer, so they can detect
                // if we missed any of their messages
                let last_received_from_peer = self.tracker.get_last_received_from(peer_id);
                log::info!(target: "ism", "[RESYNC] Sending Poll to peer {peer_id:?} with last_received_from_peer={last_received_from_peer:?}");
                if let Err(e) = self
                    .send_message_internal(Payload::Poll {
                        from_id: self.network.local_id(),
                        to_id: *peer_id,
                        last_received_from_peer,
                    })
                    .await
                {
                    log::error!(target: "ism", "Failed to send poll to new peer: {e:?}");
                    break;
                }
            }

            *current_peers_lock = connected_peers_now;
        }
    }

    async fn process_outbound(&self) {
        let pending_messages = match self.backend.get_pending_outbound().await {
            Ok(messages) => messages,
            Err(e) => {
                log::error!(target: "ism", "Failed to get pending outbound messages: {e:?}");
                return;
            }
        };

        // Group messages by PeerId
        let mut grouped_messages: HashMap<M::PeerId, Vec<M>> = HashMap::new();
        for msg in pending_messages {
            grouped_messages
                .entry(msg.destination_id())
                .or_default()
                .push(msg);
        }

        let connected_peers = &self.network.connected_peers().await;
        let local_cid = self.network.local_id();
        log::info!(target: "ism", "[ILM-OUTBOUND] CID {local_cid}: {} messages in {} groups, {} connected peers: {:?}",
            grouped_messages.values().map(|v| v.len()).sum::<usize>(),
            grouped_messages.len(),
            connected_peers.len(),
            connected_peers);

        // Process each peer's messages concurrently
        futures::stream::iter(grouped_messages).for_each_concurrent(None, |(peer_id, messages)|  {
            async move {
                if !connected_peers.contains(&peer_id) {
                    log::warn!(target: "ism", "[ILM-OUTBOUND] CID {local_cid}: Peer {peer_id} is not connected, skipping {} messages", messages.len());
                    return;
                }

                // Sort messages by MessageId
                let messages = messages.into_iter().sorted_by_key(|r| r.message_id()).unique_by(|r| r.message_id()).collect::<Vec<_>>();

                // Find the first message we can send based on ACKs
                'peer: for msg in messages {
                    let message_id = msg.message_id();
                    let last_acked = self.tracker.last_acked.get(&peer_id).map(|v| *v);
                    let last_sent = self.tracker.last_sent.get(&peer_id).map(|v| *v);
                    let can_send = self.tracker.can_send(&peer_id, &message_id);

                    log::info!(target: "ism", "[ILM-OUTBOUND] CID {local_cid} -> peer {peer_id}: msg_id={message_id}, can_send={can_send}, last_acked={last_acked:?}, last_sent={last_sent:?}");

                    if can_send {
                        log::info!(target: "ism", "[ILM-SEND] CID {local_cid} -> peer {peer_id}: SENDING msg_id={message_id}");
                        if let Err(e) = self.send_message_internal(Payload::Message(msg)).await {
                            log::error!(target: "ism", "[ILM-SEND] FAILED: {:?}", e);
                        } else {
                            log::info!(target: "ism", "[ILM-SEND] SUCCESS: msg_id={message_id}");
                            if let Err(err) = self.tracker.mark_sent(peer_id, message_id).await {
                                log::error!(target: "ism", "Failed to mark message as sent: {err:?}");
                            }
                            // Reset block counter on successful send
                            self.blocked_count.remove(&peer_id);
                            // Stop after sending the first message that can be sent
                            break 'peer;
                        }
                    } else {
                        // Increment block counter for this peer
                        let mut block_count = self.blocked_count.entry(peer_id).or_insert(0);
                        *block_count += 1;
                        let current_count = *block_count;
                        drop(block_count);

                        log::warn!(target: "ism", "[ILM-BLOCKED] CID {local_cid} -> peer {peer_id}: msg_id={message_id} blocked (awaiting ACK), consecutive_blocks={current_count}");

                        // If blocked too many times, peer likely reconnected with fresh state
                        // Clear stale tracking to allow message delivery
                        if current_count >= MAX_CONSECUTIVE_BLOCKS {
                            log::warn!(target: "ism", "[ILM-BLOCKED-RECOVERY] CID {local_cid} -> peer {peer_id}: clearing stale state after {current_count} consecutive blocks");
                            self.tracker.last_sent.remove(&peer_id);
                            self.tracker.last_acked.remove(&peer_id);
                            if let Err(e) = self.tracker.sync_backend().await {
                                log::error!(target: "ism", "[ILM-BLOCKED-RECOVERY] Failed to sync backend: {:?}", e);
                            }
                            self.blocked_count.remove(&peer_id);
                            // Continue processing - next iteration should succeed
                            continue 'peer;
                        }
                        // If we can't send the current message, stop processing this group
                        break;
                    }
                }
            }
        }).await
    }

    async fn process_inbound(&self) {
        let pending_messages = match self.backend.get_pending_inbound().await {
            Ok(messages) => messages,
            Err(e) => {
                log::error!(target: "ism", "Failed to get pending inbound messages: {e:?}");
                return;
            }
        };

        // Sort the pending messages in order by MessageID
        let pending_messages: Vec<M> = pending_messages
            .into_iter()
            .sorted_by_key(|r| r.message_id())
            .unique_by(|r| r.message_id())
            .collect();

        log::trace!(target: "ism", "~~~Processing inbound messages: {pending_messages:?}");
        if let Some(delivery) = self.local_delivery.lock().await.as_ref() {
            for message in pending_messages {
                if self
                    .tracker
                    .has_delivered
                    .contains(&(message.source_id(), message.message_id()))
                {
                    log::warn!(target: "ism", "Skipping already delivered message: {message:?}");
                    // Clear delivered message from backend
                    if let Err(e) = self
                        .backend
                        .clear_message_inbound(message.source_id(), message.message_id())
                        .await
                    {
                        log::error!(target: "ism", "Failed to clear delivered message: {e:?}");
                    }
                    continue;
                }

                match delivery.deliver(message.clone()).await {
                    Ok(()) => {
                        log::info!(target: "ism", "[ILM-INBOUND] Delivered msg_id={} from peer {}", message.message_id(), message.source_id());
                        self.tracker
                            .has_delivered
                            .insert((message.source_id(), message.message_id()));
                        // Create and send ACK
                        log::info!(target: "ism", "[ILM-ACK] Sending ACK for msg_id={} to peer {}", message.message_id(), message.source_id());
                        if let Err(e) = self
                            .send_message_internal(self.create_ack_message(&message))
                            .await
                        {
                            log::error!(target: "ism", "[ILM-ACK] FAILED to send ACK: {e:?}");
                        }

                        // Clear delivered message from backend
                        if let Err(e) = self
                            .backend
                            .clear_message_inbound(message.source_id(), message.message_id())
                            .await
                        {
                            log::error!(target: "ism", "Failed to clear delivered message: {e:?}");
                        }
                    }
                    Err(e) => {
                        log::error!(target: "ism", "Failed to deliver message {message:?}: {e:?}");
                    }
                }
            }
        } else {
            log::warn!(target: "ism", "Unable to deliver messages since local delivery has been dropped");
        }
    }

    // Modify process_network_messages to update the tracker
    async fn process_next_network_message(&self) {
        if let Some(message) = self.network.next_message().await {
            match message {
                Payload::Poll {
                    from_id,
                    last_received_from_peer,
                    ..
                } => {
                    log::info!(target: "ism", "[RESYNC] Received Poll from peer {from_id:?} with last_received_from_peer={last_received_from_peer:?}");

                    // If peer reports receiving nothing (fresh state after reconnect),
                    // fully reset our tracking state for that peer to allow fresh communication.
                    // This handles the case where the sender has stale state from before disconnect.
                    if last_received_from_peer.is_none() {
                        log::info!(target: "ism", "[RESYNC] Peer {from_id:?} reports fresh state - clearing our tracking");
                        self.tracker.last_sent.remove(&from_id);
                        self.tracker.last_acked.remove(&from_id);
                        if let Err(e) = self.tracker.sync_backend().await {
                            log::error!(target: "ism", "[RESYNC] Failed to sync backend after clearing: {:?}", e);
                        }
                    } else if let Some(their_received) = last_received_from_peer {
                        // CRITICAL: Update last_acked based on what peer reports receiving.
                        // This acts as an implicit ACK for all messages up to last_received_from_peer.
                        // Without this, after hard disconnect/reconnect, our messages remain blocked
                        // waiting for ACKs that were lost during the disconnect.
                        if let Err(e) = self.tracker.update_ack(from_id, their_received).await {
                            log::error!(target: "ism", "[RESYNC] Failed to update last_acked from Poll: {:?}", e);
                        } else {
                            log::info!(target: "ism", "[RESYNC] Updated last_acked[{from_id:?}] = {their_received:?} (implicit ACK)");
                        }

                        // Check if the peer is missing messages we sent
                        // `last_received_from_peer` is what THEY last received FROM US
                        // `last_sent` is what WE last sent TO THEM
                        let our_last_sent_to_peer =
                            self.tracker.last_sent.get(&from_id).map(|v| *v);

                        // Detect gap: we sent something but they only received part
                        if let Some(our_sent) = our_last_sent_to_peer {
                            if our_sent > their_received {
                                log::info!(target: "ism", "[RESYNC] Gap detected: we sent {:?} but peer only received {:?}", our_sent, their_received);
                                // Clear our last_sent to allow resending the blocked message
                                if let Err(e) = self.tracker.clear_last_sent(&from_id).await {
                                    log::error!(target: "ism", "[RESYNC] Failed to clear last_sent: {:?}", e);
                                }
                            }
                        }
                    }

                    // Trigger process_outbound which will now be able to send
                    // (since we cleared last_sent, can_send() will return true)
                    if self.poll_outbound_tx.send(()).is_err() {
                        log::warn!(target: "ism", "Failed to send poll signal for outbound messages");
                    }
                }

                Payload::Ack {
                    from_id,
                    message_id,
                    to_id,
                } => {
                    log::info!(target: "ism", "[ILM-ACK-RECV] Received ACK from_id={from_id} msg_id={message_id} to_id={to_id} local_id={}", self.network.local_id());
                    if to_id != self.network.local_id() {
                        log::warn!(target: "ism", "[ILM-ACK-RECV] ACK not for us - ignoring");
                        return;
                    }

                    // Update the tracker with the new ACK
                    if let Err(err) = self.tracker.update_ack(from_id, message_id).await {
                        log::error!(target: "ism", "[ILM-ACK-RECV] Failed to update tracker: {err:?}");
                    } else {
                        log::info!(target: "ism", "[ILM-ACK-RECV] Tracker updated - unblocking peer {from_id}");
                    }

                    log::trace!(target: "ism", "Received ACK from peer {from_id}, message # {message_id}");
                    if let Err(e) = self
                        .backend
                        .clear_message_outbound(from_id, message_id)
                        .await
                    {
                        log::error!(target: "ism", "Failed to clear ACKed message: {e:?}");
                    }

                    // Poll any pending outbound messages
                    if self.poll_outbound_tx.send(()).is_err() {
                        log::warn!(target: "ism", "Failed to send poll signal for outbound messages");
                    }
                }
                Payload::Message(msg) => {
                    if msg.destination_id() != self.network.local_id() {
                        log::warn!(target: "ism", "Received message for another peer");
                        return;
                    }

                    if let Ok(msgs) = self.backend.get_pending_outbound().await {
                        if msgs.iter().any(|m| {
                            m.message_id() == msg.message_id() && m.source_id() == msg.source_id()
                        }) {
                            log::warn!(target: "ism", "Received duplicate message, sending ACK");
                            if let Err(e) = self
                                .send_message_internal(self.create_ack_message(&msg))
                                .await
                            {
                                log::error!(target: "ism", "Failed to send ACK for duplicate message: {e:?}");
                            }
                            return;
                        }
                    }

                    // Check if this is a new message
                    match self
                        .tracker
                        .mark_received(msg.source_id(), msg.message_id())
                        .await
                    {
                        Ok(true) => {
                            // New message - update last_received_from for resync tracking
                            if let Err(e) = self
                                .tracker
                                .update_last_received_from(msg.source_id(), msg.message_id())
                                .await
                            {
                                log::error!(target: "ism", "Failed to update last_received_from: {e:?}");
                            }

                            // Store and process the message
                            if let Err(e) = self.backend.store_inbound(msg).await {
                                log::error!(target: "ism", "Failed to store inbound message: {e:?}");
                            }

                            if self.poll_inbound_tx.send(()).is_err() {
                                log::warn!(target: "ism", "Failed to send poll signal for inbound messages");
                            }
                        }
                        Ok(false) => {
                            // Already received this message, just send ACK
                            if let Err(e) = self
                                .send_message_internal(self.create_ack_message(&msg))
                                .await
                            {
                                log::error!(target: "ism", "Failed to send ACK for duplicate message: {e:?}");
                            }
                        }
                        Err(e) => {
                            log::error!(target: "ism", "Failed to mark message as received: {e:?}");
                        }
                    }
                }
            }
        }
    }

    /// The preferred entrypoint for sending messages. Unlike `[Self::send_raw_message]`, this
    /// ensures the message is properly created
    pub async fn send_to(
        &self,
        to: M::PeerId,
        contents: impl Into<M::Contents>,
    ) -> Result<(), NetworkError<M>> {
        let my_id = self.network.local_id();
        let next_id_for_this_peer_conn = self
            .tracker
            .get_next_id(to)
            .await
            .map_err(|err| NetworkError::BackendError(err))?;
        let message = M::construct_from_parts(my_id, to, next_id_for_this_peer_conn, contents);
        self.send_raw_message(message).await
    }

    /// This message should only be used internally or if the developer needs to manually
    /// create messages. In this case, the message ID must be an auto-incremented value to
    /// ensure uniqueness, and the source_id must match the ID of the node sending the message
    /// in the networking layer. Additionally, the source and destination fields cannot be the same
    pub async fn send_raw_message(&self, message: M) -> Result<(), NetworkError<M>> {
        if message.source_id() != self.network.local_id() {
            return Err(NetworkError::SendFailed {
                reason: "Source ID does not match network peer ID".into(),
                message,
            });
        }

        if message.destination_id() == self.network.local_id() {
            return Err(NetworkError::SendFailed {
                reason: "Cannot send message to self".into(),
                message,
            });
        }

        if self.can_run() {
            self.backend
                .store_outbound(message)
                .await
                .map_err(|err| match err {
                    BackendError::SendFailed { reason, message } => {
                        NetworkError::SendFailed { reason, message }
                    }
                    err => NetworkError::BackendError(err),
                })?;

            // NOTE: Removed poll_outbound_tx.send(()) here to prevent tight feedback loop.
            // The periodic 200ms OUTBOUND_POLL is sufficient for processing pending messages.
            // Triggering immediate poll after every send created: process_outbound →
            // send_message_internal → poll_outbound_tx → process_outbound → infinite loop
            // that flooded LocalDB requests and blocked the WASM event loop.
            Ok(())
        } else {
            Err(NetworkError::SystemShutdown)
        }
    }

    fn create_ack_message(&self, original_message: &M) -> Payload<M> {
        // Must send an ACK back with a flipped order of the source and destination
        Payload::Ack {
            from_id: original_message.destination_id(),
            to_id: original_message.source_id(),
            message_id: original_message.message_id(),
        }
    }

    /// Shutdown the message system gracefully
    /// This will stop the background tasks and wait for pending outbound messages to be processed
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), NetworkError<M>> {
        if self.is_shutting_down.fetch_or(true, Ordering::SeqCst) {
            return Ok(());
        }
        // Wait for pending messages to be processed
        let result = platform_timeout(timeout, async {
            let pending_outbound_task = async move {
                while !self
                    .backend
                    .get_pending_outbound()
                    .await
                    .map_err(NetworkError::BackendError)?
                    .is_empty()
                {
                    platform_sleep(Duration::from_millis(100)).await;
                }

                Ok(())
            };

            let pending_inbound_task = async move {
                while !self
                    .backend
                    .get_pending_inbound()
                    .await
                    .map_err(NetworkError::BackendError)?
                    .is_empty()
                {
                    platform_sleep(Duration::from_millis(100)).await;
                }

                Ok(())
            };

            citadel_io::tokio::try_join!(pending_outbound_task, pending_inbound_task)?;

            Ok::<_, NetworkError<M>>(())
        })
        .await;

        match result {
            Ok(inner_result) => inner_result?,
            Err(err) => return Err(NetworkError::ShutdownFailed(err.to_string())),
        }

        self.toggle_off();

        Ok(())
    }

    pub async fn get_connected_peers(&self) -> Vec<M::PeerId> {
        self.network
            .connected_peers()
            .await
            .into_iter()
            .sorted()
            .collect::<Vec<_>>()
    }

    /// Returns the ID of this node in the network
    pub fn local_id(&self) -> M::PeerId {
        self.network.local_id()
    }

    fn can_run(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    fn toggle_off(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    async fn send_message_internal(
        &self,
        message: Payload<M>,
    ) -> Result<(), NetworkError<Payload<M>>> {
        let res = self.network.send_message(message).await;

        if res.is_err() {
            // Since I/O is corrupt, there is no chance of safe shutdown or recovery
            // at this time. We will just set the atomic bool to false and return the error
        }

        res
    }
}
