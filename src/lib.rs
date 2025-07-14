#![forbid(unsafe_code)]

use async_trait::async_trait;
use futures::StreamExt;
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
use citadel_io::tokio::sync::Mutex;
use citadel_io::tokio::time::{sleep, Duration};

pub mod local_delivery;
pub(crate) mod message_tracker;

#[cfg(feature = "testing")]
pub mod testing;

const OUTBOUND_POLL: Duration = Duration::from_millis(200);
const INBOUND_POLL: Duration = Duration::from_millis(200);

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
pub trait Backend<M: MessageMetadata> {
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
}

const MAX_MAP_SIZE: usize = 1000;

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
        let (poll_outbound_tx, poll_outbound_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel();

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

                    citadel_io::tokio::select! {
                        res0 = poll_outbound_rx.recv() => {
                            if res0.is_none() {
                                log::warn!(target: "ism", "Poll outbound channel closed");
                                return;
                            }
                        },
                        _res1 = sleep(OUTBOUND_POLL) => {},
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

                    citadel_io::tokio::select! {
                        biased;
                        res0 = poll_inbound_rx.recv() => {
                            if res0.is_none() {
                                log::warn!(target: "ism", "Poll inbound channel closed");
                            }
                        },
                        _res1 = sleep(INBOUND_POLL) => {},
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

                    sleep(Duration::from_secs(5)).await;
                }
            };

            citadel_io::tokio::select! {
                _ = outbound_handle => {
                    log::error!(target: "ism", "Outbound processing task prematurely ended");
                },
                _ = inbound_handle => {
                    log::error!(target: "ism", "Inbound processing task prematurely ended");
                },
                _ = network_io_handle => {
                    log::error!(target: "ism", "Network IO task prematurely ended");
                },
                _ = peer_polling_handle => {
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
        drop(citadel_io::tokio::spawn(background_task));
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

            // Now, send a poll to each new connected peer
            for peer_id in connected_peers_now
                .iter()
                .filter(|id| !connected_peers_previous.contains(id))
            {
                if let Err(e) = self
                    .send_message_internal(Payload::Poll {
                        from_id: self.network.local_id(),
                        to_id: *peer_id,
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
        // Process each peer's messages concurrently
        futures::stream::iter(grouped_messages).for_each_concurrent(None, |(peer_id, messages)|  {
            async move {
                if !connected_peers.contains(&peer_id) {
                    log::warn!(target: "ism", "Peer {peer_id} is not connected, skipping message until later");
                    return;
                }

                // Sort messages by MessageId
                let messages = messages.into_iter().sorted_by_key(|r| r.message_id()).unique_by(|r| r.message_id()).collect::<Vec<_>>();

                // Find the first message we can send based on ACKs
                'peer: for msg in messages {
                    let message_id = msg.message_id();
                    if self.tracker.can_send(&peer_id, &message_id) {
                        log::trace!(target: "ism", "[CAN SEND] message: {msg:?}");
                        if let Err(e) = self.send_message_internal(Payload::Message(msg)).await {
                            log::error!(target: "ism", "Failed to send message: {e:?}");
                        } else {
                            if let Err(err) = self.tracker.mark_sent(peer_id, message_id).await {
                                log::error!(target: "ism", "Failed to mark message as sent: {err:?}");
                            }
                            // Stop after sending the first message that can be sent
                            break 'peer;
                        }
                    } else {
                        log::trace!(target: "ism", "[CANNOT SEND] message: {msg:?}");
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
                        log::trace!(target: "ism", "Successfully delivered message: {message:?}");
                        self.tracker
                            .has_delivered
                            .insert((message.source_id(), message.message_id()));
                        // Create and send ACK
                        if let Err(e) = self
                            .send_message_internal(self.create_ack_message(&message))
                            .await
                        {
                            log::error!(target: "ism", "Failed to send ACK: {e:?}");
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
                Payload::Poll { .. } => {
                    // This will trigger process_outbound() which already sends
                    // the next unacknowledged message due to head of line blocking
                    if self.poll_outbound_tx.send(()).is_err() {
                        log::warn!(target: "ism", "Failed to send poll signal for outbound messages");
                    }
                }

                Payload::Ack {
                    from_id,
                    message_id,
                    to_id,
                } => {
                    if to_id != self.network.local_id() {
                        log::warn!(target: "ism", "Received ACK for another peer");
                        return;
                    }

                    // Update the tracker with the new ACK
                    if let Err(err) = self.tracker.update_ack(from_id, message_id).await {
                        log::error!(target: "ism", "Failed to update tracker with ACK: {err:?}");
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
                            // New message, process it
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

            self.poll_outbound_tx
                .send(())
                .map_err(|_| NetworkError::SystemShutdown)?;
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
        citadel_io::tokio::time::timeout(timeout, async {
            let pending_outbound_task = async move {
                while !self
                    .backend
                    .get_pending_outbound()
                    .await
                    .map_err(NetworkError::BackendError)?
                    .is_empty()
                {
                    citadel_io::tokio::time::sleep(Duration::from_millis(100)).await;
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
                    citadel_io::tokio::time::sleep(Duration::from_millis(100)).await;
                }

                Ok(())
            };

            citadel_io::tokio::try_join!(pending_outbound_task, pending_inbound_task)?;

            Ok::<_, NetworkError<M>>(())
        })
        .await
        .map_err(|err| NetworkError::ShutdownFailed(err.to_string()))??;

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
