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

/// Poll cycles a head message waits, unacknowledged, before it is sent again.
///
/// The outbound path sends the LOWEST unsent id and breaks on the first message
/// it cannot send, and `can_send` refuses anything whose id is not greater than
/// `last_sent` -- so the message at the head cannot be retransmitted through
/// the normal route at all. Before this constant existed the only thing that
/// ever re-sent it was the emergency branch below, ten cycles later, which also
/// discarded `last_acked`: the protocol depended on its own recovery path for
/// ordinary packet loss, and everything queued behind the head waited for it.
///
/// Measured, with four ACKs in five dropped: ten of eighteen messages never
/// arrived in sixty seconds (`a_burst_survives_lost_acks`).
///
/// Counted in cycles rather than elapsed time because this crate compiles to
/// wasm32, where `Instant::now` is not available. The poll interval is 200ms,
/// so this is roughly a one-second retransmit.
/// How many messages may be in flight to one peer at a time.
///
/// This was one, and one is what a directory sync costs dearly. The outbound
/// path sent the lowest unsent id and stopped, so the whole queue waited on the
/// head's acknowledgement. Measured on the crate's own in-memory network with
/// four ACKs in five dropped: **2.7 seconds per message**. CI's file-manager
/// run had 96 queued behind one such head, which is over four minutes for a
/// sync the test gives up on long before.
///
/// A clean link never noticed, because there the head is acknowledged in
/// microseconds -- 96 messages drain in 34ms. The cost is entirely a
/// loss-recovery cost, and stop-and-wait pays it once per message rather than
/// once per window.
///
/// Eight, not more: acknowledgement is cumulative, so one surviving ACK retires
/// the whole window, and at four-in-five loss the chance that at least one of
/// eight survives is 83%. Larger windows buy little and put more unacknowledged
/// data on a link that is already dropping things.
///
/// Setting this to 1 restores the previous behaviour exactly, and the tests
/// that pin ordering and non-loss pass either way.
const SEND_WINDOW: usize = 8;

/// How long a message may be held waiting for the gap beneath it to fill.
///
/// Generous on purpose: the sender retransmits its head every
/// `RETRANSMIT_AFTER_BLOCKS` cycles (about a second) and clears its state
/// entirely after `MAX_CONSECUTIVE_BLOCKS` (about ten), so a gap that can be
/// filled is filled long before this. Passing it means the missing id is not
/// coming, and a conversation that never moves again is a worse outcome than
/// one delivery out of order announced at `warn!`.
const GAP_PATIENCE_SECS: u64 = 20;

const RETRANSMIT_AFTER_BLOCKS: u32 = 5;

/// Max consecutive blocks before clearing stale state for a peer.
///
/// This is the "the peer came back with fresh state" case, not packet loss --
/// retransmission above handles that. It throws away `last_acked`, so every
/// message the peer already confirmed can be sent again; that is a reasonable
/// thing to do once a link has been silent through fifty cycles and a wrong
/// thing to do as a routine response to a dropped ACK.
const MAX_CONSECUTIVE_BLOCKS: u32 = 50;

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
            log::info!(target: "ism", "[RESYNC-PEERS] CID {}: Peers changed from {:?} to {:?}", self.network.local_id(), connected_peers_previous, connected_peers_now);

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
        // Only when there is something to report. This loop runs every 200ms
        // per session, so unconditionally it emits 5 lines/sec/session saying
        // "0 messages" -- which drowns the `error!`s below it in the same
        // target. Those errors are the only record of a failed delivery, so
        // the noise here is what makes them unreadable rather than merely
        // verbose.
        let pending_total: usize = grouped_messages.values().map(|v| v.len()).sum();
        if pending_total > 0 {
            log::info!(target: "ism", "[ILM-OUTBOUND] CID {local_cid}: {} messages in {} groups, {} connected peers: {:?}",
                pending_total,
                grouped_messages.len(),
                connected_peers.len(),
                connected_peers);
        }

        // Process each peer's messages concurrently
        futures::stream::iter(grouped_messages).for_each_concurrent(None, |(peer_id, messages)|  {
            async move {
                if !connected_peers.contains(&peer_id) {
                    log::warn!(target: "ism", "[ILM-OUTBOUND] CID {local_cid}: Peer {peer_id} is not connected, skipping {} messages", messages.len());
                    return;
                }

                // Sort messages by MessageId
                let messages = messages.into_iter().sorted_by_key(|r| r.message_id()).unique_by(|r| r.message_id()).collect::<Vec<_>>();

                // One message at a time per peer, and STOP at the first that
                // cannot go. That is what keeps the wire ordered.
                //
                // A version of this pipelined up to eight per cycle, so a lost
                // message no longer held up the ones behind it: the lost-ACK
                // burst went from 2.5s to 0.01s. It was reverted, because the
                // RECEIVER has no contiguity gate -- `process_inbound` delivers
                // whatever is pending, sorted within the batch, and never holds
                // message N+1 waiting for N. Stop-and-wait was what made the
                // ordering hold, and revfs applies operations in the order it
                // receives them: a create arriving after the write into it is a
                // worse outcome than a slow sync.
                //
                // The retransmission below is the part that mattered, and it
                // stands: without it the head could not be repeated at all and
                // the queue stopped dead.
                // How many are behind the head, captured before the loop
                // consumes them.
                //
                // `[ILM-OUTBOUND]` reports a total across all peers once per
                // cycle at info!, and `[ILM-BLOCKED]` reports the head once per
                // transition. Neither says how much is piled up behind that
                // head for THIS peer, and that is the number which separates
                // one lost ACK from a link that has stopped: a CI run showed
                // one side at 12 queued, then 13, while the other sat at 1 and
                // recovered. Reconstructing that meant diffing consecutive
                // info lines by eye.
                let queued: usize = messages.len();

                // Room in the window: how many more may go out before the
                // oldest unacknowledged one has to come back.
                //
                // "In flight" is every id already sent and not yet acked --
                // above `last_acked`, at or below `last_sent`. Recomputed from
                // the queue each cycle rather than counted incrementally, so a
                // dropped or re-delivered message cannot leave the count
                // drifting from what is actually outstanding.
                let acked_upto = self.tracker.last_acked.get(&peer_id).map(|v| *v);
                let sent_upto = self.tracker.last_sent.get(&peer_id).map(|v| *v);
                let outstanding: usize = match sent_upto {
                    None => 0,
                    Some(sent) => messages
                        .iter()
                        .filter(|m| {
                            let id = m.message_id();
                            id <= sent && acked_upto.map(|a| id > a).unwrap_or(true)
                        })
                        .count(),
                };
                // Closed until this peer has acknowledged something.
                //
                // The receiver takes the lowest id waiting from a peer as the
                // start of the run when it has delivered nothing yet, and that
                // is only sound while one message is in flight: with the window
                // open from the start, losing the true first message would make
                // the second look like the beginning, and acknowledging it would
                // cumulatively retire the one that was lost.
                let window: usize = if acked_upto.is_some() { SEND_WINDOW } else { 1 };
                let mut budget: usize = window.saturating_sub(outstanding);
                // The head does the block accounting; the messages behind it
                // are not "blocked", they are simply not its turn.
                let mut head_accounted: bool = false;

                'peer: for msg in messages {
                    let message_id = msg.message_id();
                    let last_acked = self.tracker.last_acked.get(&peer_id).map(|v| *v);
                    let last_sent = self.tracker.last_sent.get(&peer_id).map(|v| *v);
                    let can_send = self.tracker.can_send(&peer_id, &message_id);

                    // `debug!`, not `info!`, and this is not a style choice.
                    //
                    // Before the loop stopped at the first message it could not
                    // send, this fired once per cycle. It now considers every
                    // queued message, so at the 200ms poll it is 5 lines per
                    // second per QUEUED MESSAGE -- measured at 31,918 lines in a
                    // single CI run, against 2,086 before. The same function
                    // already carries a comment about exactly this: the noise
                    // "drowns the `error!`s below it in the same target".
                    //
                    // In the WASM client each of these is a console write on the
                    // browser's main thread, so the volume is not only
                    // unreadable, it is spent.
                    log::debug!(target: "ism", "[ILM-OUTBOUND] CID {local_cid} -> peer {peer_id}: msg_id={message_id}, can_send={can_send}, last_acked={last_acked:?}, last_sent={last_sent:?}");

                    if can_send {
                        // Window full: the rest wait for an acknowledgement,
                        // not for a timer.
                        if budget == 0 {
                            log::debug!(target: "ism", "[ILM-SEND] CID {local_cid} -> peer {peer_id}: window full at {window}, {queued} queued");
                            break 'peer;
                        }
                        log::info!(target: "ism", "[ILM-SEND] CID {local_cid} -> peer {peer_id}: SENDING msg_id={message_id}");
                        if let Err(e) = self.send_message_internal(Payload::Message(msg)).await {
                            log::error!(target: "ism", "[ILM-SEND] FAILED: {:?}", e);
                            // The wire refused it. Trying the next id would put
                            // a gap on a link that just failed, so stop here and
                            // let the next cycle retry this same id.
                            break 'peer;
                        } else {
                            log::info!(target: "ism", "[ILM-SEND] SUCCESS: msg_id={message_id}");
                            if let Err(err) = self.tracker.mark_sent(peer_id, message_id).await {
                                log::error!(target: "ism", "Failed to mark message as sent: {err:?}");
                            }
                            // Reset block counter on successful send
                            self.blocked_count.remove(&peer_id);
                            budget -= 1;
                            continue 'peer;
                        }
                    } else if head_accounted {
                        // Already sent and awaiting an ACK, and not the head, so
                        // there is nothing to count and nothing to retransmit.
                        // Keep walking: a sendable id may be behind it.
                        continue 'peer;
                    } else {
                        head_accounted = true;
                        // Increment block counter for this peer
                        let mut block_count = self.blocked_count.entry(peer_id).or_insert(0);
                        *block_count += 1;
                        let current_count = *block_count;
                        drop(block_count);

                        // Announced ONCE, then quiet.
                        //
                        // Waiting for an ACK is the normal steady state of a
                        // stop-and-wait link, and this fires every 200ms per
                        // peer for as long as the wait lasts. A CI run of one
                        // stuck link is hundreds of consecutive lines of it --
                        // at `warn!`, which in the WASM client is a synchronous
                        // console write on the browser's main thread, in a loop,
                        // while the thing it is complaining about is starved of
                        // that thread.
                        //
                        // The transition INTO blocked is the news. Everything
                        // after it is the same news again.
                        if current_count == 1 {
                            log::warn!(target: "ism", "[ILM-BLOCKED] CID {local_cid} -> peer {peer_id}: msg_id={message_id} blocked, awaiting ACK, {queued} queued for this peer");
                        } else {
                            log::debug!(target: "ism", "[ILM-BLOCKED] CID {local_cid} -> peer {peer_id}: msg_id={message_id} blocked (awaiting ACK), consecutive_blocks={current_count}, {queued} queued");
                        }

                        // Silent through fifty cycles: the peer most likely
                        // came back with fresh state, and nothing we have sent
                        // means anything to it. Checked BEFORE retransmission,
                        // because at that point sending the same message again
                        // is the thing that has already failed fifty times.
                        if current_count >= MAX_CONSECUTIVE_BLOCKS {
                            log::warn!(target: "ism", "[ILM-BLOCKED-RECOVERY] CID {local_cid} -> peer {peer_id}: clearing stale state after {current_count} consecutive blocks, {queued} queued for this peer");
                            self.tracker.last_sent.remove(&peer_id);
                            self.tracker.last_acked.remove(&peer_id);
                            if let Err(e) = self.tracker.sync_backend().await {
                                log::error!(target: "ism", "[ILM-BLOCKED-RECOVERY] Failed to sync backend: {:?}", e);
                            }
                            self.blocked_count.remove(&peer_id);
                            // Let the next cycle start over with clean state.
                            break 'peer;
                        }

                        // Send it again.
                        //
                        // `can_send` is false because `msg_id > last_sent` is
                        // false -- this message has been sent and not
                        // acknowledged, which is precisely the case that calls
                        // for a retransmission rather than for waiting. The
                        // receiver de-duplicates and acknowledges again (see
                        // the `duplicate_reack` test), so a repeat costs one
                        // packet and never a duplicate delivery.
                        //
                        // `last_sent` and `last_acked` are both left alone: a
                        // retransmission moves neither, and the block counter
                        // keeps running as the retransmit clock.
                        if current_count.is_multiple_of(RETRANSMIT_AFTER_BLOCKS) {
                            // `info!`: a retransmission is expected on a lossy
                            // link, and at one per second per peer a `warn!`
                            // has the same cost as the blocked line above.
                            log::info!(target: "ism", "[ILM-RETRANSMIT] CID {local_cid} -> peer {peer_id}: msg_id={message_id}, unacknowledged for {current_count} cycles");
                            if let Err(e) = self.send_message_internal(Payload::Message(msg)).await
                            {
                                log::error!(target: "ism", "[ILM-RETRANSMIT] FAILED: {:?}", e);
                            }
                        }

                        // The head stays unacknowledged, but the window does
                        // not stop at it. Everything behind it that is still
                        // unsent and inside the window goes out on this same
                        // cycle -- which is the whole point: one surviving
                        // cumulative ACK then retires the head and the run
                        // behind it together.
                        continue 'peer;
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

        // De-duplicate by (source, id) — NOT by id alone.
        //
        // Message IDs come from `get_next_id`, which keeps a counter PER PEER,
        // so every peer's ids start at zero and collide with every other
        // peer's. De-duplicating the whole inbound queue on the id alone
        // therefore dropped one peer's message from the batch whenever another
        // peer had a message with the same number pending.
        //
        // Not data loss, and worth being precise about: the delivered message
        // is cleared from the backend afterwards, so the one that lost the tie
        // is still pending on the next poll and is delivered then. The effect
        // was a needless one-cycle delay per collision, and a de-duplication
        // rule that disagreed with the backend, which has always keyed inbound
        // by (source_id, message_id).
        //
        // The outbound path already does this correctly, because there the
        // de-duplication runs after the messages are grouped by peer.
        let pending_messages: Vec<M> = pending_messages
            .into_iter()
            .sorted_by_key(|r| r.message_id())
            .unique_by(|r| (r.source_id(), r.message_id()))
            .collect();

        log::trace!(target: "ism", "~~~Processing inbound messages: {pending_messages:?}");

        // Grouped by sender, ascending, because deliverability is per peer.
        //
        // The batch above is sorted by message id alone, and ids are per-peer
        // counters, so two peers' streams interleave in it. Walking that order
        // and asking "is this the next id from its sender" would answer for
        // whichever sender happened to sort adjacent.
        let mut by_peer: std::collections::HashMap<M::PeerId, Vec<M>> =
            std::collections::HashMap::new();
        for message in pending_messages {
            by_peer
                .entry(message.source_id())
                .or_default()
                .push(message);
        }
        for group in by_peer.values_mut() {
            group.sort_by_key(|m| m.message_id());
        }

        if let Some(delivery) = self.local_delivery.lock().await.as_ref() {
            for (peer_id, group) in by_peer {
                for (position, message) in group.into_iter().enumerate() {
                    let message_id = message.message_id();
                    let first_pending = position == 0;

                    // Already delivered, in this process or a previous one.
                    //
                    // Asked of the DURABLE frontier rather than the in-memory
                    // `has_delivered` set, because that set does not survive a
                    // restart: after one, a retransmission of an old message
                    // would fall through to the gap check below, be held as
                    // "not next", never acknowledged, and retransmitted for as
                    // long as the process lived.
                    if self.tracker.safe_to_ack(&peer_id, &message_id) {
                        // Re-ACK, do NOT just drop it.
                        //
                        // A duplicate arriving means the sender never saw our
                        // first ACK, so it is still waiting on this exact id.
                        // Suppressing the ACK is what makes retransmission
                        // useless; re-ACKing is what makes it idempotent.
                        log::info!(target: "ism", "[ILM-ACK] Re-ACKing already-delivered msg_id={message_id} to peer {peer_id}");
                        if let Err(e) = self
                            .send_message_internal(self.create_ack_message(&message))
                            .await
                        {
                            log::error!(target: "ism", "[ILM-ACK] FAILED to re-ACK duplicate: {e:?}");
                        }
                        if let Err(e) = self
                            .backend
                            .clear_message_inbound(peer_id, message_id)
                            .await
                        {
                            log::error!(target: "ism", "Failed to clear delivered message: {e:?}");
                        }
                        continue;
                    }

                    // A gap. Hold it, and hold everything behind it.
                    //
                    // More than one message is in flight per peer now, so 5 can
                    // arrive while 4 is still on the wire. Delivering 5 would be
                    // out of order — revfs applies operations in the order it
                    // receives them, and a write landing before the create it
                    // belongs to is worse than a slow sync. Worse still, the ACK
                    // that follows delivery is CUMULATIVE: acknowledging 5
                    // retires 4 at the sender, which will then never send it
                    // again. That is silent, permanent loss.
                    //
                    // Held, not dropped: it stays in the backend and is
                    // delivered on the cycle after the gap fills. The group is
                    // ascending, so nothing behind this one is contiguous
                    // either.
                    if !self
                        .tracker
                        .is_next_deliverable(&peer_id, &message_id, first_pending)
                    {
                        if self
                            .tracker
                            .held_too_long(&peer_id, &message_id, GAP_PATIENCE_SECS)
                        {
                            // Loud, because it is a real loss: advancing the
                            // frontier to this id acknowledges the gap beneath
                            // it, and the sender will never send it again.
                            log::warn!(target: "ism", "[ILM-INBOUND] Delivering msg_id={message_id} from peer {peer_id} out of order after {GAP_PATIENCE_SECS}s: the id beneath it never arrived");
                        } else {
                            log::debug!(target: "ism", "[ILM-INBOUND] Holding msg_id={message_id} from peer {peer_id}: waiting for the gap beneath it");
                            break;
                        }
                    }

                    match delivery.deliver(message.clone()).await {
                        Ok(()) => {
                            log::info!(target: "ism", "[ILM-INBOUND] Delivered msg_id={message_id} from peer {peer_id}");
                            self.tracker.has_delivered.insert((peer_id, message_id));

                            // BEFORE the ACK. The ACK says "everything up to
                            // here is delivered", and the frontier is what makes
                            // that true.
                            if let Err(e) = self.tracker.mark_delivered(peer_id, message_id).await {
                                log::error!(target: "ism", "Failed to advance the delivery frontier: {e:?}");
                            }

                            log::info!(target: "ism", "[ILM-ACK] Sending ACK for msg_id={message_id} to peer {peer_id}");
                            if let Err(e) = self
                                .send_message_internal(self.create_ack_message(&message))
                                .await
                            {
                                log::error!(target: "ism", "[ILM-ACK] FAILED to send ACK: {e:?}");
                            }

                            if let Err(e) = self
                                .backend
                                .clear_message_inbound(peer_id, message_id)
                                .await
                            {
                                log::error!(target: "ism", "Failed to clear delivered message: {e:?}");
                            }
                        }
                        Err(e) => {
                            // Left in the backend and the frontier not advanced,
                            // so the next cycle tries this same id again rather
                            // than stepping over it.
                            log::error!(target: "ism", "Failed to deliver message {message:?}: {e:?}");
                            break;
                        }
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
                            // Only up to the delivery frontier. Acknowledgement
                            // is cumulative, so an ACK for an id above it would
                            // retire the gap beneath it at the sender and lose
                            // whatever is still missing.
                            if self
                                .tracker
                                .safe_to_ack(&msg.source_id(), &msg.message_id())
                            {
                                log::warn!(target: "ism", "Received duplicate message, sending ACK");
                                if let Err(e) = self
                                    .send_message_internal(self.create_ack_message(&msg))
                                    .await
                                {
                                    log::error!(target: "ism", "Failed to send ACK for duplicate message: {e:?}");
                                }
                            } else {
                                log::debug!(target: "ism", "Duplicate msg_id={} from {} is above the delivery frontier; not ACKing", msg.message_id(), msg.source_id());
                            }
                            return;
                        }
                    }

                    // Store BEFORE marking received.
                    //
                    // This used to call `mark_received` first, which persists
                    // "(source, id) has arrived", and only then store the
                    // message — logging and dropping any store failure. The
                    // sender is not ACKed here, so it retransmits; the
                    // retransmission then matched the mark, took the duplicate
                    // branch below, and was ACKed. The sender cleared the
                    // message from its queue and the receiver never had it. One
                    // failed store meant permanent, silent loss with the sender
                    // shown "sent".
                    //
                    // Reading the tracker without recording lets the durable
                    // write happen first: if it fails, nothing claims the
                    // message arrived and the next retransmission tries again.
                    if self.tracker.has_received(msg.source_id(), msg.message_id()) {
                        // Received before -- but "received" and "delivered" are
                        // no longer the same thing. A message held behind a gap
                        // has been received and stored and must NOT be
                        // acknowledged: the ACK is cumulative and would retire
                        // the missing id beneath it.
                        if self
                            .tracker
                            .safe_to_ack(&msg.source_id(), &msg.message_id())
                        {
                            if let Err(e) = self
                                .send_message_internal(self.create_ack_message(&msg))
                                .await
                            {
                                log::error!(target: "ism", "Failed to send ACK for duplicate message: {e:?}");
                            }
                        } else {
                            // Nudge the inbound loop instead: the gap this one
                            // is waiting behind may have just been filled by the
                            // message that arrived before it.
                            log::debug!(target: "ism", "Held msg_id={} from {} re-arrived; re-examining the inbound queue", msg.message_id(), msg.source_id());
                            if self.poll_inbound_tx.send(()).is_err() {
                                log::warn!(target: "ism", "Failed to send poll signal for inbound messages");
                            }
                        }
                    } else {
                        let source_id = msg.source_id();
                        let message_id = msg.message_id();

                        if let Err(e) = self.backend.store_inbound(msg).await {
                            // Deliberately NOT marked received: leaving the
                            // sender un-ACKed is what makes this recoverable.
                            log::error!(target: "ism", "Failed to store inbound message, leaving it unacknowledged so the sender retries: {e:?}");
                        } else {
                            if let Err(e) = self.tracker.mark_received(source_id, message_id).await
                            {
                                log::error!(target: "ism", "Failed to mark message as received: {e:?}");
                            }

                            if let Err(e) = self
                                .tracker
                                .update_last_received_from(source_id, message_id)
                                .await
                            {
                                log::error!(target: "ism", "Failed to update last_received_from: {e:?}");
                            }

                            if self.poll_inbound_tx.send(()).is_err() {
                                log::warn!(target: "ism", "Failed to send poll signal for inbound messages");
                            }
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
