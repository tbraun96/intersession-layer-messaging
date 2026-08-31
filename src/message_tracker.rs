use crate::{platform_timestamp_secs, Backend, BackendError, MessageMetadata, MAX_MAP_SIZE};
use dashmap::DashMap;
use num::One;
use std::sync::Arc;

/// How many skipped ids ONE gap may contribute to the late-delivery set.
///
/// `get_next_id` numbers densely, so a real gap is small. A large one means a
/// defect or a hostile id. Deliberately well below `MAX_MAP_SIZE` so that no
/// single gap can evict most of the set it shares with every other peer.
const MAX_RECORDED_GAP: usize = 256;

pub struct MessageTracker<M: MessageMetadata, B: Backend<M>> {
    pub last_acked: DashMap<M::PeerId, M::MessageId>,
    pub last_sent: DashMap<M::PeerId, M::MessageId>,
    pub next_unique_id: DashMap<M::PeerId, M::MessageId>,
    pub received_messages: DashMap<(M::PeerId, M::MessageId), u64>,
    /// Messages already handed to the application, as a fast in-memory check.
    ///
    /// A redundant fast path, not the authority: the durable `last_delivered`
    /// frontier is what `safe_to_ack` consults, and the inbound path says so —
    /// "asked of the DURABLE frontier rather than the in-memory `has_delivered`
    /// set, because that set does not survive a restart". So evicting an entry
    /// costs at most a fallback to the check that was always correct.
    ///
    /// Bounded for that reason. It was insert-only: one entry per delivered
    /// message for the life of the process, which in a long-lived browser tab is
    /// every message ever received. Timestamped so `drop_lru_if_full` can evict
    /// the oldest, exactly as it does for `received_messages` and `skipped`.
    pub has_delivered: DashMap<(M::PeerId, M::MessageId), u64>,
    /// Tracks the highest message ID received FROM each peer (for resync)
    pub last_received_from: DashMap<M::PeerId, M::MessageId>,
    /// The highest id delivered from each peer with NO GAP beneath it.
    ///
    /// `last_received_from` is the highest id that ARRIVED, which is a
    /// different thing the moment more than one message is in flight: 5 can
    /// arrive while 4 is still on the wire. Acknowledgement is cumulative --
    /// `update_ack` keeps the maximum and discards anything lower -- so an ACK
    /// for 5 tells the sender that 4 is done too, and it will never send 4
    /// again. Acknowledging out of order is silent, permanent loss.
    ///
    /// This is the id it is safe to acknowledge up to.
    pub last_delivered: DashMap<M::PeerId, M::MessageId>,
    /// Ids the frontier was advanced PAST without ever delivering them.
    ///
    /// `held_too_long` breaks a permanent hold by delivering out of order, and
    /// that advances the frontier over the gap beneath it. The comment at that
    /// site calls it "a real loss", and it was: when the missing id finally
    /// arrived, `safe_to_ack` said it was already delivered, so it was re-ACKed
    /// and cleared and the application never saw it.
    ///
    /// It only has to be lost if it never comes. Recording what was skipped
    /// turns "permanently discarded" into "delivered late, out of order", which
    /// is the same order guarantee the gap-patience delivery already broke.
    ///
    /// In memory only, deliberately. The scenario this exists for is a stall
    /// that clears within the process; across a restart the durable frontier
    /// governs and a late arrival is discarded exactly as before -- no worse
    /// than today, and persisting it would put unbounded per-peer state on the
    /// durable path for a case that cannot be distinguished from a stale
    /// duplicate.
    ///
    /// Bounded two ways, because the entry for an id that never arrives is
    /// otherwise never removed -- and in the usual case it never does arrive,
    /// since the cumulative ACK for the out-of-order id already retired it at
    /// the sender. `MAX_RECORDED_GAP` caps what one gap may add;
    /// `drop_lru_if_full` caps the whole set at `MAX_MAP_SIZE`, evicting the
    /// oldest, which is why the value is the receipt timestamp.
    pub skipped: DashMap<(M::PeerId, M::MessageId), u64>,
    pub backend: Arc<B>,
}

impl<M, B> MessageTracker<M, B>
where
    M: MessageMetadata,
    B: Backend<M>,
{
    pub async fn new(backend: Arc<B>) -> Result<Self, BackendError<M>> {
        let mut tracker = Self {
            last_acked: Default::default(),
            last_sent: Default::default(),
            next_unique_id: Default::default(),
            received_messages: Default::default(),
            has_delivered: Default::default(),
            skipped: Default::default(),
            last_received_from: Default::default(),
            last_delivered: Default::default(),
            backend,
        };

        // Load existing state using batched request (single network roundtrip)
        // This avoids sequential await blocking in WASM which can freeze the UI
        let keys = [
            "last_acked",
            "last_sent",
            "next_unique_id",
            "received_messages",
            "last_received_from",
            "last_delivered",
        ];
        let results = tracker.backend.load_values_batched(&keys).await?;

        // Process results in order
        if let Some(Some(last_acked_bytes)) = results.first() {
            if let Ok(map) = bincode2::deserialize(last_acked_bytes) {
                tracker.last_acked = map;
            }
        }

        if let Some(Some(last_sent_bytes)) = results.get(1) {
            if let Ok(map) = bincode2::deserialize(last_sent_bytes) {
                tracker.last_sent = map;
            }
        }

        if let Some(Some(next_id_bytes)) = results.get(2) {
            if let Ok(map) = bincode2::deserialize(next_id_bytes) {
                tracker.next_unique_id = map;
            }
        }

        if let Some(Some(received_bytes)) = results.get(3) {
            if let Ok(map) = bincode2::deserialize(received_bytes) {
                tracker.received_messages = map;
            }
        }

        if let Some(Some(last_received_bytes)) = results.get(4) {
            if let Ok(map) = bincode2::deserialize(last_received_bytes) {
                tracker.last_received_from = map;
            }
        }

        if let Some(Some(last_delivered_bytes)) = results.get(5) {
            if let Ok(map) = bincode2::deserialize(last_delivered_bytes) {
                tracker.last_delivered = map;
            }
        }

        // A store written before this field existed has no `last_delivered`,
        // and the frontier decides whether an arriving message may be
        // delivered. Getting the seed wrong is not a stall, it is loss: the
        // frontier is also the highest id it is safe to acknowledge, and
        // acknowledgement is cumulative.
        //
        // `last_received_from` looks like the answer -- until this change one
        // message was in flight per peer, so what arrived was delivered in
        // order -- and it is wrong. RECEIVED is not DELIVERED. A message whose
        // local delivery failed (a closed channel, a full queue) stays pending
        // and is retried, while `last_received_from` has already moved past it.
        // Seeding from it claimed such a message as delivered and dropped it;
        // `test_hard_disconnect_queued_message_delivery` catches exactly that,
        // reporting [1, 2, 3, 4] where [0, 1, 2, 3, 4] was expected.
        //
        // So the seed is taken only where it is provably true: peers with
        // NOTHING still pending inbound. There, everything received was
        // delivered by definition. Where something is pending, the frontier is
        // left unset and the lowest pending id starts the run -- which is the
        // right answer for an upgrade, because under one-in-flight the lowest
        // undelivered id is exactly where the old build stopped.
        let still_pending = tracker
            .backend
            .get_pending_inbound()
            .await
            .unwrap_or_default();
        let mut has_pending: std::collections::HashSet<M::PeerId> =
            std::collections::HashSet::new();
        for message in &still_pending {
            has_pending.insert(message.source_id());
        }
        for entry in tracker.last_received_from.iter() {
            if has_pending.contains(entry.key()) {
                continue;
            }
            tracker
                .last_delivered
                .entry(*entry.key())
                .or_insert(*entry.value());
        }

        // Handle reconnection inconsistency: if last_sent has entries but last_acked
        // doesn't have corresponding entries for those peers, clear last_sent.
        // This happens after hard disconnect when ACK was never received.
        // Without this fix, can_send() returns false forever for (None, Some(_)) case.
        let mut needs_persist = false;
        let peers_to_clear: Vec<_> = tracker
            .last_sent
            .iter()
            .filter(|entry| !tracker.last_acked.contains_key(entry.key()))
            .map(|entry| *entry.key())
            .collect();

        for peer_id in peers_to_clear {
            log::info!(target: "ism", "[RESYNC-INIT] Clearing stale last_sent for peer {:?} (no corresponding ACK)", peer_id);
            tracker.last_sent.remove(&peer_id);
            needs_persist = true;
        }

        // Validate next_unique_id is not behind last_sent or last_acked.
        // This prevents message ID conflicts when state is partially loaded
        // (e.g., next_unique_id wasn't persisted but last_sent/last_acked was).
        for entry in tracker.last_sent.iter() {
            let peer_id = *entry.key();
            let last_sent_id = *entry.value();
            let min_next = last_sent_id + M::MessageId::one();

            let current_next = tracker.next_unique_id.get(&peer_id).map(|v| *v);
            if current_next.is_none() || current_next.unwrap() < min_next {
                log::info!(target: "ism", "[RESYNC-INIT] Updating next_unique_id[{:?}] from {:?} to {:?}",
                    peer_id, current_next, min_next);
                tracker.next_unique_id.insert(peer_id, min_next);
                needs_persist = true;
            }
        }

        // Also check against last_acked (in case last_sent was cleared but last_acked remains)
        for entry in tracker.last_acked.iter() {
            let peer_id = *entry.key();
            let last_acked_id = *entry.value();
            let min_next = last_acked_id + M::MessageId::one();

            let current_next = tracker.next_unique_id.get(&peer_id).map(|v| *v);
            if current_next.is_none() || current_next.unwrap() < min_next {
                log::info!(target: "ism", "[RESYNC-INIT] Updating next_unique_id[{:?}] from {:?} to {:?} (based on last_acked)",
                    peer_id, current_next, min_next);
                tracker.next_unique_id.insert(peer_id, min_next);
                needs_persist = true;
            }
        }

        if needs_persist {
            // Persist all modified state
            tracker
                .backend
                .store_value(
                    "last_sent",
                    &bincode2::serialize(&tracker.last_sent).unwrap(),
                )
                .await?;
            tracker
                .backend
                .store_value(
                    "next_unique_id",
                    &bincode2::serialize(&tracker.next_unique_id).unwrap(),
                )
                .await?;
        }

        Ok(tracker)
    }

    /// Records an acknowledgement, monotonically.
    ///
    /// `last_acked` is a high-water mark, and an ACK for an OLDER id must never
    /// move it backwards. Duplicate and out-of-order ACKs are normal traffic --
    /// a retransmission is acknowledged again, and nothing on this path
    /// promises ordering -- so an unconditional insert lets a late ACK re-open
    /// a window the sender has already closed, and `can_send` then re-sends
    /// messages the peer has long since received.
    ///
    /// This was always latent; re-ACKing duplicates made it routine rather
    /// than rare, which is how it surfaced.
    pub async fn update_ack(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        // The Ref is confined to this statement: holding a dashmap read guard
        // across the insert below would deadlock.
        let stale = self
            .last_acked
            .get(&peer_id)
            .map(|current| msg_id <= *current)
            .unwrap_or(false);
        if stale {
            return Ok(());
        }
        self.last_acked.insert(peer_id, msg_id);
        self.backend
            .store_value(
                "last_acked",
                &bincode2::serialize(&self.last_acked).unwrap(),
            )
            .await
    }

    pub async fn mark_sent(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        self.last_sent.insert(peer_id, msg_id);
        self.backend
            .store_value("last_sent", &bincode2::serialize(&self.last_sent).unwrap())
            .await
    }

    pub async fn get_next_id(&self, peer_id: M::PeerId) -> Result<M::MessageId, BackendError<M>> {
        let mut entry = self.next_unique_id.entry(peer_id).or_default();
        let current = *entry;
        *entry = current + M::MessageId::one();
        drop(entry);
        self.backend
            .store_value(
                "next_unique_id",
                &bincode2::serialize(&self.next_unique_id).unwrap(),
            )
            .await?;
        Ok(current)
    }

    /// Give back an id that was minted but never entered the outbound queue.
    ///
    /// `get_next_id` advances the counter and persists it BEFORE the caller
    /// knows whether the message will actually be stored. Every failure after
    /// that point -- backpressure refusal, a failed `store_outbound`, a
    /// rejected source/destination -- left a number that the receiver would
    /// wait for and never see. Ids are consumed in order, so the next real
    /// message arrives as `n+1` with `n` missing: the receiver holds it behind
    /// a gap for GAP_PATIENCE_SECS before giving up and delivering out of
    /// order. One refused send therefore costs twenty seconds of stalled
    /// delivery for that peer.
    ///
    /// Only the id at the head of the counter can be returned. If something
    /// else minted in between, `next_unique_id` has already moved past it and
    /// rewinding would hand the same number out twice -- a far worse failure
    /// than the gap. In that case this is a no-op and the gap stands.
    pub async fn release_unused_id(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        let released = {
            let mut entry = self.next_unique_id.entry(peer_id).or_default();
            // The compare and the set happen under the same entry guard, so a
            // concurrent `get_next_id` cannot slip between them.
            if *entry == message_id + M::MessageId::one() {
                *entry = message_id;
                true
            } else {
                false
            }
        };

        if !released {
            log::debug!(target: "ism", "Not reclaiming id {message_id} for peer {peer_id}: another send has already minted past it");
            return Ok(());
        }

        self.backend
            .store_value(
                "next_unique_id",
                &bincode2::serialize(&self.next_unique_id).unwrap(),
            )
            .await
    }

    pub fn can_send(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) -> bool {
        let last_acked = self.last_acked.get(peer_id);
        let last_sent = self.last_sent.get(peer_id);

        match (last_acked, last_sent) {
            (None, None) => true,
            (None, Some(last_sent)) => *msg_id > *last_sent, // Allow newer messages even without first ACK
            (Some(last_acked), Some(last_sent)) => *msg_id > *last_acked && *msg_id > *last_sent,
            (Some(last_acked), None) => *msg_id > *last_acked,
        }
    }

    /// Whether this message is the next one deliverable from that peer.
    ///
    /// Delivery has to be gapless because acknowledgement is cumulative: an ACK
    /// for 5 retires 1..5 at the sender. Delivering 5 while 4 is still missing
    /// would acknowledge 4 and lose it.
    ///
    /// `None` for a peer means nothing has been delivered yet, and then the
    /// lowest id waiting from that peer starts the run — `first_pending`.
    ///
    /// It cannot guess zero. `send_raw_message` lets a caller choose the id, and
    /// a sender resuming with a persisted `next_unique_id` against a receiver
    /// whose store was cleared begins wherever it left off. Demanding zero
    /// would hold every such message for ever.
    ///
    /// Accepting the lowest pending is only safe because a sender may not open
    /// its window until it has an acknowledgement from this peer — see
    /// SEND_WINDOW. Until then exactly one message is in flight, so "the lowest
    /// pending" and "the start of the run" are the same id and no gap can hide
    /// beneath it. Were the window open first, losing the true first message
    /// would make the second one look like the start, and acknowledging it
    /// would cumulatively retire the one that was lost.
    pub fn is_next_deliverable(
        &self,
        peer_id: &M::PeerId,
        msg_id: &M::MessageId,
        first_pending: bool,
    ) -> bool {
        match self.last_delivered.get(peer_id) {
            Some(frontier) => *msg_id == *frontier + M::MessageId::one(),
            None => first_pending,
        }
    }

    /// Whether a held message has been waiting longer than the gap beneath it
    /// can reasonably take to arrive.
    ///
    /// A permanent hold is a worse failure than an out-of-order delivery, and
    /// nothing else can break one: the sender retransmits its head and, after
    /// `MAX_CONSECUTIVE_BLOCKS`, clears its state and starts the same queue
    /// again -- so if the missing id genuinely cannot be produced, both sides
    /// wait for ever.
    ///
    /// `get_next_id` numbers messages densely per peer, so this should never
    /// fire for a stream built with `send_to`. It exists because
    /// `send_raw_message` is public and takes whatever id it is given, and
    /// because a future defect that opens a gap should cost a warning and a
    /// delay rather than a conversation that never moves again.
    ///
    /// Uses the receipt timestamp `mark_received` already records, so nothing
    /// new has to be tracked.
    pub fn held_too_long(
        &self,
        peer_id: &M::PeerId,
        msg_id: &M::MessageId,
        patience_secs: u64,
    ) -> bool {
        match self.received_messages.get(&(*peer_id, *msg_id)) {
            Some(received_at) => {
                platform_timestamp_secs().saturating_sub(*received_at) >= patience_secs
            }
            None => false,
        }
    }

    /// Whether acknowledging this id would claim anything not yet delivered.
    ///
    /// The duplicate-ACK paths need this as much as the delivery path does: a
    /// message that arrives twice while a lower one is missing must not be
    /// acknowledged just because it is a duplicate.
    pub fn safe_to_ack(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) -> bool {
        match self.last_delivered.get(peer_id) {
            Some(frontier) => *msg_id <= *frontier,
            None => false,
        }
    }

    /// Remember every id the frontier is about to skip over.
    ///
    /// Call BEFORE the out-of-order delivery advances the frontier, so the
    /// current frontier still marks the bottom of the gap.
    ///
    /// Bounded, and the bound TRUNCATES rather than refuses: the first
    /// `MAX_RECORDED_GAP` ids of the gap are recorded and any beyond that are
    /// not, so they stay permanently lost exactly as they were before this
    /// mechanism existed. Truncating beats refusing -- recording nothing for a
    /// wide gap would throw away the recoverable head of it too -- but the
    /// ceiling is real and the warning below names what it costs.
    pub fn record_skipped_gap(&self, peer_id: &M::PeerId, delivering: &M::MessageId) {
        let Some(frontier) = self.last_delivered.get(peer_id).map(|f| *f) else {
            return;
        };

        let now = platform_timestamp_secs();
        let mut id = frontier + M::MessageId::one();
        let mut recorded: usize = 0;
        while id < *delivering {
            if recorded >= MAX_RECORDED_GAP {
                log::warn!(target: "ism", "[ILM-INBOUND] Gap beneath {delivering:?} from peer {peer_id:?} is wider than {MAX_RECORDED_GAP}; recorded the first {recorded} id(s), the rest CANNOT be delivered late and are lost");
                break;
            }
            self.skipped.insert((*peer_id, id), now);
            recorded += 1;
            id = id + M::MessageId::one();
        }

        if recorded > 0 {
            log::warn!(target: "ism", "[ILM-INBOUND] Skipped {recorded} id(s) beneath {delivering:?} from peer {peer_id:?}; they will be delivered late if they arrive");
            self.drop_lru_if_full();
        }
    }

    /// Whether this id was skipped by a gap-patience delivery and never seen.
    pub fn was_skipped(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) -> bool {
        self.skipped.contains_key(&(*peer_id, *msg_id))
    }

    /// Forget a skipped id, once it has arrived and been delivered late.
    pub fn clear_skipped(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) {
        self.skipped.remove(&(*peer_id, *msg_id));
    }

    /// Record that everything up to and including `msg_id` has been delivered.
    pub async fn mark_delivered(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        let stale = self
            .last_delivered
            .get(&peer_id)
            .map(|current| msg_id <= *current)
            .unwrap_or(false);
        if stale {
            return Ok(());
        }
        self.last_delivered.insert(peer_id, msg_id);
        self.backend
            .store_value(
                "last_delivered",
                &bincode2::serialize(&self.last_delivered).unwrap(),
            )
            .await
    }

    /// Have we already accepted this message, without recording anything?
    ///
    /// `mark_received` answers the same question but records the answer as a
    /// side effect, which forces callers to commit to "received" BEFORE they
    /// have stored the message. Separating the read lets the store happen first,
    /// so a failed store leaves nothing claiming the message arrived.
    pub fn has_received(&self, peer_id: M::PeerId, msg_id: M::MessageId) -> bool {
        self.received_messages.contains_key(&(peer_id, msg_id))
            || self.has_delivered.contains_key(&(peer_id, msg_id))
    }

    pub async fn mark_received(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<bool, BackendError<M>> {
        if self.received_messages.contains_key(&(peer_id, msg_id)) {
            return Ok(false);
        }

        if self.has_delivered.contains_key(&(peer_id, msg_id)) {
            return Ok(false);
        }

        let _ = self
            .received_messages
            .insert((peer_id, msg_id), platform_timestamp_secs());
        self.drop_lru_if_full();
        self.backend
            .store_value(
                "received_messages",
                &bincode2::serialize(&self.received_messages).unwrap(),
            )
            .await?;
        Ok(true)
    }

    pub fn drop_lru_if_full(&self) {
        if self.received_messages.len() > MAX_MAP_SIZE {
            // Remove the oldest message. The value is the time since the unix epoch
            let oldest = self.received_messages.iter().min_by_key(|v| *v.value());
            if let Some(oldest) = oldest {
                let _ = self.received_messages.remove(oldest.key());
            }
        }
        // Same ceiling for the delivered set. It was insert-only, so a tab that
        // stayed open accumulated one entry per message ever received; the
        // durable frontier is the authority, so an eviction costs a fallback to
        // the check that was always correct.
        while self.has_delivered.len() > MAX_MAP_SIZE {
            let oldest = self
                .has_delivered
                .iter()
                .min_by_key(|v| *v.value())
                .map(|v| *v.key());
            match oldest {
                Some(key) => {
                    let _ = self.has_delivered.remove(&key);
                }
                None => break,
            }
        }
        // `skipped` needs this more than `received_messages` does: an id that
        // never arrives is never cleared by delivery, and the usual outcome of
        // a gap-patience skip is that it never arrives.
        while self.skipped.len() > MAX_MAP_SIZE {
            let oldest = self
                .skipped
                .iter()
                .min_by_key(|v| *v.value())
                .map(|v| *v.key());
            match oldest {
                Some(key) => {
                    let _ = self.skipped.remove(&key);
                }
                None => break,
            }
        }
    }

    /// Clears the last_sent entry for a peer, allowing messages to be resent.
    /// This is used during resync when we detect that a peer never received our message.
    pub async fn clear_last_sent(&self, peer_id: &M::PeerId) -> Result<(), BackendError<M>> {
        if self.last_sent.remove(peer_id).is_some() {
            log::info!(target: "ism", "[RESYNC] Cleared last_sent for peer {:?}, allowing resend", peer_id);
            self.backend
                .store_value("last_sent", &bincode2::serialize(&self.last_sent).unwrap())
                .await?;
        }
        Ok(())
    }

    /// Gets the last message ID received from a specific peer (for resync)
    pub fn get_last_received_from(&self, peer_id: &M::PeerId) -> Option<M::MessageId> {
        self.last_received_from.get(peer_id).map(|v| *v)
    }

    /// Updates the last received message ID from a peer
    pub async fn update_last_received_from(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        // Only update if this is a higher message ID
        let should_update = match self.last_received_from.get(&peer_id) {
            Some(current) => msg_id > *current,
            None => true,
        };

        if should_update {
            self.last_received_from.insert(peer_id, msg_id);
            self.backend
                .store_value(
                    "last_received_from",
                    &bincode2::serialize(&self.last_received_from).unwrap(),
                )
                .await?;
        }
        Ok(())
    }

    // Sync all states to the backend
    pub async fn sync_backend(&self) -> Result<(), BackendError<M>> {
        self.backend
            .store_value(
                "last_acked",
                &bincode2::serialize(&self.last_acked).unwrap(),
            )
            .await?;
        self.backend
            .store_value("last_sent", &bincode2::serialize(&self.last_sent).unwrap())
            .await?;
        self.backend
            .store_value(
                "next_unique_id",
                &bincode2::serialize(&self.next_unique_id).unwrap(),
            )
            .await?;
        self.backend
            .store_value(
                "received_messages",
                &bincode2::serialize(&self.received_messages).unwrap(),
            )
            .await?;
        self.backend
            .store_value(
                "last_received_from",
                &bincode2::serialize(&self.last_received_from).unwrap(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::MessageTracker;
    use crate::testing::{InMemoryBackend, TestMessage};
    use std::sync::Arc;

    const ALICE: usize = 1;

    async fn tracker() -> MessageTracker<TestMessage, InMemoryBackend<TestMessage>> {
        MessageTracker::new(Arc::new(InMemoryBackend::<TestMessage>::new()))
            .await
            .expect("tracker")
    }

    /// A late ACK for an older id must not move the high-water mark backwards.
    ///
    /// This is the hazard re-ACKing duplicates introduces: every retransmission
    /// now produces an acknowledgement, so an old ACK arriving after a newer
    /// one stops being rare. `can_send` gates on `msg_id > last_acked`, so a
    /// regressed mark re-opens a window the sender has already closed and it
    /// re-sends messages the peer received long ago.
    #[citadel_io::tokio::test]
    async fn an_out_of_order_ack_does_not_regress_the_high_water_mark() {
        let tracker = tracker().await;

        tracker.update_ack(ALICE, 5).await.expect("ack 5");
        tracker
            .update_ack(ALICE, 2)
            .await
            .expect("the late duplicate");

        assert!(
            !tracker.can_send(&ALICE, &3),
            "a stale ACK must not make an already-acknowledged id sendable again"
        );
        assert!(
            tracker.can_send(&ALICE, &6),
            "ids beyond the mark must still be sendable"
        );
    }

    /// The guard must not block genuine progress.
    #[citadel_io::tokio::test]
    async fn a_newer_ack_still_advances_the_mark() {
        let tracker = tracker().await;

        tracker.update_ack(ALICE, 2).await.expect("ack 2");
        tracker.update_ack(ALICE, 5).await.expect("ack 5");

        assert!(!tracker.can_send(&ALICE, &5), "5 is acknowledged");
        assert!(tracker.can_send(&ALICE, &6), "6 is past the mark");
    }
}

#[cfg(all(test, feature = "testing"))]
mod bounded_maps_tests {
    use super::*;
    use crate::testing::{InMemoryBackend, TestMessage};

    const PEER: usize = 1;

    async fn tracker() -> MessageTracker<TestMessage, InMemoryBackend<TestMessage>> {
        MessageTracker::new(Arc::new(InMemoryBackend::<TestMessage>::new()))
            .await
            .expect("tracker")
    }

    /// `has_delivered` was insert-only: one entry per message, for the life of
    /// the process. In a browser tab that stays open for a working day that is
    /// every message ever received, in a set nothing removed from — while its
    /// two siblings in this struct, `received_messages` and `skipped`, are both
    /// capped by `drop_lru_if_full`.
    ///
    /// Evicting is safe because the set is a fast path, not the authority: the
    /// durable `last_delivered` frontier is what `safe_to_ack` consults, and the
    /// inbound path says so — "asked of the DURABLE frontier rather than the
    /// in-memory `has_delivered` set, because that set does not survive a
    /// restart". An eviction costs a fallback to the check that was always
    /// correct.
    #[citadel_io::tokio::test]
    async fn the_delivered_set_is_capped() {
        let tracker = tracker().await;

        for id in 0..(MAX_MAP_SIZE * 3) {
            tracker.has_delivered.insert((PEER, id), id as u64);
            tracker.drop_lru_if_full();
        }

        assert!(
            tracker.has_delivered.len() <= MAX_MAP_SIZE + 1,
            "the delivered set holds {} entries after {} messages; nothing bounds \
             it and a long-lived tab accumulates one per message ever received",
            tracker.has_delivered.len(),
            MAX_MAP_SIZE * 3
        );
    }

    /// The opposite failure: evicting the NEWEST would make the fast path
    /// useless exactly when it matters — a duplicate arriving close behind its
    /// original — and the cap assertion above cannot tell the two apart.
    #[citadel_io::tokio::test]
    async fn it_evicts_by_age_and_keeps_the_recent() {
        let tracker = tracker().await;

        for id in 0..(MAX_MAP_SIZE * 2) {
            tracker.has_delivered.insert((PEER, id), id as u64);
            tracker.drop_lru_if_full();
        }

        let newest = MAX_MAP_SIZE * 2 - 1;
        assert!(
            tracker.has_delivered.contains_key(&(PEER, newest)),
            "the most recently delivered id was evicted, so a duplicate arriving \
             straight after its original misses the fast path"
        );
        assert!(
            !tracker.has_delivered.contains_key(&(PEER, 0)),
            "the oldest id survived, so eviction is not by age"
        );
    }
}
