use crate::{platform_timestamp_secs, Backend, BackendError, MessageMetadata, MAX_MAP_SIZE};
use dashmap::{DashMap, DashSet};
use num::One;
use std::sync::Arc;

pub struct MessageTracker<M: MessageMetadata, B: Backend<M>> {
    pub last_acked: DashMap<M::PeerId, M::MessageId>,
    pub last_sent: DashMap<M::PeerId, M::MessageId>,
    pub next_unique_id: DashMap<M::PeerId, M::MessageId>,
    pub received_messages: DashMap<(M::PeerId, M::MessageId), u64>,
    pub has_delivered: DashSet<(M::PeerId, M::MessageId)>,
    /// Tracks the highest message ID received FROM each peer (for resync)
    pub last_received_from: DashMap<M::PeerId, M::MessageId>,
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
            last_received_from: Default::default(),
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

    pub async fn update_ack(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
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

    pub fn can_send(&self, peer_id: &M::PeerId, msg_id: &M::MessageId) -> bool {
        let last_acked = self.last_acked.get(peer_id);
        let last_sent = self.last_sent.get(peer_id);

        match (last_acked, last_sent) {
            (None, None) => true,
            (None, Some(_)) => false, // Wait for first ACK
            (Some(last_acked), Some(last_sent)) => *msg_id > *last_acked && *msg_id > *last_sent,
            (Some(last_acked), None) => *msg_id > *last_acked,
        }
    }

    pub async fn mark_received(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<bool, BackendError<M>> {
        if self.received_messages.contains_key(&(peer_id, msg_id)) {
            return Ok(false);
        }

        if self.has_delivered.contains(&(peer_id, msg_id)) {
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
