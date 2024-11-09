use crate::{Backend, BackendError, MessageMetadata, MAX_MAP_SIZE};
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

pub struct MessageTracker<M: MessageMetadata, B: Backend<M>> {
    pub last_acked: DashMap<M::PeerId, M::MessageId>,
    pub last_sent: DashMap<M::PeerId, M::MessageId>,
    pub next_unique_id: DashMap<M::PeerId, M::MessageId>,
    pub received_messages: DashMap<(M::PeerId, M::MessageId), u64>,
    pub has_delivered: DashSet<(M::PeerId, M::MessageId)>,
    pub backend: Arc<B>,
}

impl<M, B> MessageTracker<M, B>
where
    M: MessageMetadata,
    B: Backend<M>,
{
    pub async fn new(backend: Arc<B>) -> Result<Self, BackendError> {
        let mut tracker = Self {
            last_acked: Default::default(),
            last_sent: Default::default(),
            next_unique_id: Default::default(),
            received_messages: Default::default(),
            has_delivered: Default::default(),
            backend,
        };

        // Load existing state
        if let Some(last_acked_bytes) = tracker.backend.load_value("last_acked").await? {
            if let Ok(map) = bincode2::deserialize(&last_acked_bytes) {
                tracker.last_acked = map;
            }
        }

        if let Some(last_sent_bytes) = tracker.backend.load_value("last_sent").await? {
            if let Ok(map) = bincode2::deserialize(&last_sent_bytes) {
                tracker.last_sent = map;
            }
        }

        if let Some(next_id_bytes) = tracker.backend.load_value("next_unique_id").await? {
            if let Ok(map) = bincode2::deserialize(&next_id_bytes) {
                tracker.next_unique_id = map;
            }
        }

        if let Some(received_bytes) = tracker.backend.load_value("received_messages").await? {
            if let Ok(map) = bincode2::deserialize(&received_bytes) {
                tracker.received_messages = map;
            }
        }

        Ok(tracker)
    }

    pub async fn update_ack(
        &self,
        peer_id: M::PeerId,
        msg_id: M::MessageId,
    ) -> Result<(), BackendError> {
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
    ) -> Result<(), BackendError> {
        self.last_sent.insert(peer_id, msg_id);
        self.backend
            .store_value("last_sent", &bincode2::serialize(&self.last_sent).unwrap())
            .await
    }

    pub async fn get_next_id(&self, peer_id: M::PeerId) -> Result<M::MessageId, BackendError> {
        let mut entry = self.next_unique_id.entry(peer_id).or_default();
        let current = *entry;
        *entry = current + 1;
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
    ) -> Result<bool, BackendError> {
        if self.received_messages.contains_key(&(peer_id, msg_id)) {
            return Ok(false);
        }

        if self.has_delivered.contains(&(peer_id, msg_id)) {
            return Ok(false);
        }

        let _ = self
            .received_messages
            .insert((peer_id, msg_id), UNIX_EPOCH.elapsed().unwrap().as_secs());
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

    // Sync all states to the backend
    pub async fn sync_backend(&self) -> Result<(), BackendError> {
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
        Ok(())
    }
}
