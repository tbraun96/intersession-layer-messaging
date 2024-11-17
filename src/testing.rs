use crate::{
    Backend, BackendError, MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport,
};
use async_trait::async_trait;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// Example InMemoryBackend implementation
#[derive(Clone)]
pub struct InMemoryBackend<M: MessageMetadata> {
    outbound: Mailbox<M::PeerId, M::MessageId, M>,
    inbound: Mailbox<M::PeerId, M::MessageId, M>,
    random_dir: std::path::PathBuf,
}

type Mailbox<PeerId, MessageId, Message> = Arc<RwLock<HashMap<(PeerId, MessageId), Message>>>;

impl<M: MessageMetadata> InMemoryBackend<M> {
    pub fn new() -> Self {
        let random_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string() + "/");
        if let Err(err) = std::fs::create_dir_all(&random_dir) {
            log::error!(target: "ism", "Failed to create random directory: {err}");
        }

        Self {
            outbound: Arc::new(RwLock::new(HashMap::new())),
            inbound: Arc::new(RwLock::new(HashMap::new())),
            random_dir,
        }
    }
}

impl<M: MessageMetadata> Default for InMemoryBackend<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<M: MessageMetadata + Clone + Send + Sync + 'static> Backend<M> for InMemoryBackend<M> {
    async fn store_outbound(&self, message: M) -> Result<(), BackendError<M>> {
        let mut outbound = self.outbound.write().await;
        let (source_id, destination_id, message_id) = (
            message.source_id(),
            message.destination_id(),
            message.message_id(),
        );

        if outbound
            .insert((destination_id, message_id), message)
            .is_some()
        {
            log::warn!(target: "ism",
                "Overwriting existing message in outbound storage dest={}/id={}",
                source_id,
                message_id
            );
        }
        Ok(())
    }

    async fn store_inbound(&self, message: M) -> Result<(), BackendError<M>> {
        let mut inbound = self.inbound.write().await;
        let (source_id, message_id) = (message.source_id(), message.message_id());

        if inbound.insert((source_id, message_id), message).is_some() {
            log::warn!(target: "ism",
                "Overwriting existing message in inbound storage src={}/id={}",
                source_id,
                message_id
            );
        }
        Ok(())
    }

    async fn clear_message_inbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        // Try to remove from both outbound and inbound
        let mut inbound = self.inbound.write().await;
        if inbound.remove(&(peer_id, message_id)).is_none() {
            log::warn!(target: "ism",
                "Failed to clear message from inbound storage src={}/id={}",
                peer_id,
                message_id
            );
        }

        Ok(())
    }

    async fn clear_message_outbound(
        &self,
        peer_id: M::PeerId,
        message_id: M::MessageId,
    ) -> Result<(), BackendError<M>> {
        let mut outbound = self.outbound.write().await;
        if outbound.remove(&(peer_id, message_id)).is_none() {
            log::warn!(target: "ism",
                "Failed to clear message from outbound storage dest={}/id={}",
                peer_id,
                message_id
            );
        }

        Ok(())
    }

    async fn get_pending_outbound(&self) -> Result<Vec<M>, BackendError<M>> {
        let outbound = self.outbound.read().await;
        Ok(outbound.values().cloned().collect())
    }

    async fn get_pending_inbound(&self) -> Result<Vec<M>, BackendError<M>> {
        let inbound = self.inbound.read().await;
        Ok(inbound.values().cloned().collect())
    }

    async fn store_value(&self, key: &str, value: &[u8]) -> Result<(), BackendError<M>> {
        // Store the bytes to the temp directory + key.bin
        let path = self.random_dir.join(format!("{key}.bin"));
        std::fs::write(path, value).map_err(|err| BackendError::StorageError(err.to_string()))
    }

    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<M>> {
        // Load the bytes from the temp directory + key.bin
        let path = self.random_dir.join(format!("{key}.bin"));
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BackendError::StorageError(err.to_string())),
        }
    }
}

pub struct InMemoryNetwork<M: MessageMetadata> {
    messages: InMemoryMessageQueue<M::PeerId, M>,
    my_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Payload<M>>>>,
    my_id: M::PeerId,
}

pub type InMemoryMessageQueue<PeerId, M> =
    Arc<RwLock<HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<Payload<M>>>>>;

impl<M: MessageMetadata> Clone for InMemoryNetwork<M> {
    fn clone(&self) -> Self {
        Self {
            messages: self.messages.clone(),
            my_rx: self.my_rx.clone(),
            my_id: self.my_id,
        }
    }
}

impl<M: MessageMetadata> Default for InMemoryNetwork<M> {
    fn default() -> Self {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            messages: Arc::new(RwLock::new(Default::default())),
            my_rx: Arc::new(Mutex::new(rx)),
            my_id: M::PeerId::default(),
        }
    }
}

impl<M: MessageMetadata> InMemoryNetwork<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_peer(&self, id: M::PeerId) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.messages.write().await.insert(id, tx);
        Self {
            messages: self.messages.clone(),
            my_rx: Arc::new(Mutex::new(rx)),
            my_id: id,
        }
    }

    pub async fn send_to_peer(
        &self,
        id: M::PeerId,
        message: Payload<M>,
    ) -> Result<(), NetworkError<Payload<M>>> {
        if let Some(tx) = self.messages.read().await.get(&id) {
            tx.send(message).map_err(|err| NetworkError::SendFailed {
                reason: err.to_string(),
                message: err.0,
            })
        } else {
            Err(NetworkError::ConnectionError("Peer not found".into()))
        }
    }
}

#[async_trait]
impl<M: MessageMetadata> UnderlyingSessionTransport for InMemoryNetwork<M> {
    type Message = M;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.my_rx.lock().await.recv().await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<M>>> {
        match &message {
            Payload::Message(msg) => {
                let peer_id = msg.destination_id();
                self.send_to_peer(peer_id, message).await
            }
            Payload::Ack { to_id, .. } => self.send_to_peer(*to_id, message).await,
            Payload::Poll { to_id, .. } => self.send_to_peer(*to_id, message).await,
        }
    }

    async fn connected_peers(&self) -> Vec<<Self::Message as MessageMetadata>::PeerId> {
        self.messages
            .read()
            .await
            .keys()
            .cloned()
            .sorted()
            .collect()
    }

    fn local_id(&self) -> <Self::Message as MessageMetadata>::PeerId {
        self.my_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestMessage {
    source_id: usize,
    destination_id: usize,
    message_id: usize,
    contents: Vec<u8>,
}

impl MessageMetadata for TestMessage {
    type PeerId = usize;
    type MessageId = usize;
    type Contents = Vec<u8>;

    fn source_id(&self) -> Self::PeerId {
        self.source_id
    }

    fn destination_id(&self) -> Self::PeerId {
        self.destination_id
    }

    fn message_id(&self) -> Self::MessageId {
        self.message_id
    }

    fn contents(&self) -> &Self::Contents {
        &self.contents
    }

    fn construct_from_parts(
        source_id: Self::PeerId,
        destination_id: Self::PeerId,
        message_id: Self::MessageId,
        contents: impl Into<Self::Contents>,
    ) -> Self
    where
        Self: Sized,
    {
        Self {
            source_id,
            destination_id,
            message_id,
            contents: contents.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
    use crate::{Backend, BackendError, MessageMetadata, NetworkError, Payload, ILM};
    use async_trait::async_trait;
    use citadel_logging::setup_log;
    use futures::stream::FuturesOrdered;
    use futures::StreamExt;
    use std::collections::HashSet;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_two_peers_send_receive() {
        let network = InMemoryNetwork::<TestMessage>::new();
        let network1 = network.add_peer(1).await;
        let network2 = network.add_peer(2).await;

        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();

        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let messenger1 = ILM::new(backend1, tx1, network1).await.unwrap();
        let messenger2 = ILM::new(backend2, tx2, network2).await.unwrap();

        // Peer 1 sends a message to Peer 2
        messenger1.send_to(2, vec![1, 2, 3]).await.unwrap();

        // Peer 2 sends a message to Peer 1
        messenger2.send_to(1, vec![4, 5, 6]).await.unwrap();

        // Peer 1 receives the message from Peer 2
        let received_message1 = rx1.recv().await.unwrap();
        assert_eq!(received_message1.source_id(), 2);
        assert_eq!(received_message1.destination_id(), 1);
        assert_eq!(received_message1.contents(), &[4, 5, 6]);

        // Peer 2 receives the message from Peer 1
        let received_message2 = rx2.recv().await.unwrap();
        assert_eq!(received_message2.source_id(), 1);
        assert_eq!(received_message2.destination_id(), 2);
        assert_eq!(received_message2.contents(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn test_message_system_basic() {
        let mut peer_futures = FuturesOrdered::new();
        const NUM_PEERS: usize = 3;
        let network = InMemoryNetwork::<TestMessage>::new();

        for this_peer_id in 0..NUM_PEERS {
            let network = network.add_peer(this_peer_id).await;
            let backend = InMemoryBackend::<TestMessage>::default();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();

            let future = async move {
                let message_contents = vec![1, 2, 3];
                // Every peer starts by sending a message to every other peer except themself
                for destination_id in 0..NUM_PEERS {
                    if destination_id == this_peer_id {
                        continue;
                    }

                    message_system
                        .send_to(destination_id, message_contents.clone())
                        .await
                        .unwrap();
                }

                // Check if the message was received by the destination peer
                let mut received_messages = vec![];
                // Receive NUM_PEERS-1 messages
                for _ in 0..NUM_PEERS - 1 {
                    if let Some(received_message) = rx.recv().await {
                        received_messages.push(received_message);
                    }
                }

                // Assertions over all messages
                for received_message in received_messages {
                    assert_eq!(received_message.contents(), &message_contents);
                    assert_ne!(received_message.source_id(), this_peer_id);
                    assert_eq!(received_message.destination_id(), this_peer_id);
                }

                1usize
            };

            peer_futures.push_back(Box::pin(future) as Pin<Box<dyn Future<Output = usize>>>);
        }

        let sum = peer_futures
            .collect::<Vec<usize>>()
            .await
            .iter()
            .sum::<usize>();
        assert_eq!(sum, NUM_PEERS);
    }

    #[tokio::test]
    async fn test_send_message_to_self() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 1,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_raw_message(message).await;
        assert!(matches!(result, Err(NetworkError::SendFailed { .. })));
    }

    #[tokio::test]
    async fn test_send_message_with_mismatched_source_id() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();

        let message = TestMessage {
            source_id: 2, // Mismatched source ID
            destination_id: 1,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_raw_message(message).await;
        assert!(matches!(result, Err(NetworkError::SendFailed { .. })));
    }

    #[tokio::test]
    async fn test_handle_acks_properly() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        message_system
            .send_raw_message(message.clone())
            .await
            .unwrap();

        let ack = Payload::Ack {
            from_id: 2,
            to_id: 1,
            message_id: 1,
        };

        network.send_to_peer(1, ack).await.unwrap();

        // wait some time for internal state to update
        sleep(Duration::from_millis(500)).await;

        // Check if the message was cleared from the backend
        let pending_outbound = backend.get_pending_outbound().await.unwrap();
        assert!(pending_outbound.is_empty());
    }

    #[tokio::test]
    async fn test_message_ordering() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;

        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        let messages = vec![
            TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: 1,
                contents: vec![1],
            },
            TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: 0,
                contents: vec![0],
            },
        ];

        for message in messages {
            log::info!(target: "ism", "Sending message: {:?}", message);
            message_system.send_raw_message(message).await.unwrap();
        }

        // Check if the messages are received in order
        let mut received_messages = vec![];
        for _ in 0..2 {
            if let Some(received_message) = rx2.recv().await {
                received_messages.push(received_message);
            }
        }

        assert_eq!(received_messages[0].message_id, 0);
        assert_eq!(received_messages[1].message_id, 1);
        assert_eq!(received_messages[0].contents(), &[0]);
        assert_eq!(received_messages[1].contents(), &[1]);
    }

    #[tokio::test]
    async fn test_stop_message_system() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();

        message_system
            .is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1, 2, 3],
        };

        let result = message_system.send_raw_message(message).await;
        assert!(matches!(result, Err(NetworkError::SystemShutdown)));
    }

    #[tokio::test]
    async fn test_stress_send_large_number_of_messages() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend, tx, network.clone()).await.unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        const NUM_MESSAGES: u8 = 255;
        let mut messages = vec![];

        for i in 0..NUM_MESSAGES {
            let message = TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: i as _,
                contents: vec![i],
            };
            messages.push(message);
        }

        for message in messages {
            message_system.send_raw_message(message).await.unwrap();
        }

        let mut received_count = 0;
        while let Some(received_message) = rx2.recv().await {
            assert_eq!(received_message.contents(), &[received_count]);
            received_count += 1;
            if received_count == NUM_MESSAGES {
                break;
            }
        }

        assert_eq!(received_count, NUM_MESSAGES);
    }

    #[tokio::test]
    async fn test_invalid_peer_handling() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        // Try to send to non-existent peer
        let message = TestMessage {
            source_id: 1,
            destination_id: 999, // Non-existent peer
            message_id: 1,
            contents: vec![1],
        };

        // Message should be stored but fail to send
        message_system.send_raw_message(message).await.unwrap();

        // Wait for processing
        sleep(Duration::from_millis(500)).await;

        // The message should remain in the backend
        let pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_message_handling() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        // Send the same message twice
        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        message_system
            .send_raw_message(message.clone())
            .await
            .unwrap();
        message_system
            .send_raw_message(message.clone())
            .await
            .unwrap();

        // Wait for processing
        sleep(Duration::from_millis(500)).await;

        // Verify that the message has been overwritten.
        let pending = backend.get_pending_outbound().await.unwrap();
        assert!(pending.is_empty());

        // Verify only one message is received
        let received = rx2.recv().await;
        assert!(received.is_some());
        assert!(rx2.try_recv().is_err()); // No second message should be received
    }

    #[tokio::test]
    async fn test_message_system_shutdown_cleanup() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        // Send a message
        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        message_system.send_raw_message(message).await.unwrap();

        // Shutdown the system
        message_system
            .is_running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for cleanup
        sleep(Duration::from_millis(500)).await;

        // Verify local delivery is dropped
        let local_delivery = message_system.local_delivery.lock().await;
        assert!(local_delivery.is_none());
    }

    #[tokio::test]
    async fn test_message_persistence_until_delivery() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        // Send message
        message_system
            .send_raw_message(message.clone())
            .await
            .unwrap();

        // Verify message is in the backend
        let pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 1);

        // Wait some time
        sleep(Duration::from_millis(500)).await;

        // Message should still be in backend since no ACK was received
        let still_pending = backend.get_pending_outbound().await.unwrap();
        assert_eq!(still_pending.len(), 1);
        assert_eq!(still_pending[0].message_id(), message.message_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bidirectional_parallel_messaging() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1, tx1, network.clone()).await.unwrap();
        let message_system2 = ILM::new(backend2, tx2, network2.clone()).await.unwrap();

        // Peer 1 sends to Peer 2
        let message1 = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        // Peer 2 sends to Peer 1
        let message2 = TestMessage {
            source_id: 2,
            destination_id: 1,
            message_id: 1,
            contents: vec![2],
        };

        // Send messages in parallel using separate threads
        let send_handle1 = tokio::spawn({
            let message1 = message1.clone();
            async move { message_system1.send_raw_message(message1).await }
        });

        let send_handle2 = tokio::spawn({
            let message2 = message2.clone();
            async move { message_system2.send_raw_message(message2).await }
        });

        let res1 = send_handle1.await.unwrap();
        let res2 = send_handle2.await.unwrap();

        assert!(res1.is_ok());
        assert!(res2.is_ok());

        // Receive messages
        let received1 = rx1.recv().await.expect("Peer 1 should receive a message");
        let received2 = rx2.recv().await.expect("Peer 2 should receive a message");

        // Verify messages
        assert_eq!(received1.source_id(), 2);
        assert_eq!(received1.destination_id(), 1);
        assert_eq!(received1.contents(), &[2]);

        assert_eq!(received2.source_id(), 1);
        assert_eq!(received2.destination_id(), 2);
        assert_eq!(received2.contents(), &[1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bidirectional_messaging_stress() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1, tx1, network.clone()).await.unwrap();
        let message_system2 = ILM::new(backend2, tx2, network2.clone()).await.unwrap();

        const NUM_MESSAGES: u8 = 255;

        // Create tasks for peer 1 sending messages
        let send_task1 = tokio::spawn({
            async move {
                for i in 0..NUM_MESSAGES {
                    let message = TestMessage {
                        source_id: 1,
                        destination_id: 2,
                        message_id: i as _,
                        contents: vec![i],
                    };
                    message_system1.send_raw_message(message).await.unwrap();
                }
            }
        });

        // Create tasks for peer 2 sending messages
        let send_task2 = tokio::spawn({
            async move {
                for i in 0..NUM_MESSAGES {
                    let message = TestMessage {
                        source_id: 2,
                        destination_id: 1,
                        message_id: i as _,
                        contents: vec![i],
                    };
                    message_system2.send_raw_message(message).await.unwrap();
                }
            }
        });

        // Create tasks for receiving messages
        let receive_task1 = tokio::spawn(async move {
            let mut received = 0;
            while received < NUM_MESSAGES {
                if let Ok(Some(msg)) =
                    tokio::time::timeout(Duration::from_secs(5), rx1.recv()).await
                {
                    assert_eq!(msg.source_id(), 2);
                    assert_eq!(msg.destination_id(), 1);
                    assert_eq!(msg.contents(), &[received]);
                    received += 1;
                } else {
                    panic!("Timeout waiting for messages at peer 1");
                }
            }
        });

        let receive_task2 = tokio::spawn(async move {
            let mut received = 0;
            while received < NUM_MESSAGES {
                if let Ok(Some(msg)) =
                    tokio::time::timeout(Duration::from_secs(5), rx2.recv()).await
                {
                    assert_eq!(msg.source_id(), 1);
                    assert_eq!(msg.destination_id(), 2);
                    assert_eq!(msg.contents(), &[received]);
                    received += 1;
                } else {
                    panic!("Timeout waiting for messages at peer 2");
                }
            }
        });

        // Wait for all tasks to complete
        let results =
            futures::future::join_all(vec![send_task1, send_task2, receive_task1, receive_task2])
                .await;

        // Check if any task failed
        for result in results {
            result.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_message_system_shutdown_during_send() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        let is_running = message_system.is_running.clone();

        // Start sending messages
        let send_handle = tokio::spawn({
            async move {
                let mut results = Vec::new();
                for i in 0..10000 {
                    let message = TestMessage {
                        source_id: 1,
                        destination_id: 2,
                        message_id: i,
                        contents: vec![1],
                    };
                    let result = message_system.send_raw_message(message).await;
                    results.push(result);
                }
                results
            }
        });

        // Wait a bit then shutdown the system
        sleep(Duration::from_millis(10)).await;
        is_running.store(false, std::sync::atomic::Ordering::Relaxed);

        // Check results
        let results = send_handle.await.unwrap();
        assert!(results
            .iter()
            .any(|r| matches!(r, Err(NetworkError::SystemShutdown))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_empty_message_contents() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1, tx1, network.clone()).await.unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![], // Empty contents
        };

        message_system1
            .send_raw_message(message.clone())
            .await
            .unwrap();
        let received = rx2.recv().await.expect("Should receive message");
        assert!(received.contents().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_local_delivery_drop() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        // Drop local delivery
        {
            let mut local_delivery = message_system.local_delivery.lock().await;
            *local_delivery = None;
        }

        // Try to process inbound messages
        let message = TestMessage {
            source_id: 2,
            destination_id: 1,
            message_id: 1,
            contents: vec![1],
        };

        // Store message directly in backend
        backend.store_inbound(message).await.unwrap();

        // Wait some time to ensure processing cycle has run
        sleep(Duration::from_millis(500)).await;

        // Message should still be in backend since delivery failed
        let pending = backend.get_pending_inbound().await.unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_max_message_id() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1, tx1, network.clone()).await.unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: usize::MAX, // Maximum possible message ID
            contents: vec![1],
        };

        message_system1
            .send_raw_message(message.clone())
            .await
            .unwrap();
        let received = rx2.recv().await.expect("Should receive message");
        assert_eq!(received.message_id(), usize::MAX);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_backend_error_handling() {
        struct FailingBackend;

        #[async_trait]
        impl Backend<TestMessage> for FailingBackend {
            async fn store_outbound(
                &self,
                _: TestMessage,
            ) -> Result<(), BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn store_inbound(&self, _: TestMessage) -> Result<(), BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn clear_message_inbound(
                &self,
                _: usize,
                _: usize,
            ) -> Result<(), BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn clear_message_outbound(
                &self,
                _: usize,
                _: usize,
            ) -> Result<(), BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn get_pending_outbound(
                &self,
            ) -> Result<Vec<TestMessage>, BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }
            async fn get_pending_inbound(
                &self,
            ) -> Result<Vec<TestMessage>, BackendError<TestMessage>> {
                Err(BackendError::StorageError("Simulated failure".into()))
            }

            async fn store_value(
                &self,
                _key: &str,
                _value: &[u8],
            ) -> Result<(), BackendError<TestMessage>> {
                Ok(())
            }

            async fn load_value(
                &self,
                _key: &str,
            ) -> Result<Option<Vec<u8>>, BackendError<TestMessage>> {
                Ok(None)
            }
        }

        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(FailingBackend, tx, network.clone()).await.unwrap();

        let message = TestMessage {
            source_id: 1,
            destination_id: 2,
            message_id: 1,
            contents: vec![1],
        };

        let result = message_system.send_raw_message(message).await;
        assert!(matches!(
            result,
            Err(NetworkError::BackendError(BackendError::StorageError(_)))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_intersession_recovery() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
            .await
            .unwrap();

        // Send messages before peer 2 connects
        for i in 0..3 {
            let message = TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: i,
                contents: vec![i as u8],
            };
            message_system1.send_raw_message(message).await.unwrap();
        }

        // Verify messages are in the backend
        let pending = backend1.get_pending_outbound().await.unwrap();
        assert_eq!(pending.len(), 3);

        // Now create peer 2's message system - this should trigger the peer polling mechanism
        let network2 = network.add_peer(2).await;
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        // Should receive all messages in order due to polling
        for i in 0..3 {
            match tokio::time::timeout(Duration::from_secs(6), rx2.recv()).await {
                Ok(Some(received)) => {
                    assert_eq!(received.message_id(), i);
                    assert_eq!(received.contents(), &[i as u8]);
                }
                Ok(None) => panic!("Channel closed"),
                Err(_) => panic!("Timeout waiting for message {}", i),
            }
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

        let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
            .await
            .unwrap();
        let _message_system2 = ILM::new(backend2, tx2, network2).await.unwrap();

        // Send some messages
        for i in 0..3 {
            let message = TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: i,
                contents: vec![i as u8],
            };
            message_system1.send_raw_message(message).await.unwrap();
        }

        // Now start graceful shutdown with a reasonable timeout
        let shutdown_result = message_system1.shutdown(Duration::from_secs(1)).await;
        assert!(shutdown_result.is_ok());

        // Verify no pending messages in backend
        let pending = backend1.get_pending_outbound().await.unwrap();
        assert!(pending.is_empty());

        let mut received_messages = vec![];
        for _ in 0..3 {
            if let Some(received_message) = rx2.recv().await {
                received_messages.push(received_message.message_id);
            }
        }

        assert_eq!(received_messages, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_shutdown_timeout() {
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let backend = InMemoryBackend::<TestMessage>::default();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let message_system = ILM::new(backend.clone(), tx, network.clone())
            .await
            .unwrap();

        // Send a message to a non-existent peer (will never be delivered)
        let message = TestMessage {
            source_id: 1,
            destination_id: 999,
            message_id: 1,
            contents: vec![1],
        };
        message_system.send_raw_message(message).await.unwrap();

        // Try to shutdown with a very short timeout
        let shutdown_result = message_system.shutdown(Duration::from_millis(10)).await;
        assert!(matches!(
            shutdown_result,
            Err(NetworkError::ShutdownFailed(_))
        ));
    }

    #[tokio::test]
    async fn test_message_id_persistence_between_sessions() {
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;

        // First session
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

            let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            // Send messages with IDs 0,1,2
            for i in 0..3 {
                let message = TestMessage {
                    source_id: 1,
                    destination_id: 2,
                    message_id: i,
                    contents: vec![i as u8],
                };
                message_system1.send_raw_message(message).await.unwrap();
            }

            // Wait for messages to be received and ACKed
            for _ in 0..3 {
                rx2.recv().await.expect("Should receive message");
            }
            sleep(Duration::from_millis(100)).await;
        }

        // Second session - should continue from where first session left off
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

            let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            // Send new message - should use ID 3
            let message = TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: 3,
                contents: vec![3],
            };
            message_system1.send_raw_message(message).await.unwrap();

            // Verify message is received with correct ID
            let received = rx2.recv().await.expect("Should receive message");
            assert_eq!(received.message_id(), 3);
        }
    }

    #[tokio::test]
    async fn test_ack_state_persistence_between_sessions() {
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;

        // First session - send messages but don't wait for ACKs
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();

            let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            let message = TestMessage {
                source_id: 1,
                destination_id: 2,
                message_id: 0,
                contents: vec![0],
            };
            message_system1.send_raw_message(message).await.unwrap();
        }

        // Second session - verify message is still pending
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();

            let _message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            // Check pending messages
            let pending = backend1.get_pending_outbound().await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].message_id(), 0);
        }
    }

    #[tokio::test]
    async fn test_crash_recovery_with_partial_state() {
        setup_log();
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let backend2 = InMemoryBackend::<TestMessage>::default();
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let network2 = network.add_peer(2).await;

        // First session - simulate crash during message sending
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();

            let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            // Send multiple messages
            for i in 0..5 {
                message_system1.send_to(2, vec![i as u8]).await.unwrap();
            }

            // Simulate immediate crash
        }

        // Give some time for any in-flight operations to complete
        sleep(Duration::from_millis(1000)).await;

        // Second session - should recover and continue properly
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();

            let _message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();
            let _message_system2 = ILM::new(backend2.clone(), tx2, network2.clone())
                .await
                .unwrap();

            log::warn!(target: "ism", "Message system 1 backend outbound: {:?}", backend1.get_pending_outbound().await.unwrap());
            log::warn!(target: "ism", "Message system 2 backend outbound: {:?}", backend2.get_pending_outbound().await.unwrap());

            log::warn!(target: "ism", "Message system 1 backend inbound: {:?}", backend1.get_pending_inbound().await.unwrap());
            log::warn!(target: "ism", "Message system 2 backend inbound: {:?}", backend2.get_pending_inbound().await.unwrap());

            let mut all_received = Vec::new();

            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(3000), rx2.recv()).await
            {
                all_received.push(msg.message_id());
            }

            log::info!(target: "ism", "All received: {:?}", all_received);
            assert_eq!(
                all_received,
                vec![0, 1, 2, 3, 4],
                "Expected to receive all messages exactly once"
            );
        }
    }

    #[tokio::test]
    async fn test_multiple_peer_state_persistence() {
        let backend1 = InMemoryBackend::<TestMessage>::default();
        let network = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
        let _network2 = network.add_peer(2).await;
        let _network3 = network.add_peer(3).await;

        // First session - send messages to multiple peers
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();

            // Send messages to both peer 2 and 3
            for peer_id in [2, 3] {
                let message = TestMessage {
                    source_id: 1,
                    destination_id: peer_id,
                    message_id: 0,
                    contents: vec![0],
                };
                message_system1.send_raw_message(message).await.unwrap();
            }
        }

        // Second session - verify state for both peers
        {
            let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
            let _message_system1 = ILM::new(backend1.clone(), tx1, network.clone())
                .await
                .unwrap();

            let pending = backend1.get_pending_outbound().await.unwrap();
            let pending_peers: HashSet<_> =
                pending.iter().map(|msg| msg.destination_id()).collect();
            assert_eq!(pending_peers.len(), 2);
            assert!(pending_peers.contains(&2));
            assert!(pending_peers.contains(&3));
        }
    }
}
