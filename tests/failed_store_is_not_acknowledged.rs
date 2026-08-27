//! A message we could not store must not be acknowledged as received.
//!
//! The receive path used to call `mark_received` FIRST — which persists
//! "(source, id) has arrived" — and only then store the message, logging and
//! dropping any store failure. No ACK is sent on that branch, so the sender
//! retransmits; but the retransmission then matched the mark, took the
//! duplicate branch, and WAS acked. The sender cleared the message from its
//! queue and the receiver never had it. One failed store meant permanent,
//! silent loss with the sender's UI showing "sent".
//!
//! Asserted through the wire, because the ACK is the thing that does the
//! damage: an ACK tells the sender it may stop retransmitting, so an ACK for a
//! message that was never stored is the exact moment the content is lost.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    Backend, BackendError, MessageMetadata, Payload, UnderlyingSessionTransport, ILM,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const LOCAL: usize = 0;
const ALICE: usize = 1;

/// Delegates everything, and fails `store_inbound` while armed.
#[derive(Clone)]
struct FlakyStore {
    inner: InMemoryBackend<TestMessage>,
    fail_inbound: Arc<AtomicBool>,
}

impl FlakyStore {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            fail_inbound: Arc::new(AtomicBool::new(false)),
        }
    }
    fn arm(&self) {
        self.fail_inbound.store(true, Ordering::SeqCst);
    }
    fn disarm(&self) {
        self.fail_inbound.store(false, Ordering::SeqCst);
    }
    /// Bypasses the injection, so a test can plant a message the ILM will read.
    async fn plant(&self, message: TestMessage) {
        self.inner.store_inbound(message).await.expect("plant");
    }
}

#[async_trait::async_trait]
impl Backend<TestMessage> for FlakyStore {
    async fn store_outbound(&self, message: TestMessage) -> Result<(), BackendError<TestMessage>> {
        self.inner.store_outbound(message).await
    }
    async fn store_inbound(&self, message: TestMessage) -> Result<(), BackendError<TestMessage>> {
        if self.fail_inbound.load(Ordering::SeqCst) {
            return Err(BackendError::StorageError("injected".to_string()));
        }
        self.inner.store_inbound(message).await
    }
    async fn clear_message_inbound(
        &self,
        peer_id: usize,
        message_id: usize,
    ) -> Result<(), BackendError<TestMessage>> {
        self.inner.clear_message_inbound(peer_id, message_id).await
    }
    async fn clear_message_outbound(
        &self,
        peer_id: usize,
        message_id: usize,
    ) -> Result<(), BackendError<TestMessage>> {
        self.inner.clear_message_outbound(peer_id, message_id).await
    }
    async fn get_pending_outbound(&self) -> Result<Vec<TestMessage>, BackendError<TestMessage>> {
        self.inner.get_pending_outbound().await
    }
    async fn get_pending_inbound(&self) -> Result<Vec<TestMessage>, BackendError<TestMessage>> {
        self.inner.get_pending_inbound().await
    }
    async fn store_value(&self, key: &str, value: &[u8]) -> Result<(), BackendError<TestMessage>> {
        self.inner.store_value(key, value).await
    }
    async fn load_value(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError<TestMessage>> {
        self.inner.load_value(key).await
    }
}

/// Waits for an ACK addressed to Alice, ignoring the polls that also flow.
async fn next_ack(alice: &InMemoryNetwork<TestMessage>, within: Duration) -> Option<usize> {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(250), alice.next_message())
            .await
        {
            Ok(Some(Payload::Ack { message_id, .. })) => return Some(message_id),
            Ok(Some(_)) => continue,
            Ok(None) => return None,
            Err(_) => continue,
        }
    }
    None
}

#[citadel_io::tokio::test]
async fn a_message_whose_store_failed_is_never_acknowledged() {
    let backend = FlakyStore::new();
    let message = TestMessage::construct_from_parts(ALICE, LOCAL, 0, b"must not be lost".to_vec());

    let network = InMemoryNetwork::<TestMessage>::new();
    let alice = network.add_peer(ALICE).await;
    let local = network.add_peer(LOCAL).await;

    let (tx, mut rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let _ilm = ILM::new(backend.clone(), tx, local)
        .await
        .expect("construct ILM");

    // Alice's delivery arrives while our storage is failing.
    backend.arm();
    alice
        .send_message(Payload::Message(message.clone()))
        .await
        .expect("alice sends");

    // It must not reach the application — there was nowhere to put it.
    assert!(
        citadel_io::tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .is_err(),
        "a message that could not be stored must not be delivered"
    );

    // And crucially it must NOT be acknowledged. An ACK here tells Alice she
    // may stop retransmitting, which is the moment the content is lost.
    assert_eq!(
        next_ack(&alice, Duration::from_secs(3)).await,
        None,
        "acknowledging a message we failed to store tells the sender to drop it"
    );

    // Storage recovers and Alice retransmits, as she will while un-acked.
    backend.disarm();
    alice
        .send_message(Payload::Message(message))
        .await
        .expect("alice retransmits");

    let delivered = citadel_io::tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("retransmission delivered within deadline")
        .expect("channel open");
    assert_eq!(delivered.message_id(), 0);
    assert_eq!(delivered.source_id(), ALICE);

    assert_eq!(
        next_ack(&alice, Duration::from_secs(10)).await,
        Some(0),
        "once stored, the message must be acknowledged"
    );
}
