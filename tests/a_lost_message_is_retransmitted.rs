//! A message the wire drops has to be sent again.
//!
//! `can_send` refuses any id that is not greater than `last_sent`, so the
//! normal route cannot repeat a message it has already sent — and the outbound
//! queue keeps that message until its ACK arrives. Before retransmission
//! existed, the only thing that ever re-sent it was the emergency branch that
//! fires after a long silence and throws away `last_acked` with it, so ordinary
//! packet loss was handled by a path built for a peer that had reconnected.
//!
//! Here the FIRST transmission of every message is dropped and nothing else is.
//! Only a retransmission can deliver anything at all.

use async_trait::async_trait;
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport, ILM,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use citadel_io::tokio::sync::Mutex;

const ALICE: usize = 1;
const BOB: usize = 2;
const BURST: usize = 6;

/// Alice's wire, dropping each message's first transmission exactly once.
#[derive(Clone)]
struct DropsFirstAttempt {
    inner: InMemoryNetwork<TestMessage>,
    dropped: Arc<Mutex<HashSet<usize>>>,
}

#[async_trait]
impl UnderlyingSessionTransport for DropsFirstAttempt {
    type Message = TestMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.inner.next_message().await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>> {
        if let Payload::Message(inner_message) = &message {
            let id = inner_message.message_id();
            // A dropped packet reports success: the sender cannot tell.
            if self.dropped.lock().await.insert(id) {
                return Ok(());
            }
        }
        self.inner.send_message(message).await
    }

    async fn connected_peers(&self) -> Vec<usize> {
        self.inner.connected_peers().await
    }

    fn local_id(&self) -> usize {
        self.inner.local_id()
    }
}

#[citadel_io::tokio::test]
async fn every_message_arrives_when_its_first_transmission_is_lost() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = DropsFirstAttempt {
        inner: network.add_peer(ALICE).await,
        dropped: Arc::new(Mutex::new(HashSet::new())),
    };
    let bob_wire = network.add_peer(BOB).await;

    let (alice_tx, _alice_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let (bob_tx, mut bob_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();

    let alice = ILM::new(InMemoryBackend::<TestMessage>::new(), alice_tx, alice_wire)
        .await
        .expect("construct Alice");
    let _bob = ILM::new(InMemoryBackend::<TestMessage>::new(), bob_tx, bob_wire)
        .await
        .expect("construct Bob");

    for n in 0..BURST {
        alice
            .send_to(BOB, format!("message {n}").into_bytes())
            .await
            .expect("queue the message");
    }

    let mut arrived: HashSet<usize> = HashSet::new();
    // Bounded so that only RETRANSMISSION can meet it.
    //
    // The emergency branch re-sends too — it clears `last_sent`, so the whole
    // window goes out again — but only after MAX_CONSECUTIVE_BLOCKS cycles at
    // the 200ms poll: ten seconds, measured at 10.4s. Retransmission at
    // RETRANSMIT_AFTER_BLOCKS is one second, measured at 5.0s. Both numbers are
    // multiples of the poll interval rather than of anything load-dependent.
    //
    // A deadline above ten seconds passes with the retransmission removed, and
    // would be asserting that this crate has an emergency path rather than that
    // it handles ordinary packet loss. Eight seconds sits between the two with
    // three seconds of headroom over the real path.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while arrived.len() < BURST && std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(500), bob_rx.recv()).await {
            Ok(Some(message)) => {
                arrived.insert(message.message_id());
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    let mut missing: Vec<usize> = (0..BURST).filter(|id| !arrived.contains(id)).collect();
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "{} of {BURST} messages did not arrive within the retransmit budget: {missing:?}",
        missing.len()
    );
}
