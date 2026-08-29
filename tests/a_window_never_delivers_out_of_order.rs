//! More than one message in flight must not cost ordering, and must not cost a
//! message.
//!
//! `SEND_WINDOW` lets up to eight messages be outstanding to one peer, which is
//! what turns a lossy link from 2.7 seconds per message into a round trip per
//! window. It also creates two hazards that stop-and-wait did not have, because
//! with one message in flight neither was reachable:
//!
//!   1. **Order.** 5 can arrive while 4 is still on the wire. revfs applies
//!      operations in the order it receives them, so a write landing before the
//!      create it belongs to is worse than a slow sync.
//!
//!   2. **Loss.** Acknowledgement is cumulative — `update_ack` keeps the
//!      maximum and discards anything lower — so an ACK for 5 tells the sender
//!      that 4 is done. Acknowledging out of order does not reorder anything;
//!      it deletes.
//!
//! The receiver answers both by delivering a peer's ids only in contiguous
//! order and acknowledging only what it has delivered. This drops the FIRST
//! message of a burst on its first transmission, so every message behind it is
//! in flight and deliverable while the one beneath them is missing — the exact
//! shape both hazards need.

use async_trait::async_trait;
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport, ILM,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;
const BURST: usize = 12;
/// Mid-stream, NOT the first message.
///
/// The window stays shut until a peer has acknowledged something, so dropping
/// id 0 puts exactly one message in flight and produces no gap at all — the
/// first version of this test did that, and passed with the contiguity gate
/// deliberately disabled. By id 3 the window is open and the seven behind it
/// are already on the wire.
const DROPPED: usize = 3;

/// Alice's wire, which swallows the first transmission of message id `DROPPED`.
///
/// Only the first: the retransmission gets through, which is what lets the run
/// behind it finally be delivered. A dropped packet is not a send error, so
/// this reports success — the sender cannot tell the difference, which is the
/// situation being modelled.
#[derive(Clone)]
struct SwallowsOneMessageOnce {
    inner: InMemoryNetwork<TestMessage>,
    swallowed: Arc<AtomicBool>,
}

#[async_trait]
impl UnderlyingSessionTransport for SwallowsOneMessageOnce {
    type Message = TestMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.inner.next_message().await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>> {
        if let Payload::Message(m) = &message {
            if m.message_id() == DROPPED && !self.swallowed.swap(true, Ordering::SeqCst) {
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
async fn a_gap_is_held_rather_than_delivered_past_or_acknowledged_away() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = SwallowsOneMessageOnce {
        inner: network.add_peer(ALICE).await,
        swallowed: Arc::new(AtomicBool::new(false)),
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

    let mut order: Vec<usize> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while order.len() < BURST && std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(500), bob_rx.recv()).await {
            Ok(Some(message)) => order.push(message.message_id()),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    // Not "all of them arrived" — the order they arrived in.
    let expected: Vec<usize> = (0..BURST).collect();
    assert_eq!(
        order, expected,
        "a peer's messages must be delivered in id order however they arrive on the wire"
    );
}

#[citadel_io::tokio::test]
async fn nothing_behind_the_gap_is_delivered_before_it_fills() {
    // The stronger reading of the same run: at no point may a later id be
    // delivered while a lower one is still missing. Checked as it happens
    // rather than at the end, so an out-of-order delivery that is later
    // corrected still fails.
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = SwallowsOneMessageOnce {
        inner: network.add_peer(ALICE).await,
        swallowed: Arc::new(AtomicBool::new(false)),
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

    let mut expecting: usize = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while expecting < BURST && std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(500), bob_rx.recv()).await {
            Ok(Some(message)) => {
                assert_eq!(
                    message.message_id(),
                    expecting,
                    "delivered id {} while {expecting} was still missing",
                    message.message_id()
                );
                expecting += 1;
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert_eq!(
        expecting, BURST,
        "only {expecting} of {BURST} were delivered"
    );
}
