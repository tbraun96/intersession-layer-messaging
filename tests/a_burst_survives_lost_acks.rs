//! A link that loses most of its acknowledgements must still deliver.
//!
//! CI's file-manager run recorded 214 sends against 24 ACKs received on one
//! link, with 936 blocks, 93 recovery cycles, eighteen messages queued, and the
//! peer never seeing the file. The clean-wire burst drains in ten
//! milliseconds (`a_burst_drains`), so what that run measured is this: the
//! behaviour of the outbound path when acknowledgements go missing.
//!
//! The path sends the lowest unsent id and BREAKS on the first message it
//! cannot send, and `can_send` refuses a message whose id is not greater than
//! `last_sent` — so the message at the head cannot be retransmitted at all.
//! Retransmission happens only through the emergency branch, which clears
//! `last_sent` AND `last_acked` after ten consecutive blocks. Everything queued
//! behind the head waits for that.
//!
//! Four ACKs in five are dropped here. That is close to the ratio the run
//! measured, and it is a thing real networks do.

use async_trait::async_trait;
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport, ILM,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;
/// Sixty, which used to be impossible here.
///
/// This was six, with a note explaining why: "ordered delivery at this loss
/// rate costs a retransmit cycle per message, so the burst size sets the
/// runtime -- eighteen takes over a minute; six makes the same point in twenty
/// seconds." That was an accurate description of stop-and-wait. Six messages
/// took 16.3 seconds.
///
/// With `SEND_WINDOW` the cost is a round trip per WINDOW rather than per
/// message, and acknowledgement is cumulative, so one ACK surviving out of
/// eight retires the whole window. Sixty at the same loss rate now runs in
/// under a second, and the burst size no longer sets the runtime.
const BURST: usize = 60;
/// One ACK in this many survives.
const ACK_SURVIVES_EVERY: usize = 5;

/// Bob's wire, with most of his acknowledgements lost on the way out.
///
/// A dropped packet is not a send error, so this reports success: the sender
/// has no way to tell the difference, which is the situation being modelled.
#[derive(Clone)]
struct LossyAcks {
    inner: InMemoryNetwork<TestMessage>,
    seen: Arc<AtomicUsize>,
}

#[async_trait]
impl UnderlyingSessionTransport for LossyAcks {
    type Message = TestMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.inner.next_message().await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>> {
        if matches!(message, Payload::Ack { .. }) {
            let n = self.seen.fetch_add(1, Ordering::SeqCst);
            if !n.is_multiple_of(ACK_SURVIVES_EVERY) {
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
async fn a_burst_still_drains_when_most_acks_are_lost() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
    let bob_wire = LossyAcks {
        inner: network.add_peer(BOB).await,
        seen: Arc::new(AtomicUsize::new(0)),
    };

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

    // Sixty seconds, because the outbound path is stop-and-wait and stays that
    // way: the receiver has no contiguity gate, so pipelining past a lost
    // message would deliver the ones behind it first, and revfs applies
    // operations in the order it receives them. Ordered delivery under this
    // much loss costs a retransmit cycle per message, and that is the price.
    //
    // The bound still discriminates the defect this test was written for.
    // Before retransmission existed, TEN OF THE EIGHTEEN never arrived at all
    // within it — the head could not be repeated, so the queue stopped dead and
    // stayed dead.
    let mut arrived: HashSet<usize> = HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
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
        "{} of {BURST} messages did not arrive with 4-in-5 ACKs lost: {missing:?}",
        missing.len()
    );
}
