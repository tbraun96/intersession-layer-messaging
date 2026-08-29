//! How long a burst takes, not only whether it arrives.
//!
//! `a_burst_drains` proves every message in a burst of eighteen reaches the
//! peer. It says nothing about when, and CI's file-manager run showed one side
//! holding `96 messages in 1 group` while the test it was serving timed out.
//!
//! A directory sync is a burst. If the per-message cost is a poll interval
//! rather than a round trip, ninety-six of them is twenty seconds of nothing
//! happening, and no assertion anywhere would notice.
//!
//! This measures it, on an in-memory network whose one-way cost is a channel
//! send. Whatever it reports is a floor: the protocol's own overhead with the
//! network taken out of the picture.
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{MessageMetadata, ILM};
use std::collections::HashSet;
use std::time::{Duration, Instant};

const ALICE: usize = 1;
const BOB: usize = 2;
const BURST: usize = 96;

/// Generous: this is a bound on a stall, not a performance target. The point is
/// to distinguish "round trip per message" from "poll interval per message" —
/// at 200ms per message a burst of 96 needs 19s and fails this; at a round trip
/// on an in-memory channel it needs milliseconds.
const BUDGET: Duration = Duration::from_secs(10);

#[citadel_io::tokio::test]
async fn a_burst_of_ninety_six_drains_in_round_trips_not_poll_intervals() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
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

    let started = Instant::now();
    let mut arrived: HashSet<usize> = HashSet::new();
    let deadline = started + Duration::from_secs(120);
    while arrived.len() < BURST && Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(500), bob_rx.recv()).await {
            Ok(Some(message)) => {
                arrived.insert(message.message_id());
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    let took = started.elapsed();

    assert_eq!(
        arrived.len(),
        BURST,
        "only {} of {BURST} arrived in {took:?}",
        arrived.len()
    );
    println!(
        "  {BURST} messages drained in {took:?} ({:?} per message)",
        took / BURST as u32
    );
    assert!(
        took < BUDGET,
        "a burst of {BURST} took {took:?} on an in-memory network. That is \
         {:?} per message, which is a poll interval rather than a round trip: \
         the sender is waiting on a timer for every single message.",
        took / BURST as u32
    );
}
