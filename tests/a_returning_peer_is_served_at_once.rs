//! Backoff must not outlive the outage that caused it.
//!
//! After enough failures the retry gap is at its maximum. When the peer comes
//! back, what was queued for it must go out promptly, not after the remainder
//! of a gap that was sized for a peer that was gone.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{IlmOptions, ILM};
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;

#[citadel_io::tokio::test]
async fn a_peer_that_returns_receives_what_was_queued_promptly() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
    drop(network.add_peer(BOB).await);

    let (alice_tx, _alice_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let alice = ILM::new(
        InMemoryBackend::<TestMessage>::new(),
        alice_tx,
        alice_wire,
        IlmOptions::LEGACY,
    )
    .await
    .expect("construct Alice");
    alice.send_to(BOB, b"queued".to_vec()).await.expect("queue");

    // Long enough for the retry gap to reach its ceiling.
    citadel_io::tokio::time::sleep(Duration::from_secs(8)).await;

    let bob_wire = network.add_peer(BOB).await;
    let (bob_tx, mut bob_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let _bob = ILM::new(
        InMemoryBackend::<TestMessage>::new(),
        bob_tx,
        bob_wire,
        IlmOptions::LEGACY,
    )
    .await
    .expect("construct Bob");

    let delivered = citadel_io::tokio::time::timeout(Duration::from_millis(1500), bob_rx.recv())
        .await
        .expect("the queued message was not delivered within 1.5s of the peer returning");
    assert!(delivered.is_some());
}
