//! A peer that is not connected is announced once, not once per poll cycle.
//!
//! `process_outbound` runs every 200ms while anything is queued, and for a peer
//! absent from the transport it logged "is not connected" at `warn!` on every
//! one of them: five lines a second per absent peer for as long as it stayed
//! away, each a synchronous console write on the browser's main thread in the
//! WASM client. The queued messages must stay queued; only the narration stops.

mod support;

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{IlmOptions, ILM};
use std::time::Duration;
use support::log_capture::LogCapture;

const ALICE: usize = 1;
const BOB: usize = 2;

#[citadel_io::tokio::test]
async fn an_absent_peer_is_announced_once_however_long_it_stays_away() {
    let logs = LogCapture::install(log::LevelFilter::Debug);

    // Bob is never added to the network, so he is not a connected peer.
    let alice_wire = InMemoryNetwork::<TestMessage>::new().add_peer(ALICE).await;
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let alice = ILM::new(
        InMemoryBackend::<TestMessage>::new(),
        tx,
        alice_wire,
        IlmOptions::LEGACY,
    )
    .await
    .expect("construct Alice");

    alice.send_to(BOB, b"hello".to_vec()).await.expect("queue");
    citadel_io::tokio::time::sleep(Duration::from_secs(5)).await;

    let announced = logs.count(log::Level::Warn, "is not connected");
    assert_eq!(
        announced, 1,
        "an absent peer was announced {announced} times in five seconds; expected exactly one"
    );
}
