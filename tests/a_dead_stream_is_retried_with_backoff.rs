//! A peer whose stream died is retried with growing gaps, and says so once.
//!
//! A peer can stay listed as connected while its stream is already dead: every
//! send then fails. The outbound loop retried the same send on every 200ms
//! cycle forever, logging an `error!` each time. The message must stay queued
//! (delivery semantics are unchanged); the retries must thin out.

mod support;

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{IlmOptions, ILM};
use std::time::Duration;
use support::log_capture::LogCapture;

const ALICE: usize = 1;
const BOB: usize = 2;

#[citadel_io::tokio::test]
async fn a_dead_stream_is_retried_with_growing_gaps_and_reported_once() {
    let logs = LogCapture::install(log::LevelFilter::Debug);

    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
    // Bob stays in the peer map but his receiver is dropped: listed as
    // connected, every send to him fails.
    drop(network.add_peer(BOB).await);

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

    let attempts = logs.count(log::Level::Info, "SENDING msg_id");
    let failures_reported = logs.count(log::Level::Error, "[ILM-SEND] FAILED");
    assert!(
        (2..=7).contains(&attempts),
        "{attempts} send attempts in five seconds: it must keep trying, but with growing gaps (25 without backoff)"
    );
    assert_eq!(
        failures_reported, 1,
        "the failure was reported {failures_reported} times; the transition is the news, once"
    );
}
