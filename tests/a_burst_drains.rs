//! Every message in a burst has to arrive, not the first few.
//!
//! CI's file-manager run recorded, for one link, in one run:
//!
//!   [ILM-BLOCKED]           936
//!   [ILM-SEND]              214
//!   [ILM-ACK-RECV]           24
//!   [ILM-DELIVER]            13
//!   [ILM-BLOCKED-RECOVERY]   93
//!
//! with eighteen messages sitting in one outbound group and the peer never
//! seeing the file that produced them. The outbound path sends the lowest
//! unsent id and BREAKS on the first message it cannot send, so a link that
//! stops acknowledging stops delivering entirely, and every message queued
//! behind the stuck one waits with it.
//!
//! This is the shape of that workload with nothing else wrong: two live peers,
//! one burst, no interference. If a burst does not drain here, the defect is in
//! the protocol rather than in the network it was measured on.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{MessageMetadata, ILM};
use std::collections::HashSet;
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;
const BURST: usize = 18;

#[citadel_io::tokio::test]
async fn every_message_in_a_burst_reaches_the_peer() {
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

    // Bounded, so a failure reports how far the burst got rather than hanging.
    let mut arrived: HashSet<usize> = HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
        "{} of {BURST} messages never arrived: {missing:?}",
        missing.len()
    );
}
