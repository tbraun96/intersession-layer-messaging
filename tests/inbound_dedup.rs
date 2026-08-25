//! Two peers can legitimately use the same message id, and both must arrive.
//!
//! `get_next_id` keeps a counter PER PEER, so every peer's ids start at zero
//! and collide across peers. The backend has always keyed inbound by
//! `(source_id, message_id)` and stores both; only the de-duplication in
//! `process_inbound` assumed ids were globally unique.
//!
//! Note what this test does and does not prove. It passes against the old
//! id-only de-duplication too, because the delivered message is cleared from
//! the backend and the one that lost the tie is picked up on the next poll —
//! so that was a one-cycle delay, not data loss. This asserts the invariant
//! that matters (both peers' messages reach the application) rather than the
//! mechanism, and it is the first test this crate has.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};
use std::collections::HashSet;
use std::time::Duration;

const LOCAL: usize = 0;
const ALICE: usize = 1;
const BOB: usize = 2;

#[citadel_io::tokio::test]
async fn messages_from_different_peers_sharing_an_id_are_both_delivered() {
    let backend = InMemoryBackend::<TestMessage>::new();

    // Same message_id (0), different senders — the exact shape produced by two
    // per-peer counters that both start at zero.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            ALICE,
            LOCAL,
            0,
            b"from alice".to_vec(),
        ))
        .await
        .expect("store alice");
    backend
        .store_inbound(TestMessage::construct_from_parts(
            BOB,
            LOCAL,
            0,
            b"from bob".to_vec(),
        ))
        .await
        .expect("store bob");

    let (tx, mut rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;

    let _ilm = ILM::new(backend, tx, network).await.expect("construct ILM");

    // Collect what is delivered, with a bound so a failure reports what DID
    // arrive rather than hanging.
    let mut sources = HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while sources.len() < 2 && std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(message)) => {
                sources.insert(message.source_id());
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(
        sources.contains(&ALICE) && sources.contains(&BOB),
        "both peers' messages should be delivered; got sources {sources:?}"
    );
}
