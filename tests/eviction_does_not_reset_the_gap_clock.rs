//! Evicting the oldest receipt is evicting the gap-patience clock.
//!
//! `received_messages` maps (peer, id) -> receipt time, and `held_too_long`
//! reads that time to decide when a message parked behind a gap has waited long
//! enough to be delivered out of order. `drop_lru_if_full` evicted the entry
//! with the OLDEST timestamp — which is exactly the message that has been
//! waiting longest, i.e. the one whose patience is about to run out.
//!
//! With the entry gone, `held_too_long` answers false (absent → false). The
//! sender retransmits, `mark_received` re-inserts with a FRESH timestamp, and
//! the clock starts over. At the map's ceiling — which is what sustained load
//! means — the gap never breaks and the conversation stops moving.
//!
//! The fix prefers a victim whose message has already been delivered, since its
//! timestamp is dead weight.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// `MAX_MAP_SIZE` in the crate. Kept as a literal here on purpose: if that
/// constant changes, this test should be re-read rather than silently follow.
const MAP_CEILING: usize = 1000;

/// The message stuck behind the gap. Given the OLDEST timestamp of all, so the
/// old "evict the oldest" rule is guaranteed to pick it.
const HELD_ID: usize = 500_000;
const HELD_RECEIVED_AT: u64 = 1;

async fn tracker_at_ceiling() -> ILM<
    TestMessage,
    InMemoryBackend<TestMessage>,
    tokio::sync::mpsc::UnboundedSender<TestMessage>,
    InMemoryNetwork<TestMessage>,
> {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend, tx, network).await.expect("ILM");

    let tracker = ilm.tracker_for_tests();

    // The held message: oldest receipt, and NOT delivered — the frontier below
    // stays under it.
    tracker
        .received_messages
        .insert((PEER, HELD_ID), HELD_RECEIVED_AT);

    // Fill past the ceiling with ids the frontier covers, i.e. delivered ones,
    // each with a newer timestamp than the held one.
    for id in 1..=(MAP_CEILING + 4) {
        tracker
            .received_messages
            .insert((PEER, id), 1_000 + id as u64);
    }
    // Everything at or below this has been delivered; HELD_ID is far above it.
    tracker.last_delivered.insert(PEER, MAP_CEILING + 4);

    ilm
}

#[citadel_io::tokio::test]
async fn the_held_message_keeps_its_clock() {
    let ilm = tracker_at_ceiling().await;
    let tracker = ilm.tracker_for_tests();

    assert!(
        tracker.received_messages.len() > MAP_CEILING,
        "the map must be over its ceiling, or the eviction under test never runs"
    );

    tracker.drop_lru_if_full();

    assert!(
        tracker.received_messages.len() <= MAP_CEILING,
        "the eviction did not bring the map back under its ceiling"
    );
    assert_eq!(
        tracker
            .received_messages
            .get(&(PEER, HELD_ID))
            .map(|entry| *entry),
        Some(HELD_RECEIVED_AT),
        "the message held behind the gap was evicted, or its clock was reset; \
         held_too_long now answers false and the gap will not break"
    );
    drop(ilm);
}

/// The opposite failure: sparing the held entry must not stop the eviction
/// doing its job, or the map grows without bound.
#[citadel_io::tokio::test]
async fn the_map_still_shrinks() {
    let ilm = tracker_at_ceiling().await;
    let tracker = ilm.tracker_for_tests();
    let before = tracker.received_messages.len();

    tracker.drop_lru_if_full();

    assert!(
        tracker.received_messages.len() < before,
        "nothing was evicted at all; the ceiling is not being enforced"
    );
    drop(ilm);
}
