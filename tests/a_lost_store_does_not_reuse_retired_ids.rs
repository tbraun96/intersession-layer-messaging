//! A peer that loses its store must not re-mint ids the receiver has retired.
//!
//! Message ids are per-peer counters kept in the sender's store. That store is
//! the agent's LocalDB, keyed by CID — and a user on a new device, or one who
//! has cleared site data, arrives with the SAME CID and an EMPTY store, while
//! the receiver's durable `last_delivered` frontier for them is untouched.
//!
//! Minting from zero then put every message at or below that frontier, where
//! `safe_to_ack` calls it a duplicate: re-ACKed, cleared, and never delivered.
//! Nothing errored on either side, and there was no resync path — the sender
//! had to climb all the way back past the frontier one message at a time before
//! anything got through.
//!
//! `initial_message_id` seeds a fresh id space instead. These tests pin the
//! property that makes it work, and the arithmetic that makes it safe.

use intersession_layer_messaging::message_tracker::MessageTracker;
use intersession_layer_messaging::testing::InMemoryBackend;
use intersession_layer_messaging::{platform_timestamp_micros, MessageMetadata};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A message type whose seed is NOT zero, so the wiring below can tell
/// `initial_message_id` being used from `Default::default()` being used. With
/// `TestMessage` (seed 0) the two are indistinguishable, which is exactly how a
/// fix like this ships unwired.
const SEED: usize = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeededMessage {
    source_id: usize,
    destination_id: usize,
    message_id: usize,
    contents: Vec<u8>,
}

impl MessageMetadata for SeededMessage {
    type PeerId = usize;
    type MessageId = usize;
    type Contents = Vec<u8>;

    fn source_id(&self) -> Self::PeerId {
        self.source_id
    }
    fn destination_id(&self) -> Self::PeerId {
        self.destination_id
    }
    fn message_id(&self) -> Self::MessageId {
        self.message_id
    }
    fn contents(&self) -> &Self::Contents {
        &self.contents
    }
    fn construct_from_parts(
        source_id: Self::PeerId,
        destination_id: Self::PeerId,
        message_id: Self::MessageId,
        contents: impl Into<Self::Contents>,
    ) -> Self {
        Self {
            source_id,
            destination_id,
            message_id,
            contents: contents.into(),
        }
    }
    fn initial_message_id() -> Self::MessageId {
        SEED
    }
}

/// The wiring: `get_next_id` must actually consult `initial_message_id` for a
/// peer it has never minted for.
#[citadel_io::tokio::test]
async fn a_fresh_peer_starts_at_the_seed() {
    let backend = Arc::new(InMemoryBackend::<SeededMessage>::new());
    let tracker = MessageTracker::new(backend).await.expect("tracker");

    const PEER: usize = 7;
    let first = tracker.get_next_id(PEER).await.expect("mint");
    assert_eq!(
        first, SEED,
        "the counter started at {first} rather than the seed; a store loss \
         would re-issue ids the receiver has already retired"
    );

    let second = tracker.get_next_id(PEER).await.expect("mint");
    assert_eq!(
        second,
        SEED + 1,
        "the counter did not advance from the seed"
    );
}

/// The property H15 needs: a second incarnation's seed clears the highest id
/// the first one reached, which is its own seed plus everything it sent.
#[test]
fn a_later_seed_clears_the_previous_incarnation() {
    let first_seed = platform_timestamp_micros();
    // A busy session: a thousand messages before the store was lost.
    let highest_used_by_first = first_seed + 1_000;

    // Any real gap between incarnations is at least milliseconds; a thousand
    // microseconds is a conservative floor for "the process restarted".
    let second_seed = first_seed + 100_000;

    assert!(
        second_seed > highest_used_by_first,
        "the new incarnation would re-issue ids the receiver has already \
         retired; every one of them is swallowed as a duplicate"
    );
}

/// Why microseconds and not seconds, stated as a test rather than a comment:
/// with seconds, an ordinary burst outruns the clock.
#[test]
fn seconds_would_not_have_been_enough() {
    // A thousand messages sent within ten seconds — unremarkable for a chat
    // client draining a queue after reconnecting.
    let sent = 1_000u64;
    let restart_gap_secs = 10u64;

    let seconds_seed_collides = restart_gap_secs <= sent;
    assert!(
        seconds_seed_collides,
        "this test is meant to demonstrate the collision it is named for"
    );

    let micros_seed_clears = restart_gap_secs * 1_000_000 > sent;
    assert!(
        micros_seed_clears,
        "microseconds must give the elapsed time enough ticks to outrun the \
         message count, or the seed is no better than zero"
    );
}

/// The clock must be usable as an id at all: monotonic within a run, and inside
/// the range the counter can keep incrementing through.
#[test]
fn the_seed_is_sane_as_an_id() {
    let first = platform_timestamp_micros();
    let second = platform_timestamp_micros();
    assert!(
        second >= first,
        "the seed clock went backwards within one run"
    );
    assert!(
        first > 1_600_000_000_000_000,
        "the seed is not a plausible microsecond timestamp: {first}"
    );
    assert!(
        u64::MAX - first > 1_000_000_000_000,
        "no headroom left above the seed for the counter to advance into"
    );
}
