//! A send that is refused must not burn the id it was given.
//!
//! `send_to` mints the next id for the peer and only THEN calls
//! `send_raw_message`, which can still refuse: backpressure
//! (`MAX_PENDING_PER_PEER`), a failed `store_outbound`, or a rejected
//! source/destination. Before this, every one of those left a number that no
//! message would ever carry.
//!
//! That is not a cosmetic leak. Ids are consumed in order at the receiver: the
//! next real message arrives as `n+1` with `n` missing, so it is held behind the
//! gap for `GAP_PATIENCE_SECS` (20s) before the receiver gives up and delivers
//! out of order. One refused send costs twenty seconds of stalled delivery for
//! that peer — and a client that hits backpressure would do it repeatedly.
//!
//! The comment at the backpressure check read "Backpressure, before an id is
//! minted". That is true only of `send_raw_message`'s own body; its only real
//! caller mints first.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// End to end: a refused send leaves the counter where it was, so the next
/// message the peer receives is the one it was expecting.
#[citadel_io::tokio::test]
async fn a_refused_send_leaves_the_counter_alone() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend, tx, network).await.expect("ILM");

    let before = ilm.peek_next_id_for_tests(PEER);

    // Refused by `send_raw_message`'s own guard: a message addressed to
    // ourselves. Every refusal path burns the id the same way; this one is the
    // cheapest to stage, where backpressure would need MAX_PENDING_PER_PEER
    // (1024) messages queued first.
    //
    // Note `send_to(LOCAL, ..)` mints against LOCAL, so LOCAL is the counter to
    // watch — PEER's is the control that must NOT move either.
    let local_before = ilm.peek_next_id_for_tests(LOCAL);
    let refused = ilm.send_to(LOCAL, b"to myself".to_vec()).await;
    assert!(
        refused.is_err(),
        "sending to self must be refused, or this test proves nothing"
    );

    assert_eq!(
        ilm.peek_next_id_for_tests(LOCAL),
        local_before,
        "the refused send burned an id; the receiver would hold the next \
         message behind the missing one for GAP_PATIENCE_SECS"
    );
    assert_eq!(
        ilm.peek_next_id_for_tests(PEER),
        before,
        "an unrelated peer's counter moved"
    );

    drop(ilm);
}

/// A successful send must still consume its id — otherwise the fix above could
/// be implemented by never advancing the counter at all, and every message
/// would reuse id 0.
#[citadel_io::tokio::test]
async fn a_successful_send_still_consumes_its_id() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend, tx, network).await.expect("ILM");

    let before = ilm.peek_next_id_for_tests(PEER);
    ilm.send_to(PEER, b"hello".to_vec())
        .await
        .expect("an ordinary send must succeed");

    assert_eq!(
        ilm.peek_next_id_for_tests(PEER),
        before + 1,
        "a successful send did not advance the counter, so ids would repeat"
    );

    drop(ilm);
}
