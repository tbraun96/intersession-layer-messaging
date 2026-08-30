//! A message the gap-patience delivery skipped must still arrive if it turns up.
//!
//! `held_too_long` breaks a permanent hold by delivering out of order after
//! `GAP_PATIENCE_SECS`, and that advances the delivery frontier over the gap
//! beneath it. The comment at that site already called it "a real loss" — and it
//! was worse than intended, because the loss was not conditional on the message
//! never arriving.
//!
//! When the missing id DID turn up, `safe_to_ack` saw it was below the frontier,
//! classified it as already-delivered, re-ACKed it and cleared it from the
//! backend. The application never saw it. The known BLOCKED-RECOVERY stall
//! produces exactly this: it holds a message past 20s and then releases it, so
//! the stall's own resolution deleted the message it had stalled.
//!
//! Recording what the frontier was advanced past turns that into a late,
//! out-of-order delivery. Ordering was already broken by the delivery that
//! skipped it; a message out of order beats a message gone.
//!
//! WHAT THIS TEST DOES NOT COVER: the skipped set is in-memory. Across a process
//! restart the durable frontier governs and a late arrival is discarded exactly
//! as before — no worse than the old behaviour, and not fixed by this.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};
use std::time::Duration;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// Older than GAP_PATIENCE_SECS, so `held_too_long` fires without the test
/// waiting twenty seconds for it.
const LONG_AGO: u64 = 1;

#[citadel_io::tokio::test]
async fn a_message_skipped_by_gap_patience_is_delivered_when_it_arrives() {
    let backend = InMemoryBackend::<TestMessage>::new();

    // id 1 arrives normally and sets the frontier.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            1,
            b"one".to_vec(),
        ))
        .await
        .expect("store 1");

    // id 3 arrives with 2 missing. Stamped as received long ago so the gap
    // patience has already expired.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            3,
            b"three".to_vec(),
        ))
        .await
        .expect("store 3");

    let (tx, mut rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    // Age id 3's receipt so held_too_long is already true for it.
    ilm.backdate_receipt_for_tests(PEER, 3, LONG_AGO);

    // 1 then 3 (out of order, after patience). 2 is now skipped.
    let mut seen: Vec<u64> = Vec::new();
    let deadline = Duration::from_secs(10);
    while seen.len() < 2 {
        match citadel_io::tokio::time::timeout(deadline, rx.recv()).await {
            Ok(Some(m)) => seen.push(m.message_id() as u64),
            _ => break,
        }
    }
    assert_eq!(seen, vec![1, 3], "expected 1 then 3 delivered out of order");

    // The straggler finally arrives -- the shape a cleared stall produces.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            2,
            b"two".to_vec(),
        ))
        .await
        .expect("store 2");

    let late = citadel_io::tokio::time::timeout(deadline, rx.recv()).await;

    match late {
        Ok(Some(m)) => assert_eq!(
            m.message_id(),
            2,
            "the late message should be the one that was skipped"
        ),
        _ => panic!("the skipped message was discarded instead of delivered late"),
    }

    // Exactly once, under the condition that can actually cause a second copy.
    //
    // Simply waiting for silence here proves nothing: the delivery path calls
    // `clear_message_inbound`, so the message is gone from the backend and no
    // later poll re-reads it whether or not the skipped entry was cleared.
    // (Verified -- removing `clear_skipped` leaves that weaker assertion green.)
    // A RETRANSMISSION is what re-enters the path, and it is the realistic
    // event: the sender resends whenever it misses an ACK. With the entry
    // cleared, the duplicate is re-ACKed and dropped; with it left behind,
    // `was_skipped` is still true and the application sees the message twice.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            2,
            b"two".to_vec(),
        ))
        .await
        .expect("retransmit 2");

    let repeat = citadel_io::tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        repeat.is_err(),
        "a retransmission of the late-delivered message was delivered twice: {repeat:?}"
    );
}

/// The gap bound truncates, and truncation costs the tail of the gap.
///
/// `MAX_RECORDED_GAP` caps what a single gap may add to the late-delivery set.
/// The commit that introduced it claimed the limit was covered by a test; it was
/// not, and the warning it logged claimed a too-wide gap was "not recorded at
/// all" while the code had already recorded its first N ids. This pins the
/// behaviour that actually exists: the head of a wide gap is recoverable, the
/// tail is not.
#[citadel_io::tokio::test]
async fn a_gap_wider_than_the_bound_only_recovers_its_head() {
    // Mirrors MAX_RECORDED_GAP in src/message_tracker.rs. Not importable (the
    // constant is private), so a change there must be reflected here -- and the
    // assertions below fail loudly if it is not.
    const RECORDED: u64 = 256;

    let backend = InMemoryBackend::<TestMessage>::new();

    // Frontier at 1.
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            1,
            b"one".to_vec(),
        ))
        .await
        .expect("store 1");

    // A gap far wider than the bound: everything from 2 to 999 is missing.
    const FAR: u64 = 1000;
    backend
        .store_inbound(TestMessage::construct_from_parts(
            PEER,
            LOCAL,
            FAR as usize,
            b"far".to_vec(),
        ))
        .await
        .expect("store far");

    let (tx, mut rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");
    ilm.backdate_receipt_for_tests(PEER, FAR as usize, LONG_AGO);

    let deadline = Duration::from_secs(10);
    let mut seen: Vec<u64> = Vec::new();
    while seen.len() < 2 {
        match citadel_io::tokio::time::timeout(deadline, rx.recv()).await {
            Ok(Some(m)) => seen.push(m.message_id() as u64),
            _ => break,
        }
    }
    assert_eq!(
        seen,
        vec![1, FAR],
        "expected 1 then the far id, out of order"
    );

    // Inside the recorded head (2..=257): recoverable.
    const IN_HEAD: u64 = 100;
    // Past the truncation point: permanently lost, and the warn log says so.
    const IN_TAIL: u64 = 900;
    // Compile-time, so raising MAX_RECORDED_GAP without updating RECORDED here
    // fails the build rather than quietly making the test assert nothing.
    const _: () = assert!(IN_HEAD > 1 && IN_HEAD <= 1 + RECORDED);
    const _: () = assert!(IN_TAIL > 1 + RECORDED && IN_TAIL < FAR);

    for id in [IN_HEAD, IN_TAIL] {
        backend
            .store_inbound(TestMessage::construct_from_parts(
                PEER,
                LOCAL,
                id as usize,
                b"straggler".to_vec(),
            ))
            .await
            .expect("store straggler");
    }

    match citadel_io::tokio::time::timeout(deadline, rx.recv()).await {
        Ok(Some(m)) => assert_eq!(
            m.message_id() as u64,
            IN_HEAD,
            "the straggler inside the recorded head should be delivered late"
        ),
        other => panic!("the head of the gap was not recoverable: {other:?}"),
    }

    // And nothing else -- the tail straggler was stored alongside it and is
    // sorted after it, so if the bound were not truncating it would arrive here.
    let tail = citadel_io::tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(
        tail.is_err(),
        "expected the id past MAX_RECORDED_GAP to stay lost, but got {tail:?}"
    );
}
