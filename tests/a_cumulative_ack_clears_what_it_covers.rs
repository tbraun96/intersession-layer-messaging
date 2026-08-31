//! An ACK acknowledges everything at or below its id. The store cleared one.
//!
//! Acknowledgement is cumulative throughout this crate. `update_ack` keeps only
//! a high-water mark and discards any id at or below it; the send window counts
//! in flight as `id <= last_sent && id > last_acked`; and `SEND_WINDOW` is
//! justified precisely on the strength of that — "one surviving ACK retires the
//! whole window, and at four-in-five loss the chance that at least one of eight
//! survives is 83%".
//!
//! `clear_message_outbound` was called with the single id the ACK named. So when
//! ACKs 1..7 were lost and the ACK for 8 arrived, the window correctly treated
//! 1..7 as retired — and their rows stayed in the outbound store for ever.
//!
//! That is not merely a leak, because `process_outbound` sorts ascending and
//! examines the lowest id first. `can_send` is false for an id at or below
//! `last_acked`, and the head branch that handles "cannot send" assumes exactly
//! one reason for it: *"`can_send` is false because `msg_id > last_sent` is
//! false -- this message has been sent and not acknowledged, which is precisely
//! the case that calls for a retransmission"*. For a cumulatively-acked row that
//! assumption is wrong. The head is retransmitted every cycle; the receiver
//! dedups and re-ACKs the same id; `update_ack` discards it as stale so
//! `last_acked` never moves; and the peer stays blocked until
//! `MAX_CONSECUTIVE_BLOCKS` (50 cycles, ten seconds) triggers BLOCKED-RECOVERY,
//! which wipes `last_sent` and `last_acked` and resets the send window to one.
//!
//! It does eventually self-heal — recovery makes `can_send` true, the row is
//! sent, acked and cleared — so this is a stall and a window reset per stale
//! row, not a permanent block. Seven lost ACKs cost seventy seconds of stalled
//! queue for that peer.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};
use std::time::Duration;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// Ids the outbound store still holds for anybody.
async fn outbound_ids(backend: &InMemoryBackend<TestMessage>) -> Vec<u64> {
    let mut ids: Vec<u64> = backend
        .get_pending_outbound()
        .await
        .expect("pending outbound")
        .iter()
        .map(|m| m.message_id() as u64)
        .collect();
    ids.sort_unstable();
    ids
}

#[citadel_io::tokio::test]
async fn an_ack_clears_every_id_at_or_below_it() {
    let backend = InMemoryBackend::<TestMessage>::new();

    // Three messages queued for the peer, as a send window of 8 permits.
    for id in 1..=3usize {
        backend
            .store_outbound(TestMessage::construct_from_parts(
                LOCAL,
                PEER,
                id,
                format!("m{id}").into_bytes(),
            ))
            .await
            .expect("store outbound");
    }

    // Precondition: without it, a refactor that stopped storing outbound rows
    // would make the assertion below pass by finding nothing to clear.
    assert_eq!(
        outbound_ids(&backend).await,
        vec![1, 2, 3],
        "the outbound store must actually hold the messages before we ack them"
    );

    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let inbox = network.clone();
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    // Only the highest ACK survives the link — the case SEND_WINDOW is sized
    // for. 1 and 2 are acknowledged by it, cumulatively.
    inbox
        .send_to_peer(
            LOCAL,
            intersession_layer_messaging::Payload::Ack {
                from_id: PEER,
                to_id: LOCAL,
                message_id: 3,
            },
        )
        .await
        .expect("deliver ack");

    // The clear happens on the inbound network task; give it cycles rather than
    // a fixed sleep so a slow machine does not read as a failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if outbound_ids(&backend).await.is_empty() {
            break;
        }
        citadel_io::tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        outbound_ids(&backend).await,
        Vec::<u64>::new(),
        "ids 1 and 2 were acknowledged by the ACK for 3 and left in the store; \
         each one now costs fifty blocked cycles and a send-window reset before \
         BLOCKED-RECOVERY clears it"
    );

    drop(ilm);
}

#[citadel_io::tokio::test]
async fn an_ack_leaves_higher_ids_alone() {
    // The opposite failure: clearing indiscriminately would drop messages that
    // have not been acknowledged at all, and the assertion above cannot see it.
    let backend = InMemoryBackend::<TestMessage>::new();

    for id in 1..=4usize {
        backend
            .store_outbound(TestMessage::construct_from_parts(
                LOCAL,
                PEER,
                id,
                format!("m{id}").into_bytes(),
            ))
            .await
            .expect("store outbound");
    }

    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let inbox = network.clone();
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    inbox
        .send_to_peer(
            LOCAL,
            intersession_layer_messaging::Payload::Ack {
                from_id: PEER,
                to_id: LOCAL,
                message_id: 2,
            },
        )
        .await
        .expect("deliver ack");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if !outbound_ids(&backend).await.contains(&1) {
            break;
        }
        citadel_io::tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let remaining = outbound_ids(&backend).await;
    assert!(
        remaining.contains(&3) && remaining.contains(&4),
        "an ACK for 2 discarded messages nobody acknowledged: {remaining:?}"
    );

    drop(ilm);
}

/// …and it must cost ONE store operation, not one per id.
///
/// Clearing the covered ids individually was correct and slow: on the
/// blob-backed production store every `clear_message_outbound` is a full queue
/// read plus a full queue write, so a cumulative ACK retiring a window of `k`
/// cost `2k` round trips to the agent and O(k²) bytes serialised — for a single
/// ACK. The two assertions above cannot see that; they only look at the
/// resulting contents.
#[citadel_io::tokio::test]
async fn a_cumulative_ack_costs_one_store_operation() {
    let backend = InMemoryBackend::<TestMessage>::new();

    const WINDOW: usize = 8;
    for id in 1..=WINDOW {
        backend
            .store_outbound(TestMessage::construct_from_parts(
                LOCAL,
                PEER,
                id,
                format!("m{id}").into_bytes(),
            ))
            .await
            .expect("store outbound");
    }
    assert_eq!(
        outbound_ids(&backend).await.len(),
        WINDOW,
        "the window must actually be queued, or there is nothing to batch"
    );

    let clears_before = backend.clear_ops();

    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let inbox = network.clone();
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    inbox
        .send_to_peer(
            LOCAL,
            intersession_layer_messaging::Payload::Ack {
                from_id: PEER,
                to_id: LOCAL,
                message_id: WINDOW,
            },
        )
        .await
        .expect("deliver ack");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if outbound_ids(&backend).await.is_empty() {
            break;
        }
        citadel_io::tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        outbound_ids(&backend).await.is_empty(),
        "the ACK did not clear the window, so the cost assertion below would be \
         measuring a clear that never happened"
    );

    let clears = backend.clear_ops() - clears_before;
    assert_eq!(
        clears, 1,
        "clearing a window of {WINDOW} took {clears} store operations; a \
         cumulative ACK must retire the ids it covers in one"
    );

    drop(ilm);
}
