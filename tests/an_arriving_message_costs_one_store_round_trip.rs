//! The tracker's persistence used to cost up to seven round trips per message.
//!
//! Two of them came from the arrival itself — `mark_received` persisted the
//! receipt map and `update_last_received_from` the per-peer high-water mark,
//! each serializing its own map and awaiting its own acknowledgement. The other
//! five came from `sync_backend`, which is called on the ACK path and on the
//! delivery path and wrote all five of its maps one at a time.
//!
//! All of it ran inline in the single sequential listener, so each was another
//! five-second `wait_for_response` window in which one lost response freezes ALL
//! inbound processing — ACKs included, so the senders retransmit into a receiver
//! that has stopped reading.
//!
//! These measure ROUND TRIPS, not keys: the in-memory backend counts one
//! operation per call whether it writes one map or five.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{MessageMetadata, Payload, ILM};
use std::time::Duration;

const LOCAL: usize = 0;
const PEER: usize = 1;

type Ilm = ILM<
    TestMessage,
    InMemoryBackend<TestMessage>,
    citadel_io::tokio::sync::mpsc::UnboundedSender<TestMessage>,
    InMemoryNetwork<TestMessage>,
>;

async fn started(backend: InMemoryBackend<TestMessage>) -> (Ilm, InMemoryNetwork<TestMessage>) {
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let inbox = network.clone();
    let ilm = ILM::new(backend, tx, network).await.expect("ILM");
    // Let startup's own writes settle, so later counts are the message's.
    citadel_io::tokio::time::sleep(Duration::from_millis(300)).await;
    (ilm, inbox)
}

async fn deliver(inbox: &InMemoryNetwork<TestMessage>, id: usize) {
    inbox
        .send_to_peer(
            LOCAL,
            Payload::Message(TestMessage::construct_from_parts(
                PEER,
                LOCAL,
                id,
                b"hello".to_vec(),
            )),
        )
        .await
        .expect("deliver message");
}

async fn wait_for_receipt(ilm: &Ilm, id: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if ilm
            .tracker_for_tests()
            .received_messages
            .contains_key(&(PEER, id))
        {
            return;
        }
        citadel_io::tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("msg {id} was never recorded as received, so any count would measure nothing");
}

/// The arrival path, isolated: a message held behind a gap is received and
/// stored but not delivered, so the delivery frontier's own write does not
/// land in the count.
#[citadel_io::tokio::test]
async fn recording_an_arrival_is_one_operation() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (ilm, inbox) = started(backend.clone()).await;

    // Establish a frontier first, so the next id is genuinely a gap rather than
    // the start of the stream.
    deliver(&inbox, 0).await;
    wait_for_receipt(&ilm, 0).await;
    citadel_io::tokio::time::sleep(Duration::from_millis(300)).await;

    let before = backend.store_ops();
    const HELD_BEHIND_A_GAP: usize = 5;
    deliver(&inbox, HELD_BEHIND_A_GAP).await;
    wait_for_receipt(&ilm, HELD_BEHIND_A_GAP).await;

    let ops = backend.store_ops() - before;
    assert_eq!(
        ops, 1,
        "recording one arrival took {ops} store operations; each is a separate \
         round trip inline in the sequential listener, and a lost response on \
         any of them freezes all inbound processing, ACKs included"
    );

    drop(ilm);
}

/// `sync_backend` is the bigger half: five maps, and it runs on the ACK path
/// and the delivery path, i.e. for ordinary traffic.
#[citadel_io::tokio::test]
async fn syncing_the_tracker_is_one_operation() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (ilm, _inbox) = started(backend.clone()).await;

    let before = backend.store_ops();
    ilm.tracker_for_tests()
        .sync_backend()
        .await
        .expect("sync_backend");

    let ops = backend.store_ops() - before;
    assert_eq!(
        ops, 1,
        "syncing the tracker took {ops} store operations for its five maps; \
         written one at a time, a failure partway through also leaves the \
         persisted set inconsistent with itself"
    );

    drop(ilm);
}
