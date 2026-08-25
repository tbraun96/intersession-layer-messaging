//! A duplicate must be acknowledged again, not silently dropped.
//!
//! The outbound path is stop-and-wait per peer, and it BREAKS on the first
//! message it cannot send. A message stays at the head of that queue until its
//! ACK arrives, so if the sender never sees one it retransmits forever and
//! every message queued behind it is never sent at all.
//!
//! That makes the receiver's duplicate handling load-bearing. Recognising a
//! duplicate and saying nothing is what renders retransmission useless: the
//! sender asks again, we drop it again, and neither side ever moves. The
//! observable result is the offline-delivery failure — the first queued message
//! arrives, the rest never do, and the sender's UI shows all of them as sent
//! because sending is all it ever got to do.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    Backend, MessageMetadata, Payload, UnderlyingSessionTransport, ILM,
};
use std::time::Duration;

const LOCAL: usize = 0;
const ALICE: usize = 1;

/// Waits for an ACK addressed to Alice, ignoring the polls that also flow.
async fn next_ack(alice: &InMemoryNetwork<TestMessage>, within: Duration) -> Option<usize> {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        match citadel_io::tokio::time::timeout(Duration::from_millis(250), alice.next_message())
            .await
        {
            Ok(Some(Payload::Ack { message_id, .. })) => return Some(message_id),
            Ok(Some(_)) => continue,
            Ok(None) => return None,
            Err(_) => continue,
        }
    }
    None
}

#[citadel_io::tokio::test]
async fn a_retransmitted_message_is_acknowledged_again() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let message = TestMessage::construct_from_parts(ALICE, LOCAL, 0, b"offline message 1".to_vec());
    backend
        .store_inbound(message.clone())
        .await
        .expect("store inbound");

    let network = InMemoryNetwork::<TestMessage>::new();
    // Alice's handle is held so her side of the wire can be inspected; the ILM
    // under test is the LOCAL one.
    let alice = network.add_peer(ALICE).await;
    let local = network.add_peer(LOCAL).await;

    let (tx, mut rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let _ilm = ILM::new(backend.clone(), tx, local)
        .await
        .expect("construct ILM");

    // First pass: delivered to the application, and acknowledged.
    let delivered = citadel_io::tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("first delivery within deadline")
        .expect("channel open");
    assert_eq!(delivered.message_id(), 0);
    assert_eq!(delivered.source_id(), ALICE);
    assert_eq!(
        next_ack(&alice, Duration::from_secs(10)).await,
        Some(0),
        "the first delivery must be acknowledged"
    );

    // The retransmission a sender makes when it never saw that ACK.
    backend
        .store_inbound(message)
        .await
        .expect("store the duplicate");

    // It must NOT reach the application twice — de-duplication still holds …
    assert!(
        citadel_io::tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .is_err(),
        "a duplicate must not be delivered to the application a second time"
    );

    // … but it MUST be acknowledged again, or the sender's queue never drains.
    assert_eq!(
        next_ack(&alice, Duration::from_secs(10)).await,
        Some(0),
        "a duplicate must be re-acknowledged; without this the sender blocks \
         on this id forever and every message behind it is never sent"
    );
}
