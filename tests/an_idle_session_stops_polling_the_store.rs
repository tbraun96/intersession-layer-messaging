//! An idle session must not keep asking the store whether it is still idle.
//!
//! Both background loops woke every 200ms and fetched the WHOLE queue. On the
//! production backend that is a round trip to the agent plus a full blob
//! deserialize — ten times a second, per session, forever, to discover nothing.
//! A browser with three sessions open paid thirty of those a second while the
//! user was reading.
//!
//! The tick itself is load-bearing: retransmission is driven by it, and a
//! message parked behind a gap is released by the GAP_PATIENCE timer rather
//! than by an event. So the poll is skipped only when the queue was observed
//! EMPTY, and any nudge processes unconditionally.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;
use std::time::Duration;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// Comfortably more than a few OUTBOUND_POLL/INBOUND_POLL intervals (200ms).
const SEVERAL_INTERVALS: Duration = Duration::from_millis(1200);

async fn spawn_ilm(backend: InMemoryBackend<TestMessage>) -> impl Sized {
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    ILM::new(backend, tx, network).await.expect("ILM")
}

#[citadel_io::tokio::test]
async fn an_idle_session_settles_and_stops_reading() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let ilm = spawn_ilm(backend.clone()).await;

    // Let the loops start and observe the empty queues at least once. The hint
    // begins `true` on purpose -- a reconnect may load a persisted queue -- so
    // the first poll always reads.
    citadel_io::tokio::time::sleep(SEVERAL_INTERVALS).await;
    let settled = backend.reads();
    assert!(
        settled > 0,
        "the loops never read at all, so this test would pass on a dead ILM"
    );

    citadel_io::tokio::time::sleep(SEVERAL_INTERVALS).await;
    assert_eq!(
        backend.reads(),
        settled,
        "an idle session kept polling the store; over {SEVERAL_INTERVALS:?} it \
         read {} more times",
        backend.reads() - settled
    );

    drop(ilm);
}

/// The other half: a session with work queued MUST keep polling, or
/// retransmission stops and a message held behind a gap is never released.
#[citadel_io::tokio::test]
async fn a_session_with_queued_work_keeps_polling() {
    use intersession_layer_messaging::{Backend, MessageMetadata};

    let backend = InMemoryBackend::<TestMessage>::new();
    backend
        .store_outbound(TestMessage::construct_from_parts(
            LOCAL,
            PEER,
            1usize,
            b"queued".to_vec(),
        ))
        .await
        .expect("store outbound");

    // A network with no route to PEER, so the message stays queued rather than
    // being sent and cleared.
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    citadel_io::tokio::time::sleep(SEVERAL_INTERVALS).await;
    let after_first = backend.reads();

    citadel_io::tokio::time::sleep(SEVERAL_INTERVALS).await;
    assert!(
        backend.reads() > after_first,
        "a session with an undelivered message stopped polling; retransmission \
         is driven by that tick, so the message would never be resent"
    );

    drop(ilm);
}
