//! Dropping an ILM must stop the four loops it started.
//!
//! `Drop` asked `Arc::strong_count(&is_running) == 1` and, if so, sent on
//! `poll_outbound_tx`. Both halves were wrong:
//!
//!   - the background task owns a clone that holds `is_running` for exactly as
//!     long as the loops run, so the count is never 1 while there is anything
//!     to stop;
//!   - the send is a poll NUDGE. The woken loop re-reads `is_running`, finds it
//!     still true, and carries on.
//!
//! So Drop was a complete no-op. Every WASM-client restart or `close_connection`
//! left the outbound, inbound, network and peer-polling loops running for ever
//! against an abandoned backend — polling every 200ms and syncing stale tracker
//! state onto the same durable per-CID keys the REPLACEMENT instance for that
//! CID writes, which is how a live instance's resync bookkeeping gets corrupted
//! by a dead one.
//!
//! This counts backend reads, because that is what a leaked loop actually does
//! to the system: it keeps touching shared durable state. Asserting that Drop
//! ran, or that a flag flipped, would pass against the old code too.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};
use std::time::Duration;

const LOCAL: usize = 0;
const PEER: usize = 1;

/// Long enough for the 200ms pollers to run several times over.
const OBSERVATION: Duration = Duration::from_millis(1200);

#[citadel_io::tokio::test]
async fn dropping_an_ilm_stops_it_touching_the_backend() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;

    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    // Something for the outbound loop to keep finding, so the loops have work
    // and a leak is visible as continuing activity rather than silence.
    backend
        .store_outbound(TestMessage::construct_from_parts(
            LOCAL,
            PEER,
            0,
            b"pending".to_vec(),
        ))
        .await
        .expect("store outbound");

    citadel_io::tokio::time::sleep(OBSERVATION).await;
    let while_alive = backend.reads();
    assert!(
        while_alive > 0,
        "the loops never ran, so this test cannot tell a stopped loop from a broken harness"
    );

    drop(ilm);
    // One poll cycle to notice.
    citadel_io::tokio::time::sleep(Duration::from_millis(400)).await;

    let after_drop = backend.reads();
    citadel_io::tokio::time::sleep(OBSERVATION).await;
    let later = backend.reads();

    assert_eq!(
        later,
        after_drop,
        "the loops kept reading the backend {} more times after the ILM was dropped",
        later - after_drop
    );
}
