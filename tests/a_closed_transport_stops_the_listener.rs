//! A closed transport used to spin the network listener at full speed.
//!
//! `process_next_network_message` was `if let Some(msg) = network.next_message().await`,
//! and its caller was a bare `loop { if !can_run() { break } process(...).await }`.
//! A tokio mpsc receiver yields `None` only once the channel is closed AND
//! drained, and it never re-opens — so after close, `next_message()` is
//! `Ready(None)` on every poll, the `if let` never matches, and the loop
//! re-enters immediately with nothing to await.
//!
//! On native this burns a core. On wasm it is worse and it is the shipping
//! configuration: the loops run under `spawn_local` on the single JS thread,
//! cooperatively scheduled, so a loop that never awaits anything pending never
//! returns control to the event loop and the TAB FREEZES. Every
//! `close_connection()` reaches this — logout, leader-tab demotion, teardown.
//!
//! The same shape was present in the inbound-poll loop, which logged "Poll
//! inbound channel closed" and fell through, while its outbound twin already
//! returned. Both are fixed; this test pins the network one, which is the loop
//! with no sleep in it at all.
//!
//! What this measures: the number of times the transport is polled AFTER it has
//! closed. With the fix that is one — the listener sees the close and stops.
//! Without it the count is however many iterations fit in 250ms.
//!
//! Two earlier versions of this test proved nothing and both passed against the
//! unfixed code. The first dropped an `InMemoryNetwork` handle, which closes
//! nothing because the senders live in a shared map every clone keeps alive. The
//! second asserted that a timer still fired, which it does regardless: the test
//! runtime is multi-threaded, so the spin burns another worker and starves no
//! timer. Only counting the polls actually observes the defect.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;
use std::time::Duration;

const LOCAL: usize = 0;

#[citadel_io::tokio::test]
async fn a_closed_transport_does_not_spin_the_listener() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;

    let ilm = ILM::new(backend, tx, network.clone())
        .await
        .expect("construct ILM");

    // Close the transport the way production does. Dropping a handle is NOT
    // enough -- the senders live in a shared map every clone keeps alive, which
    // is how the first version of this test passed against the unfixed code.
    network.disconnect_peer(LOCAL).await;

    // Give the listener room to react. Timing out on a starved executor was the
    // first version of this assertion and it proved NOTHING: the test runtime is
    // multi-threaded, so the spin ran on another worker and the timer completed
    // regardless. Count the polls instead -- that is the spin itself, and it is
    // observable no matter which thread it burns.
    citadel_io::tokio::time::sleep(Duration::from_millis(250)).await;

    let polls = network.closed_polls();
    assert!(
        polls <= 2,
        "the listener polled a closed transport {polls} times in 250ms; it should stop after the first"
    );

    drop(ilm);
}
