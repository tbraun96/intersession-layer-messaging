//! A peer that stops acknowledging must not grow the queue for ever.
//!
//! Stop-and-wait never gives up. A wedged peer keeps its head retransmitting and
//! its queue growing for as long as the application keeps sending, and nothing
//! capped it or aged it — so the backend grew without bound, which in the browser
//! means until the tab died.
//!
//! The queue is REFUSED at the door, not trimmed. Discarding a queued message
//! would put a hole in the id run, and acknowledgement is cumulative: the
//! receiver holds everything behind a hole until gap patience fires and then
//! skips the missing id permanently. Refusing before an id is minted keeps the
//! run contiguous and tells the CALLER — the difference between a message that
//! failed and a message that vanished.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};

const LOCAL: usize = 0;
/// Never added to the network, so nothing this peer is sent can ever drain.
const WEDGED: usize = 1;
const OTHER: usize = 2;

/// Mirrors MAX_PENDING_PER_PEER in src/lib.rs. Not importable (private), so a
/// change there must be reflected here — and the assertions below fail loudly if
/// it is not.
const CAP: usize = 1024;

/// Sends `count` messages to `peer` and reports how many were accepted.
///
/// A macro rather than a function: `ILM` takes four generic parameters and
/// naming them here would say nothing the call sites do not already say.
macro_rules! fill {
    ($ilm:expr, $peer:expr, $count:expr) => {{
        let mut accepted = 0usize;
        for id in 0..$count {
            let message = TestMessage::construct_from_parts(LOCAL, $peer, id, b"x".to_vec());
            if $ilm.send_raw_message(message).await.is_ok() {
                accepted += 1;
            }
        }
        accepted
    }};
}

#[citadel_io::tokio::test]
async fn a_peer_that_never_acknowledges_stops_accepting_sends() {
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    // WEDGED is deliberately absent from the network: nothing drains for it.
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    let accepted: usize = fill!(ilm, WEDGED, CAP + 50);

    assert!(
        accepted <= CAP,
        "the queue accepted {accepted} messages for a peer that acknowledges nothing; \
         nothing bounds it and the backend grows until the tab dies"
    );
    assert!(
        accepted >= CAP,
        "the queue refused at {accepted}, below the cap of {CAP} — a working burst \
         would be refused too"
    );

    let stored = backend.get_pending_outbound().await.expect("pending").len();
    assert!(
        stored <= CAP,
        "the backend holds {stored} undelivered messages for one peer"
    );

    drop(ilm);
}

#[citadel_io::tokio::test]
async fn a_refused_send_says_so_rather_than_vanishing() {
    // The caller has to be able to tell. A silent drop is what makes a wedged
    // peer look like a working one.
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    let _ = fill!(ilm, WEDGED, CAP);

    let over = TestMessage::construct_from_parts(LOCAL, WEDGED, CAP + 1, b"x".to_vec());
    let result = ilm.send_raw_message(over).await;

    assert!(result.is_err(), "the send past the cap reported success");

    drop(ilm);
}

#[citadel_io::tokio::test]
async fn one_wedged_peer_does_not_block_sends_to_another() {
    // The opposite failure: a global cap, or a cap counted across peers, would
    // let one dead conversation stop every other. The bound is per peer.
    let backend = InMemoryBackend::<TestMessage>::new();
    let (tx, _rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let network = InMemoryNetwork::<TestMessage>::new().add_peer(LOCAL).await;
    let ilm = ILM::new(backend.clone(), tx, network).await.expect("ILM");

    let _ = fill!(ilm, WEDGED, CAP);

    let to_other = TestMessage::construct_from_parts(LOCAL, OTHER, 1, b"x".to_vec());
    assert!(
        ilm.send_raw_message(to_other).await.is_ok(),
        "a full queue for one peer refused a send to a different peer"
    );

    drop(ilm);
}
