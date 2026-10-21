//! `INTERNAL_HTTP` is reported for a peer only while that peer advertises it.
//!
//! The layer above ILM asks `peer_capabilities` before it sends a
//! `citadel-ihttp/1` envelope, and a legacy client renders an envelope it
//! cannot read as an unreadable message. So the answer must be "no" until the
//! peer has said so, "no" for a peer that never says so, and "no" again the
//! moment its Ack or Poll stops saying so.

mod support;

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    DynamicCompression, FrameExtensions, IlmOptions, OutboundFrame, Payload, PeerCapabilities,
    UnderlyingSessionTransport, ILM,
};
use std::time::Duration;
use support::eventually;

const ALICE: usize = 1;
const BOB: usize = 2;
const CAROL: usize = 3;

const SERVES_IHTTP: IlmOptions = IlmOptions {
    piggyback_acks: true,
    dynamic_compression: DynamicCompression::Disabled,
    internal_http: true,
};

type Inbox = citadel_io::tokio::sync::mpsc::UnboundedReceiver<TestMessage>;

async fn recv(inbox: &mut Inbox) {
    citadel_io::tokio::time::timeout(Duration::from_secs(3), inbox.recv())
        .await
        .expect("delivered in time")
        .expect("inbox open");
}

#[citadel_io::tokio::test]
async fn only_a_peer_that_advertised_internal_http_is_reported_as_carrying_it() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let (alice_tx, mut alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let (bob_tx, mut bob_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let (carol_tx, mut carol_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice = ILM::new(
        InMemoryBackend::new(),
        alice_tx,
        network.add_peer(ALICE).await,
        SERVES_IHTTP,
    )
    .await
    .expect("alice");
    let bob = ILM::new(
        InMemoryBackend::new(),
        bob_tx,
        network.add_peer(BOB).await,
        SERVES_IHTTP,
    )
    .await
    .expect("bob");
    let carol = ILM::new(
        InMemoryBackend::new(),
        carol_tx,
        network.add_peer(CAROL).await,
        IlmOptions::LEGACY,
    )
    .await
    .expect("carol");

    // Before anyone has said anything, nobody carries it.
    assert!(!alice.peer_capabilities(BOB).carries_internal_http());
    assert!(!alice.peer_capabilities(CAROL).carries_internal_http());

    for _ in 0..2 {
        alice.send_to(BOB, b"hi bob".to_vec()).await.expect("send");
        recv(&mut bob_inbox).await;
        bob.send_to(ALICE, b"hi alice".to_vec())
            .await
            .expect("send");
        recv(&mut alice_inbox).await;
        alice
            .send_to(CAROL, b"hi carol".to_vec())
            .await
            .expect("send");
        recv(&mut carol_inbox).await;
        carol.send_to(ALICE, b"hi".to_vec()).await.expect("send");
        recv(&mut alice_inbox).await;
    }

    assert!(
        eventually(Duration::from_secs(3), || async {
            alice.peer_capabilities(BOB).carries_internal_http()
        })
        .await,
        "bob advertised internal HTTP and alice never saw it"
    );
    assert!(!alice.peer_capabilities(CAROL).carries_internal_http());
    assert!(!carol.peer_capabilities(ALICE).carries_internal_http());
}

#[citadel_io::tokio::test]
async fn a_peer_that_stops_advertising_internal_http_loses_it_at_once() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let (alice_tx, _alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice = ILM::new(
        InMemoryBackend::new(),
        alice_tx,
        network.add_peer(ALICE).await,
        SERVES_IHTTP,
    )
    .await
    .expect("alice");
    // A bare wire, so the test decides exactly what Bob advertises.
    let bob = network.add_peer(BOB).await;
    let poll = |caps: Option<PeerCapabilities>| OutboundFrame {
        payload: Payload::Poll {
            from_id: BOB,
            to_id: ALICE,
            last_received_from_peer: None,
        },
        extensions: caps.map_or(FrameExtensions::None, FrameExtensions::Advertise),
    };

    let advertised = PeerCapabilities::LEGACY.with_internal_http(true);
    bob.send_message(poll(Some(advertised)))
        .await
        .expect("advertise");
    assert!(
        eventually(Duration::from_secs(2), || async {
            alice.peer_capabilities(BOB).carries_internal_http()
        })
        .await
    );

    // Replaced by an old build: its Poll carries no advertisement.
    bob.send_message(poll(None)).await.expect("legacy poll");
    assert!(
        eventually(Duration::from_secs(2), || async {
            !alice.peer_capabilities(BOB).carries_internal_http()
        })
        .await,
        "a peer that stopped advertising kept internal HTTP"
    );
}
