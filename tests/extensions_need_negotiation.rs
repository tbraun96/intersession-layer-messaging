//! No extension ever reaches a peer that has not advertised it.
//!
//! A legacy peer cannot read an extended data frame: the workspace connector
//! hands anything it cannot parse to the UI as a plain message, so a compressed
//! frame sent to an old client is rendered as garbage in a chat. The only safe
//! rule is "not until the peer has said so, and not after it stops saying so".

mod support;

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    Backend, CompressionHint, DynamicCompression, FrameExtensions, IlmOptions, MessageMetadata,
    OutboundFrame, Payload, PeerCapabilities, UnderlyingSessionTransport, ILM,
};
use std::time::Duration;
use support::{eventually, Kind, Recorder};

const ALICE: usize = 1;
const BOB: usize = 2;

#[cfg(all(feature = "compression-brotli", feature = "compression-deflate"))]
const COMPRESSION: DynamicCompression = DynamicCompression::All;
#[cfg(not(all(feature = "compression-brotli", feature = "compression-deflate")))]
const COMPRESSION: DynamicCompression = DynamicCompression::Disabled;

const EVERYTHING: IlmOptions = IlmOptions {
    piggyback_acks: true,
    dynamic_compression: COMPRESSION,
};

type Inbox = citadel_io::tokio::sync::mpsc::UnboundedReceiver<TestMessage>;

async fn recv(inbox: &mut Inbox) -> TestMessage {
    citadel_io::tokio::time::timeout(Duration::from_secs(3), inbox.recv())
        .await
        .expect("delivered in time")
        .expect("inbox open")
}

fn json(n: usize) -> Vec<u8> {
    format!(
        "{{\"kind\":\"update\",\"n\":{n},\"body\":\"{}\"}}",
        "lorem ipsum ".repeat(40)
    )
    .into_bytes()
}

#[citadel_io::tokio::test]
async fn a_legacy_peer_is_never_sent_an_extension_in_either_direction() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = Recorder::new(network.add_peer(ALICE).await);
    let bob_wire = Recorder::new(network.add_peer(BOB).await);
    let (alice_tx, mut alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let (bob_tx, mut bob_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice = ILM::new(
        InMemoryBackend::new(),
        alice_tx,
        alice_wire.clone(),
        EVERYTHING,
    )
    .await
    .expect("alice");
    let bob = ILM::new(
        InMemoryBackend::new(),
        bob_tx,
        bob_wire.clone(),
        IlmOptions::LEGACY,
    )
    .await
    .expect("bob");

    for n in 0..6 {
        alice
            .send_to_with_hint(BOB, json(n), Some(CompressionHint::Json))
            .await
            .expect("alice sends");
        recv(&mut bob_inbox).await;
        bob.send_to(ALICE, json(n)).await.expect("bob replies");
        recv(&mut alice_inbox).await;
    }

    for sent in alice_wire.sent().await {
        if let Kind::Message(_) = sent.kind {
            assert_eq!(
                sent.extensions,
                FrameExtensions::None,
                "a data frame to a legacy peer carried an extension"
            );
        }
    }
    for sent in bob_wire.sent().await {
        assert_eq!(
            sent.extensions,
            FrameExtensions::None,
            "a legacy node sent an extension"
        );
    }
}

#[citadel_io::tokio::test]
async fn a_peer_that_stops_advertising_is_legacy_again_at_once() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = Recorder::new(network.add_peer(ALICE).await);
    // Bob is a bare wire, so the test decides exactly what he advertises.
    let bob = network.add_peer(BOB).await;
    let (alice_tx, mut alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice_backend = InMemoryBackend::<TestMessage>::new();
    let alice = ILM::new(
        alice_backend.clone(),
        alice_tx,
        alice_wire.clone(),
        EVERYTHING,
    )
    .await
    .expect("alice");

    let poll = |caps: Option<PeerCapabilities>| OutboundFrame {
        payload: Payload::Poll {
            from_id: BOB,
            to_id: ALICE,
            last_received_from_peer: None,
        },
        extensions: caps.map_or(FrameExtensions::None, FrameExtensions::Advertise),
    };
    let message = |id: usize| {
        OutboundFrame::legacy(Payload::Message(TestMessage::construct_from_parts(
            BOB,
            ALICE,
            id,
            b"from bob".to_vec(),
        )))
    };
    let alices_frames_to_bob = || async {
        alice_wire
            .sent()
            .await
            .into_iter()
            .filter(|s| matches!(s.kind, Kind::Message(_)))
            .map(|s| s.extensions)
            .collect::<Vec<_>>()
    };
    let ack_alice = |id: usize| {
        OutboundFrame::legacy(Payload::Ack {
            from_id: BOB,
            to_id: ALICE,
            message_id: id,
        })
    };

    // Bob advertises piggybacking, and sends Alice something to acknowledge.
    bob.send_message(poll(Some(PeerCapabilities::from_wire(
        PeerCapabilities::PIGGYBACK_ACKS,
    ))))
    .await
    .expect("advertise");
    bob.send_message(message(1)).await.expect("bob sends");
    recv(&mut alice_inbox).await;
    // The ACK is decided just before the inbound row is cleared.
    assert!(
        eventually(Duration::from_secs(2), || async {
            alice_backend
                .get_pending_inbound()
                .await
                .expect("inbound")
                .is_empty()
        })
        .await
    );
    alice
        .send_to(BOB, b"first".to_vec())
        .await
        .expect("alice sends");
    assert!(
        eventually(Duration::from_secs(2), || async {
            alices_frames_to_bob().await.len() == 1
        })
        .await
    );
    assert!(
        matches!(
            alices_frames_to_bob().await[0],
            FrameExtensions::Negotiated {
                piggybacked_ack: Some(1),
                ..
            }
        ),
        "an advertised peer should have had the ACK folded in: {:?}",
        alices_frames_to_bob().await
    );
    bob.send_message(ack_alice(0)).await.expect("bob acks");

    // Bob is replaced by an old build: its Poll carries no advertisement.
    bob.send_message(poll(None)).await.expect("legacy poll");
    bob.send_message(message(2)).await.expect("bob sends again");
    recv(&mut alice_inbox).await;
    alice
        .send_to(BOB, b"second".to_vec())
        .await
        .expect("alice sends");
    assert!(
        eventually(Duration::from_secs(2), || async {
            alices_frames_to_bob().await.len() == 2
        })
        .await
    );
    assert_eq!(
        alices_frames_to_bob().await[1],
        FrameExtensions::None,
        "a peer that stopped advertising was still sent an extension"
    );
    assert!(
        alice_wire
            .sent()
            .await
            .iter()
            .any(|s| s.kind == Kind::Ack(2)),
        "the ACK for a legacy peer's message must go alone"
    );
}
