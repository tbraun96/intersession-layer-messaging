//! A hint compresses a message only toward a peer with a shared codec, and no
//! hint leaves the frame exactly as the legacy protocol sends it.

#![cfg(all(feature = "compression-brotli", feature = "compression-deflate"))]

mod support;

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    Codec, CodecSet, CompressionHint, CompressionPlan, DynamicCompression, FrameExtensions,
    IlmOptions, ILM,
};
use std::time::Duration;
use support::pair::{receive, ALICE, BOB};
use support::{eventually, Kind, Recorder};

const EVERYTHING: IlmOptions = IlmOptions {
    piggyback_acks: true,
    dynamic_compression: DynamicCompression::All,
};

fn json(n: usize) -> Vec<u8> {
    format!(
        "{{\"kind\":\"update\",\"n\":{n},\"body\":\"{}\"}}",
        "lorem ipsum ".repeat(40)
    )
    .into_bytes()
}

#[citadel_io::tokio::test]
async fn only_a_hinted_message_is_compressed_and_an_unhinted_one_is_the_legacy_frame() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = Recorder::new(network.add_peer(ALICE).await);
    let bob_wire = Recorder::new(network.add_peer(BOB).await);
    let (alice_tx, _alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let (bob_tx, mut bob_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice = ILM::new(
        InMemoryBackend::new(),
        alice_tx,
        alice_wire.clone(),
        EVERYTHING,
    )
    .await
    .expect("alice");
    let _bob = ILM::new(InMemoryBackend::new(), bob_tx, bob_wire.clone(), EVERYTHING)
        .await
        .expect("bob");
    assert!(eventually(Duration::from_secs(2), || alice_wire.heard_advertisement()).await);

    alice.send_to(BOB, json(0)).await.expect("unhinted");
    receive(&mut bob_inbox, Duration::from_secs(3)).await;
    alice
        .send_to_with_hint(BOB, json(1), Some(CompressionHint::Json))
        .await
        .expect("hinted");
    receive(&mut bob_inbox, Duration::from_secs(3)).await;

    let frames: Vec<_> = alice_wire
        .sent()
        .await
        .into_iter()
        .filter(|s| matches!(s.kind, Kind::Message(_)))
        .map(|s| s.extensions)
        .collect();
    assert_eq!(
        frames[0],
        FrameExtensions::None,
        "no hint must mean the legacy frame"
    );
    assert_eq!(
        frames[1],
        FrameExtensions::Negotiated {
            piggybacked_ack: None,
            compression: Some(CompressionPlan {
                hint: CompressionHint::Json,
                codecs: CodecSet::of(&[Codec::Brotli, Codec::Deflate]),
            }),
        }
    );
}

/// The codec SET is negotiated, not a single codec: a peer that enabled only
/// deflate is offered only deflate, although this side has brotli too.
#[citadel_io::tokio::test]
async fn a_peer_with_a_subset_of_codecs_is_offered_only_that_subset() {
    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = Recorder::new(network.add_peer(ALICE).await);
    let bob_wire = Recorder::new(network.add_peer(BOB).await);
    let (alice_tx, _alice_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let (bob_tx, mut bob_inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
    let alice = ILM::new(
        InMemoryBackend::new(),
        alice_tx,
        alice_wire.clone(),
        EVERYTHING,
    )
    .await
    .expect("alice");
    let deflate_only = IlmOptions {
        piggyback_acks: false,
        dynamic_compression: DynamicCompression::Deflate,
    };
    let _bob = ILM::new(InMemoryBackend::new(), bob_tx, bob_wire, deflate_only)
        .await
        .expect("bob");
    assert!(eventually(Duration::from_secs(2), || alice_wire.heard_advertisement()).await);

    alice
        .send_to_with_hint(BOB, json(2), Some(CompressionHint::YjsUpdate))
        .await
        .expect("hinted");
    receive(&mut bob_inbox, Duration::from_secs(3)).await;

    let sent = alice_wire
        .sent()
        .await
        .into_iter()
        .find(|s| matches!(s.kind, Kind::Message(_)))
        .expect("alice sent it");
    assert_eq!(
        sent.extensions,
        FrameExtensions::Negotiated {
            piggybacked_ack: None,
            compression: Some(CompressionPlan {
                hint: CompressionHint::YjsUpdate,
                codecs: CodecSet::of(&[Codec::Deflate]),
            }),
        }
    );
}
