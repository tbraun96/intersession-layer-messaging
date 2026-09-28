//! An ACK rides on the next data frame to its peer, or goes alone within the window.
//!
//! Every delivered message used to cost its own ACK frame -- on the real stack
//! a whole SDK message plus that message's own SDK ack, about 458 bytes on the
//! wire -- although ILM's ACKs are cumulative and a reply was usually about to
//! go the same way. These pin the three things piggybacking must not break:
//! the sender's window is still retired, a message with no reply is still
//! retired promptly, and a lost carrier frame is still recovered.

mod support;

use intersession_layer_messaging::{
    DynamicCompression, FrameExtensions, IlmOptions, MessageMetadata, PIGGYBACK_ACK_WINDOW,
};
use std::time::{Duration, Instant};
use support::pair::{outbound_is_empty, pair, receive, settled, standalone_acks};
use support::{eventually, Kind};

use support::pair::{ALICE, BOB};

const PIGGYBACK: IlmOptions = IlmOptions {
    piggyback_acks: true,
    dynamic_compression: DynamicCompression::Disabled,
};

#[citadel_io::tokio::test]
async fn a_reply_carries_the_ack_and_retires_the_senders_window() {
    let (alice, mut bob) = pair(PIGGYBACK, PIGGYBACK).await;
    let acks_before = standalone_acks(&bob.wire.sent().await);

    alice
        .ilm
        .send_to(BOB, b"hello".to_vec())
        .await
        .expect("send");
    let hello = receive(&mut bob.inbox, Duration::from_secs(2)).await;
    // A reply well inside the window, as a conversation or an editing burst has.
    settled(&bob.backend).await;
    bob.ilm.send_to(ALICE, b"hi".to_vec()).await.expect("reply");

    assert!(
        eventually(Duration::from_secs(1), || outbound_is_empty(&alice.backend)).await,
        "the ACK riding on Bob's reply never retired Alice's message"
    );
    let reply = bob
        .wire
        .sent()
        .await
        .into_iter()
        .find(|s| matches!(s.kind, Kind::Message(_)))
        .expect("Bob sent his reply");
    assert_eq!(
        reply.extensions,
        FrameExtensions::Negotiated {
            piggybacked_ack: Some(hello.message_id()),
            codec: intersession_layer_messaging::Codec::None,
        },
        "the reply should have carried the ACK for {}",
        hello.message_id()
    );
    // Past the window: had the ACK not ridden along, the flush would have sent it.
    citadel_io::tokio::time::sleep(PIGGYBACK_ACK_WINDOW * 3).await;
    assert_eq!(
        standalone_acks(&bob.wire.sent().await),
        acks_before,
        "an ACK that rode on the reply was also sent alone"
    );
}

#[citadel_io::tokio::test]
async fn a_message_with_no_reply_is_acknowledged_alone_within_the_window() {
    let (alice, mut bob) = pair(PIGGYBACK, PIGGYBACK).await;

    let sent_at = Instant::now();
    alice
        .ilm
        .send_to(BOB, b"anyone there".to_vec())
        .await
        .expect("send");
    let message = receive(&mut bob.inbox, Duration::from_secs(2)).await;

    // Well under the one-second retransmit clock, so this is the flush and not
    // a retransmission being re-acknowledged.
    assert!(
        eventually(Duration::from_millis(500), || outbound_is_empty(
            &alice.backend
        ))
        .await,
        "nothing outbound from Bob, and his ACK never went alone"
    );
    let elapsed = sent_at.elapsed();
    let ack = bob
        .wire
        .sent()
        .await
        .into_iter()
        .find(|s| s.kind == Kind::Ack(message.message_id()))
        .expect("Bob sent the ACK alone");
    assert!(
        matches!(ack.extensions, FrameExtensions::Advertise(_)),
        "a standalone ACK still advertises: {:?}",
        ack.extensions
    );
    assert!(
        elapsed >= PIGGYBACK_ACK_WINDOW,
        "retired in {elapsed:?}, before the ACK could have been held"
    );
}

#[citadel_io::tokio::test]
async fn a_lost_carrier_frame_is_recovered_by_retransmission() {
    let (alice, mut bob) = pair(PIGGYBACK, PIGGYBACK).await;
    bob.wire
        .drop_when(|s| {
            matches!(
                s.extensions,
                FrameExtensions::Negotiated {
                    piggybacked_ack: Some(_),
                    ..
                }
            )
        })
        .await;

    alice
        .ilm
        .send_to(BOB, b"ping".to_vec())
        .await
        .expect("send");
    receive(&mut bob.inbox, Duration::from_secs(2)).await;
    settled(&bob.backend).await;
    bob.ilm
        .send_to(ALICE, b"pong".to_vec())
        .await
        .expect("reply");

    // The reply and the ACK it carried are both gone. Bob retransmits the reply;
    // Alice retransmits the ping, and Bob acknowledges the duplicate.
    let mut alice = alice;
    let pong = receive(&mut alice.inbox, Duration::from_secs(8)).await;
    assert_eq!(pong.contents(), &b"pong".to_vec());
    assert!(
        eventually(Duration::from_secs(8), || outbound_is_empty(&alice.backend)).await,
        "Alice's ping was never retired after its ACK was lost"
    );
    assert!(
        eventually(Duration::from_secs(8), || outbound_is_empty(&bob.backend)).await,
        "Bob's pong was never retired"
    );
    assert!(
        bob.wire.sent().await.iter().any(|s| s.dropped),
        "the test never dropped the carrier frame, so it proved nothing"
    );
}

#[citadel_io::tokio::test]
async fn a_burst_is_not_throttled_by_the_ack_window() {
    let (alice, mut bob) = pair(PIGGYBACK, PIGGYBACK).await;
    const BURST: usize = 48;
    let started = Instant::now();
    for n in 0..BURST {
        alice
            .ilm
            .send_to(BOB, format!("burst {n}").into_bytes())
            .await
            .expect("send");
    }
    for _ in 0..BURST {
        receive(&mut bob.inbox, Duration::from_secs(5)).await;
    }
    assert!(eventually(Duration::from_secs(5), || outbound_is_empty(&alice.backend)).await);
    let elapsed = started.elapsed();
    let acks = standalone_acks(&bob.wire.sent().await);
    // One ACK per PIGGYBACK_ACK_EVERY deliveries, not one per window timer:
    // held for the timer each time, six windows would cost six timers.
    assert!(
        acks <= BURST / 2,
        "{acks} ACKs for {BURST} messages; piggybacking should at least halve them"
    );
    assert!(
        elapsed < PIGGYBACK_ACK_WINDOW * (BURST as u32 / 8),
        "{BURST} messages took {elapsed:?}: the ACK window is throttling the send window"
    );
}
