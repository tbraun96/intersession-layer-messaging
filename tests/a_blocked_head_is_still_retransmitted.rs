//! The head keeps its own retransmit clock, even while the window drains.
//!
//! `blocked_count` is the retransmit clock AND the stale-state clock: the head
//! is resent every `RETRANSMIT_AFTER_BLOCKS` cycles it stays blocked, and its
//! state is cleared after `MAX_CONSECUTIVE_BLOCKS`. Both read one counter per
//! peer.
//!
//! That counter was reset on any successful send. Under stop-and-wait it could
//! only ever be the head — nothing else was sendable — so resetting was right.
//! With a send window it is usually NOT the head: the head sits unacknowledged
//! while the messages behind it go out, and each of those reset the head's
//! clock. It never reached five, so a genuinely lost head was never resent,
//! and the only thing that eventually moved the link was the ten-second
//! stale-state recovery.
//!
//! CI showed the shape after the window landed: the head advancing 0 → 3 → 9
//! while `[ILM-BLOCKED-RECOVERY]` fired at fifty consecutive blocks, which is
//! the path that should not be reached on an ordinary lost packet.
//!
//! **The bug this was written to prove does not exist**, and the reason is
//! worth writing down. Nothing behind a lost head is ever acknowledged — the
//! receiver holds it, because acknowledging past a gap would cumulatively
//! retire the gap. So `outstanding` only grows: the window fills within
//! `SEND_WINDOW` sends, the sends stop, and the head's clock runs freely from
//! there. The reset can delay the first retransmission by at most a window's
//! worth of cycles; it cannot pin it.
//!
//! The contiguity gate is what makes that true, so this test is kept as the
//! thing that would notice if the gate went away and left the reset behind.

use async_trait::async_trait;
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    MessageMetadata, NetworkError, Payload, UnderlyingSessionTransport, ILM,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;

/// Alice's wire, which swallows every transmission of message 0 for a while.
///
/// The head is genuinely lost and stays lost, so the only thing that can move
/// the link is a retransmission of it.
#[derive(Clone)]
struct SwallowsTheHead {
    inner: InMemoryNetwork<TestMessage>,
    swallowing: Arc<AtomicBool>,
}

#[async_trait]
impl UnderlyingSessionTransport for SwallowsTheHead {
    type Message = TestMessage;

    async fn next_message(&self) -> Option<Payload<Self::Message>> {
        self.inner.next_message().await
    }

    async fn send_message(
        &self,
        message: Payload<Self::Message>,
    ) -> Result<(), NetworkError<Payload<Self::Message>>> {
        if let Payload::Message(m) = &message {
            if m.message_id() == 0 && self.swallowing.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
        self.inner.send_message(message).await
    }

    async fn connected_peers(&self) -> Vec<usize> {
        self.inner.connected_peers().await
    }

    fn local_id(&self) -> usize {
        self.inner.local_id()
    }
}

/// Captures the crate's `info!` lines so retransmissions can be counted.
struct Capturing {
    lines: Arc<Mutex<Vec<String>>>,
}

impl log::Log for Capturing {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        if record.level() <= log::Level::Info {
            self.lines
                .lock()
                .unwrap()
                .push(format!("{}", record.args()));
        }
    }
    fn flush(&self) {}
}

#[citadel_io::tokio::test]
async fn the_head_is_resent_while_the_window_keeps_sending() {
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let _ = log::set_boxed_logger(Box::new(Capturing {
        lines: lines.clone(),
    }));
    log::set_max_level(log::LevelFilter::Info);

    let network = InMemoryNetwork::<TestMessage>::new();
    let swallowing = Arc::new(AtomicBool::new(true));
    let alice_wire = SwallowsTheHead {
        inner: network.add_peer(ALICE).await,
        swallowing: swallowing.clone(),
    };
    let bob_wire = network.add_peer(BOB).await;

    let (alice_tx, _alice_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let (bob_tx, _bob_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();

    let alice = ILM::new(InMemoryBackend::<TestMessage>::new(), alice_tx, alice_wire)
        .await
        .expect("construct Alice");
    let _bob = ILM::new(InMemoryBackend::<TestMessage>::new(), bob_tx, bob_wire)
        .await
        .expect("construct Bob");

    // A trickle, not a burst: the window must never fill, or sends stop and the
    // head's clock is free to run.
    alice.send_to(BOB, b"head".to_vec()).await.expect("queue");
    for _ in 0..40u8 {
        citadel_io::tokio::time::sleep(Duration::from_millis(120)).await;
        alice.send_to(BOB, b"behind".to_vec()).await.expect("queue");
    }

    let resends: usize = lines
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains("[ILM-RETRANSMIT]") && line.contains("msg_id=0"))
        .count();

    assert!(
        resends > 0,
        "the head was never resent in five seconds of blocking while the window \
         kept sending behind it; its retransmit clock is being reset by those sends"
    );
}
