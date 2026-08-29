//! A stuck link reports how much is piled up behind its head.
//!
//! `[ILM-OUTBOUND]` reports a total across every peer, once per cycle, at
//! `info!`. `[ILM-BLOCKED]` reports the head message, once per transition.
//! Neither said how many messages were queued for the peer that was stuck, and
//! that is the number which separates one lost acknowledgement from a link
//! that has stopped.
//!
//! A CI run of the file-manager suite showed exactly that gap: one side sat at
//! `12 messages`, then `13 messages`, growing and never draining, while the
//! other sat at `1` and recovered. Telling those apart meant diffing
//! consecutive `info!` lines by eye, and the only line that fires once per
//! stuck link -- the one a reader actually finds -- carried neither number.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;

/// Captures the crate's warnings so they can be read back.
struct Capturing {
    warnings: Arc<Mutex<Vec<String>>>,
}

impl log::Log for Capturing {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        if record.level() <= log::Level::Warn {
            self.warnings
                .lock()
                .unwrap()
                .push(format!("{}", record.args()));
        }
    }
    fn flush(&self) {}
}

#[citadel_io::tokio::test]
async fn the_blocked_warning_names_the_queue_depth() {
    let warnings: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let _ = log::set_boxed_logger(Box::new(Capturing {
        warnings: warnings.clone(),
    }));
    log::set_max_level(log::LevelFilter::Warn);

    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
    // Never constructed, so nothing acknowledges and the link stays blocked --
    // which is the state under measurement.
    let _bob_wire = network.add_peer(BOB).await;

    let (alice_tx, _alice_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let alice = ILM::new(InMemoryBackend::<TestMessage>::new(), alice_tx, alice_wire)
        .await
        .expect("construct Alice");

    // Five behind one head. The head goes out and is never acknowledged; the
    // other four cannot follow it, and their number is the thing being pinned.
    for index in 0..5u8 {
        alice
            .send_to(BOB, vec![index])
            .await
            .expect("queue a message");
    }

    citadel_io::tokio::time::sleep(Duration::from_secs(3)).await;

    let lines = warnings.lock().unwrap().clone();
    let blocked: Vec<&String> = lines
        .iter()
        .filter(|line| line.contains("[ILM-BLOCKED]"))
        .collect();

    assert!(
        !blocked.is_empty(),
        "the link is blocked and said nothing about it; lines were {lines:?}"
    );
    assert!(
        blocked.iter().any(|line| line.contains("5 queued")),
        "a blocked link did not report how much was behind its head; lines were {blocked:?}"
    );
}
