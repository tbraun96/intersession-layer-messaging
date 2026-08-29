//! A link that is waiting for an acknowledgement must not narrate the wait.
//!
//! `[ILM-BLOCKED]` fired at `warn!` every poll cycle -- five times a second per
//! peer -- for as long as a message went unacknowledged. One stuck link in one
//! CI run produced hundreds of consecutive lines of it, and in the WASM client
//! each is a synchronous console write on the browser's main thread: the loop
//! complaining about starvation was competing for the thread with the thing it
//! was complaining about.
//!
//! The transition INTO blocked is the news. Everything after it is the same
//! news again, and this pins that difference -- a warning that fires once per
//! stuck link rather than once per cycle.

use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::ILM;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ALICE: usize = 1;
const BOB: usize = 2;

/// Counts the log lines the crate emits, by level.
struct Counting {
    warnings: Arc<Mutex<Vec<String>>>,
}

impl log::Log for Counting {
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
async fn a_blocked_peer_warns_once_however_long_it_waits() {
    let warnings: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    // `set_boxed_logger` is process-wide and refuses a second call, so a failure
    // here means another test in this binary installed one first -- there is
    // only this test in it.
    let _ = log::set_boxed_logger(Box::new(Counting {
        warnings: warnings.clone(),
    }));
    log::set_max_level(log::LevelFilter::Debug);

    let network = InMemoryNetwork::<TestMessage>::new();
    let alice_wire = network.add_peer(ALICE).await;
    // Bob is NEVER constructed: nothing acknowledges, so the link stays blocked
    // for the whole test, which is exactly the state under measurement.
    let _bob_wire = network.add_peer(BOB).await;

    let (alice_tx, _alice_rx) = citadel_io::tokio::sync::mpsc::unbounded_channel::<TestMessage>();
    let alice = ILM::new(InMemoryBackend::<TestMessage>::new(), alice_tx, alice_wire)
        .await
        .expect("construct Alice");

    alice.send_to(BOB, b"hello".to_vec()).await.expect("queue");

    // Long enough for many poll cycles at 200ms.
    citadel_io::tokio::time::sleep(Duration::from_secs(5)).await;

    let blocked: usize = warnings
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains("[ILM-BLOCKED]"))
        .count();

    assert!(
        blocked <= 2,
        "a blocked link warned {blocked} times in five seconds; it should announce itself once"
    );
}
