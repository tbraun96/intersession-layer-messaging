//! The in-memory test backend must not do filesystem I/O.
//!
//! Its key/value store was file-backed on native: every `store_value` was a
//! synchronous `std::fs::write` on a tokio worker thread. Two of those sit on a
//! message's critical path -- the sender's `mark_sent` right after the network
//! send, and the receiver's `record_arrival` right before the inbound nudge --
//! and on ubuntu-latest one write in a few hundred stalls for 60-250ms. The
//! sender's stall is the worse one: the receiver's task was woken into the
//! sender's worker LIFO slot, which no other worker can steal, so it waited out
//! the whole write. That is what `test_send_after_idle_does_not_wait_for_the_
//! outbound_poll_timer` saw as a single ~200ms leg, and nothing in the nudge
//! path was at fault.
//!
//! Deterministic, no timing: the process temp dir is pointed at a fresh empty
//! directory for the duration of the test, a backend is exercised end to end,
//! and the directory must still be empty. With the file-backed store it gains a
//! `<uuid>/` directory holding `last_sent.bin` and friends.
use citadel_io::tokio::sync::mpsc;
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, MessageMetadata, ILM};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_memory_backend_never_touches_the_disk() {
    let sandbox = std::env::temp_dir().join(format!(
        "ilm-no-fs-{}-{}",
        std::process::id(),
        intersession_layer_messaging::platform_timestamp_micros()
    ));
    std::fs::create_dir_all(&sandbox).expect("sandbox");
    // `std::env::temp_dir` reads these on every call. Set for the whole
    // process: this crate's only temp-dir user is the backend under test.
    let previous: Vec<(&str, Option<String>)> = ["TMPDIR", "TMP", "TEMP"]
        .into_iter()
        .map(|k| (k, std::env::var(k).ok()))
        .collect();
    for (k, _) in &previous {
        std::env::set_var(k, &sandbox);
    }
    assert_eq!(
        std::env::temp_dir(),
        sandbox,
        "temp dir override did not take"
    );

    // A backend used the way the ILM uses it: a K/V write, a K/V read, and a
    // full send -> deliver -> ACK round trip, which writes last_sent,
    // received_messages, last_received_from, last_delivered and last_acked.
    let backend1 = InMemoryBackend::<TestMessage>::default();
    let backend2 = InMemoryBackend::<TestMessage>::default();
    backend1.store_value("probe", b"1").await.expect("store");
    assert_eq!(
        backend1.load_value("probe").await.expect("load"),
        Some(b"1".to_vec())
    );

    let network1 = InMemoryNetwork::<TestMessage>::new().add_peer(1).await;
    let network2 = network1.add_peer(2).await;
    let (tx1, _rx1) = mpsc::unbounded_channel();
    let (tx2, mut rx2) = mpsc::unbounded_channel();
    let ilm1 = ILM::new(backend1.clone(), tx1, network1)
        .await
        .expect("ilm1");
    let _ilm2 = ILM::new(backend2.clone(), tx2, network2)
        .await
        .expect("ilm2");
    ilm1.send_to(2, vec![7u8]).await.expect("send");
    let got = citadel_io::tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("delivered within 5s")
        .expect("channel open");
    assert_eq!(got.message_id(), 0);
    // Let the ACK land so last_acked is written too.
    citadel_io::tokio::time::sleep(Duration::from_millis(300)).await;

    let entries: Vec<_> = std::fs::read_dir(&sandbox)
        .expect("read sandbox")
        .map(|e| e.expect("entry").path())
        .collect();

    for (k, v) in previous {
        match v {
            Some(v) => std::env::set_var(k, v),
            None => std::env::remove_var(k),
        }
    }
    let _ = std::fs::remove_dir_all(&sandbox);

    assert!(
        entries.is_empty(),
        "the in-memory backend wrote to the filesystem: {entries:?}. Every such write is a \
         blocking syscall on a runtime worker, and two of them are on the message path."
    );
}
