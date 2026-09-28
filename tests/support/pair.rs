//! Two ILMs on one recorded wire, negotiated before the test begins.

use super::{eventually, Kind, Recorder, Sent};
use intersession_layer_messaging::testing::{InMemoryBackend, InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{Backend, IlmOptions, ILM};
use std::time::Duration;

pub const ALICE: usize = 1;
pub const BOB: usize = 2;

pub type Node = ILM<TestMessage, InMemoryBackend<TestMessage>, Channel, Recorder>;
pub type Channel = citadel_io::tokio::sync::mpsc::UnboundedSender<TestMessage>;
pub type Inbox = citadel_io::tokio::sync::mpsc::UnboundedReceiver<TestMessage>;

pub struct Peer {
    pub ilm: Node,
    pub wire: Recorder,
    pub backend: InMemoryBackend<TestMessage>,
    pub inbox: Inbox,
}

pub async fn pair(alice: IlmOptions, bob: IlmOptions) -> (Peer, Peer) {
    let network = InMemoryNetwork::<TestMessage>::new();
    let mut peers = Vec::new();
    for (id, options) in [(ALICE, alice), (BOB, bob)] {
        let wire = Recorder::new(network.add_peer(id).await);
        let backend = InMemoryBackend::<TestMessage>::new();
        let (tx, inbox) = citadel_io::tokio::sync::mpsc::unbounded_channel();
        let ilm = ILM::new(backend.clone(), tx, wire.clone(), options)
            .await
            .expect("construct ILM");
        peers.push(Peer {
            ilm,
            wire,
            backend,
            inbox,
        });
    }
    let bob = peers.pop().expect("bob");
    let alice = peers.pop().expect("alice");
    // Both advertise in their opening Poll; wait until each has heard the other,
    // so the first data frame is not racing the negotiation.
    for side in [&alice, &bob] {
        assert!(
            eventually(Duration::from_secs(2), || side.wire.heard_advertisement()).await,
            "the opening Polls never carried an advertisement"
        );
    }
    (alice, bob)
}

pub async fn outbound_is_empty(backend: &InMemoryBackend<TestMessage>) -> bool {
    backend
        .get_pending_outbound()
        .await
        .expect("read outbound")
        .is_empty()
}

/// Wait until `backend` has processed everything it received -- the ACK is
/// decided just before the inbound row is cleared -- so a reply sent now is
/// racing only the ACK window, not the receiver's own bookkeeping.
pub async fn settled(backend: &InMemoryBackend<TestMessage>) {
    assert!(
        eventually(Duration::from_secs(2), || async {
            backend
                .get_pending_inbound()
                .await
                .expect("read inbound")
                .is_empty()
        })
        .await,
        "the receiver never finished processing"
    );
}

pub async fn receive(inbox: &mut Inbox, within: Duration) -> TestMessage {
    citadel_io::tokio::time::timeout(within, inbox.recv())
        .await
        .expect("delivered in time")
        .expect("inbox open")
}

pub fn standalone_acks(sent: &[Sent]) -> usize {
    sent.iter()
        .filter(|s| matches!(s.kind, Kind::Ack(_)))
        .count()
}
