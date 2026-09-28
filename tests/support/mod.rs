//! A wire that records every frame in both directions and can drop chosen ones.
//!
//! The extensions ILM chooses are the thing under test, so the wire keeps them
//! exactly as handed over. It moves typed frames rather than bytes; byte
//! framing is the transport's job and is tested where the transport lives.

#![allow(dead_code)]

pub mod pair;

use async_trait::async_trait;
use citadel_io::tokio::sync::Mutex;
use intersession_layer_messaging::testing::{InMemoryNetwork, TestMessage};
use intersession_layer_messaging::{
    CapabilityEvidence, FrameExtensions, InboundFrame, MessageMetadata, NetworkError,
    OutboundFrame, Payload, UnderlyingSessionTransport,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Message(usize),
    Ack(usize),
    Poll,
}

impl Kind {
    fn of(payload: &Payload<TestMessage>) -> Self {
        match payload {
            Payload::Message(m) => Kind::Message(m.message_id()),
            Payload::Ack { message_id, .. } => Kind::Ack(*message_id),
            Payload::Poll { .. } => Kind::Poll,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Sent {
    pub kind: Kind,
    pub extensions: FrameExtensions<usize>,
    pub dropped: bool,
    pub at: Instant,
}

#[derive(Clone, Debug)]
pub struct Received {
    pub kind: Kind,
    pub evidence: CapabilityEvidence,
    pub piggybacked_ack: Option<usize>,
}

type DropRule = Box<dyn Fn(&Sent) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct Recorder {
    inner: InMemoryNetwork<TestMessage>,
    pub sent: Arc<Mutex<Vec<Sent>>>,
    pub received: Arc<Mutex<Vec<Received>>>,
    /// Dropped frames report success: a sender cannot tell a lost packet.
    drop_rule: Arc<Mutex<Option<DropRule>>>,
}

impl Recorder {
    pub fn new(inner: InMemoryNetwork<TestMessage>) -> Self {
        Self {
            inner,
            sent: Arc::default(),
            received: Arc::default(),
            drop_rule: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn drop_when(&self, rule: impl Fn(&Sent) -> bool + Send + Sync + 'static) {
        *self.drop_rule.lock().await = Some(Box::new(rule));
    }

    pub async fn sent(&self) -> Vec<Sent> {
        self.sent.lock().await.clone()
    }

    pub async fn received(&self) -> Vec<Received> {
        self.received.lock().await.clone()
    }

    /// True once this side has heard `capabilities` advertised by its peer.
    pub async fn heard_advertisement(&self) -> bool {
        self.received().await.iter().any(
            |r| matches!(r.evidence, CapabilityEvidence::Advertised(caps) if !caps.is_legacy()),
        )
    }
}

#[async_trait]
impl UnderlyingSessionTransport for Recorder {
    type Message = TestMessage;

    async fn next_message(&self) -> Option<InboundFrame<Self::Message>> {
        let frame = self.inner.next_message().await?;
        self.received.lock().await.push(Received {
            kind: Kind::of(&frame.payload),
            evidence: frame.evidence,
            piggybacked_ack: frame.piggybacked_ack,
        });
        Some(frame)
    }

    async fn send_message(
        &self,
        frame: OutboundFrame<Self::Message>,
    ) -> Result<(), NetworkError<OutboundFrame<Self::Message>>> {
        let mut record = Sent {
            kind: Kind::of(&frame.payload),
            extensions: frame.extensions,
            dropped: false,
            at: Instant::now(),
        };
        let mut rule = self.drop_rule.lock().await;
        if let Some(should_drop) = rule.as_ref() {
            if should_drop(&record) {
                record.dropped = true;
                // One-shot: the rule describes a single loss.
                *rule = None;
            }
        }
        drop(rule);
        let dropped = record.dropped;
        self.sent.lock().await.push(record);
        if dropped {
            return Ok(());
        }
        self.inner.send_message(frame).await
    }

    async fn connected_peers(&self) -> Vec<usize> {
        self.inner.connected_peers().await
    }

    fn local_id(&self) -> usize {
        self.inner.local_id()
    }
}

/// Poll `condition` until it holds or `within` passes; true if it held.
pub async fn eventually<F, Fut>(within: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        citadel_io::tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
