//! Which extensions each peer may receive, and the ACKs waiting to ride along.
//!
//! The one rule: an extension is used toward a peer only if this node enabled
//! it AND the peer advertised it. Until a peer has advertised, it is a legacy
//! peer and every frame to it is the legacy frame.

use crate::capabilities::PeerCapabilities;
use crate::compression::{can_compress, CompressionHint};
use crate::frame::{CapabilityEvidence, CompressionPlan, FrameExtensions};
use crate::options::IlmOptions;
use crate::MessageMetadata;
use dashmap::DashMap;

/// The longest an ACK waits for a data frame to ride on before it is sent alone.
///
/// Long enough to catch the reply in a conversation or the next update in an
/// editing burst, short enough to sit far inside the one-second retransmit
/// clock, so a lone message is retired well before its sender repeats it.
pub const PIGGYBACK_ACK_WINDOW: std::time::Duration = std::time::Duration::from_millis(40);

/// Deliveries from one peer after which its ACK goes out at once.
///
/// The sender's window is eight. Holding the ACK for all of them would stall a
/// burst on this timer once per window; releasing it at half keeps the sender
/// streaming while still answering four messages with one ACK.
pub const PIGGYBACK_ACK_EVERY: u32 = 4;

pub(crate) struct Negotiation<M: MessageMetadata> {
    local: PeerCapabilities,
    compresses: bool,
    peers: DashMap<M::PeerId, PeerCapabilities>,
    /// Highest id to acknowledge per peer, and how many deliveries it covers.
    pending_acks: DashMap<M::PeerId, (M::MessageId, u32)>,
    hints: DashMap<(M::PeerId, M::MessageId), CompressionHint>,
}

/// What `defer_ack` decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AckPlan<Id> {
    /// The peer cannot take a piggybacked ACK: send this one now, alone.
    SendNow,
    /// Deferred, and the first to be: start the flush clock.
    Deferred,
    /// Merged into one already waiting; its clock is running.
    Merged,
    /// Enough deliveries are waiting that the ACK covering them goes now.
    Release(Id),
}

impl<M: MessageMetadata> Negotiation<M> {
    pub(crate) fn new(options: &IlmOptions) -> Self {
        Self {
            local: PeerCapabilities::local(options),
            compresses: !PeerCapabilities::local(options).codecs().is_empty(),
            peers: DashMap::new(),
            pending_acks: DashMap::new(),
            hints: DashMap::new(),
        }
    }

    fn shared(&self, peer: &M::PeerId) -> PeerCapabilities {
        self.peers
            .get(peer)
            .map(|caps| caps.intersect(self.local))
            .unwrap_or(PeerCapabilities::LEGACY)
    }

    pub(crate) fn observe(&self, peer: M::PeerId, evidence: CapabilityEvidence) {
        if let CapabilityEvidence::Advertised(capabilities) = evidence {
            let previous = self.peers.insert(peer, capabilities);
            // Only the transition is news: every Ack and Poll re-advertises.
            if previous != Some(capabilities) {
                log::info!(target: "ism", "[ILM-CAPS] peer {peer} advertises {capabilities:?} (was {previous:?})");
            }
        }
    }

    /// Extensions for an Ack or Poll: the advertisement, if there is anything
    /// to advertise. With every option off this is `None`, so the frame is
    /// byte-identical to the legacy one.
    pub(crate) fn control_extensions(&self) -> FrameExtensions<M::MessageId> {
        if self.local.is_legacy() {
            FrameExtensions::None
        } else {
            FrameExtensions::Advertise(self.local)
        }
    }

    pub(crate) fn record_hint(&self, peer: M::PeerId, id: M::MessageId, hint: CompressionHint) {
        if self.compresses {
            self.hints.insert((peer, id), hint);
        }
    }

    /// Drop the hints for every id the peer has acknowledged.
    pub(crate) fn forget_hints_through(&self, peer: M::PeerId, acked: M::MessageId) {
        if !self.hints.is_empty() {
            self.hints
                .retain(|(hint_peer, id), _| !(*hint_peer == peer && *id <= acked));
        }
    }

    pub(crate) fn forget_hint(&self, peer: M::PeerId, id: M::MessageId) {
        self.hints.remove(&(peer, id));
    }

    pub(crate) fn defer_ack(&self, peer: M::PeerId, id: M::MessageId) -> AckPlan<M::MessageId> {
        if !self.shared(&peer).piggybacks_acks() {
            return AckPlan::SendNow;
        }
        let (first, highest, count) = self.merge_ack(peer, id);
        if count >= PIGGYBACK_ACK_EVERY {
            self.pending_acks.remove(&peer);
            AckPlan::Release(highest)
        } else if first {
            AckPlan::Deferred
        } else {
            AckPlan::Merged
        }
    }

    pub(crate) fn take_ack(&self, peer: &M::PeerId) -> Option<M::MessageId> {
        self.pending_acks.remove(peer).map(|(_, (id, _))| id)
    }

    /// Put back an ACK whose frame never left, merging with any newer one.
    /// Returns true if nothing was waiting, i.e. a flush must be scheduled.
    pub(crate) fn restore_ack(&self, peer: M::PeerId, id: M::MessageId) -> bool {
        self.merge_ack(peer, id).0
    }

    /// Fold `id` into the peer's waiting ACK. Cumulative, so the highest wins.
    fn merge_ack(&self, peer: M::PeerId, id: M::MessageId) -> (bool, M::MessageId, u32) {
        let mut entry = self.pending_acks.entry(peer).or_insert((id, 0));
        let first = entry.1 == 0;
        entry.0 = std::cmp::max(entry.0, id);
        entry.1 += 1;
        (first, entry.0, entry.1)
    }

    /// Extensions for a data frame, taking any ACK waiting for this peer.
    pub(crate) fn message_extensions(
        &self,
        peer: M::PeerId,
        id: M::MessageId,
    ) -> FrameExtensions<M::MessageId> {
        let shared = self.shared(&peer);
        if shared.is_legacy() {
            return FrameExtensions::None;
        }
        let piggybacked_ack = if shared.piggybacks_acks() {
            self.take_ack(&peer)
        } else {
            None
        };
        let codecs = shared.codecs();
        let compression = self
            .hints
            .get(&(peer, id))
            .map(|hint| *hint)
            .filter(|hint| can_compress(*hint, codecs))
            .map(|hint| CompressionPlan { hint, codecs });
        if piggybacked_ack.is_none() && compression.is_none() {
            FrameExtensions::None
        } else {
            FrameExtensions::Negotiated {
                piggybacked_ack,
                compression,
            }
        }
    }
}
