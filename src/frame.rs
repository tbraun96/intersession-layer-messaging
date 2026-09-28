//! What ILM hands its transport, and what the transport hands back.
//!
//! ILM decides every extension; the transport only encodes what it is given.
//! That split is what lets the rule "never send a peer a format it has not
//! advertised" live in one place -- `negotiation.rs` -- instead of in every
//! transport.

use crate::capabilities::PeerCapabilities;
use crate::compression::Codec;
use crate::{MessageMetadata, Payload};

/// A payload plus the extensions ILM chose for it.
#[derive(Debug)]
pub struct OutboundFrame<M: MessageMetadata> {
    pub payload: Payload<M>,
    pub extensions: FrameExtensions<M::MessageId>,
}

impl<M: MessageMetadata> OutboundFrame<M> {
    /// A frame exactly as the legacy protocol sends it.
    pub fn legacy(payload: Payload<M>) -> Self {
        Self {
            payload,
            extensions: FrameExtensions::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameExtensions<Id> {
    /// The legacy frame, byte for byte.
    None,
    /// On an Ack or Poll: this node's capabilities, carried where a legacy
    /// peer does not look. Never on a data frame.
    Advertise(PeerCapabilities),
    /// On a data frame, and ONLY toward a peer that has advertised every
    /// extension used here. A transport encodes this in a format legacy peers
    /// cannot read, which is why ILM is the only thing that produces it.
    Negotiated {
        piggybacked_ack: Option<Id>,
        codec: Codec,
    },
}

/// What the transport learned about the sender from the frame's framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityEvidence {
    /// A data frame: it says nothing about what the sender accepts.
    Silent,
    /// An Ack or Poll, carrying exactly this advertisement --
    /// `PeerCapabilities::LEGACY` when it carried none, which is how a peer
    /// that has been replaced by an older build is noticed.
    Advertised(PeerCapabilities),
}

/// A received payload plus whatever its framing carried beside it.
#[derive(Debug)]
pub struct InboundFrame<M: MessageMetadata> {
    pub payload: Payload<M>,
    pub evidence: CapabilityEvidence,
    /// A cumulative ACK from the payload's sender, folded into its data frame.
    pub piggybacked_ack: Option<M::MessageId>,
}

impl<M: MessageMetadata> InboundFrame<M> {
    /// A frame as a legacy peer sends it: an Ack or Poll with no
    /// advertisement, or a data frame with no extensions.
    pub fn legacy(payload: Payload<M>) -> Self {
        let evidence = match payload {
            Payload::Message(_) => CapabilityEvidence::Silent,
            Payload::Ack { .. } | Payload::Poll { .. } => {
                CapabilityEvidence::Advertised(PeerCapabilities::LEGACY)
            }
        };
        Self {
            payload,
            evidence,
            piggybacked_ack: None,
        }
    }

    /// The frame a lossless transport delivers for `sent`, without encoding it.
    ///
    /// For transports that move typed values rather than bytes (in-process
    /// ones); there is nothing to compress, so the codec is not carried. Byte
    /// transports decode their own framing instead.
    pub fn delivered(sent: OutboundFrame<M>) -> Self {
        let OutboundFrame {
            payload,
            extensions,
        } = sent;
        let mut frame = Self::legacy(payload);
        match extensions {
            FrameExtensions::None => {}
            FrameExtensions::Advertise(capabilities) => {
                if matches!(frame.evidence, CapabilityEvidence::Advertised(_)) {
                    frame.evidence = CapabilityEvidence::Advertised(capabilities);
                }
            }
            FrameExtensions::Negotiated {
                piggybacked_ack, ..
            } => frame.piggybacked_ack = piggybacked_ack,
        }
        frame
    }
}
