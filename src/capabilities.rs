//! What a peer has said it understands, and what this node will say about itself.
//!
//! Nothing here is ever assumed. A peer is treated as a legacy peer -- one that
//! understands only the original frames -- until it has advertised otherwise,
//! and it goes back to being one the moment a control frame arrives from it
//! without an advertisement (an old build, or a reload onto one). Sending an
//! extension a peer has not proven it can read is how a compressed frame ends
//! up rendered as a chat message on an old client, so the rule is absolute.

use crate::compression::{Codec, DynamicCompression};
use crate::options::IlmOptions;

/// A set of wire extensions, as one byte on the wire.
///
/// Unknown bits are dropped on decode rather than kept: a bit this build does
/// not know is a feature it cannot use, and keeping it would let a later
/// `contains` answer for something nobody implemented here.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerCapabilities {
    bits: u8,
}

impl PeerCapabilities {
    /// The receiver folds a piggybacked cumulative ACK out of a data frame.
    pub const PIGGYBACK_ACKS: u8 = 0b0000_0001;
    /// The receiver can decode brotli-compressed contents.
    pub const CODEC_BROTLI: u8 = 0b0000_0010;
    /// The receiver can decode raw-deflate-compressed contents.
    pub const CODEC_DEFLATE: u8 = 0b0000_0100;

    const KNOWN: u8 = Self::PIGGYBACK_ACKS | Self::CODEC_BROTLI | Self::CODEC_DEFLATE;

    /// Nothing beyond the legacy frames: what every peer is until it says otherwise.
    pub const LEGACY: Self = Self { bits: 0 };

    pub const fn from_wire(byte: u8) -> Self {
        Self {
            bits: byte & Self::KNOWN,
        }
    }

    pub const fn to_wire(self) -> u8 {
        self.bits
    }

    pub const fn is_legacy(self) -> bool {
        self.bits == 0
    }

    pub const fn piggybacks_acks(self) -> bool {
        self.bits & Self::PIGGYBACK_ACKS != 0
    }

    pub const fn decodes(self, codec: Codec) -> bool {
        match codec {
            Codec::None => true,
            Codec::Brotli => self.bits & Self::CODEC_BROTLI != 0,
            Codec::Deflate => self.bits & Self::CODEC_DEFLATE != 0,
        }
    }

    /// What both ends support: the only set either may use toward the other.
    pub const fn intersect(self, other: Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }

    /// What this node advertises, derived from its options and nothing else.
    ///
    /// A codec is advertised only if it is both compiled in and enabled: a node
    /// whose compression is `Disabled` asks peers not to compress toward it,
    /// which keeps "off" meaning off in both directions.
    pub fn local(options: &IlmOptions) -> Self {
        let mut bits = 0;
        if options.piggyback_acks {
            bits |= Self::PIGGYBACK_ACKS;
        }
        for codec in options.dynamic_compression.codecs() {
            bits |= match codec {
                Codec::None => 0,
                Codec::Brotli => Self::CODEC_BROTLI,
                Codec::Deflate => Self::CODEC_DEFLATE,
            };
        }
        Self { bits }
    }
}

impl DynamicCompression {
    /// The codecs this setting permits, strongest first.
    pub fn codecs(self) -> &'static [Codec] {
        match self {
            DynamicCompression::Disabled => &[],
            #[cfg(feature = "compression-brotli")]
            DynamicCompression::Brotli => &[Codec::Brotli],
            #[cfg(feature = "compression-deflate")]
            DynamicCompression::Deflate => &[Codec::Deflate],
            #[cfg(all(feature = "compression-brotli", feature = "compression-deflate"))]
            DynamicCompression::All => &[Codec::Brotli, Codec::Deflate],
            #[cfg(all(feature = "compression-brotli", not(feature = "compression-deflate")))]
            DynamicCompression::All => &[Codec::Brotli],
            #[cfg(all(feature = "compression-deflate", not(feature = "compression-brotli")))]
            DynamicCompression::All => &[Codec::Deflate],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_bits_are_not_kept() {
        let caps = PeerCapabilities::from_wire(0xff);
        assert_eq!(caps.to_wire(), PeerCapabilities::KNOWN);
    }

    #[test]
    fn everything_off_advertises_nothing() {
        let options = IlmOptions {
            piggyback_acks: false,
            dynamic_compression: DynamicCompression::Disabled,
        };
        assert!(PeerCapabilities::local(&options).is_legacy());
    }

    #[test]
    fn legacy_decodes_only_uncompressed() {
        assert!(PeerCapabilities::LEGACY.decodes(Codec::None));
        assert!(!PeerCapabilities::LEGACY.decodes(Codec::Brotli));
        assert!(!PeerCapabilities::LEGACY.decodes(Codec::Deflate));
        assert!(!PeerCapabilities::LEGACY.piggybacks_acks());
    }
}
