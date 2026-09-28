//! What a peer has said it understands, and what this node will say about itself.
//!
//! Nothing here is ever assumed. A peer is treated as a legacy peer -- one that
//! understands only the original frames -- until it has advertised otherwise,
//! and it goes back to being one the moment a control frame arrives from it
//! without an advertisement (an old build, or a reload onto one). Sending an
//! extension a peer has not proven it can read is how a compressed frame ends
//! up rendered as a chat message on an old client, so the rule is absolute.

use crate::compression::{Codec, CodecSet};
use crate::options::IlmOptions;

/// Protocol features plus the SET of codecs a peer can decode.
///
/// The codec set is advertised as a whole, so a peer with rill but not zstd,
/// or deflate but not brotli, is served exactly what it has.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerCapabilities {
    flags: u8,
    codecs: CodecSet,
}

impl PeerCapabilities {
    /// The receiver folds a piggybacked cumulative ACK out of a data frame.
    pub const PIGGYBACK_ACKS: u8 = 0b0000_0001;

    const KNOWN_FLAGS: u8 = Self::PIGGYBACK_ACKS;

    /// Nothing beyond the legacy frames: what every peer is until it says otherwise.
    pub const LEGACY: Self = Self {
        flags: 0,
        codecs: CodecSet::EMPTY,
    };

    pub const fn new(piggyback_acks: bool, codecs: CodecSet) -> Self {
        Self {
            flags: if piggyback_acks {
                Self::PIGGYBACK_ACKS
            } else {
                0
            },
            codecs,
        }
    }

    /// Unknown flags and codec ids are dropped: a feature this build cannot
    /// name is one it cannot use, and keeping it would let a later check
    /// answer for something nobody implemented here.
    pub const fn from_wire(flags: u8, codecs: u32) -> Self {
        Self {
            flags: flags & Self::KNOWN_FLAGS,
            codecs: CodecSet::from_wire(codecs),
        }
    }

    pub const fn flags_to_wire(self) -> u8 {
        self.flags
    }

    pub const fn codecs(self) -> CodecSet {
        self.codecs
    }

    pub const fn is_legacy(self) -> bool {
        self.flags == 0 && self.codecs.is_empty()
    }

    pub const fn piggybacks_acks(self) -> bool {
        self.flags & Self::PIGGYBACK_ACKS != 0
    }

    pub const fn decodes(self, codec: Codec) -> bool {
        self.codecs.contains(codec)
    }

    /// What both ends support: the only set either may use toward the other.
    pub const fn intersect(self, other: Self) -> Self {
        Self {
            flags: self.flags & other.flags,
            codecs: self.codecs.intersect(other.codecs),
        }
    }

    /// What this node advertises, derived from its options and nothing else.
    ///
    /// A codec is advertised only if it is both compiled in and enabled: a node
    /// whose compression is `Disabled` asks peers not to compress toward it,
    /// which keeps "off" meaning off in both directions.
    pub fn local(options: &IlmOptions) -> Self {
        Self::new(
            options.piggyback_acks,
            options
                .dynamic_compression
                .codecs()
                .intersect(CodecSet::compiled()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::DynamicCompression;

    #[test]
    fn unknown_flags_and_codec_ids_are_not_kept() {
        let caps = PeerCapabilities::from_wire(0xff, u32::MAX);
        assert_eq!(caps.flags_to_wire(), PeerCapabilities::KNOWN_FLAGS);
        for codec in Codec::ALL {
            assert!(caps.decodes(codec), "{codec:?} is known and must survive");
        }
        assert_eq!(caps.codecs().to_wire() >> (Codec::ALL.len() as u32), 0);
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
    fn legacy_decodes_only_identity() {
        for codec in Codec::ALL {
            assert_eq!(
                PeerCapabilities::LEGACY.decodes(codec),
                codec == Codec::Identity
            );
        }
        assert!(!PeerCapabilities::LEGACY.piggybacks_acks());
    }

    #[test]
    fn a_partial_codec_set_intersects_to_what_both_have() {
        let rill_and_deflate =
            PeerCapabilities::new(true, CodecSet::of(&[Codec::Rill, Codec::Deflate]));
        let brotli_and_deflate =
            PeerCapabilities::new(false, CodecSet::of(&[Codec::Brotli, Codec::Deflate]));
        let shared = rill_and_deflate.intersect(brotli_and_deflate);
        assert!(shared.decodes(Codec::Deflate));
        assert!(!shared.decodes(Codec::Rill));
        assert!(!shared.decodes(Codec::Brotli));
        assert!(!shared.piggybacks_acks());
    }
}
