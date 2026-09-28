//! The codec registry: stable wire ids, the set type peers advertise, and the
//! one trait every implementation sits behind.
//!
//! Adding a codec is an entry here plus a cargo feature -- never a wire change.
//! Its id is already reserved below, a peer that does not have it simply never
//! advertises it, and `policy` already knows where it ranks.

use super::CompressionError;

/// A codec's id on the wire. Ids are permanent: a retired codec keeps its id
/// forever, and a new one takes the next unused id.
///
/// | id | codec    | status                                               |
/// |----|----------|------------------------------------------------------|
/// | 0  | identity | always; the contents are the application's bytes     |
/// | 1  | brotli   | feature `compression-brotli`                         |
/// | 2  | deflate  | feature `compression-deflate` (raw deflate, no zlib)  |
/// | 3  | rill     | RESERVED: small-frame codec, not yet in this crate   |
/// | 4  | zstd     | feature `compression-zstd` (magicless frames, zstd-rs) |
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Codec {
    Identity,
    Brotli,
    Deflate,
    Rill,
    Zstd,
}

impl Codec {
    pub const ALL: [Codec; 5] = [
        Codec::Identity,
        Codec::Brotli,
        Codec::Deflate,
        Codec::Rill,
        Codec::Zstd,
    ];

    pub const fn to_wire(self) -> u8 {
        match self {
            Codec::Identity => 0,
            Codec::Brotli => 1,
            Codec::Deflate => 2,
            Codec::Rill => 3,
            Codec::Zstd => 4,
        }
    }

    pub const fn from_wire(id: u8) -> Result<Self, CompressionError> {
        match id {
            0 => Ok(Codec::Identity),
            1 => Ok(Codec::Brotli),
            2 => Ok(Codec::Deflate),
            3 => Ok(Codec::Rill),
            4 => Ok(Codec::Zstd),
            other => Err(CompressionError::UnknownCodec(other)),
        }
    }

    /// This build's implementation, if it was compiled in.
    pub fn implementation(self) -> Option<&'static dyn FrameCodec> {
        match self {
            #[cfg(feature = "compression-brotli")]
            Codec::Brotli => Some(&super::brotli_codec::Brotli),
            #[cfg(feature = "compression-deflate")]
            Codec::Deflate => Some(&super::deflate_codec::Deflate),
            #[cfg(feature = "compression-zstd")]
            Codec::Zstd => Some(&super::zstd_codec::Zstd),
            _ => None,
        }
    }
}

/// Codec-specific tuning, chosen by `policy` and read only by the encoder.
/// A decoder never needs it: every format here is self-describing.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Params {
    /// Brotli quality, deflate level, zstd level.
    pub level: u32,
    /// Base-2 log of the window, where the codec has one.
    pub window_log: u32,
}

/// One compression format. Pure: no I/O, no threads, no state between frames.
pub trait FrameCodec: Sync {
    fn compress(&self, raw: &[u8], params: &Params) -> Result<Vec<u8>, CompressionError>;
    /// Refuses, rather than truncates, output past `cap` bytes.
    fn decompress(&self, bytes: &[u8], cap: usize) -> Result<Vec<u8>, CompressionError>;
}

/// A set of non-identity codecs, one bit per wire id. Identity is implicit:
/// every peer can read uncompressed contents.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CodecSet {
    bits: u32,
}

impl CodecSet {
    pub const EMPTY: Self = Self { bits: 0 };

    /// Every codec id this build knows, whether or not it is compiled in.
    const KNOWN: u32 = {
        let mut bits = 0u32;
        let mut i = 1;
        while i < Codec::ALL.len() {
            bits |= 1 << Codec::ALL[i].to_wire();
            i += 1;
        }
        bits
    };

    pub fn of(codecs: &[Codec]) -> Self {
        codecs
            .iter()
            .fold(Self::EMPTY, |set, codec| set.with(*codec))
    }

    pub const fn with(self, codec: Codec) -> Self {
        match codec {
            Codec::Identity => self,
            other => Self {
                bits: self.bits | (1 << other.to_wire()),
            },
        }
    }

    /// The codecs this build can both encode and decode.
    pub fn compiled() -> Self {
        Self::of(
            &Codec::ALL
                .into_iter()
                .filter(|codec| codec.implementation().is_some())
                .collect::<Vec<_>>(),
        )
    }

    /// Ids this build does not know are dropped: a codec it cannot name is a
    /// codec it cannot use.
    pub const fn from_wire(bits: u32) -> Self {
        Self {
            bits: bits & Self::KNOWN,
        }
    }

    pub const fn to_wire(self) -> u32 {
        self.bits
    }

    pub const fn contains(self, codec: Codec) -> bool {
        match codec {
            Codec::Identity => true,
            other => self.bits & (1 << other.to_wire()) != 0,
        }
    }

    pub const fn intersect(self, other: Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }

    pub const fn is_empty(self) -> bool {
        self.bits == 0
    }
}
