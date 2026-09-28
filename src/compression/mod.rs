//! Per-message compression of a data frame's contents.
//!
//! The application says what a payload IS (`CompressionHint`); this module
//! decides what that is worth. Measured on the workspace's own traffic, brotli
//! at quality 4 / lgwin 18 had the best ratio for every class that compresses
//! at all (JSON 0.19, markdown 0.46, Yjs 0.25) and raw deflate at level 1 is
//! the CPU-cheap fallback. CBOR chat commands only pay with a shared
//! dictionary, which does not exist yet, so they are sent as they are.
//!
//! Codecs are cargo features and off by default. The wire id of every codec
//! exists regardless, so a build without a codec can still NAME it -- to refuse
//! it with an error rather than misread it.

use crate::capabilities::PeerCapabilities;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[cfg(feature = "compression-brotli")]
mod brotli_codec;
#[cfg(feature = "compression-deflate")]
mod deflate_codec;
#[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
mod limit;

/// Frames smaller than this are sent as they are.
///
/// Below it neither codec reliably wins: brotli q4 stops growing input at
/// about 100 bytes and deflate l1 at about 130, and a frame this small is
/// dominated by the transport's fixed overhead anyway.
pub const MIN_COMPRESSIBLE_LEN: usize = 128;

/// The most a compressed frame may expand to. Also the largest payload a
/// sender will compress, so a legitimate frame can never be refused by it.
///
/// This is the decompression-bomb guard: a few kilobytes of hostile brotli can
/// claim gigabytes, and the receiver stops reading at this bound instead.
pub const MAX_DECOMPRESSED_LEN: usize = 16 * 1024 * 1024;

/// What the application says a payload is. No hint means no compression.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CompressionHint {
    /// Already compressed or random: never worth a codec.
    Opaque,
    Text,
    Json,
    YjsUpdate,
    /// A CBOR command. Only pays with a shared dictionary, which is deferred.
    CborCommand,
}

impl CompressionHint {
    pub const fn as_str(self) -> &'static str {
        match self {
            CompressionHint::Opaque => "opaque",
            CompressionHint::Text => "text",
            CompressionHint::Json => "json",
            CompressionHint::YjsUpdate => "yjs-update",
            CompressionHint::CborCommand => "cbor-command",
        }
    }

    const fn wants_compression(self) -> bool {
        matches!(
            self,
            CompressionHint::Text | CompressionHint::Json | CompressionHint::YjsUpdate
        )
    }
}

impl FromStr for CompressionHint {
    type Err = CompressionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        [
            CompressionHint::Opaque,
            CompressionHint::Text,
            CompressionHint::Json,
            CompressionHint::YjsUpdate,
            CompressionHint::CborCommand,
        ]
        .into_iter()
        .find(|hint| hint.as_str() == value)
        .ok_or_else(|| CompressionError::UnknownHint(value.to_string()))
    }
}

/// Which codecs this node may use. `Disabled` is the legacy behaviour.
///
/// A variant exists only when its codec is compiled in, so a configuration
/// that asks for a codec the build does not have fails to compile rather than
/// silently sending uncompressed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DynamicCompression {
    Disabled,
    #[cfg(feature = "compression-brotli")]
    Brotli,
    #[cfg(feature = "compression-deflate")]
    Deflate,
    /// Every compiled codec; the strongest the peer also has is used.
    #[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
    All,
}

/// A codec's id on the wire. Ids are permanent; new codecs take new ids.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Codec {
    None,
    Brotli,
    Deflate,
}

impl Codec {
    pub const fn to_wire(self) -> u8 {
        match self {
            Codec::None => 0,
            Codec::Brotli => 1,
            Codec::Deflate => 2,
        }
    }

    pub const fn from_wire(id: u8) -> Result<Self, CompressionError> {
        match id {
            0 => Ok(Codec::None),
            1 => Ok(Codec::Brotli),
            2 => Ok(Codec::Deflate),
            other => Err(CompressionError::UnknownCodec(other)),
        }
    }

    /// The codec for a payload, given what BOTH ends support.
    ///
    /// `shared` must already be the intersection of this node's capabilities
    /// and the peer's; this function has no other way to know either.
    pub fn for_hint(hint: Option<CompressionHint>, shared: PeerCapabilities) -> Self {
        match hint {
            Some(hint) if hint.wants_compression() => [Codec::Brotli, Codec::Deflate]
                .into_iter()
                .find(|codec| shared.decodes(*codec))
                .unwrap_or(Codec::None),
            _ => Codec::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionError {
    UnknownCodec(u8),
    UnknownHint(String),
    /// The codec has a wire id but is not compiled into this build.
    NotCompiled(Codec),
    /// The frame expands past `MAX_DECOMPRESSED_LEN`.
    TooLarge {
        limit: usize,
    },
    Malformed(String),
}

impl Display for CompressionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::UnknownCodec(id) => write!(f, "unknown codec id {id}"),
            CompressionError::UnknownHint(hint) => write!(
                f,
                "unknown compression hint {hint:?}; expected one of opaque, text, json, yjs-update, cbor-command"
            ),
            CompressionError::NotCompiled(codec) => {
                write!(f, "codec {codec:?} is not compiled into this build")
            }
            CompressionError::TooLarge { limit } => {
                write!(f, "decompressed contents exceed {limit} bytes")
            }
            CompressionError::Malformed(reason) => write!(f, "malformed compressed contents: {reason}"),
        }
    }
}

impl std::error::Error for CompressionError {}

/// Contents as they go on the wire, and the codec that produced them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
    pub codec: Codec,
    pub bytes: Vec<u8>,
}

/// Compress `raw` with `codec` if that makes it smaller; otherwise send it raw.
///
/// "Raw" is `Codec::None`, and it is what comes back for a small payload, an
/// oversized one, or one the codec would have grown -- never a larger frame.
pub fn encode(codec: Codec, raw: Vec<u8>) -> Result<Encoded, CompressionError> {
    let worth_trying = codec != Codec::None
        && raw.len() >= MIN_COMPRESSIBLE_LEN
        && raw.len() <= MAX_DECOMPRESSED_LEN;
    if !worth_trying {
        return Ok(Encoded {
            codec: Codec::None,
            bytes: raw,
        });
    }
    let compressed = compress(codec, &raw)?;
    if compressed.len() >= raw.len() {
        return Ok(Encoded {
            codec: Codec::None,
            bytes: raw,
        });
    }
    Ok(Encoded {
        codec,
        bytes: compressed,
    })
}

/// Undo `encode`. Compressed input is refused past `MAX_DECOMPRESSED_LEN`.
pub fn decode(codec: Codec, bytes: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
    match codec {
        Codec::None => Ok(bytes),
        #[cfg(feature = "compression-brotli")]
        Codec::Brotli => brotli_codec::decompress(&bytes, MAX_DECOMPRESSED_LEN),
        #[cfg(feature = "compression-deflate")]
        Codec::Deflate => deflate_codec::decompress(&bytes, MAX_DECOMPRESSED_LEN),
        #[allow(unreachable_patterns)]
        other => Err(CompressionError::NotCompiled(other)),
    }
}

fn compress(codec: Codec, raw: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        #[cfg(feature = "compression-brotli")]
        Codec::Brotli => brotli_codec::compress(raw),
        #[cfg(feature = "compression-deflate")]
        Codec::Deflate => deflate_codec::compress(raw),
        #[allow(unreachable_patterns)]
        other => {
            let _ = raw;
            Err(CompressionError::NotCompiled(other))
        }
    }
}

#[cfg(test)]
mod tests;
