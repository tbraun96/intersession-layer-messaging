//! Per-message compression of a data frame's contents.
//!
//! The application says what a payload IS (`CompressionHint`); this module
//! decides what that is worth, in `policy`'s one table. Large JSON, text and
//! Yjs go to zstd level 3 first (within 1-4% of brotli q4's ratio, and about
//! 3x faster both ways in V8), then brotli, then raw deflate level 1 as the
//! CPU-cheap fallback. CBOR chat commands only pay with a shared dictionary,
//! which does not exist yet, so they are sent as they are.
//!
//! Codecs are cargo features and off by default. The wire id of every codec
//! exists regardless, so a build without a codec can still NAME it -- to refuse
//! it with an error rather than misread it.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[cfg(feature = "compression-brotli")]
mod brotli_codec;
mod codec;
#[cfg(feature = "compression-deflate")]
mod deflate_codec;
#[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
mod limit;
mod policy;
#[cfg(feature = "compression-zstd")]
mod zstd_codec;

pub use codec::{Codec, CodecSet, FrameCodec, Params};
pub use policy::{can_compress, policy, CodecChoice, IDENTITY, SMALL_FRAME_LIMIT};

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
/// silently sending uncompressed. A codec added to the registry gets its own
/// variant here; `All` picks it up without one.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DynamicCompression {
    Disabled,
    #[cfg(feature = "compression-brotli")]
    Brotli,
    #[cfg(feature = "compression-deflate")]
    Deflate,
    #[cfg(feature = "compression-zstd")]
    Zstd,
    /// Every compiled codec; `policy` picks among those the peer also has.
    #[cfg(any(
        feature = "compression-brotli",
        feature = "compression-deflate",
        feature = "compression-zstd"
    ))]
    All,
}

impl DynamicCompression {
    /// The codecs this setting enables, and so advertises.
    pub fn codecs(self) -> CodecSet {
        match self {
            DynamicCompression::Disabled => CodecSet::EMPTY,
            #[cfg(feature = "compression-brotli")]
            DynamicCompression::Brotli => CodecSet::of(&[Codec::Brotli]),
            #[cfg(feature = "compression-deflate")]
            DynamicCompression::Deflate => CodecSet::of(&[Codec::Deflate]),
            #[cfg(feature = "compression-zstd")]
            DynamicCompression::Zstd => CodecSet::of(&[Codec::Zstd]),
            #[cfg(any(
                feature = "compression-brotli",
                feature = "compression-deflate",
                feature = "compression-zstd"
            ))]
            DynamicCompression::All => CodecSet::compiled(),
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

/// Compress `raw` as `policy` says for `hint` and the `available` codecs, if
/// that makes it smaller; otherwise send it raw.
///
/// "Raw" is `Codec::Identity`, and it is what comes back for no hint, an
/// oversized payload, a frame below every eligible codec's floor, or one the
/// chosen codec would not shrink -- never a larger frame.
pub fn encode(
    hint: Option<CompressionHint>,
    available: CodecSet,
    raw: Vec<u8>,
) -> Result<Encoded, CompressionError> {
    if raw.len() > MAX_DECOMPRESSED_LEN {
        return Ok(identity(raw));
    }
    encode_with(policy(hint, raw.len(), available), raw)
}

/// `encode` with the choice already made.
pub fn encode_with(choice: CodecChoice, raw: Vec<u8>) -> Result<Encoded, CompressionError> {
    if choice.codec == Codec::Identity || raw.len() > MAX_DECOMPRESSED_LEN {
        return Ok(identity(raw));
    }
    let implementation = choice
        .codec
        .implementation()
        .ok_or(CompressionError::NotCompiled(choice.codec))?;
    let compressed = implementation.compress(&raw, &choice.params)?;
    if compressed.len() >= raw.len() {
        return Ok(identity(raw));
    }
    Ok(Encoded {
        codec: choice.codec,
        bytes: compressed,
    })
}

fn identity(raw: Vec<u8>) -> Encoded {
    Encoded {
        codec: Codec::Identity,
        bytes: raw,
    }
}

/// Undo `encode`. Compressed input is refused past `MAX_DECOMPRESSED_LEN`.
pub fn decode(codec: Codec, bytes: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
    match codec {
        Codec::Identity => Ok(bytes),
        other => other
            .implementation()
            .ok_or(CompressionError::NotCompiled(other))?
            .decompress(&bytes, MAX_DECOMPRESSED_LEN),
    }
}

#[cfg(test)]
mod tests;
