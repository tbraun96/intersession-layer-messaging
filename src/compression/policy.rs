//! Which codec a frame gets: ONE table, read top to bottom.
//!
//! The rows come from the traffic study's measurements on the workspace's own
//! payloads (brotli q4/lgwin18 ratios: JSON 0.19, markdown 0.46, Yjs 0.25,
//! CBOR chat 0.80; deflate l1 as the cheap fallback; brotli q4 grows nothing
//! from ~100 B and deflate l1 grows frames up to ~130 B). A codec is used only
//! if it is in `available` -- compiled here, enabled here, AND advertised by
//! the peer -- and the frame is at least its floor; otherwise the next one in
//! the row is tried, and a row that runs out means identity. Whether the
//! result is actually smaller is checked afterwards, in `encode`.
//!
//! Rill and zstd rows are written now so that turning either on is a registry
//! entry and a feature, not a policy change. Zstd sits BELOW brotli for large
//! text: the study has no zstd measurement to justify ranking it higher, and
//! "faster at equal ratio" is a claim to measure before it moves up.

use super::codec::{Codec, CodecSet, Params};
use super::CompressionHint;

/// Frames at or above this are "large": snapshots, documents, JSON listings.
/// Below it are chat commands, receipts, typing indicators and Yjs deltas,
/// which is the regime the dictionary codec is built for.
pub const SMALL_FRAME_LIMIT: usize = 1024;

/// A codec, its tuning, and the smallest frame it is worth trying on.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CodecChoice {
    pub codec: Codec,
    pub params: Params,
    pub min_len: usize,
}

pub const IDENTITY: CodecChoice = CodecChoice {
    codec: Codec::Identity,
    params: Params {
        level: 0,
        window_log: 0,
    },
    min_len: 0,
};

const BROTLI_Q4: CodecChoice = CodecChoice {
    codec: Codec::Brotli,
    params: Params {
        level: 4,
        window_log: 18,
    },
    min_len: 128,
};

const DEFLATE_L1: CodecChoice = CodecChoice {
    codec: Codec::Deflate,
    params: Params {
        level: 1,
        window_log: 15,
    },
    min_len: 128,
};

/// Floor and level are provisional until rill is integrated and measured
/// here; its static dictionary is what lets it win below brotli's floor.
const RILL: CodecChoice = CodecChoice {
    codec: Codec::Rill,
    params: Params {
        level: 0,
        window_log: 0,
    },
    min_len: 16,
};

/// Provisional until the no_std zstd is integrated and measured here.
const ZSTD: CodecChoice = CodecChoice {
    codec: Codec::Zstd,
    params: Params {
        level: 3,
        window_log: 18,
    },
    min_len: 128,
};

struct Rule {
    hint: CompressionHint,
    /// Applies to frames of `min..max` bytes.
    min: usize,
    max: usize,
    /// In order of preference.
    prefer: &'static [CodecChoice],
}

#[rustfmt::skip]
const TABLE: &[Rule] = &[
    // Already compressed or random: nothing will shrink it.
    Rule { hint: CompressionHint::Opaque, min: 0, max: usize::MAX, prefer: &[] },
    // CBOR only pays with a dictionary (0.80 without one); until rill exists
    // these go as they are, which is what the owner asked for.
    Rule { hint: CompressionHint::CborCommand, min: 0, max: usize::MAX, prefer: &[RILL] },
    Rule { hint: CompressionHint::YjsUpdate, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1] },
    Rule { hint: CompressionHint::YjsUpdate, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[BROTLI_Q4, ZSTD, DEFLATE_L1] },
    Rule { hint: CompressionHint::Json, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1] },
    Rule { hint: CompressionHint::Json, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[BROTLI_Q4, ZSTD, DEFLATE_L1] },
    Rule { hint: CompressionHint::Text, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1] },
    Rule { hint: CompressionHint::Text, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[BROTLI_Q4, ZSTD, DEFLATE_L1] },
];

/// The codec for one frame. No hint is identity: the application did not say
/// what the bytes are, so nothing is assumed about them.
pub fn policy(hint: Option<CompressionHint>, len: usize, available: CodecSet) -> CodecChoice {
    let Some(hint) = hint else {
        return IDENTITY;
    };
    TABLE
        .iter()
        .find(|rule| rule.hint == hint && (rule.min..rule.max).contains(&len))
        .and_then(|rule| {
            rule.prefer
                .iter()
                .find(|choice| available.contains(choice.codec) && len >= choice.min_len)
        })
        .copied()
        .unwrap_or(IDENTITY)
}

/// Whether any row could ever pick something other than identity for `hint`.
/// Lets ILM skip remembering hints that can never matter.
pub fn can_compress(hint: CompressionHint, available: CodecSet) -> bool {
    TABLE
        .iter()
        .any(|rule| rule.hint == hint && rule.prefer.iter().any(|c| available.contains(c.codec)))
}
