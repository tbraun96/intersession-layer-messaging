//! Which codec a frame gets: ONE table, read top to bottom.
//!
//! The rows come from measurements on the workspace's own payloads (the
//! traffic study, and the zstd-rs benchmark on the same ILM corpus). A codec
//! is used only if it is in `available` -- compiled here, enabled here, AND
//! advertised by the peer -- and the frame is at least its floor; otherwise
//! the next one in the row is tried, and a row that runs out means identity.
//! Whether the result is actually smaller is checked afterwards, in `encode`.
//!
//! Large frames (>= 1 KiB) rank zstd first: without a dictionary its ratio is
//! within 1-4% of brotli q4 (ws-json 0.196 vs 0.194, Yjs snapshots 0.122 vs
//! 0.115, markdown 0.48 vs 0.46) at about 3x the speed in V8 (compress
//! 3.4-3.6x, decompress 2.6-3.4x), and it is the codec the browser can afford
//! to ship.
//!
//! Small frames (< 1 KiB) do NOT rank zstd first, because without a
//! dictionary it does not pay there. Per frame size on the corpus (ratio,
//! zstd magicless):
//!
//! | frames      | zstd-3 | brotli-q4 | deflate-l1 | best on        |
//! |-------------|--------|-----------|------------|----------------|
//! | < 128 B     | 0.874  | 0.817     | 0.885      | zstd grows 4/6 |
//! | 128-256 B   | 0.857  | 0.835     | 0.846      | brotli 3/3     |
//! | 256-512 B   | 0.818  | 0.782     | 0.794      | brotli 62, deflate 19, zstd 0 of 81 |
//! | 512-1024 B  | 0.678  | 0.650     | 0.671      | brotli 2/2     |
//!
//! It is last in the small rows, with a 128-byte floor (below which it grows
//! most frames), so a peer that has nothing else still gets a smaller frame.
//! A trained dictionary moves it to the front of those rows (chat 0.36, Yjs
//! deltas 0.43), and that is a table edit here, not a wire change.

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

/// Level 3 is zstd's own default and the interactive setting: level 1 is
/// barely faster on these frames (ws-json 27.7 vs 34.1 us in V8) for a worse
/// ratio on everything but JSON, and the levels above 3 trade compress time a
/// chat or an editor keystroke waits on for a few percent. The window is an
/// upper bound (the encoder shrinks it to the frame), so 2^23 costs a small
/// frame nothing and lets a snapshot reach back as far as it needs.
const ZSTD_L3: CodecChoice = CodecChoice {
    codec: Codec::Zstd,
    params: Params {
        level: 3,
        window_log: 23,
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
    Rule { hint: CompressionHint::YjsUpdate, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1, ZSTD_L3] },
    Rule { hint: CompressionHint::YjsUpdate, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[ZSTD_L3, BROTLI_Q4, DEFLATE_L1] },
    Rule { hint: CompressionHint::Json, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1, ZSTD_L3] },
    Rule { hint: CompressionHint::Json, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[ZSTD_L3, BROTLI_Q4, DEFLATE_L1] },
    Rule { hint: CompressionHint::Text, min: 0, max: SMALL_FRAME_LIMIT, prefer: &[RILL, BROTLI_Q4, DEFLATE_L1, ZSTD_L3] },
    Rule { hint: CompressionHint::Text, min: SMALL_FRAME_LIMIT, max: usize::MAX, prefer: &[ZSTD_L3, BROTLI_Q4, DEFLATE_L1] },
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
