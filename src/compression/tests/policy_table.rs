//! The hint -> codec table, walked with synthetic codec sets.
//!
//! `policy` is pure data plus a lookup, so it can be tested for codecs this
//! build does not even contain: rill and zstd rows are pinned here before
//! either exists, and turning one on must not need these expectations to move.

use super::*;

fn set(codecs: &[Codec]) -> CodecSet {
    CodecSet::of(codecs)
}

fn pick(hint: Option<CompressionHint>, len: usize, available: &[Codec]) -> Codec {
    policy(hint, len, set(available)).codec
}

const SMALL: usize = 400;
const LARGE: usize = 20_000;
const EVERYTHING: [Codec; 4] = [Codec::Brotli, Codec::Deflate, Codec::Rill, Codec::Zstd];

#[test]
fn no_hint_and_opaque_are_identity_whatever_is_available() {
    for len in [0, SMALL, LARGE] {
        assert_eq!(pick(None, len, &EVERYTHING), Codec::Identity);
        assert_eq!(
            pick(Some(CompressionHint::Opaque), len, &EVERYTHING),
            Codec::Identity
        );
    }
}

#[test]
fn cbor_commands_wait_for_the_dictionary_codec() {
    let cbor = Some(CompressionHint::CborCommand);
    assert_eq!(pick(cbor, SMALL, &EVERYTHING), Codec::Rill);
    // Without rill: 0.80 under brotli is not worth a codec. Identity.
    assert_eq!(
        pick(cbor, SMALL, &[Codec::Brotli, Codec::Deflate, Codec::Zstd]),
        Codec::Identity
    );
    assert_eq!(
        pick(cbor, LARGE, &[Codec::Brotli, Codec::Deflate]),
        Codec::Identity
    );
}

#[test]
fn small_structured_frames_prefer_rill_then_brotli_then_deflate() {
    for hint in [
        CompressionHint::YjsUpdate,
        CompressionHint::Json,
        CompressionHint::Text,
    ] {
        let hint = Some(hint);
        assert_eq!(pick(hint, SMALL, &EVERYTHING), Codec::Rill);
        assert_eq!(
            pick(hint, SMALL, &[Codec::Brotli, Codec::Deflate, Codec::Zstd]),
            Codec::Brotli
        );
        assert_eq!(
            pick(hint, SMALL, &[Codec::Deflate, Codec::Zstd]),
            Codec::Deflate
        );
        // Zstd is not in the small-frame row at all.
        assert_eq!(pick(hint, SMALL, &[Codec::Zstd]), Codec::Identity);
        assert_eq!(pick(hint, SMALL, &[]), Codec::Identity);
    }
}

#[test]
fn large_frames_prefer_brotli_then_zstd_then_deflate() {
    for hint in [
        CompressionHint::YjsUpdate,
        CompressionHint::Json,
        CompressionHint::Text,
    ] {
        let hint = Some(hint);
        assert_eq!(pick(hint, LARGE, &EVERYTHING), Codec::Brotli);
        assert_eq!(
            pick(hint, LARGE, &[Codec::Zstd, Codec::Deflate, Codec::Rill]),
            Codec::Zstd
        );
        assert_eq!(
            pick(hint, LARGE, &[Codec::Deflate, Codec::Rill]),
            Codec::Deflate
        );
        // Rill is a small-frame codec and not in the large row.
        assert_eq!(pick(hint, LARGE, &[Codec::Rill]), Codec::Identity);
    }
}

#[test]
fn the_size_boundary_is_exact() {
    let json = Some(CompressionHint::Json);
    let all = &EVERYTHING;
    assert_eq!(pick(json, SMALL_FRAME_LIMIT - 1, all), Codec::Rill);
    assert_eq!(pick(json, SMALL_FRAME_LIMIT, all), Codec::Brotli);
}

#[test]
fn a_frame_below_a_codecs_floor_falls_through_to_the_next_or_identity() {
    let json = Some(CompressionHint::Json);
    // Below brotli's and deflate's 128-byte floor, neither is tried.
    assert_eq!(
        pick(json, 127, &[Codec::Brotli, Codec::Deflate]),
        Codec::Identity
    );
    assert_eq!(
        pick(json, 128, &[Codec::Brotli, Codec::Deflate]),
        Codec::Brotli
    );
    // Rill's floor is lower; that is the point of a dictionary codec.
    assert_eq!(pick(json, 64, &EVERYTHING), Codec::Rill);
    assert_eq!(pick(json, 8, &EVERYTHING), Codec::Identity);
}

#[test]
fn the_chosen_brotli_is_quality_4_window_18_and_deflate_is_level_1() {
    let json = Some(CompressionHint::Json);
    let brotli = policy(json, LARGE, set(&[Codec::Brotli]));
    assert_eq!((brotli.params.level, brotli.params.window_log), (4, 18));
    let deflate = policy(json, LARGE, set(&[Codec::Deflate]));
    assert_eq!(deflate.params.level, 1);
}

#[test]
fn a_hint_that_no_available_codec_serves_is_not_worth_remembering() {
    assert!(!can_compress(CompressionHint::Opaque, set(&EVERYTHING)));
    assert!(!can_compress(
        CompressionHint::CborCommand,
        set(&[Codec::Brotli, Codec::Deflate])
    ));
    assert!(can_compress(
        CompressionHint::CborCommand,
        set(&[Codec::Rill])
    ));
    assert!(can_compress(
        CompressionHint::YjsUpdate,
        set(&[Codec::Deflate])
    ));
    assert!(!can_compress(CompressionHint::Json, set(&[])));
}

#[test]
fn nothing_available_means_identity_even_for_a_codec_this_build_has() {
    // `available` already folds in the peer: a peer that advertised nothing
    // gets identity even if this build has every codec compiled in.
    assert_eq!(
        encode(
            Some(CompressionHint::Json),
            CodecSet::EMPTY,
            json_like(4096)
        )
        .expect("encode")
        .codec,
        Codec::Identity
    );
}
