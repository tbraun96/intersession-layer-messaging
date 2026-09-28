//! What is particular to the zstd frames: the format choices in
//! `zstd_codec.rs`, and the ways a hostile frame can differ from ours.

use super::*;

/// zstd's frame magic, little-endian, as a standard frame begins with it.
const MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

fn zstd() -> &'static dyn FrameCodec {
    Codec::Zstd.implementation().expect("compiled")
}

fn params() -> Params {
    policy(
        Some(CompressionHint::Json),
        20_000,
        CodecSet::of(&[Codec::Zstd]),
    )
    .params
}

fn frame(raw: &[u8]) -> Vec<u8> {
    zstd().compress(raw, &params()).expect("compress")
}

#[test]
fn frames_are_magicless_and_four_bytes_smaller_than_standard_ones() {
    let raw = json_like(4096);
    let ours = frame(&raw);
    assert_ne!(ours[..4], MAGIC);
    let standard = {
        let config = zstd_rs::CompressionConfig {
            level: 3,
            window_log: 23,
            checksum: false,
            content_size: true,
            dict_id: false,
            format: zstd_rs::FrameFormat::Standard,
        };
        let mut out = Vec::new();
        zstd_rs::Compressor::new(config)
            .expect("config")
            .compress(&raw, None, &mut out)
            .expect("compress");
        out
    };
    assert_eq!(standard[..4], MAGIC);
    assert_eq!(ours.len() + 4, standard.len());
    // A standard frame is not ours: the magic reads as a (bad) header.
    assert!(zstd().decompress(&standard, MAX_DECOMPRESSED_LEN).is_err());
}

#[test]
fn a_frame_followed_by_trailing_bytes_is_refused() {
    let raw = json_like(4096);
    let mut bytes = frame(&raw);
    bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    assert!(matches!(
        decode(Codec::Zstd, bytes),
        Err(CompressionError::Malformed(_))
    ));
}

#[test]
fn a_declared_size_past_the_cap_is_refused_from_the_header() {
    // One byte over a small cap: refused as too large, not as malformed, and
    // before any of it is decoded.
    let raw = json_like(4096);
    let bytes = frame(&raw);
    assert_eq!(
        zstd().decompress(&bytes, raw.len() - 1),
        Err(CompressionError::TooLarge {
            limit: raw.len() - 1
        })
    );
    assert_eq!(zstd().decompress(&bytes, raw.len()).expect("fits"), raw);
}

#[test]
fn a_level_zstd_cannot_run_is_an_error_not_a_panic() {
    for level in [0, 23, u32::MAX] {
        let result = zstd().compress(
            b"payload",
            &Params {
                level,
                window_log: 23,
            },
        );
        assert!(
            matches!(result, Err(CompressionError::Malformed(_))),
            "{level}: {result:?}"
        );
    }
}

#[test]
fn no_bytes_are_a_damaged_frame_not_an_empty_message() {
    // zstd itself reads zero bytes as zero frames; `compress` never makes that.
    assert!(matches!(
        decode(Codec::Zstd, Vec::new()),
        Err(CompressionError::Malformed(_))
    ));
    assert_eq!(decode(Codec::Zstd, frame(b"")).expect("empty frame"), b"");
}
