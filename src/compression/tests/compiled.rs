//! Behaviour that needs at least one codec compiled in.

use super::*;

fn compiled_codecs() -> Vec<Codec> {
    [
        #[cfg(feature = "compression-brotli")]
        Codec::Brotli,
        #[cfg(feature = "compression-deflate")]
        Codec::Deflate,
    ]
    .to_vec()
}

/// Deterministic noise with no structure a codec can use.
fn incompressible(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// The choice `policy` makes for a large JSON frame when only `codec` is available.
fn choice_for(codec: Codec) -> CodecChoice {
    let choice = policy(Some(CompressionHint::Json), 20_000, CodecSet::of(&[codec]));
    assert_eq!(choice.codec, codec);
    choice
}

fn compress_raw(codec: Codec, raw: &[u8]) -> Vec<u8> {
    codec
        .implementation()
        .expect("compiled")
        .compress(raw, &choice_for(codec).params)
        .expect("compress")
}

#[test]
fn every_compressing_hint_round_trips_smaller() {
    for codec in compiled_codecs() {
        for hint in [
            CompressionHint::Text,
            CompressionHint::Json,
            CompressionHint::YjsUpdate,
        ] {
            let raw = json_like(2048);
            let encoded = encode(Some(hint), CodecSet::of(&[codec]), raw.clone()).expect("encode");
            assert_eq!(encoded.codec, codec, "{hint:?} should have compressed");
            assert!(
                encoded.bytes.len() < raw.len() / 2,
                "{codec:?}: {} of {}",
                encoded.bytes.len(),
                raw.len()
            );
            assert_eq!(decode(encoded.codec, encoded.bytes).expect("decode"), raw);
        }
    }
}

#[test]
fn a_frame_the_codec_would_grow_is_sent_raw() {
    for codec in compiled_codecs() {
        let raw = incompressible(4096);
        let encoded = encode_with(choice_for(codec), raw.clone()).expect("encode");
        assert_eq!(
            encoded,
            Encoded {
                codec: Codec::Identity,
                bytes: raw
            },
            "{codec:?}"
        );
    }
}

#[test]
fn a_frame_past_the_ceiling_is_sent_raw_so_the_receiver_never_refuses_it() {
    for codec in compiled_codecs() {
        let raw = vec![0u8; MAX_DECOMPRESSED_LEN + 1];
        let encoded =
            encode(Some(CompressionHint::Json), CodecSet::of(&[codec]), raw).expect("encode");
        assert_eq!(encoded.codec, Codec::Identity);
    }
}

#[test]
fn a_decompression_bomb_is_refused_at_the_ceiling() {
    for codec in compiled_codecs() {
        // Built with the codec directly: `encode` would refuse to make it.
        let bomb = compress_raw(codec, &vec![0u8; MAX_DECOMPRESSED_LEN + 1]);
        // A real bomb: a tiny fraction of what it claims.
        assert!(
            bomb.len() < MAX_DECOMPRESSED_LEN / 100,
            "{codec:?} bomb is {} bytes",
            bomb.len()
        );
        assert_eq!(
            decode(codec, bomb),
            Err(CompressionError::TooLarge {
                limit: MAX_DECOMPRESSED_LEN
            }),
            "{codec:?}"
        );
        // Exactly at the ceiling is still accepted.
        let edge = compress_raw(codec, &vec![0u8; MAX_DECOMPRESSED_LEN]);
        assert_eq!(
            decode(codec, edge).expect("at the limit").len(),
            MAX_DECOMPRESSED_LEN
        );
    }
}

#[test]
fn malformed_input_is_an_error_not_a_panic() {
    for codec in compiled_codecs() {
        let original = json_like(4096);
        let good = compress_raw(codec, &original);
        let truncated = good[..good.len() / 2].to_vec();
        let mut flipped = good.clone();
        for byte in flipped.iter_mut().step_by(3) {
            *byte ^= 0x5a;
        }
        for (name, bytes) in [
            ("truncated", truncated),
            ("flipped", flipped),
            ("noise", incompressible(512)),
            ("empty", Vec::new()),
        ] {
            let result = std::panic::catch_unwind(|| decode(codec, bytes));
            let result = result.unwrap_or_else(|_| panic!("{codec:?} panicked on {name} input"));
            // Raw deflate has no checksum, so a damaged stream can decode to
            // other bytes; the SDK's AEAD below this layer is what rules
            // corruption out in transit. What must never happen is a panic or
            // the ORIGINAL coming back.
            if let Ok(decoded) = result {
                assert_ne!(
                    decoded, original,
                    "{codec:?} {name} decoded to the original"
                );
            }
        }
        // Brotli's stream header is strict enough that noise must not parse.
        if codec == Codec::Brotli {
            assert!(matches!(
                decode(codec, incompressible(512)),
                Err(CompressionError::Malformed(_))
            ));
        }
    }
}
