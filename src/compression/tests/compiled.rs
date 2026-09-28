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

#[test]
fn every_compressing_hint_round_trips_smaller() {
    for codec in compiled_codecs() {
        for hint in [
            CompressionHint::Text,
            CompressionHint::Json,
            CompressionHint::YjsUpdate,
        ] {
            let raw = json_like(2048);
            let chosen = Codec::for_hint(
                Some(hint),
                PeerCapabilities::from_wire(match codec {
                    Codec::Brotli => PeerCapabilities::CODEC_BROTLI,
                    _ => PeerCapabilities::CODEC_DEFLATE,
                }),
            );
            assert_eq!(chosen, codec);
            let encoded = encode(chosen, raw.clone()).expect("encode");
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
fn a_frame_below_the_threshold_is_sent_raw() {
    for codec in compiled_codecs() {
        let raw = json_like(MIN_COMPRESSIBLE_LEN - 1);
        let encoded = encode(codec, raw.clone()).expect("encode");
        assert_eq!(
            encoded,
            Encoded {
                codec: Codec::None,
                bytes: raw
            }
        );
    }
}

#[test]
fn a_frame_the_codec_would_grow_is_sent_raw() {
    for codec in compiled_codecs() {
        let raw = incompressible(1024);
        let encoded = encode(codec, raw.clone()).expect("encode");
        assert_eq!(
            encoded,
            Encoded {
                codec: Codec::None,
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
        let encoded = encode(codec, raw).expect("encode");
        assert_eq!(encoded.codec, Codec::None);
    }
}

#[test]
fn a_decompression_bomb_is_refused_at_the_ceiling() {
    for codec in compiled_codecs() {
        // Built with the codec directly: `encode` would refuse to make it.
        let bomb = compress(codec, &vec![0u8; MAX_DECOMPRESSED_LEN + 1]).expect("compress");
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
        let edge = compress(codec, &vec![0u8; MAX_DECOMPRESSED_LEN]).expect("compress");
        assert_eq!(
            decode(codec, edge).expect("at the limit").len(),
            MAX_DECOMPRESSED_LEN
        );
    }
}

#[test]
fn malformed_input_is_an_error_not_a_panic() {
    for codec in compiled_codecs() {
        let good = compress(codec, &json_like(4096)).expect("compress");
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
            // Deflate cannot always tell truncation from a short stream, and
            // an empty raw-deflate stream is not an error in every decoder.
            // What must never happen is a panic or the ORIGINAL coming back.
            if let Ok(decoded) = result {
                assert_ne!(
                    decoded,
                    json_like(4096),
                    "{codec:?} {name} decoded to the original"
                );
            }
        }
        // Noise, specifically, has to be refused by brotli: its stream
        // header is strict enough that random bytes do not parse.
        if codec == Codec::Brotli {
            assert!(matches!(
                decode(codec, incompressible(512)),
                Err(CompressionError::Malformed(_))
            ));
        }
    }
}
