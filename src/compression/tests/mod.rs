use super::*;

pub(super) fn json_like(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut n: u32 = 0;
    while out.len() < len {
        out.extend_from_slice(
            format!("{{\"id\":\"node-{n}\",\"kind\":\"room\",\"members\":[\"alice\",\"bob\"]}},")
                .as_bytes(),
        );
        n += 1;
    }
    out.truncate(len);
    out
}

/// Deterministic noise with no structure a codec can use.
#[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
pub(super) fn incompressible(len: usize) -> Vec<u8> {
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

pub(super) fn all_caps() -> PeerCapabilities {
    PeerCapabilities::from_wire(0xff)
}

#[test]
fn no_hint_and_non_compressing_hints_choose_no_codec() {
    for hint in [
        None,
        Some(CompressionHint::Opaque),
        Some(CompressionHint::CborCommand),
    ] {
        assert_eq!(Codec::for_hint(hint, all_caps()), Codec::None, "{hint:?}");
    }
}

#[test]
fn compressing_hints_prefer_brotli_then_deflate_then_nothing() {
    let brotli_only = PeerCapabilities::from_wire(PeerCapabilities::CODEC_BROTLI);
    let deflate_only = PeerCapabilities::from_wire(PeerCapabilities::CODEC_DEFLATE);
    for hint in [
        CompressionHint::Text,
        CompressionHint::Json,
        CompressionHint::YjsUpdate,
    ] {
        assert_eq!(Codec::for_hint(Some(hint), all_caps()), Codec::Brotli);
        assert_eq!(Codec::for_hint(Some(hint), brotli_only), Codec::Brotli);
        assert_eq!(Codec::for_hint(Some(hint), deflate_only), Codec::Deflate);
        assert_eq!(
            Codec::for_hint(Some(hint), PeerCapabilities::LEGACY),
            Codec::None
        );
    }
}

#[test]
fn hint_names_round_trip_and_unknown_names_are_refused() {
    for hint in [
        CompressionHint::Opaque,
        CompressionHint::Text,
        CompressionHint::Json,
        CompressionHint::YjsUpdate,
        CompressionHint::CborCommand,
    ] {
        assert_eq!(hint.as_str().parse::<CompressionHint>(), Ok(hint));
    }
    assert!(matches!(
        "JSON".parse::<CompressionHint>(),
        Err(CompressionError::UnknownHint(_))
    ));
}

#[test]
fn unknown_codec_ids_are_refused() {
    assert_eq!(Codec::from_wire(3), Err(CompressionError::UnknownCodec(3)));
    for codec in [Codec::None, Codec::Brotli, Codec::Deflate] {
        assert_eq!(Codec::from_wire(codec.to_wire()), Ok(codec));
    }
}

#[test]
fn codec_none_passes_bytes_through_untouched() {
    let raw = json_like(4096);
    let encoded = encode(Codec::None, raw.clone()).expect("encode");
    assert_eq!(
        encoded,
        Encoded {
            codec: Codec::None,
            bytes: raw.clone()
        }
    );
    assert_eq!(decode(Codec::None, raw.clone()).expect("decode"), raw);
}

#[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
mod compiled;

#[cfg(not(feature = "compression-brotli"))]
#[test]
fn a_build_without_brotli_refuses_brotli_rather_than_misreading_it() {
    assert_eq!(
        decode(Codec::Brotli, vec![1, 2, 3]),
        Err(CompressionError::NotCompiled(Codec::Brotli))
    );
    assert_eq!(
        encode(Codec::Brotli, json_like(1024)),
        Err(CompressionError::NotCompiled(Codec::Brotli))
    );
}

#[cfg(not(feature = "compression-deflate"))]
#[test]
fn a_build_without_deflate_refuses_deflate_rather_than_misreading_it() {
    assert_eq!(
        decode(Codec::Deflate, vec![1, 2, 3]),
        Err(CompressionError::NotCompiled(Codec::Deflate))
    );
}
