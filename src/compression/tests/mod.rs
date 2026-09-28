use super::*;

mod policy_table;

#[cfg(any(feature = "compression-brotli", feature = "compression-deflate"))]
mod compiled;

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
fn codec_ids_are_stable_and_unknown_ids_are_refused() {
    // Permanent: a frame written by one build is read by every later one.
    let ids: Vec<(Codec, u8)> = Codec::ALL.iter().map(|c| (*c, c.to_wire())).collect();
    assert_eq!(
        ids,
        vec![
            (Codec::Identity, 0),
            (Codec::Brotli, 1),
            (Codec::Deflate, 2),
            (Codec::Rill, 3),
            (Codec::Zstd, 4),
        ]
    );
    for codec in Codec::ALL {
        assert_eq!(Codec::from_wire(codec.to_wire()), Ok(codec));
    }
    assert_eq!(Codec::from_wire(5), Err(CompressionError::UnknownCodec(5)));
}

#[test]
fn identity_passes_bytes_through_untouched() {
    let raw = json_like(4096);
    let encoded = encode(None, CodecSet::compiled(), raw.clone()).expect("encode");
    assert_eq!(
        encoded,
        Encoded {
            codec: Codec::Identity,
            bytes: raw.clone()
        }
    );
    assert_eq!(decode(Codec::Identity, raw.clone()).expect("decode"), raw);
}

#[test]
fn reserved_codecs_are_refused_rather_than_misread() {
    for codec in [Codec::Rill, Codec::Zstd] {
        assert!(
            codec.implementation().is_none(),
            "{codec:?} is only reserved"
        );
        assert!(!CodecSet::compiled().contains(codec));
        assert_eq!(
            decode(codec, vec![1, 2, 3]),
            Err(CompressionError::NotCompiled(codec))
        );
    }
}

#[cfg(not(feature = "compression-brotli"))]
#[test]
fn a_build_without_brotli_refuses_brotli_rather_than_misreading_it() {
    assert_eq!(
        decode(Codec::Brotli, vec![1, 2, 3]),
        Err(CompressionError::NotCompiled(Codec::Brotli))
    );
    assert!(!CodecSet::compiled().contains(Codec::Brotli));
}

#[cfg(not(feature = "compression-deflate"))]
#[test]
fn a_build_without_deflate_refuses_deflate_rather_than_misreading_it() {
    assert_eq!(
        decode(Codec::Deflate, vec![1, 2, 3]),
        Err(CompressionError::NotCompiled(Codec::Deflate))
    );
    assert!(!CodecSet::compiled().contains(Codec::Deflate));
}
