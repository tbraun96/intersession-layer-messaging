//! Zstandard through `zstd-rs`: pure Rust, `no_std`, no wasm imports. The
//! browser's codec, where brotli's module size did not fit the PWA cache.
//!
//! Frames are magicless: the frame's codec id already says "zstd", so the
//! 4-byte magic number would only repeat it, and these frames never leave the
//! protocol for a tool that expects a standard `.zst`. The content size is
//! kept -- it lets the decoder refuse a bomb from its header, before decoding
//! anything -- and the checksum is not: the SDK's AEAD below this layer
//! already rules out corruption in transit.
//!
//! No dictionary yet. A dictionary trained on workspace traffic is the next
//! lever for small frames (chat 0.85 -> 0.36, Yjs deltas 0.80 -> 0.43 in the
//! zstd-rs benchmarks); it would be named by a versioned dictionary id in the
//! frame, which `WireWrapper::MessageV2` already carries, never by the header.

use super::codec::{FrameCodec, Params};
use super::CompressionError;
use zstd_rs::{CompressionConfig, Compressor, Decompressor, Error, FrameFormat};

pub(super) struct Zstd;

const FORMAT: FrameFormat = FrameFormat::Magicless;

fn config(params: &Params) -> Result<CompressionConfig, CompressionError> {
    let level = i32::try_from(params.level)
        .map_err(|_| CompressionError::Malformed(format!("zstd level {}", params.level)))?;
    Ok(CompressionConfig {
        level,
        window_log: params.window_log,
        checksum: false,
        content_size: true,
        dict_id: false,
        format: FORMAT,
    })
}

fn failed(err: Error, cap: usize) -> CompressionError {
    match err {
        Error::OutputLimit => CompressionError::TooLarge { limit: cap },
        other => CompressionError::Malformed(format!("zstd: {other}")),
    }
}

impl FrameCodec for Zstd {
    fn compress(&self, raw: &[u8], params: &Params) -> Result<Vec<u8>, CompressionError> {
        let mut compressor = Compressor::new(config(params)?)
            .map_err(|err| CompressionError::Malformed(format!("zstd config: {err}")))?;
        let mut out = Vec::new();
        compressor
            .compress(raw, None, &mut out)
            .map_err(|err| CompressionError::Malformed(format!("zstd compress: {err}")))?;
        Ok(out)
    }

    fn decompress(&self, bytes: &[u8], cap: usize) -> Result<Vec<u8>, CompressionError> {
        // Zero bytes are zero frames to zstd, which would decode to nothing.
        // `compress` never produces that, so it is a damaged frame, not a
        // legitimately empty one.
        if bytes.is_empty() {
            return Err(CompressionError::Malformed("zstd: no frame".to_string()));
        }
        let mut out = Vec::new();
        Decompressor::new()
            .decompress_format(FORMAT, bytes, None, cap, &mut out)
            .map_err(|err| failed(err, cap))?;
        Ok(out)
    }
}
