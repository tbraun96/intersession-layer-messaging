//! Raw deflate (no zlib or gzip header): the CPU-cheap fallback, pure Rust
//! through miniz_oxide so it builds for wasm32. `policy` runs it at level 1.

use super::codec::{FrameCodec, Params};
use super::limit::read_bounded;
use super::CompressionError;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::io::Write;

pub(super) struct Deflate;

impl FrameCodec for Deflate {
    fn compress(&self, raw: &[u8], params: &Params) -> Result<Vec<u8>, CompressionError> {
        let failed =
            |err: std::io::Error| CompressionError::Malformed(format!("deflate compress: {err}"));
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(params.level));
        encoder.write_all(raw).map_err(failed)?;
        encoder.finish().map_err(failed)
    }

    fn decompress(&self, bytes: &[u8], cap: usize) -> Result<Vec<u8>, CompressionError> {
        read_bounded(DeflateDecoder::new(bytes), cap)
    }
}
