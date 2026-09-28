//! Raw deflate (no zlib or gzip header) at level 1: the CPU-cheap fallback,
//! pure Rust through miniz_oxide so it builds for wasm32.

use super::limit::read_bounded;
use super::CompressionError;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::io::Write;

const LEVEL: u32 = 1;

pub(super) fn compress(raw: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(LEVEL));
    encoder
        .write_all(raw)
        .map_err(|err| CompressionError::Malformed(format!("deflate compress: {err}")))?;
    encoder
        .finish()
        .map_err(|err| CompressionError::Malformed(format!("deflate compress: {err}")))
}

pub(super) fn decompress(bytes: &[u8], limit: usize) -> Result<Vec<u8>, CompressionError> {
    read_bounded(DeflateDecoder::new(bytes), limit)
}
