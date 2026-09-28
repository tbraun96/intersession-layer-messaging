//! Brotli at quality 4, window 2^18: the best measured ratio on every
//! compressible class of workspace traffic, at a CPU cost the browser absorbs.

use super::limit::read_bounded;
use super::CompressionError;
use std::io::Write;

const QUALITY: u32 = 4;
const LG_WINDOW: u32 = 18;
const BUFFER: usize = 4096;

pub(super) fn compress(raw: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let mut writer = brotli::CompressorWriter::new(Vec::new(), BUFFER, QUALITY, LG_WINDOW);
    writer
        .write_all(raw)
        .map_err(|err| CompressionError::Malformed(format!("brotli compress: {err}")))?;
    // `into_inner` finishes the stream. No `flush` first: that would emit an
    // extra sync block, bytes on the wire that buy nothing for a whole frame.
    Ok(writer.into_inner())
}

pub(super) fn decompress(bytes: &[u8], limit: usize) -> Result<Vec<u8>, CompressionError> {
    read_bounded(brotli::Decompressor::new(bytes, BUFFER), limit)
}
