//! Brotli: the best measured ratio on every compressible class of workspace
//! traffic. `policy` runs it at quality 4, window 2^18.

use super::codec::{FrameCodec, Params};
use super::limit::read_bounded;
use super::CompressionError;
use std::io::Write;

const BUFFER: usize = 4096;

pub(super) struct Brotli;

impl FrameCodec for Brotli {
    fn compress(&self, raw: &[u8], params: &Params) -> Result<Vec<u8>, CompressionError> {
        let mut writer =
            brotli::CompressorWriter::new(Vec::new(), BUFFER, params.level, params.window_log);
        writer
            .write_all(raw)
            .map_err(|err| CompressionError::Malformed(format!("brotli compress: {err}")))?;
        // `into_inner` finishes the stream. No `flush` first: that would emit an
        // extra sync block, bytes on the wire that buy nothing for a whole frame.
        Ok(writer.into_inner())
    }

    fn decompress(&self, bytes: &[u8], cap: usize) -> Result<Vec<u8>, CompressionError> {
        read_bounded(brotli::Decompressor::new(bytes, BUFFER), cap)
    }
}
