//! Reading a decoder's output with a hard ceiling.

use super::CompressionError;
use std::io::Read;

/// Drain `decoder`, refusing to hold more than `limit` bytes of its output.
///
/// Reads at most `limit + 1` bytes: the extra one is how "exactly at the
/// limit" is told apart from "past it" without ever buffering the excess.
pub(super) fn read_bounded(decoder: impl Read, limit: usize) -> Result<Vec<u8>, CompressionError> {
    let ceiling = u64::try_from(limit)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or(CompressionError::TooLarge { limit })?;
    let mut out = Vec::new();
    decoder
        .take(ceiling)
        .read_to_end(&mut out)
        .map_err(|err| CompressionError::Malformed(err.to_string()))?;
    if out.len() > limit {
        return Err(CompressionError::TooLarge { limit });
    }
    Ok(out)
}
