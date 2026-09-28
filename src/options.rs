//! How an ILM instance behaves on the wire beyond the legacy protocol.
//!
//! There is no `Default` here, on purpose. Every construction site states what
//! it wants, so turning a traffic optimisation on or off is a visible decision
//! at the place that owns it rather than something inherited silently.

use crate::compression::DynamicCompression;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IlmOptions {
    /// Fold the cumulative ACK for a peer into the next data frame sent to it,
    /// instead of spending a frame of its own. A standalone ACK still goes out
    /// within `PIGGYBACK_ACK_WINDOW` if nothing else does, so a single message
    /// waits no longer to be retired than it did before.
    ///
    /// Used toward a peer only after that peer has advertised the same.
    pub piggyback_acks: bool,
    /// Which codecs may compress a message's contents. A message is compressed
    /// only if its sender supplied a `CompressionHint` that calls for it, and
    /// only with a codec the receiving peer has advertised.
    pub dynamic_compression: DynamicCompression,
}

impl IlmOptions {
    /// The legacy protocol exactly: no advertisement, no piggybacking, no
    /// compression. Every frame is byte-identical to what a build without
    /// these options sends.
    pub const LEGACY: Self = Self {
        piggyback_acks: false,
        dynamic_compression: DynamicCompression::Disabled,
    };
}
