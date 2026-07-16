//! Errors produced while encoding or decoding PSyncV1 messages.

/// An error while encoding or decoding a [`crate::ServerMessage`] or [`crate::ClientMessage`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// A message could not be serialized to MessagePack bytes.
    #[error("failed to encode message: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    /// A byte sequence could not be decoded as a valid PSyncV1 message.
    #[error("failed to decode message: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}
