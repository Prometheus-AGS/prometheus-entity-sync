//! MessagePack encode/decode for PSyncV1 messages.
//!
//! Uses `rmp_serde::to_vec_named` (not `to_vec`) so every message is
//! encoded as a MessagePack map keyed by field name, not a positional
//! array. This is what makes forward compatibility possible: a decoder
//! reading an older or newer message shape simply ignores map keys it
//! doesn't recognize (serde's default behavior — no `deny_unknown_fields`
//! is ever set on these types), rather than misreading positional fields
//! that shifted when a field was added or removed.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::messages::{ClientMessage, ServerMessage};

/// Encode a [`ServerMessage`] to MessagePack bytes.
pub fn encode_server(msg: &ServerMessage) -> Result<Bytes, ProtocolError> {
    let bytes = rmp_serde::to_vec_named(msg)?;
    Ok(Bytes::from(bytes))
}

/// Encode a [`ClientMessage`] to MessagePack bytes.
pub fn encode_client(msg: &ClientMessage) -> Result<Bytes, ProtocolError> {
    let bytes = rmp_serde::to_vec_named(msg)?;
    Ok(Bytes::from(bytes))
}

/// Decode a [`ServerMessage`] from MessagePack bytes.
///
/// Forward-compatible: unrecognized map keys in `bytes` (e.g. fields added
/// by a future protocol version) are silently ignored rather than causing
/// a decode error.
pub fn decode_server(bytes: &[u8]) -> Result<ServerMessage, ProtocolError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

/// Decode a [`ClientMessage`] from MessagePack bytes.
///
/// Forward-compatible: unrecognized map keys in `bytes` are silently
/// ignored rather than causing a decode error.
pub fn decode_client(bytes: &[u8]) -> Result<ClientMessage, ProtocolError> {
    Ok(rmp_serde::from_slice(bytes)?)
}
