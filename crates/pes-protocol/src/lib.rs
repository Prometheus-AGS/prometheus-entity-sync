//! PSyncV1 wire protocol: ServerMessage / ClientMessage MessagePack codec.
#![warn(missing_docs)]

mod codec;
mod error;
mod messages;

pub use codec::{decode_client, decode_server, encode_client, encode_server};
pub use error::ProtocolError;
pub use messages::{
    ClientMessage, ServerMessage, ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
