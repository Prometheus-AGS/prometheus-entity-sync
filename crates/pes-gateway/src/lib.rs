//! Sync gateway: WebSocket connection lifecycle, auth, snapshot + delta delivery.

pub mod error;

pub use error::{GatewayErrorCode, GatewayErrorResponse};
