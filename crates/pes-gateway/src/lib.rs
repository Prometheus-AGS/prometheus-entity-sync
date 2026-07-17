//! Sync gateway: WebSocket connection lifecycle, auth, snapshot + delta delivery.
#![warn(missing_docs)]

pub mod auth;
mod connection;
pub mod error;
pub mod server;

pub use auth::{JwtValidationConfig, JwtValidator};
pub use connection::{ConnectionHandler, SHUTDOWN_ERROR_CODE};
pub use error::{GatewayErrorCode, GatewayErrorResponse};
pub use server::{GatewayConfig, GatewayServer};
