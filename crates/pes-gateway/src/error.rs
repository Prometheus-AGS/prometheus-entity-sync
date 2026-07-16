//! Client-facing error redaction boundary.
//!
//! [`pes_core::SyncError`] variants carry internal detail that must never
//! reach a client verbatim: `SyncError::Database` wraps a raw
//! [`sqlx::Error`], which can include query text fragments and driver-level
//! detail, and `SyncError::BucketAssignmentFailed` messages can embed rule
//! ids and other server-internal identifiers. This module is the single
//! place in `pes-gateway` where a [`SyncError`] is converted into something
//! safe to serialize back over the wire — every response path (HTTP or
//! WebSocket) must go through [`GatewayErrorResponse::from`] rather than
//! calling `SyncError::to_string()` directly.

use pes_core::SyncError;

/// A coarse, client-safe classification of what went wrong. Deliberately
/// has no variant that distinguishes *why* an internal error occurred —
/// that detail belongs in server-side logs, not the client response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayErrorCode {
    /// The JWT was expired, malformed, or otherwise failed authentication.
    AuthInvalid,
    /// The connection attempted to subscribe to a bucket it is not
    /// authorized for.
    BucketDenied,
    /// A wire-protocol message was malformed or out of sequence.
    ProtocolError,
    /// Anything else — database errors, unexpected internal failures.
    /// Deliberately generic so no internal detail leaks to the client.
    Internal,
}

/// A redacted, client-safe representation of a [`SyncError`]. Safe to
/// serialize and send directly in an HTTP or WebSocket error response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayErrorResponse {
    /// Coarse classification of the failure.
    pub code: GatewayErrorCode,
    /// A static, redacted, human-readable message safe to show a client.
    pub message: String,
}

impl From<&SyncError> for GatewayErrorResponse {
    fn from(err: &SyncError) -> Self {
        match err {
            SyncError::AuthError(_) => GatewayErrorResponse {
                code: GatewayErrorCode::AuthInvalid,
                message: "authentication failed".to_string(),
            },
            SyncError::BucketAssignmentFailed(_) => GatewayErrorResponse {
                code: GatewayErrorCode::BucketDenied,
                message: "bucket assignment could not be resolved".to_string(),
            },
            SyncError::ProtocolError(_) => GatewayErrorResponse {
                code: GatewayErrorCode::ProtocolError,
                message: "protocol error".to_string(),
            },
            // Database, LsnGap, ChecksumMismatch, and any future
            // #[non_exhaustive] variant all collapse to the same generic
            // internal-error response — none of their detail (query
            // fragments, driver errors, internal LSN/checksum state) is
            // safe to expose to a client.
            _ => GatewayErrorResponse {
                code: GatewayErrorCode::Internal,
                message: "internal error".to_string(),
            },
        }
    }
}

impl From<SyncError> for GatewayErrorResponse {
    fn from(err: SyncError) -> Self {
        GatewayErrorResponse::from(&err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_error() -> SyncError {
        // sqlx::Error::Protocol carries an arbitrary string, standing in for
        // driver-level detail (e.g. a fragment of query text) that must
        // never reach a client.
        SyncError::Database(sqlx::Error::Protocol(
            "detail: column \"secret_internal_column\" does not exist in query \
             SELECT secret_internal_column FROM internal_table"
                .to_string(),
        ))
    }

    #[test]
    fn database_error_is_redacted_to_generic_internal_message() {
        let response = GatewayErrorResponse::from(&database_error());
        assert_eq!(response.code, GatewayErrorCode::Internal);
        assert_eq!(response.message, "internal error");
        assert!(!response.message.contains("secret_internal_column"));
        assert!(!response.message.contains("SELECT"));
    }

    #[test]
    fn bucket_assignment_failed_message_is_generalized_not_passed_through() {
        let err = SyncError::BucketAssignmentFailed(
            "rule 'internal_admin_rule' parameter 'user_id' query returned an unsupported \
             column type: mismatched types; Rust type `alloc::string::String` (as SQL type \
             `TEXT`) is not compatible with SQL type `BOOL`"
                .to_string(),
        );
        let response = GatewayErrorResponse::from(&err);
        assert_eq!(response.code, GatewayErrorCode::BucketDenied);
        assert_eq!(response.message, "bucket assignment could not be resolved");
        assert!(!response.message.contains("internal_admin_rule"));
        assert!(!response.message.contains("mismatched types"));
    }

    #[test]
    fn auth_error_is_redacted_to_generic_message() {
        let err = SyncError::AuthError("JWT expired: exp=1700000000, now=1700003600".to_string());
        let response = GatewayErrorResponse::from(&err);
        assert_eq!(response.code, GatewayErrorCode::AuthInvalid);
        assert_eq!(response.message, "authentication failed");
        // The raw exp/now timestamps are server-internal detail (they can
        // aid clock-skew or token-lifetime fingerprinting) and must not
        // appear in the client-facing message.
        assert!(!response.message.contains("1700000000"));
    }

    #[test]
    fn protocol_error_is_redacted() {
        let err =
            SyncError::ProtocolError("unexpected message type 0xFF in state Handshake".to_string());
        let response = GatewayErrorResponse::from(&err);
        assert_eq!(response.code, GatewayErrorCode::ProtocolError);
        assert_eq!(response.message, "protocol error");
    }

    #[test]
    fn lsn_gap_collapses_to_generic_internal_error() {
        let err = SyncError::LsnGap {
            expected: pes_core::PgLsn(100),
            actual: pes_core::PgLsn(50),
        };
        let response = GatewayErrorResponse::from(&err);
        assert_eq!(response.code, GatewayErrorCode::Internal);
        assert_eq!(response.message, "internal error");
    }

    #[test]
    fn checksum_mismatch_collapses_to_generic_internal_error() {
        let err = SyncError::ChecksumMismatch {
            expected: pes_core::BucketChecksum(1),
            actual: pes_core::BucketChecksum(2),
        };
        let response = GatewayErrorResponse::from(&err);
        assert_eq!(response.code, GatewayErrorCode::Internal);
        assert_eq!(response.message, "internal error");
    }

    #[test]
    fn owned_syncerror_conversion_matches_reference_conversion() {
        let err = database_error();
        let by_ref = GatewayErrorResponse::from(&err);
        let by_value = GatewayErrorResponse::from(err);
        assert_eq!(by_ref, by_value);
    }
}
