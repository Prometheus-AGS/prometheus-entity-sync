//! Forward-compatibility test: bytes containing fields the current
//! `ServerMessage`/`ClientMessage` definitions don't know about must
//! decode successfully, silently ignoring the extra data — this is what
//! lets a server add a field to a future protocol version without
//! breaking older clients still running this crate's version, and
//! vice versa.
//!
//! Simulated by hand-constructing a shadow struct shaped like a "future"
//! version of one variant (same recognized fields, plus one extra) and
//! encoding *that*, then decoding the bytes as the real, current type.

use pes_core::BucketId;
use pes_protocol::{decode_server, ServerMessage};
use serde::Serialize;

/// Mirrors `ServerMessage::SnapshotBegin`'s shape with one additional
/// field (`future_field`) that a hypothetical PSyncV2 might add. Uses the
/// same externally-tagged enum representation serde applies by default to
/// `ServerMessage`, so the wire shape matches exactly except for the
/// extra key.
#[derive(Serialize)]
enum FutureServerMessage {
    SnapshotBegin {
        bucket_id: BucketId,
        total_rows: u64,
        protocol_version: u8,
        future_field: String,
    },
}

#[test]
fn decode_ignores_unknown_field_in_snapshot_begin() {
    let future_msg = FutureServerMessage::SnapshotBegin {
        bucket_id: BucketId("user_entities".to_string()),
        total_rows: 500,
        protocol_version: 2,
        future_field: "some value from a future protocol version".to_string(),
    };
    let bytes = rmp_serde::to_vec_named(&future_msg).expect("encode future shape");

    let decoded = decode_server(&bytes).expect(
        "decoding a message with an extra unrecognized field must succeed, not error",
    );

    match decoded {
        ServerMessage::SnapshotBegin {
            bucket_id,
            total_rows,
            protocol_version,
        } => {
            assert_eq!(bucket_id, BucketId("user_entities".to_string()));
            assert_eq!(total_rows, 500);
            assert_eq!(protocol_version, 2);
        }
        other => panic!("expected SnapshotBegin, got {other:?}"),
    }
}

/// Mirrors `ServerMessage::Keepalive` with an extra field, exercising a
/// second variant to make sure forward-compat isn't accidental to one
/// specific field layout.
#[derive(Serialize)]
enum FutureServerMessage2 {
    Keepalive {
        server_time_ms: u64,
        server_region: String,
    },
}

#[test]
fn decode_ignores_unknown_field_in_keepalive() {
    let future_msg = FutureServerMessage2::Keepalive {
        server_time_ms: 1_700_000_000_000,
        server_region: "us-east-1".to_string(),
    };
    let bytes = rmp_serde::to_vec_named(&future_msg).expect("encode future shape");

    let decoded = decode_server(&bytes).expect("decoding with an extra field must succeed");

    match decoded {
        ServerMessage::Keepalive { server_time_ms } => {
            assert_eq!(server_time_ms, 1_700_000_000_000);
        }
        other => panic!("expected Keepalive, got {other:?}"),
    }
}
