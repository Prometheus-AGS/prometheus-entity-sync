//! Round-trip tests: every `ServerMessage` and `ClientMessage` variant must
//! survive `encode → decode` with no data loss.

use std::collections::HashMap;

use pes_core::{BucketChecksum, BucketId, BucketOp, Op, PgLsn};
use pes_protocol::{
    decode_client, decode_server, encode_client, encode_server, ClientMessage, ServerMessage,
};

// --- ServerMessage variants ---

#[test]
fn roundtrip_snapshot_begin() {
    let msg = ServerMessage::SnapshotBegin {
        bucket_id: BucketId("user_entities".to_string()),
        total_rows: 12_345,
        protocol_version: 1,
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_snapshot_batch() {
    let msg = ServerMessage::SnapshotBatch {
        bucket_id: BucketId("user_entities".to_string()),
        rows: vec![
            serde_json::json!({"id": "1", "name": "alice"}),
            serde_json::json!({"id": "2", "name": "bob"}),
        ],
        offset: 3,
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_snapshot_complete() {
    let msg = ServerMessage::SnapshotComplete {
        bucket_id: BucketId("user_entities".to_string()),
        checksum: BucketChecksum(0xDEAD_BEEF),
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_delta() {
    let bucket_id = BucketId("user_entities".to_string());
    let msg = ServerMessage::Delta {
        bucket_id: bucket_id.clone(),
        ops: vec![
            BucketOp {
                lsn: PgLsn(1),
                bucket_id: bucket_id.clone(),
                entity_type: "Todo".to_string(),
                entity_id: "t1".to_string(),
                op: Op::Upsert(serde_json::json!({"title": "buy milk"})),
            },
            BucketOp {
                lsn: PgLsn(2),
                bucket_id: bucket_id.clone(),
                entity_type: "Todo".to_string(),
                entity_id: "t2".to_string(),
                op: Op::Delete,
            },
            BucketOp {
                lsn: PgLsn(3),
                bucket_id,
                entity_type: "Todo".to_string(),
                entity_id: "t3".to_string(),
                op: Op::CrdtPatch(vec![0x00, 0xFF, 0x10, 0x20]),
            },
        ],
        lsn: PgLsn(3),
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_checkpoint() {
    let mut bucket_checksums = HashMap::new();
    bucket_checksums.insert(BucketId("b1".to_string()), BucketChecksum(1));
    bucket_checksums.insert(BucketId("b2".to_string()), BucketChecksum(2));
    let msg = ServerMessage::Checkpoint {
        lsn: PgLsn(100),
        bucket_checksums,
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_keepalive() {
    let msg = ServerMessage::Keepalive {
        server_time_ms: 1_700_000_000_000,
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_error() {
    let msg = ServerMessage::Error {
        code: 4000,
        message: "unsupported protocol version".to_string(),
    };
    let bytes = encode_server(&msg).unwrap();
    assert_eq!(decode_server(&bytes).unwrap(), msg);
}

// --- ClientMessage variants ---

#[test]
fn roundtrip_subscribe() {
    let msg = ClientMessage::Subscribe {
        buckets: vec!["user_entities".to_string(), "tenant_shared".to_string()],
        token: "eyJ...".to_string(),
        resume_lsn: Some(PgLsn(42)),
        protocol_version: 1,
    };
    let bytes = encode_client(&msg).unwrap();
    assert_eq!(decode_client(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_subscribe_no_resume_lsn() {
    let msg = ClientMessage::Subscribe {
        buckets: vec!["user_entities".to_string()],
        token: "eyJ...".to_string(),
        resume_lsn: None,
        protocol_version: 1,
    };
    let bytes = encode_client(&msg).unwrap();
    assert_eq!(decode_client(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_ack() {
    let msg = ClientMessage::Ack { lsn: PgLsn(99) };
    let bytes = encode_client(&msg).unwrap();
    assert_eq!(decode_client(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_write() {
    let msg = ClientMessage::Write {
        entity_type: "Todo".to_string(),
        entity_id: "t1".to_string(),
        op: Op::Upsert(serde_json::json!({"title": "new todo"})),
    };
    let bytes = encode_client(&msg).unwrap();
    assert_eq!(decode_client(&bytes).unwrap(), msg);
}

#[test]
fn roundtrip_ping() {
    let msg = ClientMessage::Ping;
    let bytes = encode_client(&msg).unwrap();
    assert_eq!(decode_client(&bytes).unwrap(), msg);
}
