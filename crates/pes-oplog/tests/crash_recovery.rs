//! Crash recovery tests: ops committed before a simulated crash (dropping
//! the `BucketOpLog`, closing the underlying redb file handle) must be
//! recoverable in full after reopening the same database file.
//!
//! `redb` write transactions are atomic — a transaction either commits
//! completely or not at all, so there is no "torn write" state to
//! reproduce at this crate's API surface. What we *can* and do verify:
//! (1) every op whose `append()` call returned `Ok` before the drop is
//! still present after reopening, in the same LSN order; (2) an op whose
//! `append()` was never called (simulating work queued but not yet
//! flushed when the process died) is correctly absent — i.e. recovery
//! reflects exactly the last *committed* LSN, not a hoped-for later one.

use std::time::Duration;

use futures::StreamExt;
use pes_core::{BucketId, BucketOp, Op, PgLsn};
use pes_oplog::BucketOpLog;

fn make_op(bucket: &BucketId, lsn: u64) -> BucketOp {
    BucketOp {
        lsn: PgLsn(lsn),
        bucket_id: bucket.clone(),
        entity_type: "CrashTest".to_string(),
        entity_id: format!("e{lsn}"),
        op: Op::Upsert(serde_json::json!({"lsn": lsn})),
    }
}

#[tokio::test]
async fn ops_committed_before_crash_survive_reopen() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db_path = tmp_dir.path().join("crash_test.redb");
    let bucket = BucketId("crash_bucket".to_string());

    {
        let log = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
        for lsn in 0..10u64 {
            log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
                .await
                .unwrap();
        }
        // Simulate the process dying here: `log` is dropped without any
        // explicit close/flush call. redb has already fsync'd each
        // committed write transaction, so nothing further is needed for
        // durability — that's the property this test verifies.
    }

    let reopened = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
    let recovered: Vec<_> = reopened
        .drain_since(&bucket, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;

    assert_eq!(recovered.len(), 10, "all committed ops must survive reopen");
    for (i, op) in recovered.iter().enumerate() {
        assert_eq!(op.lsn.0, i as u64, "LSN order must be preserved after reopen");
    }
}

#[tokio::test]
async fn checksum_survives_reopen_and_matches_pre_crash_value() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db_path = tmp_dir.path().join("crash_checksum_test.redb");
    let bucket = BucketId("crash_bucket".to_string());

    let pre_crash_checksum = {
        let log = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
        for lsn in 0..5u64 {
            log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
                .await
                .unwrap();
        }
        log.checksum(&bucket).await.unwrap()
    };

    let reopened = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
    let post_reopen_checksum = reopened.checksum(&bucket).await.unwrap();

    assert_eq!(
        pre_crash_checksum, post_reopen_checksum,
        "checksum must be identical before and after a crash+reopen cycle"
    );
}

/// The "last committed LSN" after a crash is exactly the highest LSN whose
/// `append()` call returned `Ok` before the drop — an op that was never
/// appended (e.g. queued in application memory but not yet flushed to the
/// oplog when the process died) is correctly absent, not silently
/// fabricated or rolled forward.
#[tokio::test]
async fn uncommitted_op_is_absent_after_reopen() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db_path = tmp_dir.path().join("crash_partial_test.redb");
    let bucket = BucketId("crash_bucket".to_string());

    {
        let log = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
        log.append(&bucket, PgLsn(0), make_op(&bucket, 0))
            .await
            .unwrap();
        log.append(&bucket, PgLsn(1), make_op(&bucket, 1))
            .await
            .unwrap();
        // LSN 2 is deliberately never appended — simulates work that was
        // in flight (e.g. still being serialized) when the process died.
    }

    let reopened = BucketOpLog::open(&db_path, Duration::from_secs(3600)).unwrap();
    let recovered: Vec<_> = reopened
        .drain_since(&bucket, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;

    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[1].lsn.0, 1, "highest committed LSN is 1, not 2");
}
