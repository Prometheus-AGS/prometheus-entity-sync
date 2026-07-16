//! Concurrency tests: multiple appenders and readers hitting the same
//! `BucketOpLog` simultaneously must never lose, duplicate, or corrupt ops.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pes_core::{BucketId, BucketOp, Op, PgLsn};
use pes_oplog::BucketOpLog;

const APPENDER_COUNT: usize = 10;
const READER_COUNT: usize = 5;
const OPS_PER_APPENDER: usize = 50;

fn make_op(bucket: &BucketId, lsn: u64, appender_idx: usize) -> BucketOp {
    BucketOp {
        lsn: PgLsn(lsn),
        bucket_id: bucket.clone(),
        entity_type: "ConcurrencyTest".to_string(),
        entity_id: format!("appender-{appender_idx}"),
        op: Op::Upsert(serde_json::json!({"appender": appender_idx, "lsn": lsn})),
    }
}

/// 10 concurrent appenders write disjoint LSN ranges into the SAME bucket
/// (a realistic contention scenario — many WAL events fanning into one
/// bucket's op log) while 5 concurrent readers repeatedly drain the log.
/// After all appenders finish, a final drain must see every op exactly
/// once, with no gaps in the LSN sequence.
#[tokio::test]
async fn concurrent_appenders_and_readers_lose_no_ops() {
    let log = Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap());
    let bucket = BucketId("contended_bucket".to_string());

    // Each appender owns a disjoint LSN range so the final "no ops lost"
    // check can assert exact LSN coverage without needing a global lock
    // to hand out LSNs (that allocation is the WAL-to-bucket-router's job
    // in the real system, out of scope for this crate).
    let appender_handles: Vec<_> = (0..APPENDER_COUNT)
        .map(|appender_idx| {
            let log = Arc::clone(&log);
            let bucket = bucket.clone();
            tokio::spawn(async move {
                let base_lsn = (appender_idx * OPS_PER_APPENDER) as u64;
                for i in 0..OPS_PER_APPENDER {
                    let lsn = PgLsn(base_lsn + i as u64);
                    let op = make_op(&bucket, lsn.0, appender_idx);
                    log.append(&bucket, lsn, op).await.unwrap();
                }
            })
        })
        .collect();

    // Readers run concurrently with appenders — they may observe a
    // partial log (that's fine; redb's MVCC guarantees each read sees a
    // consistent snapshot, never a torn write), so readers here just
    // assert internal consistency (no duplicate LSNs within one read),
    // not completeness.
    let reader_handles: Vec<_> = (0..READER_COUNT)
        .map(|_| {
            let log = Arc::clone(&log);
            let bucket = bucket.clone();
            tokio::spawn(async move {
                for _ in 0..5 {
                    let ops: Vec<_> = log
                        .drain_since(&bucket, PgLsn(0))
                        .filter_map(|r| async move { r.ok() })
                        .collect()
                        .await;
                    let lsns: HashSet<u64> = ops.iter().map(|op| op.lsn.0).collect();
                    assert_eq!(
                        lsns.len(),
                        ops.len(),
                        "a single read must never observe duplicate LSNs"
                    );
                }
            })
        })
        .collect();

    for h in appender_handles {
        h.await.unwrap();
    }
    for h in reader_handles {
        h.await.unwrap();
    }

    let final_ops: Vec<_> = log
        .drain_since(&bucket, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;

    let expected_total = APPENDER_COUNT * OPS_PER_APPENDER;
    assert_eq!(final_ops.len(), expected_total, "no ops should be lost");

    let final_lsns: HashSet<u64> = final_ops.iter().map(|op| op.lsn.0).collect();
    assert_eq!(
        final_lsns.len(),
        expected_total,
        "no ops should be duplicated"
    );
    for expected_lsn in 0..expected_total as u64 {
        assert!(
            final_lsns.contains(&expected_lsn),
            "LSN {expected_lsn} is missing — gap in the op log"
        );
    }
}

/// Sanity check that ops across *different* buckets never bleed into each
/// other's range scan, even under concurrent writes to both.
#[tokio::test]
async fn concurrent_writes_to_different_buckets_stay_isolated() {
    let log = Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap());
    let bucket_a = BucketId("bucket_a".to_string());
    let bucket_b = BucketId("bucket_b".to_string());

    let log_a = Arc::clone(&log);
    let bucket_a2 = bucket_a.clone();
    let a_handle = tokio::spawn(async move {
        for i in 0..25u64 {
            log_a
                .append(&bucket_a2, PgLsn(i), make_op(&bucket_a2, i, 0))
                .await
                .unwrap();
        }
    });

    let log_b = Arc::clone(&log);
    let bucket_b2 = bucket_b.clone();
    let b_handle = tokio::spawn(async move {
        for i in 0..25u64 {
            log_b
                .append(&bucket_b2, PgLsn(i), make_op(&bucket_b2, i, 1))
                .await
                .unwrap();
        }
    });

    a_handle.await.unwrap();
    b_handle.await.unwrap();

    let a_ops: Vec<_> = log
        .drain_since(&bucket_a, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    let b_ops: Vec<_> = log
        .drain_since(&bucket_b, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;

    assert_eq!(a_ops.len(), 25);
    assert_eq!(b_ops.len(), 25);
    assert!(a_ops.iter().all(|op| op.bucket_id == bucket_a));
    assert!(b_ops.iter().all(|op| op.bucket_id == bucket_b));
}
