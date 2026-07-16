//! Compaction tests: removing old ops must not corrupt the running
//! checksum, and reads issued concurrently with compaction must never
//! observe a torn/partial state.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pes_core::{BucketId, BucketOp, Op, PgLsn};
use pes_oplog::BucketOpLog;

fn make_op(bucket: &BucketId, lsn: u64) -> BucketOp {
    BucketOp {
        lsn: PgLsn(lsn),
        bucket_id: bucket.clone(),
        entity_type: "CompactionTest".to_string(),
        entity_id: format!("e{lsn}"),
        op: Op::Upsert(serde_json::json!({"lsn": lsn})),
    }
}

/// Compaction with a TTL long enough that nothing is eligible for removal
/// must leave the checksum completely unchanged.
#[tokio::test]
async fn compact_with_nothing_eligible_preserves_checksum() {
    let log = BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap();
    let bucket = BucketId("b1".to_string());
    for lsn in 0..10u64 {
        log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
            .await
            .unwrap();
    }

    let checksum_before = log.checksum(&bucket).await.unwrap();
    let removed = log.compact().await.unwrap();
    let checksum_after = log.checksum(&bucket).await.unwrap();

    assert_eq!(removed, 0);
    assert_eq!(
        checksum_before, checksum_after,
        "checksum must not change when compaction removes nothing"
    );
}

/// Compaction is documented (see `BucketOpLog::compact` doc comment) to
/// leave checksums untouched by design — the running checksum is a fold
/// over every op ever appended, not just currently-retained ones, so a
/// client's checksum expectation (formed before compaction ran) stays
/// valid. This test locks that contract in: even when ops ARE removed,
/// the checksum must be unchanged.
#[tokio::test]
async fn compact_removing_ops_still_preserves_checksum() {
    let log = BucketOpLog::in_memory(Duration::from_secs(0)).unwrap();
    let bucket = BucketId("b1".to_string());
    for lsn in 0..10u64 {
        log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
            .await
            .unwrap();
    }

    let checksum_before = log.checksum(&bucket).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let removed = log.compact().await.unwrap();
    assert_eq!(removed, 10, "with ttl=0, everything should be eligible");

    let checksum_after = log.checksum(&bucket).await.unwrap();
    assert_eq!(
        checksum_before, checksum_after,
        "checksum must survive compaction unchanged, by design"
    );

    let remaining: Vec<_> = log
        .drain_since(&bucket, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    assert!(remaining.is_empty(), "all ops should have been removed");
}

/// Compaction only removes ops older than the cutoff — ops appended after
/// compaction starts (or recently enough to still be within the TTL) must
/// survive, and the checksum must still reflect the full fold, not a
/// partial one that "forgot" the removed ops.
#[tokio::test]
async fn compact_partial_removal_preserves_checksum_and_remaining_ops() {
    let log = BucketOpLog::in_memory(Duration::from_millis(500)).unwrap();
    let bucket = BucketId("b1".to_string());

    // First batch: will be old enough to compact.
    for lsn in 0..5u64 {
        log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Second batch: appended just before compact() runs, must survive.
    for lsn in 5..10u64 {
        log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
            .await
            .unwrap();
    }

    let checksum_before = log.checksum(&bucket).await.unwrap();
    let removed = log.compact().await.unwrap();
    let checksum_after = log.checksum(&bucket).await.unwrap();

    assert_eq!(removed, 5, "only the first (older) batch should be removed");
    assert_eq!(
        checksum_before, checksum_after,
        "checksum reflects the full history regardless of what compaction removed"
    );

    let remaining: Vec<_> = log
        .drain_since(&bucket, PgLsn(0))
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;
    assert_eq!(remaining.len(), 5);
    for op in &remaining {
        assert!(op.lsn.0 >= 5, "only the surviving second batch should remain");
    }
}

/// A read (`drain_since`) issued concurrently with `compact()` must
/// observe a consistent state — either the pre-compaction or
/// post-compaction view, never a state with some-but-not-all of a
/// transaction's removals applied. redb's MVCC is what guarantees this;
/// this test exercises it under real concurrency rather than asserting it
/// only by code inspection.
#[tokio::test]
async fn concurrent_read_during_compact_sees_consistent_state() {
    let log = Arc::new(BucketOpLog::in_memory(Duration::from_millis(100)).unwrap());
    let bucket = BucketId("b1".to_string());
    for lsn in 0..20u64 {
        log.append(&bucket, PgLsn(lsn), make_op(&bucket, lsn))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let log_reader = Arc::clone(&log);
    let bucket_reader = bucket.clone();
    let reader = tokio::spawn(async move {
        let mut observed_counts = Vec::new();
        for _ in 0..10 {
            let ops: Vec<_> = log_reader
                .drain_since(&bucket_reader, PgLsn(0))
                .filter_map(|r| async move { r.ok() })
                .collect()
                .await;
            observed_counts.push(ops.len());
        }
        observed_counts
    });

    let log_compactor = Arc::clone(&log);
    let compactor = tokio::spawn(async move { log_compactor.compact().await.unwrap() });

    let (observed_counts, _removed) = tokio::join!(reader, compactor);
    let observed_counts = observed_counts.unwrap();

    // Every observed count must be a valid state: either all 20 ops (read
    // happened before compaction's write transaction committed) or 0 ops
    // (read happened after) — redb's MVCC means no in-between value like
    // 7 or 13 is possible, since compact() removes everything in one
    // atomic write transaction.
    for count in observed_counts {
        assert!(
            count == 0 || count == 20,
            "observed a torn read during compaction: {count} ops (expected 0 or 20)"
        );
    }
}
