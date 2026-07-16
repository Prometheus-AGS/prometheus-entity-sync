//! Benchmark: 10,000 appends spread across 100 buckets, run concurrently.
//!
//! Validates the proposal's success criterion: "Sustains 10,000 concurrent
//! appends/second." `criterion` measures wall-clock time for the whole
//! batch; divide `10_000 / elapsed_secs` to get appends/sec from the report.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::future::join_all;
use pes_core::{BucketId, BucketOp, Op, PgLsn};
use pes_oplog::BucketOpLog;
use tokio::runtime::Runtime;

const TOTAL_APPENDS: usize = 10_000;
const BUCKET_COUNT: usize = 100;
const APPENDS_PER_BUCKET: usize = TOTAL_APPENDS / BUCKET_COUNT;

fn bench_concurrent_appends(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("10k_appends_100_buckets_concurrent", |b| {
        b.to_async(&rt).iter_batched(
            || Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).expect("open oplog")),
            |log| async move {
                let mut handles = Vec::with_capacity(BUCKET_COUNT);
                for bucket_idx in 0..BUCKET_COUNT {
                    let log = Arc::clone(&log);
                    handles.push(tokio::spawn(async move {
                        let bucket_id = BucketId(format!("bench_bucket_{bucket_idx}"));
                        for i in 0..APPENDS_PER_BUCKET {
                            let lsn = PgLsn(i as u64);
                            let op = BucketOp {
                                lsn,
                                bucket_id: bucket_id.clone(),
                                entity_type: "BenchEntity".to_string(),
                                entity_id: format!("e{i}"),
                                op: Op::Upsert(serde_json::json!({"i": i})),
                            };
                            log.append(&bucket_id, lsn, op)
                                .await
                                .expect("append succeeds");
                        }
                    }));
                }
                join_all(handles).await;
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_concurrent_appends
}
criterion_main!(benches);
