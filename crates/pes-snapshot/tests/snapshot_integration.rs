//! Integration tests for `SnapshotStream` against a real Postgres instance
//! managed by `testcontainers` — no manual container lifecycle management,
//! no dependency on a pre-existing dev database.

use std::collections::HashMap;
use std::time::Instant;

use futures::StreamExt;
use pes_core::BucketAssignment;
use pes_core::BucketId;
use pes_snapshot::{checksum_batches, SnapshotStream};
use sqlx::postgres::PgPoolOptions;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

async fn start_postgres_with_fixture(row_count: usize) -> (ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to test postgres");

    sqlx::query("CREATE TABLE snapshot_fixture (id BIGINT PRIMARY KEY, _version INT NOT NULL, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create fixture table");

    // Bulk-insert via generate_series — far faster than row-by-row inserts
    // for 100K rows, and keeps this test's own runtime well under control.
    sqlx::query(
        "INSERT INTO snapshot_fixture (id, _version, name) \
         SELECT i, 1, 'row_' || i FROM generate_series(1, $1::bigint) AS i",
    )
    .bind(row_count as i64)
    .execute(&pool)
    .await
    .expect("seed fixture rows");

    (container, pool)
}

fn single_bucket_assignment() -> Vec<BucketAssignment> {
    let mut data_queries = HashMap::new();
    data_queries.insert(
        "snapshot_fixture".to_string(),
        "SELECT * FROM snapshot_fixture".to_string(),
    );
    vec![BucketAssignment {
        bucket_id: BucketId("snapshot_test_bucket".to_string()),
        rule_id: "snapshot_test_rule".to_string(),
        parameters: HashMap::new(),
        data_queries,
    }]
}

async fn start_postgres_with_uuid_fixture(row_count: usize) -> (ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to test postgres");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\"")
        .execute(&pool)
        .await
        .expect("enable pgcrypto for gen_random_uuid()");
    sqlx::query("CREATE TABLE uuid_snapshot_fixture (id UUID PRIMARY KEY, _version INT NOT NULL, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create uuid fixture table");

    // Bulk-insert via generate_series + gen_random_uuid(), matching the
    // bigint fixture's generation style.
    sqlx::query(
        "INSERT INTO uuid_snapshot_fixture (id, _version, name) \
         SELECT gen_random_uuid(), 1, 'row_' || i FROM generate_series(1, $1::bigint) AS i",
    )
    .bind(row_count as i64)
    .execute(&pool)
    .await
    .expect("seed uuid fixture rows");

    (container, pool)
}

fn uuid_bucket_assignment() -> Vec<BucketAssignment> {
    let mut data_queries = HashMap::new();
    data_queries.insert(
        "uuid_snapshot_fixture".to_string(),
        "SELECT * FROM uuid_snapshot_fixture".to_string(),
    );
    vec![BucketAssignment {
        bucket_id: BucketId("uuid_snapshot_test_bucket".to_string()),
        rule_id: "uuid_snapshot_test_rule".to_string(),
        parameters: HashMap::new(),
        data_queries,
    }]
}

/// Snapshot a 100,000-row table: verify every row is emitted exactly once,
/// the batch checksum sequence is deterministic across two independent
/// runs, and the whole operation completes well within the proposal's
/// <5s success criterion.
#[tokio::test]
async fn snapshot_100k_rows_row_count_and_checksum_determinism() {
    const ROW_COUNT: usize = 100_000;
    let (_container, pool) = start_postgres_with_fixture(ROW_COUNT).await;

    let started = Instant::now();
    let stream = SnapshotStream::new(pool.clone(), single_bucket_assignment());
    let batches: Vec<_> = stream
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.expect("batch ok"))
        .collect();
    let elapsed = started.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(total_rows, ROW_COUNT, "every row must be emitted exactly once");
    assert!(
        batches.last().expect("at least one batch").is_last,
        "the final batch must be marked is_last"
    );
    assert!(
        batches[..batches.len() - 1].iter().all(|b| !b.is_last),
        "only the final batch may be marked is_last"
    );

    assert!(
        elapsed.as_secs_f64() < 5.0,
        "snapshot of {ROW_COUNT} rows took {elapsed:?}, exceeding the 5s success criterion"
    );

    // Determinism: re-run the snapshot and compare the folded checksum
    // sequence. The fixture table is not modified between runs.
    let stream2 = SnapshotStream::new(pool, single_bucket_assignment());
    let batches2: Vec<_> = stream2
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.expect("batch ok"))
        .collect();

    let checksums1: Vec<_> = batches.iter().map(|b| b.batch_checksum).collect();
    let checksums2: Vec<_> = batches2.iter().map(|b| b.batch_checksum).collect();
    assert_eq!(
        checksum_batches(&checksums1),
        checksum_batches(&checksums2),
        "two snapshots of unchanged data must produce identical folded checksums"
    );
}

/// Dropping a `SnapshotStream` mid-iteration (after only 3 batches) must
/// not leak the pool connection it was using — verified by checking the
/// pool's idle-connection count returns to its pre-stream value shortly
/// after the stream is dropped.
#[tokio::test]
async fn cancelling_stream_after_three_batches_does_not_leak_connection() {
    // Small batch size so 3 batches is a small, fast fixture, not 30,000 rows.
    const BATCH_SIZE: usize = 50;
    const ROW_COUNT: usize = 500;
    let (_container, pool) = start_postgres_with_fixture(ROW_COUNT).await;

    {
        let stream = SnapshotStream::new(pool.clone(), single_bucket_assignment())
            .with_batch_size(BATCH_SIZE);
        let mut s = Box::pin(stream.stream());
        for _ in 0..3 {
            let batch = s.next().await.expect("batch available").expect("batch ok");
            assert_eq!(batch.rows.len(), BATCH_SIZE);
        }
        // `s` (and the connection it was borrowing from `pool`) is dropped
        // here, before the stream reaches its last batch.
    }

    // Give sqlx a moment to return the connection to the pool after drop.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The invariant that matters is "no connection is permanently stuck
    // outside the pool" — checked by asserting every connection the pool
    // has ever opened is now idle (num_idle == size), not that num_idle
    // matches some pre-stream snapshot. A pre/post equality check on
    // num_idle() is unreliable here because sqlx's pool is lazy: the
    // *first* real assertion in this test is what actually triggers the
    // pool's first connection, so "before" and "after" aren't comparable
    // baselines regardless of leak behavior.
    assert_eq!(
        pool.num_idle() as u32,
        pool.size(),
        "every pool connection must be idle after the cancelled stream is dropped — a stuck busy connection would indicate a leak"
    );

    // The pool must still be fully usable after the cancelled stream.
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM snapshot_fixture")
        .fetch_one(&pool)
        .await
        .expect("pool still usable after cancelled stream");
    assert_eq!(row.0, ROW_COUNT as i64);
}

/// Two independent `SnapshotStream::stream()` calls over the same
/// unchanged data must produce identical checksums, batch-by-batch — not
/// just an identical final fold (already covered above), but identical at
/// every intermediate step, proving pagination itself is deterministic.
#[tokio::test]
async fn two_identical_snapshots_produce_identical_batch_checksums() {
    const ROW_COUNT: usize = 2_000;
    const BATCH_SIZE: usize = 300;
    let (_container, pool) = start_postgres_with_fixture(ROW_COUNT).await;

    let batches1: Vec<_> = SnapshotStream::new(pool.clone(), single_bucket_assignment())
        .with_batch_size(BATCH_SIZE)
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.expect("batch ok"))
        .collect();

    let batches2: Vec<_> = SnapshotStream::new(pool, single_bucket_assignment())
        .with_batch_size(BATCH_SIZE)
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.expect("batch ok"))
        .collect();

    assert_eq!(batches1.len(), batches2.len());
    for (b1, b2) in batches1.iter().zip(batches2.iter()) {
        assert_eq!(
            b1.batch_checksum, b2.batch_checksum,
            "batch at offset {} must have identical checksum across runs",
            b1.offset
        );
        assert_eq!(b1.rows.len(), b2.rows.len());
    }
}

/// Regression test for the `IdCast::Text` keyset-pagination path with a
/// `UUID` primary key: before the fix, `fetch_page`'s generated WHERE
/// clause cast only the bound cursor (`($1::text)::text`), never `sq.id`
/// itself, so the comparison was `uuid > text` — an operator Postgres does
/// not have (`error returned from database: operator does not exist: uuid
/// > text`). This table has zero overlap with `snapshot_fixture`'s `BIGINT`
/// id, which only ever exercises `IdCast::Numeric`.
///
/// A small batch size forces multiple pages (and thus a real, non-null
/// cursor bind on the second page onward), so this actually exercises the
/// `sq.id::text > ($1::text)::text` comparison rather than only the
/// first-page `$1 IS NULL` branch.
#[tokio::test]
async fn snapshot_uuid_id_column_paginates_without_type_error() {
    const ROW_COUNT: usize = 250;
    const BATCH_SIZE: usize = 40;
    let (_container, pool) = start_postgres_with_uuid_fixture(ROW_COUNT).await;

    let stream = SnapshotStream::new(pool, uuid_bucket_assignment()).with_batch_size(BATCH_SIZE);
    let batches: Vec<_> = stream
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.expect("batch ok — must not hit 'operator does not exist: uuid > text'"))
        .collect();

    let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(total_rows, ROW_COUNT, "every row must be emitted exactly once");
    assert!(batches.len() > 1, "test must exercise multiple pages (a real cursor bind), not just the first");
    assert!(
        batches.last().expect("at least one batch").is_last,
        "the final batch must be marked is_last"
    );
    assert!(
        batches[..batches.len() - 1].iter().all(|b| !b.is_last),
        "only the final batch may be marked is_last"
    );

    // No duplicate ids across pages — proves the keyset cursor actually
    // advanced past each page's last row rather than, say, silently
    // re-fetching the same page.
    let mut seen_ids = std::collections::HashSet::new();
    for batch in &batches {
        for row in &batch.rows {
            let id = row.get("id").and_then(|v| v.as_str()).expect("row has string id");
            assert!(seen_ids.insert(id.to_string()), "id {id} appeared in more than one batch");
        }
    }
}
