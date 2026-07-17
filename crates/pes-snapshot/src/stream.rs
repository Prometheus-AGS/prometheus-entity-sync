//! Keyset-paginated snapshot streaming over a set of [`BucketAssignment`]s.

use futures::Stream;
use pes_core::{BucketAssignment, BucketChecksum, BucketId, SyncError};
use sqlx::{PgPool, Row};

use crate::checksum::checksum_rows;

/// Default batch size: 10,000 rows, per the proposal.
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// One page of a bucket's snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotBatch {
    /// The bucket this batch belongs to.
    pub bucket_id: BucketId,
    /// The name of the data query within the bucket this batch is for
    /// (a [`BucketAssignment`] can have multiple named `data_queries`;
    /// each is streamed as its own sequence of batches).
    pub table_name: String,
    /// The rows in this batch, as JSON objects (via Postgres `row_to_json`).
    pub rows: Vec<serde_json::Value>,
    /// 0-based index of this batch within its `(bucket_id, table_name)` sequence.
    pub offset: usize,
    /// Whether this is the last batch for this `(bucket_id, table_name)` pair.
    pub is_last: bool,
    /// Checksum over this batch's rows (see [`crate::checksum_rows`]).
    pub batch_checksum: BucketChecksum,
}

/// Streams the current Postgres snapshot for a set of [`BucketAssignment`]s
/// in batches, using keyset (not `OFFSET`) pagination for safety on large,
/// concurrently-modified tables.
///
/// # Row identity convention
///
/// Every `data_queries` SELECT is wrapped as a subquery and must expose an
/// `id` column — this is the keyset cursor's pagination key. Tables synced
/// through `pes-snapshot` are expected to follow this convention (the same
/// one implied by the `sync-rules-reference.md` examples, all of which
/// select `entities`/similar tables with an `id` primary key).
pub struct SnapshotStream {
    pool: PgPool,
    assignments: Vec<BucketAssignment>,
    batch_size: usize,
}

impl SnapshotStream {
    /// Construct a snapshot stream over `assignments`, using the default
    /// batch size ([`DEFAULT_BATCH_SIZE`]).
    pub fn new(pool: PgPool, assignments: Vec<BucketAssignment>) -> Self {
        Self {
            pool,
            assignments,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Override the batch size (default [`DEFAULT_BATCH_SIZE`]). Primarily
    /// useful for tests exercising pagination boundaries without needing
    /// 10,000-row fixtures.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Stream all batches for all of this snapshot's bucket assignments,
    /// in assignment order, then in `data_queries` iteration order within
    /// each assignment.
    pub fn stream(self) -> impl Stream<Item = Result<SnapshotBatch, SyncError>> + Send + 'static {
        let SnapshotStream {
            pool,
            assignments,
            batch_size,
        } = self;

        async_stream::try_stream! {
            for assignment in assignments {
                for (table_name, query) in assignment.data_queries {
                    let id_cast = detect_id_cast(&pool, &query).await?;
                    let mut cursor: Option<String> = None;
                    let mut offset = 0usize;
                    loop {
                        let rows = fetch_page(&pool, &query, cursor.as_deref(), batch_size, id_cast).await?;
                        let is_last = rows.len() < batch_size;
                        let next_cursor = rows.last().and_then(|r| r.get("id")).and_then(row_id_as_string);

                        let row_values: Vec<serde_json::Value> = rows;
                        let batch_checksum = checksum_rows(&row_values);

                        yield SnapshotBatch {
                            bucket_id: assignment.bucket_id.clone(),
                            table_name: table_name.clone(),
                            rows: row_values,
                            offset,
                            is_last,
                            batch_checksum,
                        };

                        if is_last {
                            break;
                        }
                        cursor = next_cursor;
                        offset += 1;
                    }
                }
            }
        }
    }
}

fn row_id_as_string(id: &serde_json::Value) -> Option<String> {
    match id {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The SQL cast to apply to a keyset cursor bind so it compares correctly
/// against the target query's `id` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdCast {
    /// `id` is a numeric type (`int2`/`int4`/`int8`/`numeric`/`float4`/`float8`).
    Numeric,
    /// `id` is anything else (`text`, `uuid`, `varchar`, ...) — compared as text.
    Text,
}

impl IdCast {
    fn as_sql(self) -> &'static str {
        match self {
            IdCast::Numeric => "numeric",
            IdCast::Text => "text",
        }
    }
}

/// Determine `query`'s `id` column cast by asking Postgres directly via
/// `pg_typeof`, run once per `(bucket, data_query)` before pagination
/// starts.
///
/// This must happen even on the very first page (no cursor yet): Postgres
/// type-checks a `WHERE ... OR ...` clause's operators at parse time for
/// ALL branches, not just the one that ends up evaluated at runtime — so
/// `WHERE ($1 IS NULL OR sq.id > $1::text)` still fails to parse against a
/// `bigint` id column even when the `$1 IS NULL` branch is the one that's
/// true. There is no way to defer the cast decision until a real cursor
/// value exists; it must be known up front.
async fn detect_id_cast(pool: &PgPool, query: &str) -> Result<IdCast, SyncError> {
    let type_query = format!(
        "SELECT pg_typeof(sq.id)::text AS id_type FROM ({query}) AS sq LIMIT 1"
    );
    let row = sqlx::query(&type_query)
        .fetch_optional(pool)
        .await
        .map_err(SyncError::Database)?;

    let Some(row) = row else {
        // Empty result set — no rows to introspect the type from. The
        // cast choice is moot (the pagination loop will fetch zero rows
        // either way), so default to Text, the more permissive comparison.
        return Ok(IdCast::Text);
    };

    let type_name: String = row.try_get("id_type").map_err(SyncError::Database)?;
    Ok(match type_name.as_str() {
        "smallint" | "integer" | "bigint" | "numeric" | "real" | "double precision" => {
            IdCast::Numeric
        }
        _ => IdCast::Text,
    })
}

/// Fetch one page of `query`'s results, ordered and paginated by `id`.
///
/// `query` is wrapped as a subquery so any `SELECT` from `sync-rules.toml`
/// (already fully resolved by [`crate::checksum`]'s caller — see
/// `BucketAssigner::resolve_rule`, which substitutes `{bucket_parameters.X}`
/// via the allowlisted [`pes_rules`] template engine before this crate ever
/// sees the query string) can be paginated uniformly without needing to
/// parse or rewrite its `WHERE`/`ORDER BY` clauses.
async fn fetch_page(
    pool: &PgPool,
    query: &str,
    cursor: Option<&str>,
    limit: usize,
    id_cast: IdCast,
) -> Result<Vec<serde_json::Value>, SyncError> {
    // SECURITY: `query` is interpolated via `format!` here, which would be
    // a SQL injection risk for *user-controlled* input — but `query` is
    // never user-controlled. It is a `BucketAssignment.data_queries` entry,
    // meaning it already passed through `BucketAssigner::resolve_rule` and
    // `pes_rules::template::substitute`, which only ever inserts values
    // that matched the `^[a-zA-Z0-9_-]{1,128}$` allowlist (see
    // `pes-rules/src/template.rs`) in place of `{bucket_parameters.X}`
    // placeholders. SQL has no bind-parameter syntax for subquery/table
    // expressions, so wrapping via string interpolation is the only way to
    // apply uniform keyset pagination to an arbitrary trusted SELECT —
    // the cursor and limit values (the only per-request, still-untrusted
    // inputs to *this* function) are bound normally via `$1`/`$2` below.
    //
    // `id_cast` (determined once per query by `detect_id_cast`, before
    // pagination starts — see its doc comment for why it can't be decided
    // lazily from the cursor value) ensures the comparison happens in the
    // id column's real type: comparing `sq.id::text > $1::text` for a
    // numeric id column would corrupt keyset ordering ('2' > '10'
    // lexicographically, but 2 < 10 numerically) — caught by the 100K-row
    // integration test, which returned 749,966 rows instead of 100,000
    // before this fix.
    //
    // Both sides of the comparison must be cast to `cast`, not just the
    // bound cursor: `sq.id` retains its native column type (e.g. `uuid`),
    // and Postgres has no `uuid > text`/`uuid > numeric` operator, so
    // casting only `$1` still fails to parse for any non-numeric id type
    // (`error returned from database: operator does not exist: uuid >
    // text`). Casting `sq.id` too — `sq.id::{cast} > ($1::text)::{cast}` —
    // makes both sides the same type regardless of the column's native type.
    let wrapped = format!(
        "SELECT row_to_json(sq) AS row_data FROM ({query}) AS sq WHERE ($1::text IS NULL OR sq.id::{cast} > ($1::text)::{cast}) ORDER BY sq.id LIMIT $2",
        cast = id_cast.as_sql(),
    );

    let rows = sqlx::query(&wrapped)
        .bind(cursor)
        .bind(limit as i64)
        .fetch_all(pool)
        .await
        .map_err(SyncError::Database)?;

    rows.into_iter()
        .map(|row| {
            let json: serde_json::Value = row.try_get("row_data").map_err(SyncError::Database)?;
            Ok(json)
        })
        .collect()
}
