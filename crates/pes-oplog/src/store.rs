//! `BucketOpLog` — per-bucket append-only operation log backed by redb.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::Stream;
use pes_core::{BucketChecksum, BucketId, BucketOp, PgLsn, SyncError};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::error::OpLogError;
use crate::key::{lsn_from_key, make_bucket_range_end, make_key, make_range_start};

/// Table: composite key (`bucket_id` + `lsn`, see [`crate::key`]) → MessagePack-encoded [`BucketOp`] envelope.
const BUCKET_OPS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bucket_ops");

/// Table: `bucket_id` UTF-8 bytes → running `u64` checksum (little-endian).
const BUCKET_CHECKSUMS: TableDefinition<&[u8], u64> = TableDefinition::new("bucket_checksums");

/// A single stored op envelope: the `BucketOp` payload plus the wall-clock
/// time it was appended, used by [`BucketOpLog::compact`] to decide
/// eligibility for removal. `BucketOp` itself carries no timestamp — LSNs
/// are monotonic but not directly convertible to wall-clock time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredOp {
    op: BucketOp,
    appended_at_unix_secs: u64,
}

/// Per-bucket append-only operation log.
///
/// Ops are ordered by [`PgLsn`], queryable by range via [`Self::drain_since`],
/// and compactable by age via [`Self::compact`]. Backed by an embedded
/// `redb` database — no external dependencies at runtime.
pub struct BucketOpLog {
    db: Arc<Database>,
    compaction_ttl: Duration,
}

impl std::fmt::Debug for BucketOpLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketOpLog")
            .field("compaction_ttl", &self.compaction_ttl)
            .finish_non_exhaustive()
    }
}

impl BucketOpLog {
    /// Default compaction TTL: 7 days.
    pub const DEFAULT_COMPACTION_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

    /// Open (or create) the redb database at `path`, with the given
    /// `compaction_ttl` for [`Self::compact`].
    pub fn open(path: impl AsRef<Path>, compaction_ttl: Duration) -> Result<Self, OpLogError> {
        let db = Database::create(path.as_ref())?;
        Self::from_database(db, compaction_ttl)
    }

    /// Open an in-memory redb instance (no file backing). Useful for tests.
    pub fn in_memory(compaction_ttl: Duration) -> Result<Self, OpLogError> {
        use redb::backends::InMemoryBackend;
        let db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        Self::from_database(db, compaction_ttl)
    }

    fn from_database(db: Database, compaction_ttl: Duration) -> Result<Self, OpLogError> {
        {
            let wtx = db.begin_write()?;
            let _ = wtx.open_table(BUCKET_OPS)?;
            let _ = wtx.open_table(BUCKET_CHECKSUMS)?;
            wtx.commit()?;
        }
        Ok(Self {
            db: Arc::new(db),
            compaction_ttl,
        })
    }

    /// Append an op for `bucket_id` at `lsn`. Returns the stored LSN.
    ///
    /// Updates the bucket's running checksum in the same write transaction,
    /// so `append` and the checksum update are atomic — a reader can never
    /// observe a new op without the checksum having advanced to match.
    pub async fn append(&self, bucket_id: &BucketId, lsn: PgLsn, op: BucketOp) -> Result<PgLsn, SyncError> {
        let db = Arc::clone(&self.db);
        let bucket_id = bucket_id.0.clone();
        let stored = StoredOp {
            op,
            appended_at_unix_secs: unix_now_secs(),
        };
        let payload = rmp_serde::to_vec(&stored).map_err(|e| OpLogError::Codec(e.to_string()))?;

        tokio::task::spawn_blocking(move || -> Result<PgLsn, OpLogError> {
            let key = make_key(&bucket_id, lsn.0);
            let wtx = db.begin_write()?;
            {
                let mut ops_table = wtx.open_table(BUCKET_OPS)?;
                ops_table.insert(key.as_slice(), payload.as_slice())?;

                let mut checksums_table = wtx.open_table(BUCKET_CHECKSUMS)?;
                let prev = checksums_table
                    .get(bucket_id.as_bytes())?
                    .map(|v| v.value())
                    .unwrap_or(0);
                let next = fold_checksum(prev, &payload);
                checksums_table.insert(bucket_id.as_bytes(), next)?;
            }
            wtx.commit()?;
            Ok(lsn)
        })
        .await
        .map_err(|e| OpLogError::Task(e.to_string()))?
        .map_err(SyncError::from)
    }

    /// Stream all ops for `bucket_id` since (and including) `from_lsn`, in
    /// ascending LSN order.
    pub fn drain_since(
        &self,
        bucket_id: &BucketId,
        from_lsn: PgLsn,
    ) -> impl Stream<Item = Result<BucketOp, SyncError>> + Send + 'static {
        let db = Arc::clone(&self.db);
        let bucket_id_str = bucket_id.0.clone();

        async_stream::try_stream! {
            let range_start = make_range_start(&bucket_id_str, from_lsn.0);
            let range_end = make_bucket_range_end(&bucket_id_str);
            let db2 = Arc::clone(&db);
            let bucket_id_for_blocking = bucket_id_str.clone();

            let ops: Vec<BucketOp> = tokio::task::spawn_blocking(move || -> Result<Vec<BucketOp>, OpLogError> {
                let rtx = db2.begin_read()?;
                let table = rtx.open_table(BUCKET_OPS)?;
                let range = table.range(range_start.as_slice()..range_end.as_slice())?;

                let mut out = Vec::new();
                for entry in range {
                    let (k, v) = entry?;
                    let key_bytes = k.value();
                    // Defensive: skip anything that doesn't decode as an LSN
                    // suffix rather than panicking on malformed data.
                    if lsn_from_key(key_bytes).is_none() {
                        continue;
                    }
                    let stored: StoredOp = rmp_serde::from_slice(v.value())
                        .map_err(|e| OpLogError::Codec(e.to_string()))?;
                    out.push(stored.op);
                }
                let _ = bucket_id_for_blocking; // retained for future tracing context
                Ok(out)
            })
            .await
            .map_err(|e| OpLogError::Task(e.to_string()))?
            .map_err(SyncError::from)?;

            for op in ops {
                yield op;
            }
        }
    }

    /// Return the running checksum for `bucket_id`'s entire log.
    ///
    /// Returns a [`BucketChecksum`] of `0` for a bucket with no ops
    /// appended yet.
    pub async fn checksum(&self, bucket_id: &BucketId) -> Result<BucketChecksum, SyncError> {
        let db = Arc::clone(&self.db);
        let bucket_id = bucket_id.0.clone();

        tokio::task::spawn_blocking(move || -> Result<BucketChecksum, OpLogError> {
            let rtx = db.begin_read()?;
            let table = rtx.open_table(BUCKET_CHECKSUMS)?;
            let value = table
                .get(bucket_id.as_bytes())?
                .map(|v| v.value())
                .unwrap_or(0);
            Ok(BucketChecksum(value))
        })
        .await
        .map_err(|e| OpLogError::Task(e.to_string()))?
        .map_err(SyncError::from)
    }

    /// Remove all ops older than `compaction_ttl`. Returns the number of
    /// ops removed.
    ///
    /// Checksums are intentionally left untouched by compaction: the
    /// running checksum is a fold over every op ever appended, not just
    /// the currently-retained ones, so a client's LSN/checksum expectation
    /// (formed before compaction) stays valid — compaction only reclaims
    /// storage for ops old enough that no live client should still be
    /// asking for them via `drain_since`.
    ///
    /// Age is tracked at whole-second resolution (`appended_at_unix_secs`),
    /// not sub-second — acceptable given `compaction_ttl` defaults to 7
    /// days, but means an op appended less than one second ago may already
    /// be eligible for removal under an unusually small `compaction_ttl`
    /// (e.g. `Duration::from_secs(0)` removes everything immediately).
    pub async fn compact(&self) -> Result<u64, SyncError> {
        let db = Arc::clone(&self.db);
        let cutoff = unix_now_secs().saturating_sub(self.compaction_ttl.as_secs());

        tokio::task::spawn_blocking(move || -> Result<u64, OpLogError> {
            let wtx = db.begin_write()?;
            let mut removed = 0u64;
            {
                let mut table = wtx.open_table(BUCKET_OPS)?;
                let keys_to_remove: Vec<Vec<u8>> = {
                    let mut to_remove = Vec::new();
                    for entry in table.iter()? {
                        let (k, v) = entry?;
                        let stored: StoredOp = rmp_serde::from_slice(v.value())
                            .map_err(|e| OpLogError::Codec(e.to_string()))?;
                        if stored.appended_at_unix_secs < cutoff {
                            to_remove.push(k.value().to_vec());
                        }
                    }
                    to_remove
                };
                for key in keys_to_remove {
                    table.remove(key.as_slice())?;
                    removed += 1;
                }
            }
            wtx.commit()?;
            Ok(removed)
        })
        .await
        .map_err(|e| OpLogError::Task(e.to_string()))?
        .map_err(SyncError::from)
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Fold a new op's serialized bytes into a running checksum.
///
/// Uses a simple FNV-1a-style fold over `(prev, payload)` — sufficient for
/// detecting divergence between client and server state (the purpose of
/// [`BucketChecksum`]), not a cryptographic integrity guarantee.
fn fold_checksum(prev: u64, payload: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = prev ^ 0xcbf29ce484222325;
    for &byte in payload {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
