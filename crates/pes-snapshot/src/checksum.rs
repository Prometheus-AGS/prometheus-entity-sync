//! Deterministic `xxh3` checksums over snapshot rows.
//!
//! Two levels: [`checksum_rows`] hashes one batch's rows (each row
//! identified by its `id` and — where present — a `_version` column,
//! per the sync-rules convention that synced tables expose a stable
//! primary key named `id`). [`checksum_batches`] folds the ordered
//! sequence of per-batch checksums into a single bucket-level checksum, so
//! a client can verify it received every batch, in order, with nothing
//! dropped or duplicated — without re-hashing the full row set.

use pes_core::BucketChecksum;
use xxhash_rust::xxh3::Xxh3;

/// Compute a deterministic checksum over one batch of rows.
///
/// Each row must be a JSON object containing an `id` field (string or
/// number) and, if present, a `_version` field — both are hashed;
/// remaining fields are not, since the checksum's purpose is detecting
/// *which rows* diverged, not deep row-content equality (a `data_queries`
/// SELECT's non-key columns can change shape across sync-rules versions
/// without invalidating snapshot integrity checks).
///
/// Rows are hashed in the order given — callers are responsible for
/// passing PK-sorted rows (which [`crate::SnapshotStream`] already
/// guarantees via its keyset `ORDER BY id` pagination), since a checksum
/// over an unsorted row set would not be deterministic across runs.
pub fn checksum_rows(rows: &[serde_json::Value]) -> BucketChecksum {
    let mut hasher = Xxh3::new();
    for row in rows {
        hash_row_identity(&mut hasher, row);
    }
    BucketChecksum(hasher.digest())
}

fn hash_row_identity(hasher: &mut Xxh3, row: &serde_json::Value) {
    if let Some(id) = row.get("id") {
        hash_json_scalar(hasher, id);
    }
    hasher.update(b"|");
    if let Some(version) = row.get("_version") {
        hash_json_scalar(hasher, version);
    }
    hasher.update(b";");
}

fn hash_json_scalar(hasher: &mut Xxh3, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => hasher.update(s.as_bytes()),
        serde_json::Value::Number(n) => hasher.update(n.to_string().as_bytes()),
        serde_json::Value::Bool(b) => hasher.update(&[u8::from(*b)]),
        serde_json::Value::Null => hasher.update(b"null"),
        // Object/Array ids are not part of the `id`/`_version` convention;
        // fall back to their canonical JSON string form so the checksum is
        // still well-defined rather than silently skipping the field.
        other => hasher.update(other.to_string().as_bytes()),
    }
}

/// Fold an ordered sequence of per-batch checksums into a single
/// bucket-level checksum.
///
/// Order matters: folding `[a, b]` produces a different result than
/// `[b, a]`, so a client that received the same batches out of order (or
/// missed one) will compute a different final checksum than the server —
/// exactly the property needed to detect gaps or reordering, not just
/// missing rows.
pub fn checksum_batches(batch_checksums: &[BucketChecksum]) -> BucketChecksum {
    let mut hasher = Xxh3::new();
    for checksum in batch_checksums {
        hasher.update(&checksum.0.to_le_bytes());
    }
    BucketChecksum(hasher.digest())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_batch_is_deterministic() {
        let a = checksum_rows(&[]);
        let b = checksum_rows(&[]);
        assert_eq!(a, b);
    }

    #[test]
    fn same_rows_same_order_produce_same_checksum() {
        let rows = vec![
            json!({"id": "1", "_version": 1, "name": "a"}),
            json!({"id": "2", "_version": 1, "name": "b"}),
        ];
        let a = checksum_rows(&rows);
        let b = checksum_rows(&rows);
        assert_eq!(a, b);
    }

    #[test]
    fn different_row_order_produces_different_checksum() {
        let rows_asc = vec![json!({"id": "1"}), json!({"id": "2"})];
        let rows_desc = vec![json!({"id": "2"}), json!({"id": "1"})];
        assert_ne!(checksum_rows(&rows_asc), checksum_rows(&rows_desc));
    }

    #[test]
    fn different_version_produces_different_checksum() {
        let v1 = vec![json!({"id": "1", "_version": 1})];
        let v2 = vec![json!({"id": "1", "_version": 2})];
        assert_ne!(checksum_rows(&v1), checksum_rows(&v2));
    }

    #[test]
    fn non_key_field_changes_do_not_affect_checksum() {
        let a = vec![json!({"id": "1", "_version": 1, "name": "alice"})];
        let b = vec![json!({"id": "1", "_version": 1, "name": "bob"})];
        assert_eq!(
            checksum_rows(&a),
            checksum_rows(&b),
            "checksum is over id+_version identity, not full row content"
        );
    }

    #[test]
    fn missing_version_field_is_handled() {
        let rows = vec![json!({"id": "1"})];
        // Must not panic; result is still deterministic.
        let a = checksum_rows(&rows);
        let b = checksum_rows(&rows);
        assert_eq!(a, b);
    }

    #[test]
    fn batch_checksum_order_matters() {
        let a = BucketChecksum(1);
        let b = BucketChecksum(2);
        assert_ne!(checksum_batches(&[a, b]), checksum_batches(&[b, a]));
    }

    #[test]
    fn batch_checksum_missing_batch_changes_result() {
        let a = BucketChecksum(1);
        let b = BucketChecksum(2);
        let c = BucketChecksum(3);
        assert_ne!(checksum_batches(&[a, b, c]), checksum_batches(&[a, c]));
    }

    #[test]
    fn empty_batches_is_deterministic() {
        assert_eq!(checksum_batches(&[]), checksum_batches(&[]));
    }
}
