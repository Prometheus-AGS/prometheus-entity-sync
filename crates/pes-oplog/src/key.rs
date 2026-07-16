//! Composite key encoding for the `bucket_ops` redb table.
//!
//! Layout: `bucket_id_len(2 BE)` | `bucket_id_bytes(variable)` | `lsn(8 BE)`.
//!
//! `bucket_id` is a variable-length UTF-8 string, unlike FRF's fixed-width
//! UUID keys, so its length is stored as a 2-byte prefix. Encoding the LSN
//! in big-endian ensures a lexicographic byte-range scan over the table
//! visits ops for a given bucket in ascending LSN order — the same
//! prefix-scan technique `frf-store-redb` uses for its entity/tenant keys.

const LEN_PREFIX_BYTES: usize = 2;
const LSN_BYTES: usize = 8;

/// Maximum bucket id length in bytes. Bucket ids are validated elsewhere
/// (`pes_rules::validate`) to match `[a-z][a-z0-9_-]*`, which in practice
/// is far shorter than this; this bound only exists so the 2-byte length
/// prefix can never overflow (`u16::MAX` = 65,535).
pub const MAX_BUCKET_ID_LEN: usize = u16::MAX as usize;

/// Build the composite key for one op: `bucket_id` + `lsn`.
pub fn make_key(bucket_id: &str, lsn: u64) -> Vec<u8> {
    debug_assert!(bucket_id.len() <= MAX_BUCKET_ID_LEN);
    let id_bytes = bucket_id.as_bytes();
    let mut buf = Vec::with_capacity(LEN_PREFIX_BYTES + id_bytes.len() + LSN_BYTES);
    buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(&lsn.to_be_bytes());
    buf
}

/// Lower bound (inclusive) for a range scan of all ops for `bucket_id`
/// starting at `from_lsn`.
pub fn make_range_start(bucket_id: &str, from_lsn: u64) -> Vec<u8> {
    make_key(bucket_id, from_lsn)
}

/// Upper bound (exclusive) for a range scan of all ops for `bucket_id`,
/// regardless of LSN. Same length-prefix, followed by an all-`0xFF` LSN
/// suffix — no valid LSN can exceed `u64::MAX`, so this is a safe
/// exclusive upper bound for a `..` range (redb ranges are `start..end`,
/// so using `u64::MAX` bytes as `end` would exclude an op stored at
/// exactly `lsn = u64::MAX`; that LSN is not achievable in practice, since
/// `PgLsn` values come from real Postgres WAL positions, but we still
/// scan with a range whose end matches [`make_key`]'s encoding shape).
pub fn make_bucket_range_end(bucket_id: &str) -> Vec<u8> {
    make_key(bucket_id, u64::MAX)
}

/// Extract the LSN from the trailing 8 bytes of a composite key.
pub fn lsn_from_key(key: &[u8]) -> Option<u64> {
    if key.len() < LSN_BYTES {
        return None;
    }
    let mut bytes = [0u8; LSN_BYTES];
    bytes.copy_from_slice(&key[key.len() - LSN_BYTES..]);
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrip() {
        let key = make_key("user_entities", 42);
        assert_eq!(lsn_from_key(&key), Some(42));
    }

    #[test]
    fn keys_sort_by_lsn_within_bucket() {
        let k1 = make_key("b1", 1);
        let k2 = make_key("b1", 2);
        let k3 = make_key("b1", 100);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn keys_are_isolated_by_bucket_prefix() {
        // Different bucket ids must not interleave in sort order such that
        // a range scan for one bucket could pick up another bucket's ops.
        let a_lsn_100 = make_key("aaa", 100);
        let b_lsn_1 = make_key("bbb", 1);
        assert!(a_lsn_100 < b_lsn_1, "all 'aaa' keys must sort before any 'bbb' key");
    }

    #[test]
    fn range_start_matches_from_lsn() {
        let start = make_range_start("b1", 5);
        assert_eq!(lsn_from_key(&start), Some(5));
    }

    #[test]
    fn range_end_is_greater_than_any_real_lsn_in_bucket() {
        let end = make_bucket_range_end("b1");
        let real_op = make_key("b1", 1_000_000);
        assert!(real_op < end);
    }

    #[test]
    fn lsn_from_key_rejects_short_input() {
        assert_eq!(lsn_from_key(&[1, 2, 3]), None);
    }
}
