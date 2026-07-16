use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Postgres Log Sequence Number — monotonically increasing WAL position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PgLsn(pub u64);

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Postgres LSNs are conventionally rendered as two hex halves: XXXXXXXX/XXXXXXXX
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// A sync rule definition loaded from `sync-rules.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRule {
    /// Unique identifier for this rule within `sync-rules.toml`.
    pub id: String,
    /// Human-readable description shown in tooling and docs.
    pub description: Option<String>,
    /// Parameter names resolved from JWT claims via `parameter_queries`.
    pub parameters: Vec<String>,
    /// SQL queries to resolve each parameter from JWT claims.
    pub parameter_queries: HashMap<String, String>,
    /// SQL queries defining which rows go into this bucket.
    pub data_queries: HashMap<String, String>,
}

/// A bucket assignment for one user, resolved from a [`SyncRule`] plus JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketAssignment {
    /// The resolved bucket this user is assigned to.
    pub bucket_id: BucketId,
    /// The [`SyncRule::id`] this assignment was resolved from.
    pub rule_id: String,
    /// Parameter values resolved from JWT claims via `parameter_queries`.
    pub parameters: HashMap<String, serde_json::Value>,
    /// Data queries with parameters substituted.
    pub data_queries: HashMap<String, String>,
}

/// Opaque bucket identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketId(pub String);

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Claims extracted from a sync JWT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// The JWT `sub` claim — the authenticated user's identifier.
    pub sub: String,
    /// Optional tenant identifier for multi-tenant deployments.
    pub tenant_id: Option<String>,
    /// The JWT `exp` claim — Unix timestamp when this token expires.
    pub exp: u64,
    /// Any additional claims not otherwise modeled, flattened into this map.
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// A single op appended to a `BucketOpLog`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketOp {
    /// The WAL position this op was derived from.
    pub lsn: PgLsn,
    /// The bucket this op belongs to.
    pub bucket_id: BucketId,
    /// The entity type this op applies to (e.g. `"Todo"`).
    pub entity_type: String,
    /// The identifier of the entity this op applies to.
    pub entity_id: String,
    /// The operation payload.
    pub op: Op,
}

/// The operation payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Op {
    /// Insert or update the entity with the given JSON representation.
    Upsert(serde_json::Value),
    /// Remove the entity.
    Delete,
    /// Loro CRDT binary patch.
    CrdtPatch(Vec<u8>),
}

/// Running checksum for a bucket's op log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketChecksum(pub u64);

/// Sync engine errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SyncError {
    /// The bucket assigner could not resolve a bucket assignment for the given claims.
    #[error("bucket assignment failed: {0}")]
    BucketAssignmentFailed(String),
    /// A gap was detected in the WAL sequence — the client is missing ops.
    #[error("LSN gap detected: expected {expected}, got {actual}")]
    LsnGap {
        /// The LSN the client expected to receive next.
        expected: PgLsn,
        /// The LSN actually received.
        actual: PgLsn,
    },
    /// The running checksum for a bucket's op log did not match the expected value.
    #[error("checksum mismatch: expected {expected:?}, got {actual:?}")]
    ChecksumMismatch {
        /// The checksum the client expected.
        expected: BucketChecksum,
        /// The checksum actually computed.
        actual: BucketChecksum,
    },
    /// A PSyncV1 wire protocol message was malformed or out of sequence.
    #[error("protocol error: {0}")]
    ProtocolError(String),
    /// JWT validation or authorization failed.
    #[error("auth error: {0}")]
    AuthError(String),
    /// An underlying database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    static_assertions::assert_impl_all!(SyncError: Send, Sync);

    #[test]
    fn sync_error_is_send_sync_static() {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<SyncError>();
    }

    fn roundtrip<T>(value: &T)
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + fmt::Debug,
    {
        let json = serde_json::to_string(value).expect("serialize");
        let back: T = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(value, &back);
    }

    #[test]
    fn pg_lsn_roundtrips() {
        roundtrip(&PgLsn(123_456_789));
    }

    #[test]
    fn pg_lsn_display_format() {
        let lsn = PgLsn(0x1_0000_0000 | 0x2A);
        assert_eq!(lsn.to_string(), "1/2A");
    }

    #[test]
    fn bucket_id_roundtrips() {
        roundtrip(&BucketId("user_42".to_string()));
    }

    #[test]
    fn bucket_id_display_format() {
        assert_eq!(BucketId("user_42".to_string()).to_string(), "user_42");
    }

    #[test]
    fn bucket_checksum_roundtrips() {
        roundtrip(&BucketChecksum(987));
    }

    #[test]
    fn sync_rule_roundtrips() {
        let mut parameter_queries = HashMap::new();
        parameter_queries.insert(
            "user_id".to_string(),
            "SELECT id FROM users WHERE auth_user_id = token_parameters.sub".to_string(),
        );
        let mut data_queries = HashMap::new();
        data_queries.insert(
            "entities".to_string(),
            "SELECT * FROM entities WHERE owner_id = bucket_parameters.user_id".to_string(),
        );
        let rule = SyncRule {
            id: "user_entities".to_string(),
            description: Some("per-user entity bucket".to_string()),
            parameters: vec!["user_id".to_string()],
            parameter_queries,
            data_queries,
        };
        let json = serde_json::to_string(&rule).expect("serialize");
        let back: SyncRule = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(rule.id, back.id);
        assert_eq!(rule.parameters, back.parameters);
        assert_eq!(rule.parameter_queries, back.parameter_queries);
        assert_eq!(rule.data_queries, back.data_queries);
    }

    #[test]
    fn bucket_assignment_roundtrips() {
        let mut parameters = HashMap::new();
        parameters.insert("user_id".to_string(), serde_json::json!("abc-123"));
        let mut data_queries = HashMap::new();
        data_queries.insert(
            "entities".to_string(),
            "SELECT * FROM entities WHERE owner_id = 'abc-123'".to_string(),
        );
        let assignment = BucketAssignment {
            bucket_id: BucketId("user_entities:abc-123".to_string()),
            rule_id: "user_entities".to_string(),
            parameters,
            data_queries,
        };
        let json = serde_json::to_string(&assignment).expect("serialize");
        let back: BucketAssignment = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(assignment.bucket_id, back.bucket_id);
        assert_eq!(assignment.rule_id, back.rule_id);
        assert_eq!(assignment.parameters, back.parameters);
        assert_eq!(assignment.data_queries, back.data_queries);
    }

    #[test]
    fn token_claims_roundtrips_with_custom_claims() {
        let mut custom = HashMap::new();
        custom.insert("role".to_string(), serde_json::json!("admin"));
        let claims = TokenClaims {
            sub: "user-1".to_string(),
            tenant_id: Some("tenant-9".to_string()),
            exp: 1_900_000_000,
            custom,
        };
        let json = serde_json::to_string(&claims).expect("serialize");
        let back: TokenClaims = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(claims.sub, back.sub);
        assert_eq!(claims.tenant_id, back.tenant_id);
        assert_eq!(claims.exp, back.exp);
        assert_eq!(claims.custom, back.custom);
    }

    #[test]
    fn bucket_op_upsert_roundtrips() {
        let op = BucketOp {
            lsn: PgLsn(42),
            bucket_id: BucketId("b1".to_string()),
            entity_type: "Todo".to_string(),
            entity_id: "todo-1".to_string(),
            op: Op::Upsert(serde_json::json!({"title": "buy milk"})),
        };
        let json = serde_json::to_string(&op).expect("serialize");
        let back: BucketOp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(op.lsn, back.lsn);
        assert_eq!(op.bucket_id, back.bucket_id);
        assert_eq!(op.entity_type, back.entity_type);
        assert_eq!(op.entity_id, back.entity_id);
        match (&op.op, &back.op) {
            (Op::Upsert(a), Op::Upsert(b)) => assert_eq!(a, b),
            _ => panic!("expected Op::Upsert on both sides"),
        }
    }

    #[test]
    fn bucket_op_delete_roundtrips() {
        let op = BucketOp {
            lsn: PgLsn(43),
            bucket_id: BucketId("b1".to_string()),
            entity_type: "Todo".to_string(),
            entity_id: "todo-1".to_string(),
            op: Op::Delete,
        };
        let json = serde_json::to_string(&op).expect("serialize");
        let back: BucketOp = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(back.op, Op::Delete));
    }

    #[test]
    fn op_crdt_patch_roundtrips_arbitrary_bytes() {
        // Includes zero bytes, high bytes, and non-UTF8 sequences to stress
        // whatever encoding serde_json chooses for Vec<u8> (base64 by default
        // via serde's #[derive], since Loro patches are opaque binary).
        let bytes: Vec<u8> = vec![0x00, 0xFF, 0x01, 0xFE, 0x80, 0x7F, 0xC0, 0xC1, 0xF5];
        let op = Op::CrdtPatch(bytes.clone());
        let json = serde_json::to_string(&op).expect("serialize");
        let back: Op = serde_json::from_str(&json).expect("deserialize");
        match back {
            Op::CrdtPatch(round) => assert_eq!(round, bytes),
            _ => panic!("expected Op::CrdtPatch"),
        }
    }

    proptest! {
        #[test]
        fn op_crdt_patch_roundtrips_any_bytes(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..512)) {
            let op = Op::CrdtPatch(bytes.clone());
            let json = serde_json::to_string(&op).expect("serialize");
            let back: Op = serde_json::from_str(&json).expect("deserialize");
            match back {
                Op::CrdtPatch(round) => prop_assert_eq!(round, bytes),
                _ => prop_assert!(false, "expected Op::CrdtPatch"),
            }
        }
    }
}
