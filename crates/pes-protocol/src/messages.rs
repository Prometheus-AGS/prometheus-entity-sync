//! PSyncV1 message types: [`ServerMessage`] and [`ClientMessage`].

use std::collections::HashMap;

use pes_core::{BucketChecksum, BucketId, BucketOp, Op, PgLsn};
use serde::{Deserialize, Serialize};

/// Current PSyncV1 protocol version. Sent in [`ClientMessage::Subscribe`]
/// and echoed back in [`ServerMessage::SnapshotBegin`].
pub const PROTOCOL_VERSION: u8 = 1;

/// Error code for [`ServerMessage::Error`] when a client's
/// `protocol_version` is not supported by this server.
pub const ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION: u16 = 4000;

/// Messages sent from the sync server to a client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ServerMessage {
    /// Sent once per bucket at the start of initial snapshot delivery.
    SnapshotBegin {
        /// The bucket this snapshot is for.
        bucket_id: BucketId,
        /// Total number of rows the client should expect across all
        /// [`ServerMessage::SnapshotBatch`] messages for this bucket.
        total_rows: u64,
        /// The protocol version this server is using for this connection.
        protocol_version: u8,
    },
    /// One page of a bucket's snapshot.
    SnapshotBatch {
        /// The bucket this batch belongs to.
        bucket_id: BucketId,
        /// The rows in this batch, as JSON objects.
        rows: Vec<serde_json::Value>,
        /// 0-based offset of this batch within the bucket's snapshot.
        offset: u64,
    },
    /// Sent once per bucket after the last [`ServerMessage::SnapshotBatch`],
    /// carrying the checksum the client should verify against.
    SnapshotComplete {
        /// The bucket whose snapshot just completed.
        bucket_id: BucketId,
        /// The bucket's checksum, for the client to verify integrity.
        checksum: BucketChecksum,
    },
    /// A batch of live ops for one bucket, delivered after the initial
    /// snapshot phase.
    Delta {
        /// The bucket these ops belong to.
        bucket_id: BucketId,
        /// The ops themselves, in order.
        ops: Vec<BucketOp>,
        /// The LSN of the last op in `ops` — the client should `Ack` this.
        lsn: PgLsn,
    },
    /// Periodic checkpoint summarizing every subscribed bucket's current
    /// state, for client-side integrity verification without needing a
    /// fresh snapshot.
    Checkpoint {
        /// The LSN this checkpoint was taken at.
        lsn: PgLsn,
        /// Every subscribed bucket's checksum as of `lsn`.
        bucket_checksums: HashMap<BucketId, BucketChecksum>,
    },
    /// Periodic keepalive, also usable by the client to measure clock skew.
    Keepalive {
        /// Server wall-clock time, milliseconds since the Unix epoch.
        server_time_ms: u64,
    },
    /// A fatal or informational error. See `ERROR_CODE_*` constants for
    /// well-known codes.
    Error {
        /// A numeric error code (see `ERROR_CODE_*` constants).
        code: u16,
        /// A human-readable description.
        message: String,
    },
}

/// Messages sent from a client to the sync server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ClientMessage {
    /// Sent once at connection start to request buckets and authenticate.
    Subscribe {
        /// The bucket ids the client wants to subscribe to.
        buckets: Vec<String>,
        /// The client's JWT.
        token: String,
        /// If resuming an existing sync session, the last LSN the client
        /// has already applied — the server resumes from just after this.
        resume_lsn: Option<PgLsn>,
        /// The protocol version this client speaks.
        protocol_version: u8,
    },
    /// Acknowledges receipt and application of all ops up to and including `lsn`.
    Ack {
        /// The highest LSN the client has applied.
        lsn: PgLsn,
    },
    /// A client-initiated write, to be applied upstream (e.g. to Postgres)
    /// and re-broadcast to other subscribers via the normal delta path.
    Write {
        /// The entity type being written.
        entity_type: String,
        /// The identifier of the entity being written.
        entity_id: String,
        /// The write operation itself.
        op: Op,
    },
    /// Liveness probe; the server should respond with a [`ServerMessage::Keepalive`].
    Ping,
}
