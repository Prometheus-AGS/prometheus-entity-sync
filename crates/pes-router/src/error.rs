//! Errors produced by [`crate::WalToBucketRouter`].

/// An error while consuming WAL change events or routing them to buckets.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RouterError {
    /// The `LogBroker` subscription failed to open or was interrupted.
    #[error("broker subscription error: {0}")]
    Broker(String),
    /// An `EventEnvelope`'s payload could not be deserialized as an `EntityChange`.
    #[error("event decode error: {0}")]
    Decode(String),
    /// Appending a routed op to a bucket's oplog failed.
    #[error("oplog append error: {0}")]
    OpLog(#[from] pes_core::SyncError),
}
