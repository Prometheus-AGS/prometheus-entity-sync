//! Errors produced by [`crate::BucketOpLog`].

/// An error while appending to, reading from, or compacting a [`crate::BucketOpLog`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpLogError {
    /// The underlying redb database could not be opened or created.
    #[error("failed to open redb database: {0}")]
    DatabaseOpen(#[source] redb::DatabaseError),
    /// A redb transaction could not be started or committed.
    #[error("redb transaction error: {0}")]
    Transaction(#[source] redb::TransactionError),
    /// A redb table operation failed.
    #[error("redb table error: {0}")]
    Table(#[source] redb::TableError),
    /// A redb storage-level operation failed.
    #[error("redb storage error: {0}")]
    Storage(#[source] redb::StorageError),
    /// A redb commit failed.
    #[error("redb commit error: {0}")]
    Commit(#[source] redb::CommitError),
    /// An op could not be serialized to or deserialized from MessagePack.
    #[error("MessagePack (de)serialization error: {0}")]
    Codec(String),
    /// A blocking task spawned to run a redb operation panicked or was cancelled.
    #[error("background task error: {0}")]
    Task(String),
}

impl From<redb::DatabaseError> for OpLogError {
    fn from(e: redb::DatabaseError) -> Self {
        OpLogError::DatabaseOpen(e)
    }
}

impl From<redb::TransactionError> for OpLogError {
    fn from(e: redb::TransactionError) -> Self {
        OpLogError::Transaction(e)
    }
}

impl From<redb::TableError> for OpLogError {
    fn from(e: redb::TableError) -> Self {
        OpLogError::Table(e)
    }
}

impl From<redb::StorageError> for OpLogError {
    fn from(e: redb::StorageError) -> Self {
        OpLogError::Storage(e)
    }
}

impl From<redb::CommitError> for OpLogError {
    fn from(e: redb::CommitError) -> Self {
        OpLogError::Commit(e)
    }
}

impl From<OpLogError> for pes_core::SyncError {
    fn from(e: OpLogError) -> Self {
        pes_core::SyncError::ProtocolError(e.to_string())
    }
}
