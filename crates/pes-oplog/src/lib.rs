//! Per-bucket append-only operation log backed by redb.
#![warn(missing_docs)]

mod error;
mod key;
mod store;

pub use error::OpLogError;
pub use store::BucketOpLog;
