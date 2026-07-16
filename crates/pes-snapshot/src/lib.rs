//! `SnapshotStream` — initial snapshot streaming with keyset cursor pagination.
#![warn(missing_docs)]

mod checksum;
mod stream;

pub use checksum::{checksum_batches, checksum_rows};
pub use stream::{SnapshotBatch, SnapshotStream, DEFAULT_BATCH_SIZE};
