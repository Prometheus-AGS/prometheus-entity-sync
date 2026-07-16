//! WAL→Bucket routing pipeline: consumes `frf-postgres-cdc` change events
//! (via an FRF `LogBroker` subscription — see `router.rs` for why not a
//! direct `Stream`) and fans them out into the appropriate buckets'
//! `pes-oplog`s.
#![warn(missing_docs)]

mod error;
mod metrics;
mod router;

pub use error::RouterError;
pub use metrics::RouterMetrics;
pub use router::{WalToBucketRouter, BACKPRESSURE_CAPACITY};
