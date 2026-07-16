//! `WalToBucketRouter` — routes WAL change events (via an FRF `LogBroker`
//! subscription) through [`pes_rules::BucketAssigner::find_affected_buckets`]
//! into the affected buckets' [`pes_oplog::BucketOpLog`]s.
//!
//! # Event source
//!
//! The proposal describes consuming a `Stream<Item = ChangeEvent>` directly
//! from `frf-postgres-cdc`, but that crate's actual API
//! (`PostgresCdcConsumer::run_until_shutdown`) *publishes* decoded WAL
//! changes to an injected [`frf_ports::LogBroker`] rather than exposing a
//! pull-based stream — see `frf-postgres-cdc/src/consumer.rs`. This router
//! therefore subscribes to that broker's channel instead, which is the real
//! integration point.
//!
//! # LSN caveat
//!
//! `frf-postgres-cdc` captures the true Postgres LSN internally
//! (`consumer.rs:127`, `event.lsn.0`) but only uses it for WAL feedback
//! acknowledgment — it is never included in the [`frf_domain::EventEnvelope`]
//! published to the broker. The envelope instead carries a broker-local
//! monotonic [`frf_domain::Offset`], starting at `Offset::BEGINNING`. This
//! router uses that `Offset` as the [`pes_core::PgLsn`] on every routed
//! [`pes_core::BucketOp`] — it is **not a real Postgres LSN**, only a
//! per-channel monotonic ordering token. `pes-oplog`'s ordering and range
//! scan guarantees only depend on monotonicity, not on the value having any
//! particular relationship to a Postgres WAL position, so this is safe for
//! op-log ordering purposes but must not be treated as a true WAL position
//! anywhere else in the system.

use std::sync::Arc;
use std::time::Instant;

use frf_domain::{ChannelId, EntityChange, EventEnvelope};
use frf_ports::LogBroker;
use pes_core::{BucketId, BucketOp, Op, PgLsn};
use pes_oplog::BucketOpLog;
use pes_rules::BucketAssigner;
use tokio::sync::mpsc;

use crate::error::RouterError;
use crate::metrics::RouterMetrics;

/// Bounded channel capacity between the broker subscription and the oplog
/// writer fan-out, per the proposal's backpressure requirement.
pub const BACKPRESSURE_CAPACITY: usize = 1000;

/// Routes WAL change events from an FRF `LogBroker` subscription into the
/// affected buckets' op logs.
pub struct WalToBucketRouter<L: LogBroker> {
    broker: Arc<L>,
    channel_id: ChannelId,
    consumer_id: String,
    assigner: Arc<BucketAssigner>,
    oplog: Arc<BucketOpLog>,
    metrics: Arc<RouterMetrics>,
}

impl<L: LogBroker> WalToBucketRouter<L> {
    /// Construct a router that will subscribe to `channel_id` on `broker`
    /// (the same channel `PostgresCdcConsumer` publishes WAL changes to),
    /// route matched events through `assigner`, and append to `oplog`.
    pub fn new(
        broker: Arc<L>,
        channel_id: ChannelId,
        consumer_id: impl Into<String>,
        assigner: Arc<BucketAssigner>,
        oplog: Arc<BucketOpLog>,
        metrics: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            broker,
            channel_id,
            consumer_id: consumer_id.into(),
            assigner,
            oplog,
            metrics,
        }
    }

    /// Consume WAL change events and route them to buckets indefinitely.
    /// Returns only on subscription end or an unrecoverable error.
    pub async fn run(self) -> Result<(), RouterError> {
        use futures::StreamExt;

        let mut stream = self
            .broker
            .subscribe(
                self.channel_id,
                self.consumer_id.clone(),
                frf_domain::Offset::BEGINNING,
            )
            .await
            .map_err(|e| RouterError::Broker(e.to_string()))?;

        // Backpressure: a bounded channel between event receipt and the
        // fan-out writer task. When the writer falls behind, `send` on a
        // full channel awaits — which naturally slows how fast this loop
        // pulls the next event off `stream`, throttling WAL consumption
        // rather than buffering unboundedly in memory.
        let (tx, mut rx) = mpsc::channel::<EventEnvelope>(BACKPRESSURE_CAPACITY);

        let assigner = Arc::clone(&self.assigner);
        let oplog = Arc::clone(&self.oplog);
        let metrics = Arc::clone(&self.metrics);
        let writer_handle = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                route_one_event(
                    Arc::clone(&assigner),
                    Arc::clone(&oplog),
                    Arc::clone(&metrics),
                    envelope,
                )
                .await;
            }
        });

        while let Some(result) = stream.next().await {
            let envelope = result.map_err(|e| RouterError::Broker(e.to_string()))?;
            self.metrics.record_event_received();
            self.metrics
                .set_oplog_queue_depth(&self.channel_id.to_string(), rx_capacity_gauge(&tx));
            if tx.send(envelope).await.is_err() {
                // Writer task ended (e.g. panicked) — nothing more to do.
                break;
            }
        }

        drop(tx);
        let _ = writer_handle.await;
        Ok(())
    }
}

/// Best-effort queue-depth reading for the metrics gauge: `capacity -
/// current permits` approximates how many envelopes are currently queued
/// waiting for the writer task, without needing a separate atomic counter.
fn rx_capacity_gauge(tx: &mpsc::Sender<EventEnvelope>) -> f64 {
    (BACKPRESSURE_CAPACITY - tx.capacity()) as f64
}

/// Route one `EventEnvelope` to every affected bucket, appending a
/// [`BucketOp`] to each via one `tokio::spawn`ed task per bucket — real OS
/// thread-pool fan-out (not just cooperative `join_all` concurrency), so a
/// panic in one bucket's append doesn't affect any other bucket's, and the
/// runtime can schedule the writes across multiple worker threads.
async fn route_one_event(
    assigner: Arc<BucketAssigner>,
    oplog: Arc<BucketOpLog>,
    metrics: Arc<RouterMetrics>,
    envelope: EventEnvelope,
) {
    let started = Instant::now();

    let change: EntityChange = match serde_json::from_value(envelope.payload.clone()) {
        Ok(change) => change,
        Err(e) => {
            tracing::warn!(error = %e, "skipping envelope with undecodable EntityChange payload");
            return;
        }
    };

    let affected = assigner.find_affected_buckets(&change.entity_type, &change.data);
    if affected.is_empty() {
        return;
    }

    // SECURITY (LSN caveat): see this module's doc comment — `envelope.offset`
    // is a broker-local monotonic token, not the true Postgres LSN.
    let lsn = PgLsn(envelope.offset.0);
    let op = classify_op(&change);

    let handles: Vec<_> = affected
        .into_iter()
        .map(|bucket_id: BucketId| {
            let oplog = Arc::clone(&oplog);
            let op = op.clone();
            let entity_type = change.entity_type.clone();
            let entity_id = change.entity_id.to_string();
            tokio::spawn(async move {
                let bucket_op = BucketOp {
                    lsn,
                    bucket_id: bucket_id.clone(),
                    entity_type,
                    entity_id,
                    op,
                };
                match oplog.append(&bucket_id, lsn, bucket_op).await {
                    Ok(_) => Some(bucket_id),
                    Err(e) => {
                        tracing::error!(error = %e, bucket_id = %bucket_id.0, "failed to append routed op to oplog");
                        None
                    }
                }
            })
        })
        .collect();

    let results = futures::future::join_all(handles).await;
    for bucket_id in results.into_iter().flatten().flatten() {
        metrics.record_op_routed(&bucket_id.0);
    }

    metrics.record_routing_latency_ms(started.elapsed().as_secs_f64() * 1000.0);
}

fn classify_op(change: &EntityChange) -> Op {
    match change.op {
        frf_domain::ChangeOp::Delete => Op::Delete,
        frf_domain::ChangeOp::Insert | frf_domain::ChangeOp::Update | frf_domain::ChangeOp::Upsert => {
            Op::Upsert(change.data.clone())
        }
        // `ChangeOp` is #[non_exhaustive] in frf-domain — treat any future
        // variant conservatively as an upsert (preserve the row rather
        // than silently drop it) until this router is updated to handle
        // it explicitly.
        _ => Op::Upsert(change.data.clone()),
    }
}
