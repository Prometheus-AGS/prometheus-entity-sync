//! Prometheus metrics for [`crate::WalToBucketRouter`].

use prometheus::{
    Encoder, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry,
    TextEncoder,
};

/// Metrics exposed by the router, per the proposal:
/// - `pes_wal_events_received_total` (counter)
/// - `pes_wal_events_routed_total` (counter, label: `bucket_id`)
/// - `pes_routing_latency_ms` (histogram)
/// - `pes_oplog_queue_depth` (gauge, label: `bucket_id`)
pub struct RouterMetrics {
    registry: Registry,
    wal_events_received_total: IntCounter,
    wal_events_routed_total: IntCounterVec,
    routing_latency_ms: Histogram,
    oplog_queue_depth: GaugeVec,
}

impl RouterMetrics {
    /// Register all router metrics on a fresh [`Registry`].
    pub fn new() -> Self {
        let registry = Registry::new();

        let wal_events_received_total = IntCounter::new(
            "pes_wal_events_received_total",
            "Total WAL change events received from the CDC broker subscription",
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(wal_events_received_total.clone()))
            .expect("metric name is unique");

        let wal_events_routed_total = IntCounterVec::new(
            Opts::new(
                "pes_wal_events_routed_total",
                "Total ops appended to bucket oplogs, by bucket_id",
            ),
            &["bucket_id"],
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(wal_events_routed_total.clone()))
            .expect("metric name is unique");

        let routing_latency_ms = Histogram::with_opts(HistogramOpts::new(
            "pes_routing_latency_ms",
            "Time from WAL event receipt to all affected buckets' oplog appends completing, in milliseconds",
        ))
        .expect("valid metric definition");
        registry
            .register(Box::new(routing_latency_ms.clone()))
            .expect("metric name is unique");

        let oplog_queue_depth = GaugeVec::new(
            Opts::new(
                "pes_oplog_queue_depth",
                "Current depth of the bounded backpressure channel feeding a bucket's oplog writer",
            ),
            &["bucket_id"],
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(oplog_queue_depth.clone()))
            .expect("metric name is unique");

        Self {
            registry,
            wal_events_received_total,
            wal_events_routed_total,
            routing_latency_ms,
            oplog_queue_depth,
        }
    }

    /// Increment the total-events-received counter.
    pub fn record_event_received(&self) {
        self.wal_events_received_total.inc();
    }

    /// Increment the routed-ops counter for `bucket_id`.
    pub fn record_op_routed(&self, bucket_id: &str) {
        self.wal_events_routed_total
            .with_label_values(&[bucket_id])
            .inc();
    }

    /// Record end-to-end routing latency for one WAL event, in milliseconds.
    pub fn record_routing_latency_ms(&self, millis: f64) {
        self.routing_latency_ms.observe(millis);
    }

    /// Set the current backpressure queue depth for `bucket_id`.
    pub fn set_oplog_queue_depth(&self, bucket_id: &str, depth: f64) {
        self.oplog_queue_depth
            .with_label_values(&[bucket_id])
            .set(depth);
    }

    /// Render all registered metrics in Prometheus text exposition format,
    /// suitable for a `/metrics` HTTP endpoint.
    pub fn render(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .expect("prometheus text encoding cannot fail for well-formed metrics");
        String::from_utf8(buffer).expect("prometheus text encoder always emits valid UTF-8")
    }
}

impl Default for RouterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_start_at_zero() {
        let metrics = RouterMetrics::new();
        let rendered = metrics.render();
        assert!(rendered.contains("pes_wal_events_received_total 0"));
    }

    #[test]
    fn record_event_received_increments_counter() {
        let metrics = RouterMetrics::new();
        metrics.record_event_received();
        metrics.record_event_received();
        let rendered = metrics.render();
        assert!(rendered.contains("pes_wal_events_received_total 2"));
    }

    #[test]
    fn record_op_routed_increments_per_bucket_label() {
        let metrics = RouterMetrics::new();
        metrics.record_op_routed("bucket_a");
        metrics.record_op_routed("bucket_a");
        metrics.record_op_routed("bucket_b");
        let rendered = metrics.render();
        assert!(rendered.contains("bucket_id=\"bucket_a\""));
        assert!(rendered.contains("bucket_id=\"bucket_b\""));
    }

    #[test]
    fn set_oplog_queue_depth_updates_gauge() {
        let metrics = RouterMetrics::new();
        metrics.set_oplog_queue_depth("bucket_a", 42.0);
        let rendered = metrics.render();
        assert!(rendered.contains("pes_oplog_queue_depth"));
        assert!(rendered.contains("42"));
    }

    #[test]
    fn routing_latency_observations_appear_in_histogram() {
        let metrics = RouterMetrics::new();
        metrics.record_routing_latency_ms(15.5);
        let rendered = metrics.render();
        assert!(rendered.contains("pes_routing_latency_ms"));
    }
}
