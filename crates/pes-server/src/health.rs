//! HTTP server for `/health`, `/metrics`, and `/ready` — bound to a
//! separate port from the WebSocket sync gateway, per the proposal.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use pes_router::RouterMetrics;
use serde::Serialize;
use std::sync::atomic::AtomicBool;

/// Shared state the health server reads from — connection count (from
/// `GatewayServer::connection_count()`), router metrics, and WAL-replication
/// readiness.
#[derive(Clone)]
pub struct HealthState {
    pub connection_count: Arc<AtomicUsize>,
    pub router_metrics: Arc<RouterMetrics>,
    /// Set once the WAL replication pipeline (CDC consumer + router) has
    /// started successfully. `/ready` returns 503 until this is true.
    pub wal_replication_active: Arc<AtomicBool>,
    /// Current replication lag, in milliseconds, as best-effort telemetry
    /// for `/health`'s `lag_ms` field. `None` until the first WAL event has
    /// been observed (no lag measurement exists yet).
    pub lag_ms: Arc<std::sync::atomic::AtomicI64>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    connections: usize,
    lag_ms: i64,
}

async fn health(State(state): State<HealthState>) -> Response {
    let body = HealthResponse {
        status: "healthy",
        connections: state.connection_count.load(Ordering::Relaxed),
        lag_ms: state.lag_ms.load(Ordering::Relaxed),
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn ready(State(state): State<HealthState>) -> Response {
    if state.wal_replication_active.load(Ordering::Relaxed) {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

async fn metrics(State(state): State<HealthState>) -> Response {
    let body = state.router_metrics.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// Build the health/metrics/ready router.
pub fn health_router(state: HealthState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::atomic::AtomicI64;
    use tower::ServiceExt;

    fn test_state() -> HealthState {
        HealthState {
            connection_count: Arc::new(AtomicUsize::new(0)),
            router_metrics: Arc::new(RouterMetrics::new()),
            wal_replication_active: Arc::new(AtomicBool::new(false)),
            lag_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Task 10: start the server (router), hit `/health`, verify response
    /// fields — matches the proposal's exact spec: `GET /health` -> `200 {
    /// "status": "healthy", "connections": N, "lag_ms": N }`.
    #[tokio::test]
    async fn health_returns_200_with_expected_fields() {
        let state = test_state();
        state.connection_count.store(3, Ordering::Relaxed);
        state.lag_ms.store(42, Ordering::Relaxed);

        let app = health_router(state);
        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["connections"], 3);
        assert_eq!(json["lag_ms"], 42);
    }

    #[tokio::test]
    async fn ready_returns_503_before_wal_replication_is_active() {
        let state = test_state();
        let app = health_router(state);
        let response = app
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ready_returns_200_once_wal_replication_is_active() {
        let state = test_state();
        state.wal_replication_active.store(true, Ordering::Relaxed);
        let app = health_router(state);
        let response = app
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_text_format() {
        let state = test_state();
        let app = health_router(state);
        let response = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("pes_wal_events_received_total"));
    }
}
