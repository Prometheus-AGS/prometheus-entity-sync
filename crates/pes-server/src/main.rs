//! `pes-server` — the deployable `prometheus-entity-sync` binary: loads
//! `config.toml`, wires the WAL→bucket routing pipeline and the WebSocket
//! sync gateway together, serves `/health` `/ready` `/metrics` on a
//! separate port, and shuts down gracefully on SIGTERM.

mod broker;
mod config;
mod health;
mod shutdown;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use frf_domain::{ChannelId, TenantId};
use pes_gateway::{GatewayConfig, GatewayServer, JwtValidationConfig, JwtValidator};
use pes_oplog::BucketOpLog;
use pes_router::{RouterMetrics, WalToBucketRouter};
use pes_rules::BucketAssigner;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

use crate::broker::InProcessBroker;
use crate::config::{AuthConfig, Config};
use crate::health::{health_router, HealthState};

/// Channel path WAL events are published/subscribed under within the
/// in-process broker. Arbitrary but must match between the CDC consumer's
/// publish side and the router's subscribe side.
const WAL_CHANNEL_PATH: &str = "entity/changes";
const REPLICATION_SLOT_NAME: &str = "pes_server_slot";
const PUBLICATION_NAME: &str = "pes_pub";
const ROUTER_CONSUMER_ID: &str = "pes_server_router";

/// How long the graceful-shutdown routine waits for connections to drain
/// after notifying clients, per the proposal's "wait up to 30s" spec.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config_path = std::env::var("PES_CONFIG_PATH").unwrap_or_else(|_| "./config.toml".to_string());
    let config = match config::load_config(&config_path) {
        Ok(config) => config,
        Err(e) => {
            // Clean, non-panicking exit on config error — the proposal's
            // "missing required config causes clear error, not panic"
            // success criterion.
            eprintln!("pes-server: failed to load config from '{config_path}': {e}");
            std::process::exit(1);
        }
    };

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(config))
}

async fn run(config: Config) -> anyhow::Result<()> {
    let write_pool = PgPoolOptions::new()
        .max_connections(config.postgres.max_pool_size)
        .connect(&config.postgres.url)
        .await?;

    let rule_set = Arc::new(pes_rules::parse_sync_rules(&PathBuf::from(&config.sync_rules.path))?);
    let assigner = Arc::new(BucketAssigner::new(rule_set, write_pool.clone(), Duration::from_secs(3600))?);

    let oplog_data_dir = PathBuf::from(&config.oplog.data_dir);
    std::fs::create_dir_all(&oplog_data_dir)?;
    let compaction_ttl = Duration::from_secs(config.oplog.compaction_ttl_days * 24 * 60 * 60);
    let oplog = Arc::new(BucketOpLog::open(oplog_data_dir.join("oplog.redb"), compaction_ttl)?);

    let jwt_validator = Arc::new(JwtValidator::new(match &config.auth {
        AuthConfig::Hmac { secret } => JwtValidationConfig::HmacSha256 { secret: secret.clone() },
        AuthConfig::Jwks { jwks_url } => JwtValidationConfig::Rs256Jwks { jwks_url: jwks_url.clone() },
    }));

    // --- WAL pipeline: PostgresCdcConsumer -> InProcessBroker -> WalToBucketRouter -> oplog ---
    let broker = Arc::new(InProcessBroker::new());
    let cdc_config = frf_postgres_cdc::CdcConfig::new(
        config.postgres.url.clone(),
        REPLICATION_SLOT_NAME,
        PUBLICATION_NAME,
        TenantId::from_uuid(uuid::Uuid::nil()),
        WAL_CHANNEL_PATH,
    );
    let cdc_consumer = frf_postgres_cdc::PostgresCdcConsumer::new(cdc_config, Arc::clone(&broker));
    let (cdc_shutdown_tx, cdc_shutdown_rx) = tokio::sync::watch::channel(false);
    let cdc_handle = tokio::spawn(async move { cdc_consumer.run_until_shutdown(cdc_shutdown_rx).await });

    let router_metrics = Arc::new(RouterMetrics::new());
    let router = WalToBucketRouter::new(
        Arc::clone(&broker),
        ChannelId::new(),
        ROUTER_CONSUMER_ID,
        Arc::clone(&assigner),
        Arc::clone(&oplog),
        Arc::clone(&router_metrics),
    );
    let router_handle = tokio::spawn(router.run());

    // Give the replication slot a moment to establish before declaring
    // readiness — matches the same warm-up window used in this pipeline's
    // own E2E tests (pes-router, pes-gateway).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let wal_replication_active = Arc::new(AtomicBool::new(true));

    // --- WebSocket gateway ---
    let gateway_addr = format!("{}:{}", config.server.host, config.server.port);
    let gateway_config = GatewayConfig {
        max_connections: config.server.max_connections,
        ..GatewayConfig::default()
    };
    let gateway_server = GatewayServer::bind(
        &gateway_addr,
        gateway_config,
        Arc::clone(&assigner),
        Arc::clone(&oplog),
        Arc::clone(&jwt_validator),
        write_pool.clone(),
    )
    .await?;
    let connection_count = Arc::clone(gateway_server.connection_count());
    tracing::info!(addr = %gateway_addr, "pes-server: WebSocket gateway listening");

    let gateway_shutdown = CancellationToken::new();
    let gateway_run_token = gateway_shutdown.clone();
    let gateway_handle = tokio::spawn(gateway_server.run(gateway_run_token));

    // --- Health/metrics/ready HTTP server ---
    let health_state = HealthState {
        connection_count: Arc::clone(&connection_count),
        router_metrics: Arc::clone(&router_metrics),
        wal_replication_active: Arc::clone(&wal_replication_active),
        lag_ms: Arc::new(AtomicI64::new(0)),
    };
    let metrics_addr = format!("{}:{}", config.server.host, config.metrics.port);
    let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr).await?;
    tracing::info!(addr = %metrics_addr, "pes-server: health/metrics server listening");
    let health_handle = tokio::spawn(async move {
        axum::serve(metrics_listener, health_router(health_state)).await
    });

    // --- SIGTERM -> graceful shutdown ---
    shutdown::wait_for_sigterm().await?;
    tracing::info!("pes-server: SIGTERM received, beginning graceful shutdown");

    gateway_shutdown.cancel();
    let _ = cdc_shutdown_tx.send(true);

    let drain_deadline = tokio::time::Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    while connection_count.load(Ordering::Relaxed) > 0 && tokio::time::Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let remaining = connection_count.load(Ordering::Relaxed);
    if remaining > 0 {
        tracing::warn!(remaining, "pes-server: shutdown deadline reached with connections still open, forcing exit");
    } else {
        tracing::info!("pes-server: all connections drained, exiting cleanly");
    }

    health_handle.abort();
    gateway_handle.abort();
    router_handle.abort();
    cdc_handle.abort();

    Ok(())
}
