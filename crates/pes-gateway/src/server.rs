//! `GatewayServer` — accepts WebSocket connections and spawns a
//! [`crate::connection::ConnectionHandler`] per connection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pes_oplog::BucketOpLog;
use pes_rules::BucketAssigner;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

use crate::auth::JwtValidator;
use crate::connection::ConnectionHandler;

/// Runtime configuration for a [`GatewayServer`].
pub struct GatewayConfig {
    /// Maximum number of simultaneously open connections. New connections
    /// beyond this limit are rejected at accept time.
    pub max_connections: usize,
    /// Interval at which each connection sends a `Keepalive` message.
    pub keepalive_interval: Duration,
    /// Interval at which each connection's delta-subscription polls its
    /// buckets' oplogs for new ops (see `pes-gateway`'s design note on why
    /// this is polling, not push: `BucketOpLog` has no live-notification
    /// mechanism).
    pub delta_poll_interval: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            keepalive_interval: Duration::from_secs(30),
            delta_poll_interval: Duration::from_millis(100),
        }
    }
}

/// The WebSocket sync gateway server.
pub struct GatewayServer {
    listener: TcpListener,
    config: Arc<GatewayConfig>,
    assigner: Arc<BucketAssigner>,
    oplog: Arc<BucketOpLog>,
    jwt_validator: Arc<JwtValidator>,
    write_pool: PgPool,
    connection_count: Arc<AtomicUsize>,
}

impl GatewayServer {
    /// Bind a new gateway server to `addr`.
    pub async fn bind(
        addr: &str,
        config: GatewayConfig,
        assigner: Arc<BucketAssigner>,
        oplog: Arc<BucketOpLog>,
        jwt_validator: Arc<JwtValidator>,
        write_pool: PgPool,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            assigner,
            oplog,
            jwt_validator,
            write_pool,
            connection_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The local address this server is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections indefinitely, spawning a
    /// [`ConnectionHandler`] task per accepted WebSocket connection.
    /// Returns only on an unrecoverable accept error.
    pub async fn run(self) -> std::io::Result<()> {
        loop {
            let (tcp_stream, peer_addr) = self.listener.accept().await?;

            // Connection limit check happens before the (relatively
            // expensive) WebSocket handshake, so an at-capacity server
            // rejects excess connections cheaply rather than completing a
            // handshake it's about to immediately tear down.
            if self.connection_count.load(Ordering::Relaxed) >= self.config.max_connections {
                tracing::warn!(%peer_addr, "rejecting connection: at max_connections capacity");
                drop(tcp_stream);
                continue;
            }

            let config = Arc::clone(&self.config);
            let assigner = Arc::clone(&self.assigner);
            let oplog = Arc::clone(&self.oplog);
            let jwt_validator = Arc::clone(&self.jwt_validator);
            let write_pool = self.write_pool.clone();
            let connection_count = Arc::clone(&self.connection_count);

            tokio::spawn(async move {
                connection_count.fetch_add(1, Ordering::Relaxed);
                let ws_stream = match accept_async(tcp_stream).await {
                    Ok(ws) => ws,
                    Err(e) => {
                        tracing::warn!(%peer_addr, error = %e, "WebSocket handshake failed");
                        connection_count.fetch_sub(1, Ordering::Relaxed);
                        return;
                    }
                };

                let handler = ConnectionHandler::new(
                    ws_stream,
                    config,
                    assigner,
                    oplog,
                    jwt_validator,
                    write_pool,
                );
                if let Err(e) = handler.run().await {
                    tracing::debug!(%peer_addr, error = %e, "connection ended");
                }

                connection_count.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }
}
