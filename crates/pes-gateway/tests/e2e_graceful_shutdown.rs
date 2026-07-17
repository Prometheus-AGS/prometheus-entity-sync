//! E2E test for `GatewayServer::run`'s graceful-shutdown API (added for
//! `v4-pes-server-binary`, whose SIGTERM handling needs a way to stop
//! accepting new connections and notify existing clients before exit).
//!
//! Covers:
//! - a client connected when the shared `CancellationToken` is cancelled
//!   receives `ServerMessage::Error { code: 1001, .. }` and the socket closes
//! - after cancellation, a new connection attempt is refused (the accept
//!   loop itself has stopped, not just individual connections)
//! - `GatewayServer::connection_count()` reaches zero once all connections
//!   have finished closing, which is what a caller implementing the
//!   proposal's "wait up to 30s for clients to disconnect" step polls

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use pes_gateway::{GatewayConfig, GatewayServer, JwtValidationConfig, JwtValidator, SHUTDOWN_ERROR_CODE};
use pes_oplog::BucketOpLog;
use pes_protocol::{decode_server, encode_client, ClientMessage, ServerMessage};
use pes_rules::{BucketAssigner, SyncRuleSet};
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const JWT_SECRET: &str = "e2e-shutdown-test-secret";

async fn start_postgres() -> (ContainerAsync<GenericImage>, sqlx::PgPool) {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres");
    let container = image.start().await.expect("start postgres container");
    let port = container.get_host_port_ipv4(5432).await.expect("get mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to test postgres");
    (container, pool)
}

fn empty_assigner(pool: sqlx::PgPool) -> Arc<BucketAssigner> {
    let rule_set = Arc::new(SyncRuleSet {
        version: "1".to_string(),
        rules: HashMap::new(),
    });
    Arc::new(BucketAssigner::new(rule_set, pool, Duration::from_secs(3600)).expect("valid empty rule set"))
}

#[tokio::test]
async fn shutdown_notifies_connected_clients_and_stops_accepting() {
    let (_container, pool) = start_postgres().await;
    let assigner = empty_assigner(pool.clone());
    let oplog = Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap());
    let jwt_validator = Arc::new(JwtValidator::new(JwtValidationConfig::HmacSha256 {
        secret: JWT_SECRET.to_string(),
    }));

    let server = GatewayServer::bind(
        "127.0.0.1:0",
        GatewayConfig::default(),
        assigner,
        oplog,
        jwt_validator,
        pool,
    )
    .await
    .expect("bind gateway server");
    let addr = server.local_addr().expect("local addr");
    let connection_count = Arc::clone(server.connection_count());

    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let server_handle = tokio::spawn(server.run(run_shutdown));

    // Connect and Subscribe (resume_lsn set, so no snapshot delivery — this
    // test only cares about the shutdown notification, not snapshot data).
    let url = format!("ws://{addr}");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url).await.expect("connect");
    let subscribe = ClientMessage::Subscribe {
        buckets: vec!["any-bucket".to_string()],
        token: valid_jwt(),
        resume_lsn: Some(pes_core::PgLsn(0)),
        protocol_version: pes_protocol::PROTOCOL_VERSION,
    };
    let bytes = encode_client(&subscribe).expect("encode subscribe");
    ws_stream.send(Message::Binary(bytes.to_vec())).await.expect("send subscribe");

    // Give the connection a moment to actually establish server-side before
    // triggering shutdown, so connection_count reflects it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while connection_count.load(Ordering::Relaxed) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(connection_count.load(Ordering::Relaxed), 1, "connection should be tracked before shutdown");

    // Trigger graceful shutdown.
    shutdown.cancel();

    // The client should receive a shutdown Error frame, then the socket closes.
    let mut saw_shutdown_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(Some(next)) = tokio::time::timeout(remaining, ws_stream.next()).await else {
            break;
        };
        match next {
            Ok(Message::Binary(bytes)) => {
                if let Ok(ServerMessage::Error { code, .. }) = decode_server(&bytes) {
                    if code == SHUTDOWN_ERROR_CODE {
                        saw_shutdown_error = true;
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            _ => continue,
        }
    }
    assert!(saw_shutdown_error, "client should receive a shutdown Error frame (code {SHUTDOWN_ERROR_CODE})");

    // The accept loop itself should have stopped: run() returns once
    // shutdown fires, and a fresh connection attempt should fail (nothing
    // listening — TcpListener was dropped along with the consumed server).
    server_handle.await.expect("run() task should not panic").expect("run() should return Ok after shutdown");

    let fresh_attempt = tokio_tungstenite::connect_async(&url).await;
    assert!(fresh_attempt.is_err(), "no new connections should be accepted after shutdown");

    // connection_count should reach zero once the existing connection's
    // handler task finishes closing (asynchronous — poll with a deadline,
    // matching what a real pes-server shutdown routine would do).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while connection_count.load(Ordering::Relaxed) != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(connection_count.load(Ordering::Relaxed), 0, "all connections should have drained after shutdown");
}

fn valid_jwt() -> String {
    use jsonwebtoken::{encode, EncodingKey, Header};
    let claims = pes_core::TokenClaims {
        sub: "user-1".to_string(),
        tenant_id: None,
        exp: 9_999_999_999,
        custom: HashMap::new(),
    };
    encode(&Header::new(jsonwebtoken::Algorithm::HS256), &claims, &EncodingKey::from_secret(JWT_SECRET.as_bytes()))
        .expect("encode JWT")
}
