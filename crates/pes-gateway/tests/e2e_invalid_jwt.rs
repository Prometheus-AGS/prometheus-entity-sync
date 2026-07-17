//! E2E test: a client presenting an invalid JWT in its `Subscribe` handshake
//! is rejected — receives a `ServerMessage::Error` and the connection closes
//! — before any snapshot or delta data is ever sent.
//!
//! Uses a real `GatewayServer` bound to an ephemeral local port, backed by a
//! real (empty, non-CDC-wired) Postgres testcontainers instance — the write
//! pool is only needed for `GatewayServer::bind`'s signature; no snapshot
//! delivery is exercised in this test since the handshake fails first.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, EncodingKey, Header};
use pes_core::TokenClaims;
use pes_gateway::{GatewayConfig, GatewayServer, JwtValidationConfig, JwtValidator};
use pes_oplog::BucketOpLog;
use pes_protocol::{decode_server, encode_client, ClientMessage, ServerMessage};
use pes_rules::{BucketAssigner, SyncRuleSet};
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_tungstenite::tungstenite::Message;

const JWT_SECRET: &str = "e2e-jwt-test-secret";

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

async fn start_server() -> (ContainerAsync<GenericImage>, std::net::SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>) {
    let (container, pool) = start_postgres().await;
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
    let handle = tokio::spawn(server.run());

    (container, addr, handle)
}

fn expired_claims() -> TokenClaims {
    TokenClaims {
        sub: "user-1".to_string(),
        tenant_id: None,
        exp: 1, // 1970-01-01T00:00:01Z — long expired.
        custom: HashMap::new(),
    }
}

/// A client whose JWT is validly signed but expired must be rejected with a
/// `ServerMessage::Error` (code 4001, per `GatewayErrorCode::AuthInvalid`'s
/// `wire_code()`) and receive no `SnapshotBegin`/`Delta` before the
/// connection closes.
#[tokio::test]
async fn expired_jwt_is_rejected_before_any_data_is_sent() {
    let (_container, addr, server_handle) = start_server().await;
    let url = format!("ws://{addr}");

    let expired_token = encode(
        &Header::new(jsonwebtoken::Algorithm::HS256),
        &expired_claims(),
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode expired JWT");

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url).await.expect("connect");
    let subscribe = ClientMessage::Subscribe {
        buckets: vec!["any-bucket".to_string()],
        token: expired_token,
        resume_lsn: None,
        protocol_version: pes_protocol::PROTOCOL_VERSION,
    };
    let bytes = encode_client(&subscribe).expect("encode subscribe");
    ws_stream
        .send(Message::Binary(bytes.to_vec()))
        .await
        .expect("send subscribe");

    let mut saw_error = false;
    let mut saw_forbidden_message = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(next) = tokio::time::timeout(remaining, ws_stream.next()).await else {
            break;
        };
        let Some(next) = next else {
            // Connection closed — expected after the Error response.
            break;
        };
        match next {
            Ok(Message::Binary(bytes)) => {
                let Ok(msg) = decode_server(&bytes) else {
                    continue;
                };
                match msg {
                    ServerMessage::Error { code, .. } => {
                        saw_error = true;
                        assert_eq!(
                            code, 4001,
                            "expected GatewayErrorCode::AuthInvalid's wire code (4001)"
                        );
                    }
                    ServerMessage::SnapshotBegin { .. }
                    | ServerMessage::SnapshotBatch { .. }
                    | ServerMessage::SnapshotComplete { .. }
                    | ServerMessage::Delta { .. } => {
                        saw_forbidden_message = true;
                    }
                    _ => {}
                }
            }
            Ok(Message::Close(_)) => break,
            _ => continue,
        }
    }

    server_handle.abort();

    assert!(saw_error, "expected a ServerMessage::Error for the expired JWT");
    assert!(
        !saw_forbidden_message,
        "no snapshot or delta data should ever be sent to a client with an invalid JWT"
    );
}

/// A client presenting a JWT signed with the wrong secret (bad signature,
/// not expiry) must be rejected the same way.
#[tokio::test]
async fn wrongly_signed_jwt_is_rejected_before_any_data_is_sent() {
    let (_container, addr, server_handle) = start_server().await;
    let url = format!("ws://{addr}");

    let claims = TokenClaims {
        sub: "user-1".to_string(),
        tenant_id: None,
        exp: 9_999_999_999,
        custom: HashMap::new(),
    };
    let wrongly_signed_token = encode(
        &Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"not-the-real-secret"),
    )
    .expect("encode wrongly-signed JWT");

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url).await.expect("connect");
    let subscribe = ClientMessage::Subscribe {
        buckets: vec!["any-bucket".to_string()],
        token: wrongly_signed_token,
        resume_lsn: None,
        protocol_version: pes_protocol::PROTOCOL_VERSION,
    };
    let bytes = encode_client(&subscribe).expect("encode subscribe");
    ws_stream
        .send(Message::Binary(bytes.to_vec()))
        .await
        .expect("send subscribe");

    let next = tokio::time::timeout(Duration::from_secs(5), ws_stream.next())
        .await
        .expect("should respond within 5s")
        .expect("should receive a message before close");

    server_handle.abort();

    let Ok(Message::Binary(bytes)) = next else {
        panic!("expected a binary Error frame, got {next:?}");
    };
    let msg = decode_server(&bytes).expect("decode ServerMessage");
    let ServerMessage::Error { code, .. } = msg else {
        panic!("expected ServerMessage::Error, got {msg:?}");
    };
    assert_eq!(code, 4001, "expected GatewayErrorCode::AuthInvalid's wire code (4001)");
}
