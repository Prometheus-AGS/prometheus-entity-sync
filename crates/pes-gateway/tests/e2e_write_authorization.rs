//! E2E regression test for a CRITICAL finding from pre-archive security
//! review: `ConnectionHandler::handle_write` originally authorized writes by
//! `entity_type` alone, never checking whether the specific `entity_id`
//! actually belongs to the writing client's own authorized bucket. That let
//! any client authorized to write *any* row of a given entity type write,
//! delete, or CRDT-patch *any other client's* row of that type.
//!
//! This test seeds two distinct owners, each with their own row in
//! `entities`, and proves:
//! - a client CAN write its own row
//! - a client CANNOT write another owner's row (rejected with
//!   `GatewayErrorCode::AuthInvalid`'s wire code, 4001, and the row is left
//!   unmodified)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, EncodingKey, Header};
use pes_core::{Op, TokenClaims};
use pes_gateway::{GatewayConfig, GatewayServer, JwtValidationConfig, JwtValidator};
use pes_oplog::BucketOpLog;
use pes_protocol::{decode_server, encode_client, ClientMessage, ServerMessage};
use pes_rules::{BucketAssigner, SyncRuleSet};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_tungstenite::tungstenite::Message;

const JWT_SECRET: &str = "e2e-write-authz-test-secret";
const OWNER_A_AUTH_SUB: &str = "auth-sub-owner-a";
const OWNER_B_AUTH_SUB: &str = "auth-sub-owner-b";

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
        .max_connections(10)
        .connect(&url)
        .await
        .expect("connect to test postgres");
    (container, pool)
}

struct Fixture {
    owner_a_id: uuid::Uuid,
    owner_b_id: uuid::Uuid,
    entity_owned_by_a: uuid::Uuid,
    entity_owned_by_b: uuid::Uuid,
}

async fn seed_two_owners(pool: &sqlx::PgPool) -> Fixture {
    // `users.id` is TEXT (not UUID): `BucketAssigner::resolve_rule`'s
    // parameter-query result extraction only decodes `String`/`i64` column
    // types (see `pes_rules::assigner::resolve_rule`), so the resolved
    // `user_id` parameter value must be a `String`-decodable column even
    // though its content is UUID-shaped — matches the convention used by
    // `e2e_delta_propagation.rs`'s fixture.
    sqlx::query("CREATE TABLE users (id TEXT PRIMARY KEY, auth_user_id TEXT NOT NULL UNIQUE)")
        .execute(pool)
        .await
        .expect("create users table");
    sqlx::query(
        "CREATE TABLE entities (id UUID PRIMARY KEY, owner_id UUID NOT NULL, payload TEXT)",
    )
    .execute(pool)
    .await
    .expect("create entities table");

    let owner_a_id = uuid::Uuid::new_v4();
    let owner_b_id = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, auth_user_id) VALUES ($1, $2)")
        .bind(owner_a_id.to_string())
        .bind(OWNER_A_AUTH_SUB)
        .execute(pool)
        .await
        .expect("seed owner A");
    sqlx::query("INSERT INTO users (id, auth_user_id) VALUES ($1, $2)")
        .bind(owner_b_id.to_string())
        .bind(OWNER_B_AUTH_SUB)
        .execute(pool)
        .await
        .expect("seed owner B");

    let entity_owned_by_a = uuid::Uuid::new_v4();
    let entity_owned_by_b = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO entities (id, owner_id, payload) VALUES ($1, $2, 'original-a')")
        .bind(entity_owned_by_a)
        .bind(owner_a_id)
        .execute(pool)
        .await
        .expect("seed entity owned by A");
    sqlx::query("INSERT INTO entities (id, owner_id, payload) VALUES ($1, $2, 'original-b')")
        .bind(entity_owned_by_b)
        .bind(owner_b_id)
        .execute(pool)
        .await
        .expect("seed entity owned by B");

    Fixture {
        owner_a_id,
        owner_b_id,
        entity_owned_by_a,
        entity_owned_by_b,
    }
}

fn make_assigner(pool: sqlx::PgPool) -> Arc<BucketAssigner> {
    let mut parameter_queries = HashMap::new();
    parameter_queries.insert(
        "user_id".to_string(),
        "SELECT id FROM users WHERE auth_user_id = $1".to_string(),
    );
    let mut data_queries = HashMap::new();
    data_queries.insert(
        "entities".to_string(),
        "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}".to_string(),
    );
    let rule = pes_core::SyncRule {
        id: "user_entities".to_string(),
        description: None,
        parameters: vec!["user_id".to_string()],
        parameter_queries,
        data_queries,
    };
    let rule_set = Arc::new(SyncRuleSet {
        version: "1".to_string(),
        rules: HashMap::from([(rule.id.clone(), rule)]),
    });
    Arc::new(BucketAssigner::new(rule_set, pool, Duration::from_secs(3600)).expect("valid rule set"))
}

fn jwt_for(auth_sub: &str) -> String {
    let claims = TokenClaims {
        sub: auth_sub.to_string(),
        tenant_id: None,
        exp: 9_999_999_999,
        custom: HashMap::new(),
    };
    encode(
        &Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode JWT")
}

struct Harness {
    addr: std::net::SocketAddr,
    _container: ContainerAsync<GenericImage>,
    server_handle: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Harness {
    async fn start() -> (Self, sqlx::PgPool, Fixture) {
        let (container, pool) = start_postgres().await;
        let fixture = seed_two_owners(&pool).await;
        let assigner = make_assigner(pool.clone());
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
            pool.clone(),
        )
        .await
        .expect("bind gateway server");
        let addr = server.local_addr().expect("local addr");
        let server_handle = tokio::spawn(server.run(tokio_util::sync::CancellationToken::new()));

        (
            Harness {
                addr,
                _container: container,
                server_handle,
            },
            pool,
            fixture,
        )
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }
}

/// Connect, `Subscribe` as `auth_sub` (skipping snapshot delivery via
/// `resume_lsn`, since this test only cares about the write path), then send
/// one `ClientMessage::Write` and return the first `ServerMessage` response
/// (if any arrives within 5s).
///
/// `resume_lsn: Some(PgLsn(0))` is safe specifically in this test (unlike
/// `e2e_delta_propagation.rs`, which switched to `None` after a real bug —
/// see that file's `connect_and_subscribe` doc comment): this test never
/// asserts on receiving a `Delta`, only on the immediate `Write` response
/// (an `Error` for the rejected case, or a DB-state poll for the accepted
/// case), so the LSN-0 ambiguity that broke delta delivery elsewhere
/// doesn't affect anything this test actually checks.
async fn subscribe_and_write(
    url: &str,
    auth_sub: &str,
    entity_type: &str,
    entity_id: uuid::Uuid,
    op: Op,
) -> Option<ServerMessage> {
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(url).await.expect("connect");
    let subscribe = ClientMessage::Subscribe {
        buckets: vec!["user_entities".to_string()],
        token: jwt_for(auth_sub),
        resume_lsn: Some(pes_core::PgLsn(0)),
        protocol_version: pes_protocol::PROTOCOL_VERSION,
    };
    let bytes = encode_client(&subscribe).expect("encode subscribe");
    ws_stream
        .send(Message::Binary(bytes.to_vec()))
        .await
        .expect("send subscribe");

    let write = ClientMessage::Write {
        entity_type: entity_type.to_string(),
        entity_id: entity_id.to_string(),
        op,
    };
    let bytes = encode_client(&write).expect("encode write");
    ws_stream
        .send(Message::Binary(bytes.to_vec()))
        .await
        .expect("send write");

    let next = tokio::time::timeout(Duration::from_secs(5), ws_stream.next()).await;
    match next {
        Ok(Some(Ok(Message::Binary(bytes)))) => decode_server(&bytes).ok(),
        _ => None,
    }
}

/// The write-path CRITICAL fix: a client authorized for `user_entities`
/// (which grants access to *some* rows of `entities`) must NOT be able to
/// write a row it does not own, even though it passes the entity-type check.
#[tokio::test]
async fn writing_another_owners_entity_is_rejected_and_row_is_unmodified() {
    let (harness, pool, fixture) = Harness::start().await;
    let url = harness.ws_url();

    // Owner A attempts to overwrite owner B's entity.
    let response = subscribe_and_write(
        &url,
        OWNER_A_AUTH_SUB,
        "entities",
        fixture.entity_owned_by_b,
        Op::Upsert(serde_json::json!("attacker-controlled-payload")),
    )
    .await;

    harness.server_handle.abort();

    match response {
        Some(ServerMessage::Error { code, .. }) => {
            assert_eq!(code, 4001, "expected GatewayErrorCode::AuthInvalid's wire code (4001)");
        }
        other => panic!("expected ServerMessage::Error, got {other:?}"),
    }

    let row = sqlx::query("SELECT owner_id, payload FROM entities WHERE id = $1")
        .bind(fixture.entity_owned_by_b)
        .fetch_one(&pool)
        .await
        .expect("entity B should still exist");
    let owner_id: uuid::Uuid = row.get("owner_id");
    let payload: Option<String> = row.get("payload");
    assert_eq!(owner_id, fixture.owner_b_id, "ownership must be unchanged");
    assert_eq!(
        payload.as_deref(),
        Some("original-b"),
        "owner B's row must be unmodified by owner A's rejected write"
    );
}

/// Sanity check: the fix must not be overly strict — a client writing its
/// own, genuinely-owned row must still succeed.
#[tokio::test]
async fn writing_own_entity_succeeds() {
    let (harness, pool, fixture) = Harness::start().await;
    let url = harness.ws_url();

    let response = subscribe_and_write(
        &url,
        OWNER_A_AUTH_SUB,
        "entities",
        fixture.entity_owned_by_a,
        Op::Upsert(serde_json::json!("updated-by-owner-a")),
    )
    .await;

    // A successful write produces no immediate response frame in the
    // current protocol (the effect propagates asynchronously via the normal
    // WAL/delta pipeline) — so `response` may be `None` (timed out waiting,
    // which is expected/correct here) or, if the connection's keepalive/
    // delta-poll ticked first, some other non-Error message. What matters
    // is that no `Error` was sent AND the row was actually updated.
    if let Some(ServerMessage::Error { code, message }) = &response {
        panic!("unexpected rejection of a legitimate same-owner write: code={code} message={message}");
    }

    // Poll briefly: the write is applied inside handle_write synchronously
    // before any response, but give the connection a moment in case of
    // scheduling jitter before asserting on DB state.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut payload = None;
    let mut owner_id = None;
    while tokio::time::Instant::now() < deadline {
        let row = sqlx::query("SELECT owner_id, payload FROM entities WHERE id = $1")
            .bind(fixture.entity_owned_by_a)
            .fetch_one(&pool)
            .await
            .expect("entity A should exist");
        let current: Option<String> = row.get("payload");
        // `payload` is a TEXT column; `apply_write` binds the
        // `Op::Upsert`'s `serde_json::Value` directly, which Postgres
        // stores as that value's JSON text representation (quotes
        // included for a JSON string) — not the bare string.
        if current.as_deref() == Some("\"updated-by-owner-a\"") {
            payload = current;
            owner_id = Some(row.get::<uuid::Uuid, _>("owner_id"));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    harness.server_handle.abort();

    assert_eq!(
        payload.as_deref(),
        Some("\"updated-by-owner-a\""),
        "owner A's own write to their own row must succeed"
    );
    assert_eq!(
        owner_id,
        Some(fixture.owner_a_id),
        "a legitimate write must not change the row's ownership"
    );
}
