//! E2E integration tests: real Postgres logical replication → real
//! `PostgresCdcConsumer` → `WalToBucketRouter` → real `BucketOpLog`.
//!
//! Uses `testcontainers` with a Postgres image explicitly configured for
//! logical replication (`wal_level=logical`, replication slots/senders) —
//! the default `testcontainers-modules` Postgres image does not enable
//! this. A mock [`LogBroker`] bridges `PostgresCdcConsumer` directly to
//! `WalToBucketRouter` in-process, following the same pattern FRF's own
//! `frf-postgres-cdc/tests/cdc_integration.rs` uses — this scopes the test
//! to the WAL→router→oplog path itself, not Iggy's transport reliability
//! (which is `frf-broker-iggy`'s own test suite's concern).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use frf_domain::{Channel, ChannelId, Cursor, EventEnvelope, Offset, TenantId};
use frf_ports::{EventStream, LogBroker, PortError};
use frf_postgres_cdc::{CdcConfig, PostgresCdcConsumer};
use pes_core::{BucketAssignment, BucketId};
use pes_oplog::BucketOpLog;
use pes_router::{RouterMetrics, WalToBucketRouter};
use pes_rules::{BucketAssigner, SyncRuleSet};
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::watch;

/// An in-process `LogBroker` bridging one producer (`PostgresCdcConsumer`)
/// to one consumer (`WalToBucketRouter`), with no real message broker
/// involved — a `tokio::sync::mpsc` channel dressed up behind the
/// `LogBroker` trait.
struct InProcessBroker {
    tx: Mutex<Option<tokio::sync::mpsc::Sender<Result<EventEnvelope, PortError>>>>,
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<Result<EventEnvelope, PortError>>>>,
    next_offset: Mutex<u64>,
}

impl InProcessBroker {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        Self {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
            next_offset: Mutex::new(0),
        }
    }
}

#[async_trait]
impl LogBroker for InProcessBroker {
    async fn publish(&self, mut envelope: EventEnvelope) -> Result<Offset, PortError> {
        let offset = {
            let mut next = self.next_offset.lock().unwrap();
            let o = Offset(*next);
            *next += 1;
            o
        };
        envelope.offset = offset;
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(Ok(envelope)).await;
        }
        Ok(offset)
    }

    async fn subscribe(
        &self,
        _channel_id: ChannelId,
        _consumer_id: String,
        _from: Offset,
    ) -> Result<EventStream, PortError> {
        let rx = self
            .rx
            .lock()
            .unwrap()
            .take()
            .expect("subscribe called more than once in this test broker");
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn seek(&self, _cursor: Cursor) -> Result<(), PortError> {
        Ok(())
    }

    async fn ack(
        &self,
        _channel_id: ChannelId,
        _consumer_id: &str,
        _offset: Offset,
    ) -> Result<(), PortError> {
        Ok(())
    }

    async fn ensure_channel(&self, _channel: Channel) -> Result<(), PortError> {
        Ok(())
    }
}

/// Start a Postgres container with logical replication enabled, seed a
/// publication covering `entities` and `unrelated_table`, and return a
/// connection pool plus the container handle (kept alive by the caller).
async fn start_postgres_with_logical_replication() -> (ContainerAsync<GenericImage>, sqlx::PgPool, String) {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ]);

    let container = image.start().await.expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to test postgres");

    // `entities.id` and `unrelated_table.id` MUST be UUID-formatted: FRF's
    // decode.rs (frf-postgres-cdc) hard-requires the primary key of any
    // watched table to parse as a `frf_domain::EntityId` (a UUID newtype)
    // — a plain string like "entity-1" fails to decode with "invalid
    // entity id ... invalid character" and the row is silently skipped
    // (logged as a WARN, not surfaced as an error). `users` is not part of
    // the CDC publication, so its id has no such constraint, but it must
    // still equal what `owner_id` compares against for
    // `find_affected_buckets`'s matching to work, so a fixed UUID is used
    // for both to keep the fixture legible.
    sqlx::query("CREATE TABLE users (id TEXT PRIMARY KEY, auth_user_id TEXT NOT NULL UNIQUE)")
        .execute(&pool)
        .await
        .expect("create users table");
    sqlx::query("CREATE TABLE entities (id UUID PRIMARY KEY, owner_id TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create entities table");
    sqlx::query("CREATE TABLE unrelated_table (id UUID PRIMARY KEY, value TEXT)")
        .execute(&pool)
        .await
        .expect("create unrelated_table");
    sqlx::query("INSERT INTO users (id, auth_user_id) VALUES ($1, 'auth-sub-1')")
        .bind(TEST_USER_ID)
        .execute(&pool)
        .await
        .expect("seed user");
    sqlx::query("CREATE PUBLICATION pes_pub FOR TABLE entities, unrelated_table")
        .execute(&pool)
        .await
        .expect("create publication");

    (container, pool, url)
}

/// Fixed UUID for the seeded test user, used as both `users.id` and
/// `entities.owner_id` so `find_affected_buckets`'s owner-column matching
/// has a stable value to compare against.
const TEST_USER_ID: &str = "00000000-0000-0000-0000-000000000001";

async fn make_assigner(pool: sqlx::PgPool) -> Arc<BucketAssigner> {
    let mut parameter_queries = std::collections::HashMap::new();
    parameter_queries.insert(
        "user_id".to_string(),
        "SELECT id FROM users WHERE auth_user_id = $1".to_string(),
    );
    let mut data_queries = std::collections::HashMap::new();
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
        rules: std::collections::HashMap::from([(rule.id.clone(), rule)]),
    });

    let assigner = Arc::new(
        BucketAssigner::new(rule_set, pool, Duration::from_secs(3600)).expect("valid rule set"),
    );

    // Prime the cache: find_affected_buckets only ever consults cached
    // assignments (that's the whole "no DB roundtrip" point), so a real
    // client must have called assign() at least once before routing can
    // find them. This mirrors what the real gateway does on client connect.
    let claims = pes_core::TokenClaims {
        sub: "auth-sub-1".to_string(),
        tenant_id: None,
        exp: 9_999_999_999,
        custom: std::collections::HashMap::new(),
    };
    let assignments: Vec<BucketAssignment> = assigner.assign(&claims).await.expect("assign succeeds");
    assert_eq!(assignments.len(), 1, "test fixture expects exactly one matching bucket");

    assigner
}

/// Scenario: INSERT into `entities` (a watched, bucket-relevant table) →
/// WAL event → routed op appears in the correct bucket's oplog.
#[tokio::test]
async fn insert_into_entities_routes_op_to_correct_bucket() {
    let _ = tracing_subscriber::fmt().with_env_filter("debug").try_init();
    let (_container, pool, url) = start_postgres_with_logical_replication().await;
    let assigner = make_assigner(pool.clone()).await;
    let oplog = Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap());
    let metrics = Arc::new(RouterMetrics::new());
    let broker = Arc::new(InProcessBroker::new());

    let cdc_config = CdcConfig::new(
        url,
        "pes_test_slot",
        "pes_pub",
        TenantId::from_uuid(uuid::Uuid::nil()),
        "entity/changes",
    );
    let consumer = PostgresCdcConsumer::new(cdc_config, Arc::clone(&broker));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cdc_handle = tokio::spawn(async move { consumer.run_until_shutdown(shutdown_rx).await });

    let router = WalToBucketRouter::new(
        Arc::clone(&broker),
        ChannelId::new(),
        "pes_test_router",
        Arc::clone(&assigner),
        Arc::clone(&oplog),
        Arc::clone(&metrics),
    );
    let router_handle = tokio::spawn(router.run());

    // Give the replication slot time to establish before writing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let entity_id = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO entities (id, owner_id) VALUES ($1, $2)")
        .bind(entity_id)
        .bind(TEST_USER_ID)
        .execute(&pool)
        .await
        .expect("insert entity");

    // Poll the oplog for the routed op rather than a fixed sleep, up to a
    // generous timeout — WAL propagation latency varies by environment.
    let bucket_id = BucketId("user_entities".to_string());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;
    while tokio::time::Instant::now() < deadline {
        let checksum = oplog.checksum(&bucket_id).await.unwrap();
        if checksum.0 != 0 {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(true);
    cdc_handle.abort();
    router_handle.abort();

    assert!(
        found,
        "expected an op to appear in the user_entities bucket's oplog within 10s"
    );
}

/// Scenario: INSERT into a table not covered by any `data_queries` →
/// zero ops appended to any bucket.
#[tokio::test]
async fn insert_into_unrelated_table_appends_zero_ops() {
    let (_container, pool, url) = start_postgres_with_logical_replication().await;
    let assigner = make_assigner(pool.clone()).await;
    let oplog = Arc::new(BucketOpLog::in_memory(Duration::from_secs(3600)).unwrap());
    let metrics = Arc::new(RouterMetrics::new());
    let broker = Arc::new(InProcessBroker::new());

    let cdc_config = CdcConfig::new(
        url,
        "pes_test_slot2",
        "pes_pub",
        TenantId::from_uuid(uuid::Uuid::nil()),
        "entity/changes",
    );
    let consumer = PostgresCdcConsumer::new(cdc_config, Arc::clone(&broker));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cdc_handle = tokio::spawn(async move { consumer.run_until_shutdown(shutdown_rx).await });

    let router = WalToBucketRouter::new(
        Arc::clone(&broker),
        ChannelId::new(),
        "pes_test_router2",
        Arc::clone(&assigner),
        Arc::clone(&oplog),
        Arc::clone(&metrics),
    );
    let router_handle = tokio::spawn(router.run());

    tokio::time::sleep(Duration::from_millis(500)).await;

    sqlx::query("INSERT INTO unrelated_table (id, value) VALUES ($1, 'hello')")
        .bind(uuid::Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("insert unrelated row");

    // Give the pipeline a generous window to (not) route anything.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let _ = shutdown_tx.send(true);
    cdc_handle.abort();
    router_handle.abort();

    let bucket_id = BucketId("user_entities".to_string());
    let checksum = oplog.checksum(&bucket_id).await.unwrap();
    assert_eq!(
        checksum.0, 0,
        "an unrelated table's row must never produce a routed op in any bucket"
    );
}
