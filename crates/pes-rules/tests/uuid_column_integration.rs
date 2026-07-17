//! Real-Postgres regression test for the `template::substitute` quoting bug:
//! substituting a UUID-typed `bucket_parameters` value into a `data_queries`
//! string must produce SQL that Postgres actually accepts, not just a string
//! that looks plausible. `assigner_matrix.rs` and `assigner_proptest.rs`
//! exercise `BucketAssigner::assign`'s resolution logic against a live
//! Postgres, but never execute the *rendered* `data_queries` SQL — this test
//! closes that gap by running the full `BucketAssigner::assign` →
//! `template::substitute` → real SQL execution path against a UUID `owner_id`
//! column, following the same `testcontainers_modules::postgres::Postgres`
//! pattern as `pes-snapshot/tests/snapshot_integration.rs`.

use std::collections::HashMap;
use std::time::Duration;

use pes_core::{SyncRule, TokenClaims};
use pes_rules::{BucketAssigner, SyncRuleSet};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

async fn start_postgres_with_uuid_fixture() -> (ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default().start().await.expect("start postgres container");
    let port = container.get_host_port_ipv4(5432).await.expect("get mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to test postgres");

    sqlx::query("CREATE TABLE users (id TEXT PRIMARY KEY, auth_user_id TEXT NOT NULL UNIQUE)")
        .execute(&pool)
        .await
        .expect("create users table");
    // The bug this test guards against only reproduces against a non-numeric
    // column: a bare-token substitution happens to parse (as arithmetic) for
    // integer columns, but fails outright for UUID/text columns.
    sqlx::query("CREATE TABLE entities (id UUID PRIMARY KEY, owner_id UUID NOT NULL, payload TEXT)")
        .execute(&pool)
        .await
        .expect("create entities table");

    let owner_uuid = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, auth_user_id) VALUES ($1, 'auth-sub-1')")
        .bind(owner_uuid.to_string())
        .execute(&pool)
        .await
        .expect("seed user");
    sqlx::query("INSERT INTO entities (id, owner_id, payload) VALUES ($1, $2, 'hello')")
        .bind(uuid::Uuid::new_v4())
        .bind(owner_uuid)
        .execute(&pool)
        .await
        .expect("seed entity");

    (container, pool)
}

fn user_entities_rule() -> SyncRule {
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
    SyncRule {
        id: "user_entities".to_string(),
        description: None,
        parameters: vec!["user_id".to_string()],
        parameter_queries,
        data_queries,
    }
}

fn test_claims() -> TokenClaims {
    TokenClaims {
        sub: "auth-sub-1".to_string(),
        tenant_id: None,
        exp: 9_999_999_999,
        custom: HashMap::new(),
    }
}

/// The regression this test exists for: pre-fix, `template::substitute`
/// rendered `owner_id = {bucket_parameters.user_id}` with the UUID inserted
/// unquoted, producing e.g. `owner_id = 550e8400-e29b-41d4-a716-...`, which
/// Postgres parses as an arithmetic expression against a UUID column and
/// rejects with `operator does not exist: uuid - integer` (or `text =
/// integer` for a text column). Post-fix, the value is quoted
/// (`owner_id = '550e8400-...'`), which Postgres accepts and correctly
/// matches against the UUID column.
#[tokio::test]
async fn assign_renders_executable_sql_against_uuid_owner_column() {
    let (_container, pool) = start_postgres_with_uuid_fixture().await;

    let rule = user_entities_rule();
    let rule_set = std::sync::Arc::new(SyncRuleSet {
        version: "1".to_string(),
        rules: HashMap::from([(rule.id.clone(), rule)]),
    });
    let assigner = BucketAssigner::new(rule_set, pool.clone(), Duration::from_secs(3600)).expect("valid rule set");

    let assignments = assigner.assign(&test_claims()).await.expect("assign succeeds");
    assert_eq!(assignments.len(), 1, "the JWT sub should resolve to exactly one bucket");

    let rendered_query = assignments[0]
        .data_queries
        .get("entities")
        .expect("entities data query rendered");

    // The actual regression check: execute the rendered SQL against real
    // Postgres. Pre-fix, this `fetch_all` call itself fails with a Postgres
    // syntax/type error before any row-count assertion is reached.
    let rows = sqlx::query(rendered_query)
        .fetch_all(&pool)
        .await
        .expect("rendered data query must be valid, executable SQL against a UUID column");

    assert_eq!(rows.len(), 1, "rendered query should match the single seeded entity");
    let payload: String = rows[0].try_get("payload").expect("payload column");
    assert_eq!(payload, "hello");
}
