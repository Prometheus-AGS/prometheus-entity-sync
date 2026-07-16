-- Test schema for BucketAssigner integration tests
-- (tests/assigner_matrix.rs, tests/assigner_proptest.rs).
--
-- Apply with:
--   psql -h localhost -p 55432 -U postgres -d pes_test -f crates/pes-rules/tests/schema.sql

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    auth_user_id TEXT NOT NULL UNIQUE,
    tenant_id TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL
);

INSERT INTO users (id, auth_user_id, tenant_id) VALUES
    ('user-1', 'auth-sub-1', 'tenant-a'),
    ('user-2', 'auth-sub-2', NULL)
ON CONFLICT (id) DO NOTHING;

INSERT INTO entities (id, owner_id) VALUES
    ('entity-1', 'user-1'),
    ('entity-2', 'user-1')
ON CONFLICT (id) DO NOTHING;

-- Fixture for the "value fails the safe-value allowlist" branch coverage
-- test: a value column containing characters (spaces, quotes) that fail
-- template::validate_safe_value even though they came from Postgres, not
-- directly from a JWT claim.
CREATE TABLE IF NOT EXISTS unsafe_values (
    id TEXT PRIMARY KEY,
    auth_user_id TEXT NOT NULL UNIQUE,
    value TEXT
);

INSERT INTO unsafe_values (id, auth_user_id, value) VALUES
    ('u1', 'unsafe-sub', 'has spaces and quotes'' here')
ON CONFLICT (id) DO NOTHING;

-- Fixture for the "unsupported column type" branch coverage test: a
-- boolean column, which is neither String nor i64 and so falls through
-- both try_get attempts in resolve_rule.
CREATE TABLE IF NOT EXISTS bool_values (
    id TEXT PRIMARY KEY,
    auth_user_id TEXT NOT NULL UNIQUE,
    flag BOOLEAN
);

INSERT INTO bool_values (id, auth_user_id, flag) VALUES
    ('b1', 'bool-sub', TRUE)
ON CONFLICT (id) DO NOTHING;
