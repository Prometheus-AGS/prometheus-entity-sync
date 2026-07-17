-- Minimal schema matching examples/docker-compose/sync-rules.toml's
-- user_entities bucket: a users table for JWT-sub -> user_id resolution,
-- and an entities table watched by the WAL pipeline. See
-- crates/pes-router/tests/e2e_wal_routing.rs for why entities.id must be
-- UUID (frf-postgres-cdc's WAL decoder requires it).

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    auth_user_id TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS entities (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    payload TEXT
);

CREATE PUBLICATION pes_pub FOR TABLE entities;
