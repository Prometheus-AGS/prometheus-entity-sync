# prometheus-entity-sync

A Rust-native, MIT-licensed sync engine for bidirectional Postgres → PGlite →
SQLite replication. Built independently of PowerSync (FSL-1.1-ALv2) on top of
[flint-realtime-fabric](https://github.com/prometheusags/flint-realtime-fabric)
(FRF), reusing its WAL CDC, CRDT (Loro), and op-log machinery.

## What it is

- **`pes-server`** — a standalone sync gateway that reads Postgres WAL, buckets
  changes per-user via a TOML sync-rule DSL, and streams them to clients over
  a WebSocket + MessagePack wire protocol (PSyncV1).
- **Client SDKs** — TypeScript (browser, PGlite), Dart (Flutter, SQLite via
  `drift`), Rust (native), and a Tauri plugin (desktop, via `pglite-oxide`).
- **Security model** — a `BucketAssigner` maps JWT claims to authorized data
  buckets using parameterized SQL only; no string interpolation of
  user-controlled values.

## Quick start

```bash
# Server
cargo run -p pes-server

# TypeScript SDK
pnpm install
pnpm --filter @prometheus-ags/entity-sync-core build
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Postgres (source of truth)                                   │
└───────────────────────────┬─────────────────────────────────┘
                             │ WAL (frf-postgres-cdc)
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  pes-server                                                    │
│  ├── pes-rules     — TOML sync rule DSL, BucketAssigner        │
│  ├── pes-oplog     — per-bucket op log (frf-store-redb)        │
│  ├── pes-snapshot  — keyset-paginated initial snapshot         │
│  ├── pes-protocol  — PSyncV1 MessagePack codec                 │
│  └── pes-gateway   — WebSocket connection lifecycle + auth     │
└───────────────────────────┬─────────────────────────────────┘
                             │ PSyncV1 (WebSocket + MessagePack)
              ┌──────────────┼──────────────┬─────────────────┐
              ▼              ▼              ▼                 ▼
    entity-sync-core   pes-sdk-rust   prometheus_entity_sync   entity-sync-tauri
    (TypeScript/PGlite) (Rust native)  (Dart/Flutter/SQLite)   (Tauri/pglite-oxide)
```

## Crates

| Crate | Purpose |
|-------|---------|
| `pes-core` | Domain types shared across all crates |
| `pes-rules` | Sync rule TOML DSL + BucketAssigner (security boundary) |
| `pes-oplog` | Per-bucket append-only op log |
| `pes-snapshot` | Initial snapshot streaming |
| `pes-protocol` | PSyncV1 wire protocol codec |
| `pes-gateway` | WebSocket gateway |
| `pes-server` | Server binary |
| `pes-sdk-rust` | Native Rust client SDK |

## Packages

| Package | Purpose |
|---------|---------|
| `@prometheus-ags/entity-sync-core` | Protocol client, reconnect, JWT management |
| `@prometheus-ags/entity-sync-pglite` | PGlite extension applying delta ops |
| `@prometheus-ags/entity-sync-react` | React hooks |
| `@prometheus-ags/entity-sync-tauri` | Tauri IPC bindings |

## License

MIT
