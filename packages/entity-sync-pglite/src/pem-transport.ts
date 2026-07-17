/**
 * `prometheusSync` — an `EntityTransport<T>` implementation
 * (`@prometheus-ags/entity-graph-core`) backed by `prometheus-entity-sync`:
 * a local-first PGlite database kept current by a `SyncClient` WebSocket
 * connection to a `pes-gateway` server.
 *
 * Design note on the `EntityTransport` contract mismatch: `EntityTransport`
 * is a pull-based `list()`/`get()` contract with an optional push
 * `subscribe()`; `prometheus-entity-sync` is fundamentally push-based (a
 * persistent stream of snapshot + delta ops). This transport reconciles the
 * two by making PGlite — kept current by the sync stream, per
 * `applyOps`/`prometheusSync` in `./extension.js` — the source `list()`/
 * `get()` read from. `subscribe()` then layers `ChangeEvent` notifications
 * on top so PEM's realtime-aware hooks re-render without polling.
 *
 * `write()` is NOT part of `EntityTransport` (the interface has no mutation
 * method) — it's exposed alongside the returned transport object for a
 * consumer's `useEntityMutation({ mutate: transport.write })` wiring, per
 * the proposal's design.
 */

import type {
  ChangeEvent,
  EntityTransport,
  FilterClause,
  FilterGroup,
  FilterSpec,
  ListQuery,
  ListResult,
  SortClause,
} from "@prometheus-ags/entity-graph-core";
import { TerminalError, TransientError } from "@prometheus-ags/entity-graph-core";
import type { PGlite, PGliteInterface } from "@electric-sql/pglite";
import { SyncClient, type Op, type SyncClientConfig, type SyncStatus } from "@prometheus-ags/entity-sync-core";

import { createStatusStore, type StatusStore } from "./status-store.js";

export interface PrometheusSyncConfig<T> {
  serverUrl: string;
  bucket: string;
  getToken: () => Promise<string>;
  /** Local PGlite table name (and the sync gateway's `entity_type` for `Write`). */
  table: string;
  primaryKey: keyof T;
  entityType: string;
  /** An already-created PGlite instance backing this table. */
  db: PGliteInterface | PGlite;
  onStatus?: (status: SyncStatus) => void;
}

export interface PrometheusSyncTransport<T extends object> extends EntityTransport<T> {
  /** Write a mutation through `SyncClient`; for `useEntityMutation({ mutate: transport.write })`. */
  write: (input: WriteInput<T>) => Promise<T>;
  /**
   * A Zustand-compatible (`getState`/`setState`/`subscribe`) observable of
   * this transport's `SyncStatus`, so a consumer can subscribe outside
   * React (or wrap it in `useSyncExternalStore`) rather than only via the
   * one-shot `onStatus` config callback.
   */
  statusStore: StatusStore;
  /** Convenience for `statusStore.getState()`. */
  getStatus: () => SyncStatus;
  /** Tear down the underlying `SyncClient` connection. */
  disconnect: () => void;
}

export type WriteInput<T> =
  | { kind: "upsert"; id: string; data: T }
  | { kind: "delete"; id: string };

/**
 * Build a `prometheusSync` `EntityTransport<T>`. Pass the result to
 * `registerEntityTransport(entityType, transport)`.
 */
export function prometheusSync<T extends object>(config: PrometheusSyncConfig<T>): PrometheusSyncTransport<T> {
  const { serverUrl, bucket, getToken, table, primaryKey, db } = config;

  const statusStore = createStatusStore({ state: "disconnected" });
  const clientConfig: SyncClientConfig = {
    serverUrl,
    getToken,
    onStatus: (status) => {
      statusStore.setState(status);
      config.onStatus?.(status);
    },
  };
  const client = new SyncClient(clientConfig);
  let subscribed = false;

  function ensureSubscribed(): void {
    if (subscribed) return;
    subscribed = true;
    client.subscribe([bucket]);
  }

  const identify = (row: T): string => String(row[primaryKey]);

  return {
    identify,
    authoritative: true, // PGlite is kept current by the sync stream; no remote round-trip needed to render.
    staleTime: undefined, // Never considered stale — the sync stream, not polling, is what keeps this current.

    async list(q: ListQuery): Promise<ListResult<T>> {
      ensureSubscribed();
      const { whereClause, params } = compileFilter(q.filter);
      const orderClause = compileSort(q.sort);
      const limit = q.limit ?? 200;
      const offset = typeof q.cursor === "number" ? q.cursor : 0;

      const sql =
        `SELECT * FROM ${identifierName(table)}` +
        (whereClause ? ` WHERE ${whereClause}` : "") +
        (orderClause ? ` ORDER BY ${orderClause}` : "") +
        ` LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;

      try {
        const result = await db.query<T>(sql, [...params, limit, offset]);
        const rows = result.rows;
        const nextCursor = rows.length === limit ? offset + limit : null;
        return { rows, total: null, nextCursor };
      } catch (error) {
        throw mapPGliteError(error);
      }
    },

    async get(id: string, signal?: AbortSignal): Promise<T | null> {
      ensureSubscribed();
      if (signal?.aborted) {
        throw new TerminalError("aborted before query started");
      }
      try {
        const result = await db.query<T>(
          `SELECT * FROM ${identifierName(table)} WHERE id::text = $1 LIMIT 1`,
          [id],
        );
        return result.rows[0] ?? null;
      } catch (error) {
        throw mapPGliteError(error);
      }
    },

    subscribe(onChange: (ev: ChangeEvent<T>) => void): () => void {
      ensureSubscribed();
      return client.onDelta((delta) => {
        if (delta.bucketId !== bucket) return;
        for (const op of delta.ops) {
          if (op.entity_type !== table) continue;
          void handleOp<T>(db, table, op.entity_id, op.op, onChange);
        }
      });
    },

    async write(input: WriteInput<T>): Promise<T> {
      ensureSubscribed();
      if (input.kind === "delete") {
        await client.write(table, input.id, "Delete");
        return input as unknown as T; // no row to return for a delete; caller's normalize should handle this case
      }
      await client.write(table, input.id, { Upsert: input.data });
      return input.data;
    },

    statusStore,
    getStatus: () => statusStore.getState(),
    disconnect: () => client.disconnect(),
  };
}

/**
 * Convert one `BucketOp` into a `ChangeEvent` and invoke `onChange`.
 *
 * `Upsert` carries its row inline. `CrdtPatch` doesn't — `apply.ts` merges
 * the patch directly into PGlite's `crdt_state` column, so the *entity
 * graph's* row shape isn't available from the op alone. Rather than
 * emitting a rowless `ChangeEvent` (which the engine's `useEntities`
 * subscribe handler silently ignores — it only calls `upsertEntity` when
 * `ev.row` is truthy, see `packages/entity-graph-react/src/hooks/use-entities.ts`
 * — meaning the graph would silently miss the update), this reads the
 * merged row back from PGlite so the graph actually receives it.
 */
async function handleOp<T extends object>(
  db: PGliteInterface | PGlite,
  table: string,
  entityId: string,
  op: Op,
  onChange: (ev: ChangeEvent<T>) => void,
): Promise<void> {
  if (op === "Delete") {
    onChange({ op: "delete", id: entityId });
    return;
  }
  if (typeof op === "object" && "Upsert" in op) {
    onChange({ op: "update", id: entityId, row: op.Upsert as T });
    return;
  }
  if (typeof op === "object" && "CrdtPatch" in op) {
    try {
      const result = await db.query<T>(`SELECT * FROM ${identifierName(table)} WHERE id::text = $1 LIMIT 1`, [
        entityId,
      ]);
      const row = result.rows[0];
      if (row) {
        onChange({ op: "update", id: entityId, row });
      }
    } catch {
      // Best-effort: if the post-merge read fails, the entity simply
      // doesn't get an update notification this round — the next
      // successful delta or an explicit refetch will catch it up.
    }
  }
}

/** Compile a `FilterSpec` into a parameterized SQL WHERE fragment. */
function compileFilter(filter: FilterSpec | null | undefined): { whereClause: string; params: unknown[] } {
  if (!filter) {
    return { whereClause: "", params: [] };
  }
  const params: unknown[] = [];

  if (!Array.isArray(filter) && filter.logic === "or") {
    const fragments = (filter as FilterGroup).clauses
      .filter((c): c is FilterClause => !("logic" in c))
      .map((c) => clauseToSql(c, params))
      .filter((s): s is string => s !== null);
    return { whereClause: fragments.length > 0 ? `(${fragments.join(" OR ")})` : "", params };
  }

  const clauses = Array.isArray(filter) ? filter : (filter as FilterGroup).clauses;
  const fragments = (clauses as FilterClause[])
    .filter((c): c is FilterClause => !("logic" in c))
    .map((c) => clauseToSql(c, params))
    .filter((s): s is string => s !== null);
  return { whereClause: fragments.join(" AND "), params };
}

function clauseToSql(c: FilterClause, params: unknown[]): string | null {
  const field = identifierName(c.field);
  switch (c.op) {
    case "eq":
      params.push(c.value);
      return `${field} = $${params.length}`;
    case "neq":
      params.push(c.value);
      return `${field} != $${params.length}`;
    case "gt":
      params.push(c.value);
      return `${field} > $${params.length}`;
    case "gte":
      params.push(c.value);
      return `${field} >= $${params.length}`;
    case "lt":
      params.push(c.value);
      return `${field} < $${params.length}`;
    case "lte":
      params.push(c.value);
      return `${field} <= $${params.length}`;
    case "in":
      params.push(c.value);
      return `${field} = ANY($${params.length})`;
    case "isNull":
      return `${field} IS NULL`;
    case "isNotNull":
      return `${field} IS NOT NULL`;
    case "contains":
      params.push(`%${String(c.value)}%`);
      return `${field} ILIKE $${params.length}`;
    case "startsWith":
      params.push(`${String(c.value)}%`);
      return `${field} ILIKE $${params.length}`;
    case "endsWith":
      params.push(`%${String(c.value)}`);
      return `${field} ILIKE $${params.length}`;
    case "matches":
      params.push(String(c.value));
      return `${field} ILIKE $${params.length}`;
    case "custom":
      // Local-only predicate; not translatable to SQL, skip on the server side.
      return null;
    default:
      return null;
  }
}

function compileSort(sort: SortClause[] | null | undefined): string {
  if (!sort || sort.length === 0) return "";
  return sort
    .map((s) => {
      const direction = s.direction === "desc" ? "DESC" : "ASC";
      const nulls = s.nulls === "first" ? "NULLS FIRST" : s.nulls === "last" ? "NULLS LAST" : "";
      return `${identifierName(s.field)} ${direction}${nulls ? ` ${nulls}` : ""}`;
    })
    .join(", ");
}

/**
 * `table`/`field` names originate from this transport's own config
 * (`PrometheusSyncConfig.table`) and `FilterSpec`/`SortSpec` field names —
 * both caller-supplied at registration time, not per-request client input —
 * but are still interpolated into SQL (no bind-parameter syntax exists for
 * identifiers), so a defensive allowlist check guards against a
 * misconfigured or unexpectedly dynamic field name reaching a query string.
 */
function identifierName(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new TerminalError(`unsafe SQL identifier: '${name}'`);
  }
  return `"${name}"`;
}

function mapPGliteError(error: unknown): TerminalError | TransientError {
  if (error instanceof TerminalError || error instanceof TransientError) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  // PGlite/Postgres errors carry a `code` (SQLSTATE); syntax/undefined-table
  // errors are structural (terminal), everything else is treated as
  // transient — a local WASM query failing is unlikely to be a genuine
  // "will never succeed" condition the way a 4xx from a remote API is, but
  // erring toward transient here avoids masking a real terminal failure as
  // an infinite retry loop for the common case (missing/renamed table).
  const code = (error && typeof error === "object" && "code" in error) ? String((error as { code?: unknown }).code ?? "") : "";
  if (code === "42P01" || code === "42703") {
    // undefined_table / undefined_column
    return new TerminalError(message, { cause: error });
  }
  return new TransientError(message, { cause: error });
}
