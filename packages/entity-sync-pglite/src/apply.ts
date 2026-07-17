/**
 * Applies `BucketOp`s from a `Delta` as SQL mutations against a local
 * PGlite database. Each op's `entity_type` is used directly as the target
 * table name — matching `pes-gateway`'s own write-path convention (see
 * `docs/sync-rules-reference.md`: a `data` query name is the real table
 * name), and `entity_id` is always bound as a query parameter, never
 * interpolated.
 */

import type { BucketOp, Op } from "@prometheus-ags/entity-sync-core";
import type { PGliteInterface } from "@electric-sql/pglite";

/**
 * Apply a batch of ops to `db` inside a single transaction, so a partial
 * batch failure never leaves the local database in a state that mixes ops
 * from one `Delta` with only some of its mutations applied.
 */
export async function applyOps(db: PGliteInterface, ops: BucketOp[]): Promise<void> {
  await db.transaction(async (tx) => {
    for (const op of ops) {
      await applyOne(tx, op);
    }
  });
}

async function applyOne(tx: { query: PGliteInterface["query"] }, bucketOp: BucketOp): Promise<void> {
  const table = identifier(bucketOp.entity_type);
  const { entity_id: entityId, op } = bucketOp;

  if (op === "Delete") {
    await tx.query(`DELETE FROM ${table} WHERE id::text = $1`, [entityId]);
    return;
  }
  if ("Upsert" in op) {
    await applyUpsert(tx, table, entityId, op.Upsert);
    return;
  }
  if ("CrdtPatch" in op) {
    await applyCrdtPatch(tx, table, entityId, op.CrdtPatch);
    return;
  }
  assertNever(op);
}

async function applyUpsert(
  tx: { query: PGliteInterface["query"] },
  table: string,
  entityId: string,
  value: unknown,
): Promise<void> {
  await tx.query(
    `INSERT INTO ${table} (id, payload) VALUES ($1, $2) ` +
      `ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`,
    [entityId, JSON.stringify(value)],
  );
}

async function applyCrdtPatch(
  tx: { query: PGliteInterface["query"] },
  table: string,
  entityId: string,
  patchBytes: Uint8Array,
): Promise<void> {
  // Applying a Loro CRDT patch (merging against locally-stored bytes)
  // requires the CRDT runtime itself, which this package doesn't depend on
  // — CrdtPatch ops are stored as opaque bytes so a higher layer (or a
  // future crdt-aware extension) can merge them, rather than silently
  // dropping the update.
  await tx.query(
    `INSERT INTO ${table} (id, crdt_state) VALUES ($1, $2) ` +
      `ON CONFLICT (id) DO UPDATE SET crdt_state = EXCLUDED.crdt_state`,
    [entityId, patchBytes],
  );
}

/**
 * `entity_type` becomes a table name interpolated directly into SQL (no
 * bind-parameter syntax exists for identifiers). It originates from the
 * sync gateway's own `Delta` messages — server-controlled, not arbitrary
 * client input — but this local allowlist check is defense in depth: reject
 * anything that isn't a plain SQL identifier before it ever reaches a query
 * string.
 */
function identifier(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`refusing to apply op for unsafe table name '${name}'`);
  }
  return `"${name}"`;
}

function assertNever(value: never): never {
  throw new Error(`unrecognized Op variant: ${JSON.stringify(value)}`);
}
