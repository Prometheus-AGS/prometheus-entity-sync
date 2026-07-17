/**
 * Integration tests for `prometheusSync` (`PrometheusSyncTransport`)
 * against a REAL `pes-server` running via `examples/docker-compose/` (from
 * `v4-pes-server-binary`) — not a mock. Requires Docker; opt-in via
 * `pnpm run test:integration`.
 *
 * Covers the two scenarios from `v4-pem-sync-transport`'s remaining tasks:
 * - two tabs, same user: a write from one tab becomes visible to the other
 * - two tabs, different users: no cross-user data leak
 *
 * "Two tabs" is modeled as two independent `prometheusSync` transports
 * (each with its own PGlite instance and `SyncClient`) against the same
 * server — the real unit under test (bucket-scoped delta fan-out, PGlite
 * apply, transport read-back) doesn't depend on an actual browser tab.
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { execSync, spawnSync } from "node:child_process";
import { createHmac } from "node:crypto";
import path from "node:path";
import pg from "pg";
import { PGlite } from "@electric-sql/pglite";
import { prometheusSync, type PrometheusSyncTransport } from "../../src/pem-transport.js";

const COMPOSE_DIR = path.resolve(__dirname, "../../../../examples/docker-compose");
const AUTH_SECRET = "integration-test-secret";
const SERVER_URL = "ws://localhost:8080";
// Host port 55433 — see entity-sync-core's docker-compose integration test
// for why (avoids colliding with both a locally-installed Postgres and an
// unrelated container already using 55432 on this dev machine).
const PG_CONNECTION = "postgres://postgres:postgres@localhost:55433/postgres";
const BUCKET = "user_entities";

interface TestRow {
  id: string;
  payload: string;
}

/** Minimal HMAC-SHA256 JWT signer — no dependency needed for this one use. */
function signJwt(payload: Record<string, unknown>, secret: string): string {
  const base64url = (input: Buffer | string) =>
    Buffer.from(input).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const header = base64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64url(JSON.stringify(payload));
  const signature = createHmac("sha256", secret).update(`${header}.${body}`).digest();
  return `${header}.${body}.${base64url(signature)}`;
}

function tokenFor(authSub: string): () => Promise<string> {
  return async () => signJwt({ sub: authSub, exp: Math.floor(Date.now() / 1000) + 3600 }, AUTH_SECRET);
}

let pool: pg.Pool;

function dockerCompose(args: string): void {
  execSync(`docker compose ${args}`, {
    cwd: COMPOSE_DIR,
    env: { ...process.env, AUTH_SECRET, DOCKER_HOST: process.env.DOCKER_HOST },
    stdio: "inherit",
  });
}

async function waitForHealthy(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch("http://localhost:9090/ready");
      if (res.status === 200) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`pes-server did not become ready within ${timeoutMs}ms`);
}

beforeAll(async () => {
  dockerCompose("up --build -d");
  await waitForHealthy(120_000);
  pool = new pg.Pool({ connectionString: PG_CONNECTION });
  await pool.query("DELETE FROM entities");
  await pool.query("DELETE FROM users");
}, 150_000);

afterAll(async () => {
  await pool?.end();
  spawnSync("docker", ["compose", "down", "-v"], {
    cwd: COMPOSE_DIR,
    env: { ...process.env, AUTH_SECRET, DOCKER_HOST: process.env.DOCKER_HOST },
  });
});

/**
 * `apply.ts`'s `applyOps` writes an `Upsert`'s whole value into a single
 * `payload` column (`INSERT INTO table (id, payload) VALUES ($1, $2)`),
 * not a mirror of the source Postgres table's columns — this is the local
 * envelope schema `prometheusSync`'s `list()`/`get()` reads back from, and
 * matches `CrdtPatch`'s `crdt_state` column too.
 */
async function createLocalDb(table: string): Promise<InstanceType<typeof PGlite>> {
  const db = new PGlite();
  await db.exec(
    `CREATE TABLE IF NOT EXISTS "${table}" (id TEXT PRIMARY KEY, payload TEXT, crdt_state BYTEA)`,
  );
  return db;
}

/** Create a `users` row (JWT sub -> user_id mapping) and return the user_id. */
async function createUser(authSub: string): Promise<string> {
  const userId = crypto.randomUUID();
  await pool.query("INSERT INTO users (id, auth_user_id) VALUES ($1, $2)", [userId, authSub]);
  return userId;
}

function waitForLive(transport: PrometheusSyncTransport<TestRow>, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    if (transport.getStatus().state === "live") {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      unsubscribe();
      reject(new Error(`timed out after ${timeoutMs}ms waiting for status "live"`));
    }, timeoutMs);
    const unsubscribe = transport.statusStore.subscribe((status) => {
      if (status.state === "live") {
        clearTimeout(timer);
        unsubscribe();
        resolve();
      }
    });
  });
}

/**
 * `EntityTransport.subscribe`/`.get` are optional on the interface (a
 * transport may not support realtime or single-row fetches), but
 * `prometheusSync`'s concrete return always provides both — these narrow
 * `PrometheusSyncTransport`'s inherited-optional signatures back to
 * required for test call sites, rather than scattering `!` everywhere.
 */
function requireSubscribe<T extends object>(
  transport: PrometheusSyncTransport<T>,
): NonNullable<PrometheusSyncTransport<T>["subscribe"]> {
  if (!transport.subscribe) throw new Error("expected transport.subscribe to be defined");
  return transport.subscribe;
}

function requireGet<T extends object>(
  transport: PrometheusSyncTransport<T>,
): NonNullable<PrometheusSyncTransport<T>["get"]> {
  if (!transport.get) throw new Error("expected transport.get to be defined");
  return transport.get;
}

function waitForChange(
  transport: PrometheusSyncTransport<TestRow>,
  predicate: (id: string) => boolean,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      unsubscribe();
      reject(new Error(`timed out after ${timeoutMs}ms waiting for a matching change`));
    }, timeoutMs);
    const unsubscribe = requireSubscribe(transport)((ev) => {
      if (predicate(ev.id)) {
        clearTimeout(timer);
        unsubscribe();
        resolve();
      }
    });
  });
}

describe("two tabs, same user: mutual visibility of writes", () => {
  it("a write in one PGlite instance becomes visible in the other via the sync gateway", async () => {
    const authSub = "pem-same-user";
    const userId = await createUser(authSub);
    const table = "entities";

    const dbA = await createLocalDb(table);
    const dbB = await createLocalDb(table);

    const tabA = prometheusSync<TestRow>({
      serverUrl: SERVER_URL,
      bucket: BUCKET,
      getToken: tokenFor(authSub),
      table,
      primaryKey: "id",
      entityType: table,
      db: dbA,
    });
    const tabB = prometheusSync<TestRow>({
      serverUrl: SERVER_URL,
      bucket: BUCKET,
      getToken: tokenFor(authSub),
      table,
      primaryKey: "id",
      entityType: table,
      db: dbB,
    });

    // list()/get() call ensureSubscribed() internally, so a cheap get()
    // for a nonexistent id is enough to kick off the Subscribe handshake
    // for both tabs.
    await requireGet(tabA)("nonexistent");
    await requireGet(tabB)("nonexistent");
    // Wait for both to reach "live" before writing directly to Postgres —
    // otherwise the insert can race Subscribe handling and land in the
    // (empty) snapshot instead of arriving as a Delta, which tabB's
    // subscribe() below never listens for. See entity-sync-core's
    // docker-compose integration test for the full explanation of this
    // race (found and fixed in this same session).
    await Promise.all([waitForLive(tabA, 10_000), waitForLive(tabB, 10_000)]);

    const entityId = crypto.randomUUID();
    const changeSeen = waitForChange(tabB, (id) => id === entityId, 10_000);

    await pool.query("INSERT INTO entities (id, owner_id, payload) VALUES ($1, $2, $3)", [
      entityId,
      userId,
      "from-tab-a",
    ]);

    await changeSeen;

    const rowInB = await requireGet(tabB)(entityId);
    expect(rowInB).not.toBeNull();

    tabA.disconnect();
    tabB.disconnect();
  }, 20_000);
});

describe("two tabs, different users: no cross-user data leak", () => {
  it("a write owned by user A never reaches user B's transport", async () => {
    const authSubA = "pem-user-a";
    const authSubB = "pem-user-b";
    const userIdA = await createUser(authSubA);
    await createUser(authSubB);
    const table = "entities";

    const dbA = await createLocalDb(table);
    const dbB = await createLocalDb(table);

    const tabA = prometheusSync<TestRow>({
      serverUrl: SERVER_URL,
      bucket: BUCKET,
      getToken: tokenFor(authSubA),
      table,
      primaryKey: "id",
      entityType: table,
      db: dbA,
    });
    const tabB = prometheusSync<TestRow>({
      serverUrl: SERVER_URL,
      bucket: BUCKET,
      getToken: tokenFor(authSubB),
      table,
      primaryKey: "id",
      entityType: table,
      db: dbB,
    });

    await requireGet(tabA)("nonexistent");
    await requireGet(tabB)("nonexistent");
    await Promise.all([waitForLive(tabA, 10_000), waitForLive(tabB, 10_000)]);

    let leaked = false;
    const unsubscribeB = requireSubscribe(tabB)(() => {
      leaked = true;
    });

    const entityId = crypto.randomUUID();
    const changeSeenByA = waitForChange(tabA, (id) => id === entityId, 10_000);

    await pool.query("INSERT INTO entities (id, owner_id, payload) VALUES ($1, $2, $3)", [
      entityId,
      userIdA,
      "owned-by-a",
    ]);

    // Confirm tabA (the real owner) does receive it, proving the write and
    // delta pipeline are actually live and not just silent for everyone.
    await changeSeenByA;

    // Give tabB's bucket-scoped poll loop several ticks to have delivered
    // a (wrongly) leaked delta if the server-side bucket isolation were
    // broken — poll_deltas ticks roughly every 50ms server-side.
    await new Promise((r) => setTimeout(r, 1000));

    unsubscribeB();
    expect(leaked).toBe(false);

    const rowInB = await requireGet(tabB)(entityId);
    expect(rowInB).toBeNull();

    tabA.disconnect();
    tabB.disconnect();
  }, 20_000);
});
