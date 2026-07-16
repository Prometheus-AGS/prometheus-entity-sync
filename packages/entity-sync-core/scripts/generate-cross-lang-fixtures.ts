/**
 * Generates cross-language wire-format fixtures: encodes a set of
 * `ServerMessage`/`ClientMessage` values with the TypeScript codec and
 * writes the resulting MessagePack bytes to disk as hex, one file per
 * message. `crates/pes-protocol/tests/cross_lang_roundtrip.rs` reads
 * these files back, decodes them with the Rust codec, and asserts the
 * fields match what this script encoded — proving the two codecs agree
 * on wire format, not just that each one round-trips with itself.
 *
 * Run with: `pnpm --filter @prometheus-ags/entity-sync-core run generate-fixtures`
 */

import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { encodeServer, encodeClient } from "../src/codec.js";
import type { ServerMessage, ClientMessage } from "../src/messages.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
// Shared with the Rust test — both sides read/write this directory.
const FIXTURES_DIR = join(
  __dirname,
  "../../../crates/pes-protocol/tests/fixtures/cross_lang",
);

mkdirSync(FIXTURES_DIR, { recursive: true });

function writeFixture(name: string, bytes: Uint8Array): void {
  const hex = Buffer.from(bytes).toString("hex");
  writeFileSync(join(FIXTURES_DIR, `${name}.hex`), hex, "utf-8");
}

const snapshotBegin: ServerMessage = {
  SnapshotBegin: {
    bucket_id: "user_entities",
    total_rows: 12345,
    protocol_version: 1,
  },
};
writeFixture("server_snapshot_begin", encodeServer(snapshotBegin));

const keepalive: ServerMessage = {
  Keepalive: { server_time_ms: 1700000000000 },
};
writeFixture("server_keepalive", encodeServer(keepalive));

const error: ServerMessage = {
  Error: { code: 4000, message: "unsupported protocol version" },
};
writeFixture("server_error", encodeServer(error));

const subscribe: ClientMessage = {
  Subscribe: {
    buckets: ["user_entities", "tenant_shared"],
    token: "eyJ.test.token",
    resume_lsn: 42,
    protocol_version: 1,
  },
};
writeFixture("client_subscribe", encodeClient(subscribe));

const ack: ClientMessage = { Ack: { lsn: 99 } };
writeFixture("client_ack", encodeClient(ack));

const write: ClientMessage = {
  Write: {
    entity_type: "Todo",
    entity_id: "t1",
    op: { Upsert: { title: "cross-language test" } },
  },
};
writeFixture("client_write", encodeClient(write));

const ping: ClientMessage = "Ping";
writeFixture("client_ping", encodeClient(ping));

console.log(`Wrote 7 cross-language fixtures to ${FIXTURES_DIR}`);
