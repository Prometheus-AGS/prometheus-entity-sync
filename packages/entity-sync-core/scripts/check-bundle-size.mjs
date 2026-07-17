// Verifies dist/index.bundle.js (produced by `rollup -c`) is under the
// 20 KB gzipped budget from openspec/changes/v4-entity-sync-ts-sdk/proposal.md.

import { readFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import path from "node:path";

const BUDGET_BYTES = 20 * 1024;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const bundlePath = path.join(__dirname, "..", "dist", "index.bundle.js");

const source = readFileSync(bundlePath);
const gzipped = gzipSync(source, { level: 9 });

const rawKb = (source.byteLength / 1024).toFixed(2);
const gzipKb = (gzipped.byteLength / 1024).toFixed(2);

console.log(`entity-sync-core bundle: ${rawKb} KB raw, ${gzipKb} KB gzipped (budget: 20 KB gzipped)`);

if (gzipped.byteLength > BUDGET_BYTES) {
  console.error(`FAIL: bundle exceeds the 20 KB gzipped budget by ${(gzipped.byteLength - BUDGET_BYTES) / 1024} KB`);
  process.exit(1);
}

console.log("PASS: bundle is within the 20 KB gzipped budget");
