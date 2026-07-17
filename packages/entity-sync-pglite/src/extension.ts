/**
 * `prometheusSync` — PGlite `Extension` wiring a `SyncClient` to a local
 * PGlite database: deltas are applied as SQL mutations via `applyOps`, and
 * the extension exposes a `db.sync` namespace for bucket subscription and
 * status control.
 */

import { SyncClient, type SyncClientConfig, type SyncStatus } from "@prometheus-ags/entity-sync-core";
import type { Extension, ExtensionSetupResult, PGliteInterface } from "@electric-sql/pglite";

import { applyOps } from "./apply.js";

export interface SyncNamespace {
  subscribeBucket(bucket: string, options?: { resumeLsn?: string }): Promise<void>;
  getStatus(): SyncStatus;
  pause(): void;
  resume(): void;
}

/**
 * Build the `prometheusSync` PGlite extension. Pass the result under
 * `extensions: { sync: prometheusSync(config) }` in `PGlite.create`'s
 * options — the resulting `db.sync` namespace matches `SyncNamespace`.
 */
export function prometheusSync(config: SyncClientConfig): Extension<SyncNamespace> {
  return {
    name: "prometheus-sync",
    setup: async (pg: PGliteInterface): Promise<ExtensionSetupResult<SyncNamespace>> => {
      let paused = false;
      const subscribedBuckets = new Set<string>();

      const client = new SyncClient({
        ...config,
        onStatus: (status) => {
          config.onStatus?.(status);
        },
      });

      client.onDelta((delta) => {
        if (paused) {
          return;
        }
        void applyOps(pg, delta.ops);
      });

      const namespaceObj: SyncNamespace = {
        subscribeBucket: async (bucket, options) => {
          subscribedBuckets.add(bucket);
          client.subscribe(Array.from(subscribedBuckets), options?.resumeLsn);
        },
        getStatus: () => client.getStatus(),
        pause: () => {
          paused = true;
        },
        resume: () => {
          paused = false;
        },
      };

      return {
        namespaceObj,
        close: async () => {
          client.disconnect();
        },
      };
    },
  };
}
