export { prometheusSync } from "./extension.js";
export type { SyncNamespace } from "./extension.js";
export { applyOps } from "./apply.js";

/**
 * `prometheusSync` from `./pem-transport.js` is renamed on export to avoid
 * colliding with the PGlite `Extension` factory of the same name above —
 * both are called `prometheusSync` in their respective proposals
 * (`v4-entity-sync-ts-sdk`'s PGlite extension vs. `v4-pem-sync-transport`'s
 * `EntityTransport<T>` factory), but they serve different integration
 * points and can't share one export name from this package.
 */
export { prometheusSync as prometheusSyncTransport } from "./pem-transport.js";
export type { PrometheusSyncConfig, PrometheusSyncTransport, WriteInput } from "./pem-transport.js";
export { createStatusStore } from "./status-store.js";
export type { StatusStore } from "./status-store.js";
