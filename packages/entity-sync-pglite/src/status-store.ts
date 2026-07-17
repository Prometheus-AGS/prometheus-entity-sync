/**
 * A minimal Zustand-compatible store (`getState`/`setState`/`subscribe`) for
 * a `SyncClient`'s status. "Zustand-compatible" means interface-compatible
 * — this package doesn't depend on the `zustand` npm package itself, to
 * avoid a second store instance in an app that already uses its own
 * `zustand` (PEM's `useGraphStore` among them) and to keep this package's
 * bundle footprint small. The shape (`getState`, `setState`, `subscribe`)
 * is exactly what `zustand`'s vanilla `createStore` produces, so this can
 * be dropped into `useSyncExternalStore` or wrapped by a `useStore(store,
 * selector)` hook the same way a real Zustand store would be.
 */

import type { SyncStatus } from "@prometheus-ags/entity-sync-core";

export interface StatusStore {
  getState: () => SyncStatus;
  setState: (status: SyncStatus) => void;
  subscribe: (listener: (status: SyncStatus, previous: SyncStatus) => void) => () => void;
}

export function createStatusStore(initial: SyncStatus): StatusStore {
  let state = initial;
  const listeners = new Set<(status: SyncStatus, previous: SyncStatus) => void>();

  return {
    getState: () => state,
    setState: (next: SyncStatus) => {
      const previous = state;
      state = next;
      for (const listener of listeners) {
        listener(state, previous);
      }
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
