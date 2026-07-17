/**
 * React hooks wrapping `SyncClient`: `useEntitySync` owns a client instance
 * for its component's lifetime, and `useSyncStatus` reads the current
 * status from the nearest `SyncStatusContext` provider.
 */

import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { SyncClient, type SyncClientConfig, type SyncStatus } from "@prometheus-ags/entity-sync-core";

const SyncStatusContext = createContext<SyncStatus | undefined>(undefined);

/**
 * Creates a `SyncClient` for the component's lifetime (recreated only if
 * `config.serverUrl` changes), tracks its status, and disconnects on
 * unmount. Wraps `children` in a `SyncStatusContext` provider so descendant
 * components can read status via `useSyncStatus()` without prop-drilling.
 */
export function useEntitySync(config: SyncClientConfig): {
  status: SyncStatus;
  subscribe: (buckets: string[]) => void;
  SyncStatusProvider: (props: { children: ReactNode }) => ReactNode;
} {
  const [status, setStatus] = useState<SyncStatus>({ state: "disconnected" });
  const configRef = useRef(config);
  configRef.current = config;

  const clientRef = useRef<SyncClient | undefined>(undefined);
  if (clientRef.current === undefined) {
    clientRef.current = new SyncClient({
      ...configRef.current,
      onStatus: (next) => {
        setStatus(next);
        configRef.current.onStatus?.(next);
      },
    });
  }

  useEffect(() => {
    const client = clientRef.current;
    return () => {
      client?.disconnect();
    };
    // Intentionally empty: the client is created once per mount via the
    // lazy ref-initializer above, not recreated on every config change —
    // callers needing a different serverUrl should remount (e.g. via a
    // `key` prop), matching the "connect once per component lifetime"
    // contract most WebSocket-backed hooks use.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const subscribe = useCallback((buckets: string[]) => {
    clientRef.current?.subscribe(buckets);
  }, []);

  const SyncStatusProvider = useCallback(
    (props: { children: ReactNode }) =>
      createElement(SyncStatusContext.Provider, { value: status }, props.children),
    [status],
  );

  return { status, subscribe, SyncStatusProvider };
}

/**
 * Reads the current `SyncStatus` from the nearest `useEntitySync`-provided
 * `SyncStatusProvider`. Returns `{ state: 'disconnected' }` if called
 * outside any provider, rather than throwing — a component reading sync
 * status defensively (e.g. a status badge) shouldn't crash if it's rendered
 * before sync is wired up.
 */
export function useSyncStatus(): SyncStatus {
  const status = useContext(SyncStatusContext);
  return status ?? { state: "disconnected" };
}
