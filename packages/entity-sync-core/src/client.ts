/**
 * `SyncClient` — PSyncV1 WebSocket connection lifecycle: connect, Subscribe
 * handshake, snapshot/delta dispatch, reconnect with backoff, proactive JWT
 * refresh, and client-initiated writes.
 */

import { decodeServer, encodeClient } from "./codec.js";
import type { BucketOp, ClientMessage, Op, ServerMessage } from "./messages.js";
import { PROTOCOL_VERSION } from "./messages.js";
import { ReconnectScheduler } from "./reconnect.js";
import { scheduleJwtRefresh } from "./jwt.js";

export interface SyncClientConfig {
  /** Gateway WebSocket URL, e.g. `wss://sync.example.com`. */
  serverUrl: string;
  /** Called on connect and proactively before token expiry. */
  getToken: () => Promise<string>;
  onStatus?: (status: SyncStatus) => void;
}

export type SyncStatus =
  | { state: "connecting" }
  | { state: "syncing"; lsn: string }
  | { state: "live"; lsn: string }
  | { state: "error"; error: Error }
  | { state: "disconnected" };

/** A batch of live ops for one bucket — the public-facing shape of `ServerMessage`'s `Delta` variant. */
export interface Delta {
  bucketId: string;
  ops: BucketOp[];
  lsn: string;
}

/** One page of a bucket's initial snapshot — the public-facing shape of `ServerMessage`'s `SnapshotBatch` variant. */
export interface SnapshotBatch {
  bucketId: string;
  rows: unknown[];
  offset: number;
}

type DeltaHandler = (delta: Delta) => void;
type SnapshotHandler = (batch: SnapshotBatch) => void;

/**
 * Drives one logical sync session over a WebSocket connection to a
 * `pes-gateway` server, transparently reconnecting (with backoff) and
 * re-subscribing to the same buckets from the last acknowledged LSN.
 */
export class SyncClient {
  private readonly config: SyncClientConfig;
  private ws: WebSocket | undefined;
  private readonly deltaHandlers = new Set<DeltaHandler>();
  private readonly snapshotHandlers = new Set<SnapshotHandler>();
  private readonly reconnectScheduler = new ReconnectScheduler();
  private refreshTimer: ReturnType<typeof setTimeout> | undefined;
  private subscribedBuckets: string[] = [];
  private lastLsn: string | undefined;
  private status: SyncStatus = { state: "disconnected" };
  private explicitlyDisconnected = false;

  constructor(config: SyncClientConfig) {
    this.config = config;
  }

  /**
   * Subscribe to `buckets`. Safe to call before the first connect (the
   * subscription is sent once the socket opens) or on an already-connected
   * client (sends a fresh `Subscribe` immediately). `resumeLsn`, if given,
   * skips initial snapshot delivery for the requested buckets.
   */
  subscribe(buckets: string[], resumeLsn?: string): void {
    this.subscribedBuckets = buckets;
    if (resumeLsn !== undefined) {
      this.lastLsn = resumeLsn;
    }
    if (this.ws?.readyState === WebSocket.OPEN) {
      void this.sendSubscribe();
    } else {
      void this.connect();
    }
  }

  /** Register a handler for `Delta` messages. Returns an unsubscribe function. */
  onDelta(handler: DeltaHandler): () => void {
    this.deltaHandlers.add(handler);
    return () => this.deltaHandlers.delete(handler);
  }

  /** Register a handler for `SnapshotBatch` messages. Returns an unsubscribe function. */
  onSnapshot(handler: SnapshotHandler): () => void {
    this.snapshotHandlers.add(handler);
    return () => this.snapshotHandlers.delete(handler);
  }

  /** Send a `ClientMessage::Write`. Resolves once the frame is flushed to the socket (not once the server has applied it). */
  async write(entityType: string, entityId: string, op: Op): Promise<void> {
    await this.send({ Write: { entity_type: entityType, entity_id: entityId, op } });
  }

  /** Tear down the connection and stop all reconnect/refresh scheduling. */
  disconnect(): void {
    this.explicitlyDisconnected = true;
    this.reconnectScheduler.cancel();
    this.clearRefreshTimer();
    this.ws?.close();
    this.ws = undefined;
    this.setStatus({ state: "disconnected" });
  }

  private async connect(): Promise<void> {
    this.explicitlyDisconnected = false;
    this.setStatus({ state: "connecting" });

    let token: string;
    try {
      token = await this.config.getToken();
    } catch (error) {
      this.setStatus({ state: "error", error: toError(error) });
      this.scheduleReconnect();
      return;
    }

    const ws = new WebSocket(this.config.serverUrl);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.addEventListener("open", () => {
      void this.sendSubscribe(token);
      this.scheduleTokenRefresh(token);
    });

    ws.addEventListener("message", (event) => {
      this.handleMessage(event.data as ArrayBuffer);
    });

    ws.addEventListener("close", () => {
      this.clearRefreshTimer();
      if (!this.explicitlyDisconnected) {
        this.setStatus({ state: "disconnected" });
        this.scheduleReconnect();
      }
    });

    ws.addEventListener("error", () => {
      // The 'close' event follows every WebSocket error and drives
      // reconnect scheduling; this handler only surfaces status.
      this.setStatus({ state: "error", error: new Error("WebSocket error") });
    });
  }

  private scheduleReconnect(): void {
    this.reconnectScheduler.schedule(() => {
      void this.connect();
    });
  }

  private scheduleTokenRefresh(token: string): void {
    this.clearRefreshTimer();
    this.refreshTimer = scheduleJwtRefresh(token, () => {
      void this.refreshAndResubscribe();
    });
  }

  private async refreshAndResubscribe(): Promise<void> {
    let token: string;
    try {
      token = await this.config.getToken();
    } catch (error) {
      this.setStatus({ state: "error", error: toError(error) });
      return;
    }
    await this.sendSubscribe(token);
    this.scheduleTokenRefresh(token);
  }

  private async sendSubscribe(token?: string): Promise<void> {
    const resolvedToken = token ?? (await this.config.getToken());
    await this.send({
      Subscribe: {
        buckets: this.subscribedBuckets,
        token: resolvedToken,
        resume_lsn: this.lastLsn !== undefined ? Number(this.lastLsn) : null,
        protocol_version: PROTOCOL_VERSION,
      },
    });
  }

  private async send(msg: ClientMessage): Promise<void> {
    if (this.ws === undefined || this.ws.readyState !== WebSocket.OPEN) {
      // No connection yet (or mid-reconnect); connect() will send the
      // pending Subscribe once open. Writes issued while disconnected are
      // dropped rather than queued — the caller's Promise rejects so they
      // can retry, matching the "no silent data loss" principle without
      // introducing unbounded queuing.
      if (typeof msg === "object" && "Write" in msg) {
        throw new Error("cannot write: not connected");
      }
      return;
    }
    this.ws.send(encodeClient(msg));
  }

  private handleMessage(data: ArrayBuffer): void {
    const msg: ServerMessage = decodeServer(new Uint8Array(data));

    if ("SnapshotBegin" in msg) {
      this.setStatus({ state: "syncing", lsn: this.lastLsn ?? "0" });
      return;
    }
    if ("SnapshotBatch" in msg) {
      const { bucket_id, rows, offset } = msg.SnapshotBatch;
      for (const handler of this.snapshotHandlers) {
        handler({ bucketId: bucket_id, rows, offset });
      }
      return;
    }
    if ("SnapshotComplete" in msg) {
      this.setStatus({ state: "live", lsn: this.lastLsn ?? "0" });
      return;
    }
    if ("Delta" in msg) {
      const { bucket_id, ops, lsn } = msg.Delta;
      this.lastLsn = String(lsn);
      this.setStatus({ state: "live", lsn: this.lastLsn });
      for (const handler of this.deltaHandlers) {
        handler({ bucketId: bucket_id, ops, lsn: this.lastLsn });
      }
      void this.send({ Ack: { lsn } });
      return;
    }
    if ("Checkpoint" in msg) {
      this.lastLsn = String(msg.Checkpoint.lsn);
      return;
    }
    if ("Keepalive" in msg) {
      return;
    }
    if ("Error" in msg) {
      this.setStatus({
        state: "error",
        error: new Error(`sync gateway error ${msg.Error.code}: ${msg.Error.message}`),
      });
    }
  }

  private setStatus(status: SyncStatus): void {
    this.status = status;
    this.config.onStatus?.(status);
  }

  /** Current status, for callers that don't want to hold onStatus state themselves. */
  getStatus(): SyncStatus {
    return this.status;
  }

  private clearRefreshTimer(): void {
    if (this.refreshTimer !== undefined) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = undefined;
    }
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
