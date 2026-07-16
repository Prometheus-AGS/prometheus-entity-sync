/**
 * PSyncV1 message types, mirroring `pes-protocol/src/messages.rs`.
 *
 * Rust's `serde` derive uses the externally-tagged enum representation by
 * default:
 * - A variant with fields encodes as a single-key map: `{ VariantName: { ...fields } }`.
 * - A unit variant (no fields, e.g. `ClientMessage::Ping`, `Op::Delete`)
 *   encodes as a bare string: `"VariantName"`, NOT `{ VariantName: null }`.
 *
 * Newtype wrappers like `BucketId(pub String)` and `PgLsn(pub u64)`
 * serialize transparently as their inner value — a `BucketId` is just a
 * JSON/MessagePack string on the wire, never `{ "0": "..." }`.
 *
 * These shapes were verified empirically against the real Rust encoder
 * (`rmp_serde::to_vec_named`), not inferred from the Rust source alone —
 * see the `wire_shape_check` scratch test used during development.
 */

export const PROTOCOL_VERSION = 1;
export const ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION = 4000;

/** A single op appended to a bucket op log. Mirrors `pes_core::BucketOp`. */
export interface BucketOp {
  lsn: number;
  bucket_id: string;
  entity_type: string;
  entity_id: string;
  op: Op;
}

/**
 * Mirrors `pes_core::Op`. `Delete` is a unit variant → bare string `"Delete"`.
 * `Upsert`/`CrdtPatch` carry a payload → single-key object.
 */
export type Op =
  | { Upsert: unknown }
  | "Delete"
  | { CrdtPatch: Uint8Array };

export type ServerMessage =
  | {
      SnapshotBegin: {
        bucket_id: string;
        total_rows: number;
        protocol_version: number;
      };
    }
  | {
      SnapshotBatch: {
        bucket_id: string;
        rows: unknown[];
        offset: number;
      };
    }
  | {
      SnapshotComplete: {
        bucket_id: string;
        checksum: number;
      };
    }
  | {
      Delta: {
        bucket_id: string;
        ops: BucketOp[];
        lsn: number;
      };
    }
  | {
      Checkpoint: {
        lsn: number;
        bucket_checksums: Record<string, number>;
      };
    }
  | {
      Keepalive: {
        server_time_ms: number;
      };
    }
  | {
      Error: {
        code: number;
        message: string;
      };
    };

export type ClientMessage =
  | {
      Subscribe: {
        buckets: string[];
        token: string;
        resume_lsn: number | null;
        protocol_version: number;
      };
    }
  | {
      Ack: {
        lsn: number;
      };
    }
  | {
      Write: {
        entity_type: string;
        entity_id: string;
        op: Op;
      };
    }
  /** `Ping` is a unit variant → bare string `"Ping"`. */
  | "Ping";
