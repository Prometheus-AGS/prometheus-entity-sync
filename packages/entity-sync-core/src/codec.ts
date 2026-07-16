/**
 * MessagePack encode/decode for PSyncV1 messages — the TypeScript
 * counterpart to `pes-protocol/src/codec.rs`.
 *
 * Uses `@msgpack/msgpack`, which encodes plain JS objects as MessagePack
 * maps (matching Rust's `rmp_serde::to_vec_named`) and Rust's externally-
 * tagged enum representation, per `messages.ts`'s doc comment. No custom
 * encoder/decoder config is needed: the wire shapes line up because both
 * sides use their language's natural default representation for
 * map-of-string-keys and string/bytes types.
 */

import { encode, decode } from "@msgpack/msgpack";
import type { ClientMessage, ServerMessage } from "./messages.js";

/** Encode a {@link ServerMessage} to MessagePack bytes. */
export function encodeServer(msg: ServerMessage): Uint8Array {
  return encode(msg);
}

/** Encode a {@link ClientMessage} to MessagePack bytes. */
export function encodeClient(msg: ClientMessage): Uint8Array {
  return encode(msg);
}

/**
 * Decode a {@link ServerMessage} from MessagePack bytes.
 *
 * Forward-compatible: unrecognized map keys in `bytes` (e.g. fields added
 * by a future protocol version) are silently ignored by
 * `@msgpack/msgpack`'s default decode behavior, matching the Rust side's
 * `serde` default (no `deny_unknown_fields`).
 */
export function decodeServer(bytes: Uint8Array): ServerMessage {
  return decode(bytes) as ServerMessage;
}

/**
 * Decode a {@link ClientMessage} from MessagePack bytes. See
 * {@link decodeServer} for the forward-compatibility contract.
 */
export function decodeClient(bytes: Uint8Array): ClientMessage {
  return decode(bytes) as ClientMessage;
}
