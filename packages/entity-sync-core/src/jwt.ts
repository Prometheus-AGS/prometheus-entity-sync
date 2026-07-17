/**
 * JWT expiry detection and proactive refresh scheduling. The sync protocol
 * itself doesn't carry token lifetime — this module decodes the `exp` claim
 * client-side (base64url JSON payload, no signature verification: this is
 * purely a refresh-timing hint, not a trust boundary) and schedules
 * `getToken()` to be called 60 seconds before expiry so the connection can
 * re-`Subscribe` with a fresh token before the server rejects the old one.
 */

const REFRESH_LEAD_MS = 60_000;

/** Decode a JWT's `exp` claim (Unix seconds) without verifying its signature. */
export function decodeJwtExpiry(token: string): number | undefined {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return undefined;
  }
  try {
    const payloadJson = base64UrlDecode(parts[1]!);
    const payload: unknown = JSON.parse(payloadJson);
    if (
      typeof payload === "object" &&
      payload !== null &&
      "exp" in payload &&
      typeof (payload as { exp: unknown }).exp === "number"
    ) {
      return (payload as { exp: number }).exp;
    }
    return undefined;
  } catch {
    return undefined;
  }
}

function base64UrlDecode(segment: string): string {
  const base64 = segment.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
  // atob is available in browsers and Node 18+; this SDK targets both.
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

/**
 * Schedules a refresh callback `REFRESH_LEAD_MS` before `token`'s `exp`
 * claim. Returns `undefined` (and schedules nothing) if `token` has no
 * decodable `exp` claim, or if expiry is already within the lead window (the
 * caller should refresh immediately in that case instead).
 *
 * `now` and `random`-equivalent scheduling determinism aren't needed here
 * since this only wraps `setTimeout`; `nowMs` is injectable for tests.
 */
export function scheduleJwtRefresh(
  token: string,
  onRefresh: () => void,
  nowMs: () => number = Date.now,
): ReturnType<typeof setTimeout> | undefined {
  const expSeconds = decodeJwtExpiry(token);
  if (expSeconds === undefined) {
    return undefined;
  }
  const expMs = expSeconds * 1_000;
  const refreshAtMs = expMs - REFRESH_LEAD_MS;
  const delay = refreshAtMs - nowMs();
  if (delay <= 0) {
    return undefined;
  }
  return setTimeout(onRefresh, delay);
}

export { REFRESH_LEAD_MS };
