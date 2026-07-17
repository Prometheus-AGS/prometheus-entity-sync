import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { decodeJwtExpiry, REFRESH_LEAD_MS, scheduleJwtRefresh } from "../jwt.js";

/** Build a syntactically-valid (unsigned) JWT with the given payload — this
 * module never verifies signatures, only decodes the `exp` claim. */
function fakeJwt(payload: Record<string, unknown>): string {
  const header = base64UrlEncode(JSON.stringify({ alg: "none", typ: "JWT" }));
  const body = base64UrlEncode(JSON.stringify(payload));
  return `${header}.${body}.`;
}

function base64UrlEncode(value: string): string {
  const bytes = new TextEncoder().encode(value);
  const binary = String.fromCharCode(...bytes);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

describe("decodeJwtExpiry", () => {
  it("decodes the exp claim from a well-formed JWT", () => {
    const token = fakeJwt({ sub: "user-1", exp: 1_900_000_000 });
    expect(decodeJwtExpiry(token)).toBe(1_900_000_000);
  });

  it("returns undefined for a token with no exp claim", () => {
    const token = fakeJwt({ sub: "user-1" });
    expect(decodeJwtExpiry(token)).toBeUndefined();
  });

  it("returns undefined for a malformed token (wrong segment count)", () => {
    expect(decodeJwtExpiry("not-a-jwt")).toBeUndefined();
    expect(decodeJwtExpiry("only.two")).toBeUndefined();
  });

  it("returns undefined for a token whose payload segment isn't valid base64url JSON", () => {
    expect(decodeJwtExpiry("header.%%%not-base64%%%.signature")).toBeUndefined();
  });

  it("returns undefined when exp is present but not a number", () => {
    const header = base64UrlEncode(JSON.stringify({ alg: "none" }));
    const body = base64UrlEncode(JSON.stringify({ exp: "not-a-number" }));
    expect(decodeJwtExpiry(`${header}.${body}.`)).toBeUndefined();
  });
});

describe("scheduleJwtRefresh", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it(`schedules the callback ${REFRESH_LEAD_MS}ms before expiry`, () => {
    const nowSeconds = 1_000;
    const expSeconds = nowSeconds + 120; // expires in 120s
    const token = fakeJwt({ exp: expSeconds });
    const onRefresh = vi.fn();

    const nowMs = () => nowSeconds * 1_000;
    scheduleJwtRefresh(token, onRefresh, nowMs);

    // Expected delay: (120s - 60s lead) = 60s = 60000ms
    vi.advanceTimersByTime(60_000 - 1);
    expect(onRefresh).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it("returns undefined and schedules nothing for a token with no exp claim", () => {
    const token = fakeJwt({ sub: "user-1" });
    const onRefresh = vi.fn();

    const handle = scheduleJwtRefresh(token, onRefresh);
    expect(handle).toBeUndefined();

    vi.advanceTimersByTime(10 * 60_000);
    expect(onRefresh).not.toHaveBeenCalled();
  });

  it("returns undefined when expiry is already within the lead window", () => {
    const nowSeconds = 1_000;
    // Expires in 30s — less than the 60s lead window.
    const token = fakeJwt({ exp: nowSeconds + 30 });
    const onRefresh = vi.fn();

    const nowMs = () => nowSeconds * 1_000;
    const handle = scheduleJwtRefresh(token, onRefresh, nowMs);
    expect(handle).toBeUndefined();
  });

  it("returns undefined when the token is already expired", () => {
    const nowSeconds = 1_000;
    const token = fakeJwt({ exp: nowSeconds - 10 });
    const onRefresh = vi.fn();

    const nowMs = () => nowSeconds * 1_000;
    const handle = scheduleJwtRefresh(token, onRefresh, nowMs);
    expect(handle).toBeUndefined();
  });
});
