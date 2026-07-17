import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  backoffDelayMs,
  INITIAL_DELAY_MS,
  MAX_DELAY_MS,
  JITTER_RATIO,
  ReconnectScheduler,
} from "../reconnect.js";

describe("backoffDelayMs", () => {
  it("starts at INITIAL_DELAY_MS for the first attempt (no jitter)", () => {
    const noJitter = () => 0.5; // random() = 0.5 -> jitter offset of 0
    expect(backoffDelayMs(1, noJitter)).toBe(INITIAL_DELAY_MS);
  });

  it("doubles on each successive attempt", () => {
    const noJitter = () => 0.5;
    expect(backoffDelayMs(1, noJitter)).toBe(1_000);
    expect(backoffDelayMs(2, noJitter)).toBe(2_000);
    expect(backoffDelayMs(3, noJitter)).toBe(4_000);
    expect(backoffDelayMs(4, noJitter)).toBe(8_000);
  });

  it("caps at MAX_DELAY_MS for large attempt numbers", () => {
    const noJitter = () => 0.5;
    expect(backoffDelayMs(10, noJitter)).toBe(MAX_DELAY_MS);
    expect(backoffDelayMs(100, noJitter)).toBe(MAX_DELAY_MS);
  });

  it("applies up to ±20% jitter around the base delay", () => {
    const base = INITIAL_DELAY_MS * 2 ** 3; // attempt 4 -> 8000ms base
    const maxJitter = base * JITTER_RATIO;

    const atMax = backoffDelayMs(4, () => 1); // random()=1 -> +jitterSpan
    const atMin = backoffDelayMs(4, () => 0); // random()=0 -> -jitterSpan
    expect(atMax).toBeLessThanOrEqual(base + maxJitter);
    expect(atMin).toBeGreaterThanOrEqual(base - maxJitter);
    expect(atMax).toBeGreaterThan(atMin);
  });

  it("never returns a negative delay even with extreme jitter", () => {
    expect(backoffDelayMs(1, () => 0)).toBeGreaterThanOrEqual(0);
  });

  it("treats attempt 0 or negative the same as attempt 1", () => {
    const noJitter = () => 0.5;
    expect(backoffDelayMs(0, noJitter)).toBe(INITIAL_DELAY_MS);
    expect(backoffDelayMs(-5, noJitter)).toBe(INITIAL_DELAY_MS);
  });
});

describe("ReconnectScheduler", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not fire before the scheduled delay elapses", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    const onReconnect = vi.fn();

    scheduler.schedule(onReconnect);
    vi.advanceTimersByTime(INITIAL_DELAY_MS - 1);
    expect(onReconnect).not.toHaveBeenCalled();
  });

  it("fires once the scheduled delay elapses", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    const onReconnect = vi.fn();

    scheduler.schedule(onReconnect);
    vi.advanceTimersByTime(INITIAL_DELAY_MS);
    expect(onReconnect).toHaveBeenCalledTimes(1);
  });

  it("increases the delay on each successive schedule call (no reset between)", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    const first = vi.fn();
    const second = vi.fn();

    scheduler.schedule(first);
    vi.advanceTimersByTime(INITIAL_DELAY_MS);
    expect(first).toHaveBeenCalledTimes(1);

    scheduler.schedule(second);
    vi.advanceTimersByTime(INITIAL_DELAY_MS * 2 - 1);
    expect(second).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(second).toHaveBeenCalledTimes(1);
  });

  it("resets the attempt counter back to INITIAL_DELAY_MS after reset()", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    const first = vi.fn();
    const afterReset = vi.fn();

    scheduler.schedule(first);
    vi.advanceTimersByTime(INITIAL_DELAY_MS);
    scheduler.reset();

    scheduler.schedule(afterReset);
    vi.advanceTimersByTime(INITIAL_DELAY_MS - 1);
    expect(afterReset).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(afterReset).toHaveBeenCalledTimes(1);
  });

  it("cancel() prevents a pending scheduled reconnect from firing", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    const onReconnect = vi.fn();

    scheduler.schedule(onReconnect);
    scheduler.cancel();
    vi.advanceTimersByTime(MAX_DELAY_MS * 2);
    expect(onReconnect).not.toHaveBeenCalled();
  });

  it("cancel() on an already-idle scheduler is a no-op, not an error", () => {
    const scheduler = new ReconnectScheduler(() => 0.5);
    expect(() => scheduler.cancel()).not.toThrow();
  });
});
