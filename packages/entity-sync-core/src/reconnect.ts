/**
 * Exponential backoff reconnect scheduling: starts at 1s, doubles each
 * attempt, caps at 30s, with ±20% jitter to avoid a thundering herd of
 * clients reconnecting in lockstep after a shared server restart.
 */

export const INITIAL_DELAY_MS = 1_000;
export const MAX_DELAY_MS = 30_000;
export const JITTER_RATIO = 0.2;

/**
 * Compute the delay before reconnect attempt number `attempt` (1-indexed:
 * the first retry after a disconnect is `attempt = 1`).
 *
 * `random` is injectable for deterministic testing; defaults to `Math.random`.
 */
export function backoffDelayMs(attempt: number, random: () => number = Math.random): number {
  const exponential = INITIAL_DELAY_MS * 2 ** Math.max(0, attempt - 1);
  const capped = Math.min(exponential, MAX_DELAY_MS);
  const jitterSpan = capped * JITTER_RATIO;
  // Uniform in [capped - jitterSpan, capped + jitterSpan].
  const jitter = (random() * 2 - 1) * jitterSpan;
  return Math.max(0, Math.round(capped + jitter));
}

/**
 * Drives repeated reconnect attempts with exponential backoff. Owns no
 * timers itself until `schedule` is called; `cancel` clears any pending
 * attempt (e.g. on an explicit `disconnect()`).
 */
export class ReconnectScheduler {
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private readonly random: () => number;

  constructor(random: () => number = Math.random) {
    this.random = random;
  }

  /** Schedule `onReconnect` after the next backoff delay, incrementing the attempt counter. */
  schedule(onReconnect: () => void): void {
    this.attempt += 1;
    const delay = backoffDelayMs(this.attempt, this.random);
    this.timer = setTimeout(onReconnect, delay);
  }

  /** Reset the attempt counter — call this after a successful reconnect. */
  reset(): void {
    this.attempt = 0;
  }

  /** Cancel any pending scheduled reconnect. */
  cancel(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}
