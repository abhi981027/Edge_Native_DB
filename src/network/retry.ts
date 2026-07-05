export type RetryOptions = {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number; // 0–1: fraction of delay to randomize (prevents thundering herd)
};

export const DEFAULT_RETRY: RetryOptions = {
  maxAttempts: 4,
  baseDelayMs: 250,
  maxDelayMs: 8_000,
  jitterFactor: 0.3,
};

type CircuitState = 'closed' | 'open' | 'half-open';

/**
 * Circuit Breaker — stops hammering a downed upstream.
 *
 * States:
 *   closed    → normal operation, requests flow through
 *   open      → upstream is down, requests fail fast (no network call made)
 *   half-open → recovery window; one probe is allowed through
 *
 * Transition closed→open after `failureThreshold` consecutive failures.
 * Transitions open→half-open after `recoveryTimeMs`.
 * Transitions half-open→closed on success; half-open→open on failure.
 */
export class CircuitBreaker {
  private state: CircuitState = 'closed';
  private consecutiveFailures = 0;
  private openedAt = 0;

  constructor(
    private readonly failureThreshold = 3,
    private readonly recoveryTimeMs = 15_000
  ) {}

  isOpen(): boolean {
    if (this.state === 'open') {
      if (Date.now() - this.openedAt >= this.recoveryTimeMs) {
        this.state = 'half-open';
        return false; // allow one probe
      }
      return true;
    }
    return false;
  }

  onSuccess(): void {
    this.state = 'closed';
    this.consecutiveFailures = 0;
  }

  onFailure(): void {
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.failureThreshold) {
      this.state = 'open';
      this.openedAt = Date.now();
    }
  }

  getState(): CircuitState {
    return this.state;
  }

  getStats() {
    return {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      openedAt: this.openedAt || null,
      recoveryAt:
        this.state === 'open'
          ? this.openedAt + this.recoveryTimeMs
          : null,
    };
  }
}

/**
 * Execute `fn` with exponential backoff + jitter retries.
 * Respects the circuit breaker: fails fast when circuit is open.
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  opts: RetryOptions = DEFAULT_RETRY,
  circuit?: CircuitBreaker
): Promise<T> {
  let attempt = 0;

  for (;;) {
    if (circuit?.isOpen()) {
      throw new Error('Circuit open — skipping network call');
    }

    try {
      const result = await fn();
      circuit?.onSuccess();
      return result;
    } catch (err) {
      circuit?.onFailure();
      attempt++;

      if (attempt >= opts.maxAttempts) throw err;

      const base = Math.min(opts.baseDelayMs * 2 ** (attempt - 1), opts.maxDelayMs);
      const jitter = base * opts.jitterFactor * Math.random();
      await sleep(base + jitter);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
