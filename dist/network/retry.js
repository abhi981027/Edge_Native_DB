"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CircuitBreaker = exports.DEFAULT_RETRY = void 0;
exports.withRetry = withRetry;
exports.DEFAULT_RETRY = {
    maxAttempts: 4,
    baseDelayMs: 250,
    maxDelayMs: 8_000,
    jitterFactor: 0.3,
};
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
class CircuitBreaker {
    failureThreshold;
    recoveryTimeMs;
    state = 'closed';
    consecutiveFailures = 0;
    openedAt = 0;
    constructor(failureThreshold = 3, recoveryTimeMs = 15_000) {
        this.failureThreshold = failureThreshold;
        this.recoveryTimeMs = recoveryTimeMs;
    }
    isOpen() {
        if (this.state === 'open') {
            if (Date.now() - this.openedAt >= this.recoveryTimeMs) {
                this.state = 'half-open';
                return false; // allow one probe
            }
            return true;
        }
        return false;
    }
    onSuccess() {
        this.state = 'closed';
        this.consecutiveFailures = 0;
    }
    onFailure() {
        this.consecutiveFailures++;
        if (this.consecutiveFailures >= this.failureThreshold) {
            this.state = 'open';
            this.openedAt = Date.now();
        }
    }
    getState() {
        return this.state;
    }
    getStats() {
        return {
            state: this.state,
            consecutiveFailures: this.consecutiveFailures,
            openedAt: this.openedAt || null,
            recoveryAt: this.state === 'open'
                ? this.openedAt + this.recoveryTimeMs
                : null,
        };
    }
}
exports.CircuitBreaker = CircuitBreaker;
/**
 * Execute `fn` with exponential backoff + jitter retries.
 * Respects the circuit breaker: fails fast when circuit is open.
 */
async function withRetry(fn, opts = exports.DEFAULT_RETRY, circuit) {
    let attempt = 0;
    for (;;) {
        if (circuit?.isOpen()) {
            throw new Error('Circuit open — skipping network call');
        }
        try {
            const result = await fn();
            circuit?.onSuccess();
            return result;
        }
        catch (err) {
            circuit?.onFailure();
            attempt++;
            if (attempt >= opts.maxAttempts)
                throw err;
            const base = Math.min(opts.baseDelayMs * 2 ** (attempt - 1), opts.maxDelayMs);
            const jitter = base * opts.jitterFactor * Math.random();
            await sleep(base + jitter);
        }
    }
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
//# sourceMappingURL=retry.js.map