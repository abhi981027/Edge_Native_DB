export type RetryOptions = {
    maxAttempts: number;
    baseDelayMs: number;
    maxDelayMs: number;
    jitterFactor: number;
};
export declare const DEFAULT_RETRY: RetryOptions;
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
export declare class CircuitBreaker {
    private readonly failureThreshold;
    private readonly recoveryTimeMs;
    private state;
    private consecutiveFailures;
    private openedAt;
    constructor(failureThreshold?: number, recoveryTimeMs?: number);
    isOpen(): boolean;
    onSuccess(): void;
    onFailure(): void;
    getState(): CircuitState;
    getStats(): {
        state: CircuitState;
        consecutiveFailures: number;
        openedAt: number | null;
        recoveryAt: number | null;
    };
}
/**
 * Execute `fn` with exponential backoff + jitter retries.
 * Respects the circuit breaker: fails fast when circuit is open.
 */
export declare function withRetry<T>(fn: () => Promise<T>, opts?: RetryOptions, circuit?: CircuitBreaker): Promise<T>;
export {};
//# sourceMappingURL=retry.d.ts.map