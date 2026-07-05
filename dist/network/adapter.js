"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OfflineSimulator = void 0;
/**
 * OfflineSimulator — wraps any NetworkAdapter with injectable failures.
 *
 * Controls:
 *   setOffline(true)       — all calls throw immediately (simulates no network)
 *   setLatency(ms)         — adds a fixed delay before the real call
 *   setTimeoutMs(ms > 0)   — races the real call against a hard deadline;
 *                            deadline reached → throws "Network timeout"
 *
 * The timeout simulation is distinct from latency:
 *   - latency: call succeeds but slowly
 *   - timeout: call is killed mid-flight after N ms (simulates TCP stall / hung
 *              connection — the case retry logic most needs to handle correctly)
 */
class OfflineSimulator {
    inner;
    _offline = false;
    _latencyMs = 0;
    _timeoutMs = 0;
    constructor(inner) {
        this.inner = inner;
    }
    // ─── Simulation controls ──────────────────────────────────────────────────
    setOffline(offline) { this._offline = offline; }
    setLatency(ms) { this._latencyMs = Math.max(0, ms); }
    setTimeoutMs(ms) { this._timeoutMs = Math.max(0, ms); }
    isSimulatingOffline() { return this._offline; }
    getLatency() { return this._latencyMs; }
    getTimeoutMs() { return this._timeoutMs; }
    // ─── NetworkAdapter ───────────────────────────────────────────────────────
    async push(ops) {
        this.guardOffline();
        await this.simulateLatency();
        return this.withTimeout(() => this.inner.push(ops));
    }
    async pull(since) {
        this.guardOffline();
        await this.simulateLatency();
        return this.withTimeout(() => this.inner.pull(since));
    }
    async isAvailable() {
        if (this._offline)
            return false;
        return this.inner.isAvailable();
    }
    // Forward the optional sync() method when the inner adapter supports it
    async sync(ops) {
        this.guardOffline();
        await this.simulateLatency();
        if (!this.inner.sync)
            throw new Error('Inner adapter does not support sync()');
        return this.withTimeout(() => this.inner.sync(ops));
    }
    // ─── Private ──────────────────────────────────────────────────────────────
    guardOffline() {
        if (this._offline)
            throw new Error('Network unavailable (simulated offline)');
    }
    simulateLatency() {
        if (this._latencyMs <= 0)
            return Promise.resolve();
        return new Promise(resolve => setTimeout(resolve, this._latencyMs));
    }
    /**
     * Race `fn()` against a hard deadline.
     *
     * When _timeoutMs > 0 the promise from fn() is raced against a rejection
     * timer. If the timer wins, the error message includes the configured limit
     * so it's visible in the error log UI.
     *
     * The inner call is still running after the timeout fires — this simulates
     * a real TCP timeout where the OS-level connection is still open but the
     * application has given up waiting. Retry safety: since the server-side
     * INSERT OR IGNORE is idempotent, a retry after a timed-out request that
     * actually reached the server is completely safe.
     */
    withTimeout(fn) {
        if (this._timeoutMs <= 0)
            return fn();
        let timer;
        const deadline = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`Network timeout after ${this._timeoutMs}ms (simulated)`)), this._timeoutMs);
        });
        return Promise.race([fn(), deadline]).finally(() => clearTimeout(timer));
    }
}
exports.OfflineSimulator = OfflineSimulator;
//# sourceMappingURL=adapter.js.map