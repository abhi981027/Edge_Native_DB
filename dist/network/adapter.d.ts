import { NetworkAdapter, Operation, PullResult, PushResult, SyncResult } from '../core/types';
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
export declare class OfflineSimulator implements NetworkAdapter {
    private readonly inner;
    private _offline;
    private _latencyMs;
    private _timeoutMs;
    constructor(inner: NetworkAdapter);
    setOffline(offline: boolean): void;
    setLatency(ms: number): void;
    setTimeoutMs(ms: number): void;
    isSimulatingOffline(): boolean;
    getLatency(): number;
    getTimeoutMs(): number;
    push(ops: Operation[]): Promise<PushResult>;
    pull(since: number): Promise<PullResult>;
    isAvailable(): Promise<boolean>;
    sync(ops: Operation[]): Promise<SyncResult>;
    private guardOffline;
    private simulateLatency;
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
    private withTimeout;
}
//# sourceMappingURL=adapter.d.ts.map