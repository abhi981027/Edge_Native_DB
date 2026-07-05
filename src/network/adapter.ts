import {
  NetworkAdapter,
  Operation,
  PullResult,
  PushResult,
  SyncResult,
} from '../core/types';

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
export class OfflineSimulator implements NetworkAdapter {
  private _offline   = false;
  private _latencyMs = 0;
  private _timeoutMs = 0;

  constructor(private readonly inner: NetworkAdapter) {}

  // ─── Simulation controls ──────────────────────────────────────────────────

  setOffline(offline: boolean): void     { this._offline   = offline; }
  setLatency(ms: number): void           { this._latencyMs = Math.max(0, ms); }
  setTimeoutMs(ms: number): void         { this._timeoutMs = Math.max(0, ms); }

  isSimulatingOffline(): boolean         { return this._offline; }
  getLatency(): number                   { return this._latencyMs; }
  getTimeoutMs(): number                 { return this._timeoutMs; }

  // ─── NetworkAdapter ───────────────────────────────────────────────────────

  async push(ops: Operation[]): Promise<PushResult> {
    this.guardOffline();
    await this.simulateLatency();
    return this.withTimeout(() => this.inner.push(ops));
  }

  async pull(since: number): Promise<PullResult> {
    this.guardOffline();
    await this.simulateLatency();
    return this.withTimeout(() => this.inner.pull(since));
  }

  async isAvailable(): Promise<boolean> {
    if (this._offline) return false;
    return this.inner.isAvailable();
  }

  // Forward the optional sync() method when the inner adapter supports it
  async sync(ops: Operation[]): Promise<SyncResult> {
    this.guardOffline();
    await this.simulateLatency();
    if (!this.inner.sync) throw new Error('Inner adapter does not support sync()');
    return this.withTimeout(() => this.inner.sync!(ops));
  }

  // ─── Private ──────────────────────────────────────────────────────────────

  private guardOffline(): void {
    if (this._offline) throw new Error('Network unavailable (simulated offline)');
  }

  private simulateLatency(): Promise<void> {
    if (this._latencyMs <= 0) return Promise.resolve();
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
  private withTimeout<T>(fn: () => Promise<T>): Promise<T> {
    if (this._timeoutMs <= 0) return fn();

    let timer: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`Network timeout after ${this._timeoutMs}ms (simulated)`)),
        this._timeoutMs
      );
    });

    return Promise.race([fn(), deadline]).finally(() => clearTimeout(timer));
  }
}
