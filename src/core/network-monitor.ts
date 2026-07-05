/**
 * NetworkMonitor — independent connectivity probe with change events.
 *
 * The SyncManager's main loop already calls isAvailable() once per 5-second
 * cycle. That means after a 10-minute outage, the device waits up to 5 MORE
 * seconds before the first retry attempt after reconnection.
 *
 * NetworkMonitor runs at a faster cadence (default 3s) and emits onChange
 * ONLY when the state flips. When it fires onChange(true), the SyncManager
 * triggers an immediate cycle — closing the reconnection gap to ≤ 3 seconds.
 *
 * It also fires onChange(false) the instant the probe fails, allowing the
 * engine to update the UI "connected" indicator without waiting for the next
 * main sync attempt to fail.
 *
 * Design:
 *   - Purely reactive (event-driven, not polling from the caller)
 *   - No retry logic inside the probe — just a single isAvailable() call
 *   - Runs independently of the sync loop; the two do NOT coordinate
 *   - Probe errors are treated as "offline" (conservative)
 */

const PROBE_INTERVAL_MS = 3_000;

export class NetworkMonitor {
  private _online = false;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly listeners = new Set<(online: boolean) => void>();

  constructor(private readonly probe: () => Promise<boolean>) {}

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  start(): void {
    void this.poll(); // immediate first probe
    this.timer = setInterval(() => this.poll(), PROBE_INTERVAL_MS);
  }

  stop(): void {
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
  }

  // ─── State ────────────────────────────────────────────────────────────────

  get online(): boolean { return this._online; }

  // ─── Observation ──────────────────────────────────────────────────────────

  /**
   * Subscribe to connectivity changes.
   * fn(true)  → network just came back
   * fn(false) → network just went down
   * Returns unsubscribe function.
   */
  onChange(fn: (online: boolean) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  // ─── Internal ─────────────────────────────────────────────────────────────

  private async poll(): Promise<void> {
    const was = this._online;
    try {
      this._online = await this.probe();
    } catch {
      this._online = false; // probe error = offline (conservative)
    }
    if (this._online !== was) {
      this.listeners.forEach(fn => fn(this._online));
    }
  }
}
