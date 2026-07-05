import { HybridLogicalClock } from './hlc';
import { ConflictResolver } from './conflict';
import { NetworkAdapter, Operation, QueueItem, StorageAdapter, SyncAttempt, SyncError, SyncState, SyncStatus } from './types';
/**
 * SyncManager — owns the full background sync lifecycle.
 *
 * ─── Background loop ─────────────────────────────────────────────────────────
 * A 5-second setInterval drives steady-state sync. The NetworkMonitor runs
 * independently at 3s and triggers an IMMEDIATE cycle the instant connectivity
 * is restored — closing the reconnection gap from ≤5s to ≤3s.
 *
 * ─── Exponential backoff (two tiers) ─────────────────────────────────────────
 * Tier 1 — HTTP adapter:  4 retries, 250ms → 8s (transient errors, TCP resets)
 * Tier 2 — Cycle level:   15s → 30s → 60s  (sustained outages, server down)
 *
 * ─── Queue system ────────────────────────────────────────────────────────────
 * SQLite (oplog WHERE synced=0) is the durable queue.
 * SyncQueue adds per-item lifecycle state in memory:
 *   queued → sending → sent | failed
 * Failed items keep their attempt count and last error across retries.
 *
 * ─── Network detection ───────────────────────────────────────────────────────
 * NetworkMonitor polls isAvailable() every 3s independently of the sync loop.
 * On reconnect it fires onChange(true) → runCycle() is called immediately.
 * This means data written during an outage reaches the server within 3 seconds
 * of connectivity being restored, not the next 5s timer tick.
 */
export declare class SyncManager {
    private readonly nodeId;
    private readonly storage;
    private readonly network;
    private readonly hlc;
    private readonly resolver;
    private readonly onStatusPatch;
    private state;
    private syncTimer;
    private retryTimer;
    private nextRetryAt;
    private consecutiveFailures;
    private attemptCounter;
    private readonly errorLog;
    private readonly syncLog;
    private readonly listeners;
    private readonly queue;
    private readonly monitor;
    constructor(nodeId: string, storage: StorageAdapter, network: NetworkAdapter, hlc: HybridLogicalClock, resolver: ConflictResolver, onStatusPatch: (patch: Partial<SyncStatus>) => void);
    start(): void;
    stop(): void;
    pause(): void;
    resume(): void;
    forceSync(): Promise<void>;
    getState(): SyncState;
    getErrors(): SyncError[];
    getSyncLog(): SyncAttempt[];
    getNextRetryAt(): number | null;
    /** Current per-item queue state (in-memory overlay on SQLite). */
    getQueue(): QueueItem[];
    /** Called by the engine after every local write. */
    refreshQueue(): void;
    onStateChange(fn: (state: SyncState) => void): () => void;
    private runCycle;
    injectRemoteOp(op: Operation): void;
    private applyRemoteOps;
    private transition;
    private clearRetryTimer;
}
//# sourceMappingURL=sync-manager.d.ts.map