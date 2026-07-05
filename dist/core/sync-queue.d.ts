import { Operation, QueueItem } from './types';
/**
 * SyncQueue — in-memory lifecycle tracker for pending operations.
 *
 * The durable source of truth is SQLite (`oplog WHERE synced = 0`).
 * This class adds per-item state that SQLite doesn't track:
 *
 *   queued   — waiting in the queue, not yet sent
 *   sending  — currently in-flight to the server
 *   sent     — acknowledged by server (briefly visible, then removed)
 *   failed   — last attempt threw; will be retried next cycle
 *
 * Items move through the lifecycle:
 *
 *   new op written
 *     → queued
 *   sync cycle starts, batch picked up
 *     → sending
 *   server acknowledges
 *     → sent  (auto-removed after 2s so UI can render the green flash)
 *   network error / timeout
 *     → failed (stays until next successful send; attempts++, lastError set)
 *
 * The queue is rebuilt from storage on startup via syncFromStorage(),
 * called at the beginning of each sync cycle. Failed items from a
 * previous cycle keep their attempt count when the new cycle starts.
 *
 * Thread safety: JavaScript is single-threaded; no locking needed.
 */
export declare class SyncQueue {
    private readonly items;
    private readonly listeners;
    /**
     * Reconcile in-memory state with storage.
     * Called at the start of each sync cycle and after local writes.
     * - Adds ops that appeared in storage but aren't in memory yet.
     * - Removes ops no longer in storage (were marked synced).
     * - Preserves failure history for ops still pending.
     */
    syncFromStorage(ops: Operation[]): void;
    /** Batch is now in-flight. */
    markSending(ids: string[]): void;
    /** Server acknowledged these op IDs. */
    markSent(ids: string[]): void;
    /** Send attempt failed; items stay in queue for next retry. */
    markFailed(ids: string[], error: string): void;
    /** Ordered by queuedAt ascending (oldest first = FIFO). */
    getItems(): QueueItem[];
    getItem(opId: string): QueueItem | undefined;
    size(): number;
    pendingCount(): number;
    failedCount(): number;
    onChange(fn: () => void): () => void;
    private notify;
}
//# sourceMappingURL=sync-queue.d.ts.map