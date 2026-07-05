import { Operation, QueueItem, QueueItemStatus } from './types';

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
export class SyncQueue {
  private readonly items = new Map<string, QueueItem>();
  private readonly listeners = new Set<() => void>();

  // ─── Lifecycle methods called by SyncManager ─────────────────────────────

  /**
   * Reconcile in-memory state with storage.
   * Called at the start of each sync cycle and after local writes.
   * - Adds ops that appeared in storage but aren't in memory yet.
   * - Removes ops no longer in storage (were marked synced).
   * - Preserves failure history for ops still pending.
   */
  syncFromStorage(ops: Operation[]): void {
    const pendingIds = new Set(ops.map(o => o.id));

    // Add newly queued ops
    for (const op of ops) {
      if (!this.items.has(op.id)) {
        this.items.set(op.id, {
          opId:        op.id,
          entityType:  op.entityType,
          entityId:    op.entityId,
          operation:   op.type,
          queuedAt:    op.hlc.wallTime,
          attempts:    0,
          lastAttempt: null,
          lastError:   null,
          status:      'queued',
        });
      }
    }

    // Remove items that are no longer pending (storage confirmed them synced).
    // Keep 'sending' items in place — they may still be in-flight.
    for (const [id, item] of this.items) {
      if (!pendingIds.has(id) && item.status !== 'sending') {
        this.items.delete(id);
      }
    }

    // Reset any stale 'sending' markers from a previous crashed cycle
    for (const item of this.items.values()) {
      if (item.status === 'sending' && !pendingIds.has(item.opId)) {
        this.items.delete(item.opId);
      }
    }

    this.notify();
  }

  /** Batch is now in-flight. */
  markSending(ids: string[]): void {
    for (const id of ids) {
      const item = this.items.get(id);
      if (item) {
        item.status      = 'sending';
        item.lastAttempt = Date.now();
      }
    }
    this.notify();
  }

  /** Server acknowledged these op IDs. */
  markSent(ids: string[]): void {
    for (const id of ids) {
      const item = this.items.get(id);
      if (item) {
        item.status    = 'sent';
        item.attempts += 1;
        item.lastError = null;
      }
    }
    this.notify();

    // Brief "sent" flash then purge — gives the UI time to render green
    setTimeout(() => {
      for (const id of ids) this.items.delete(id);
      this.notify();
    }, 2_000);
  }

  /** Send attempt failed; items stay in queue for next retry. */
  markFailed(ids: string[], error: string): void {
    for (const id of ids) {
      const item = this.items.get(id);
      if (item) {
        item.status    = 'failed';
        item.attempts += 1;
        item.lastError = error.slice(0, 120); // cap log length
      }
    }
    this.notify();
  }

  // ─── Read ─────────────────────────────────────────────────────────────────

  /** Ordered by queuedAt ascending (oldest first = FIFO). */
  getItems(): QueueItem[] {
    return [...this.items.values()].sort((a, b) => a.queuedAt - b.queuedAt);
  }

  getItem(opId: string): QueueItem | undefined {
    return this.items.get(opId);
  }

  size(): number { return this.items.size; }

  pendingCount(): number {
    return [...this.items.values()].filter(i => i.status !== 'sent').length;
  }

  failedCount(): number {
    return [...this.items.values()].filter(i => i.status === 'failed').length;
  }

  // ─── Observation ──────────────────────────────────────────────────────────

  onChange(fn: () => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  private notify(): void {
    this.listeners.forEach(fn => fn());
  }
}
