import { v4 as uuidv4 } from 'uuid';
import { HybridLogicalClock } from './hlc';
import { ConflictResolver } from './conflict';
import { CrdtMerger } from './crdt-merger';
import { MergeRegistry } from './merge-registry';
import { NetworkMonitor } from './network-monitor';
import { SyncQueue } from './sync-queue';
import {
  Conflict,
  ConflictVersion,
  Entity,
  HLC,
  NetworkAdapter,
  Operation,
  QueueItem,
  StorageAdapter,
  SyncAttempt,
  SyncError,
  SyncState,
  SyncStatus,
} from './types';

const SYNC_INTERVAL_MS = 5_000;
const PUSH_BATCH_SIZE  = 50;
const MAX_ERROR_LOG    = 50;
const MAX_SYNC_LOG     = 30;

// Cycle-level retry delays. Fire after ALL HTTP-level retries are exhausted.
const CYCLE_RETRY_DELAYS = [15_000, 30_000, 60_000] as const;

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
export class SyncManager {
  private state: SyncState = 'idle';
  private syncTimer: ReturnType<typeof setInterval> | null = null;
  private retryTimer: ReturnType<typeof setTimeout>  | null = null;
  private nextRetryAt: number | null = null;
  private consecutiveFailures = 0;
  private attemptCounter = 0;

  private readonly errorLog:   SyncError[]  = [];
  private readonly syncLog:    SyncAttempt[] = [];
  private readonly listeners = new Set<(state: SyncState) => void>();

  private readonly queue:   SyncQueue;
  private readonly monitor: NetworkMonitor;

  constructor(
    private readonly nodeId: string,
    private readonly storage: StorageAdapter,
    private readonly network: NetworkAdapter,
    private readonly hlc: HybridLogicalClock,
    private readonly resolver: ConflictResolver,
    private readonly onStatusPatch: (patch: Partial<SyncStatus>) => void
  ) {
    this.queue   = new SyncQueue();
    this.monitor = new NetworkMonitor(() => this.network.isAvailable());

    // Seed queue from any ops that survived a crash/restart
    this.queue.syncFromStorage(this.storage.getPendingOps());
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  start(): void {
    // Independent network probe — triggers immediate sync on reconnect
    this.monitor.start();
    this.monitor.onChange(online => {
      this.onStatusPatch({ connected: online });
      if (online && (this.state === 'offline' || this.state === 'idle')) {
        this.clearRetryTimer();
        this.transition('idle');
        void this.runCycle();
      }
    });

    this.syncTimer = setInterval(() => this.runCycle(), SYNC_INTERVAL_MS);
    void this.runCycle(); // eager first attempt
  }

  stop(): void {
    this.monitor.stop();
    if (this.syncTimer) { clearInterval(this.syncTimer); this.syncTimer = null; }
    this.clearRetryTimer();
    this.transition('idle');
  }

  pause():  void { this.clearRetryTimer(); this.transition('paused'); }
  resume(): void {
    if (this.state !== 'paused') return;
    this.transition('idle');
    void this.runCycle();
  }

  async forceSync(): Promise<void> {
    if (this.state === 'paused') return;
    this.clearRetryTimer();
    if (this.state === 'retrying') this.transition('idle');
    await this.runCycle();
  }

  // ─── Observability ────────────────────────────────────────────────────────

  getState(): SyncState        { return this.state; }
  getErrors(): SyncError[]     { return [...this.errorLog]; }
  getSyncLog(): SyncAttempt[]  { return [...this.syncLog]; }
  getNextRetryAt(): number | null { return this.nextRetryAt; }

  /** Current per-item queue state (in-memory overlay on SQLite). */
  getQueue(): QueueItem[] { return this.queue.getItems(); }

  /** Called by the engine after every local write. */
  refreshQueue(): void {
    this.queue.syncFromStorage(this.storage.getPendingOps());
  }

  onStateChange(fn: (state: SyncState) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  // ─── Core sync cycle ──────────────────────────────────────────────────────

  private async runCycle(): Promise<void> {
    if (this.state === 'syncing' || this.state === 'paused' || this.state === 'retrying') return;

    // ── Step 1: check connectivity ────────────────────────────────────────
    const available = await this.network.isAvailable().catch(() => false);
    this.onStatusPatch({ connected: available });

    if (!available) { this.transition('offline'); return; }

    this.transition('syncing');
    this.onStatusPatch({ syncing: true });

    const attempt: SyncAttempt = {
      id:          ++this.attemptCounter,
      startedAt:   Date.now(),
      completedAt: null,
      durationMs:  null,
      sent:        0,
      received:    0,
      success:     false,
      error:       null,
    };

    try {
      // ── Step 2: read pending queue ────────────────────────────────────
      const pending = this.storage.getPendingOps();
      this.queue.syncFromStorage(pending);
      attempt.sent = pending.length;

      const pendingIds = pending.map(o => o.id);
      if (pendingIds.length > 0) this.queue.markSending(pendingIds);

      let accepted:  string[]    = [];
      let remoteOps: Operation[] = [];

      // ── Step 3: push + pull (one round-trip when adapter supports sync()) ──
      if (this.network.sync) {
        const result = await this.network.sync(pending);
        accepted  = result.accepted;
        remoteOps = result.remoteOps;
      } else {
        // Legacy separate push / pull
        for (let i = 0; i < pending.length; i += PUSH_BATCH_SIZE) {
          const batch = pending.slice(i, i + PUSH_BATCH_SIZE);
          const res   = await this.network.push(batch);
          accepted.push(...res.accepted);
        }
        const since  = this.storage.getLastSeenSequence();
        const pulled = await this.network.pull(since);
        remoteOps    = pulled.ops;
        if (pulled.sequence > since) this.storage.setLastSeenSequence(pulled.sequence);
      }

      // ── Step 5: mark sent ops as synced ───────────────────────────────
      if (accepted.length > 0) {
        this.queue.markSent(accepted);
        for (const id of accepted) this.storage.markOpSynced(id);
        this.storage.markChangesSynced(accepted);
      } else if (pendingIds.length > 0) {
        // Nothing accepted but no error thrown — treat as still pending
        this.queue.syncFromStorage(this.storage.getPendingOps());
      }
      this.onStatusPatch({ pendingOps: this.storage.getPendingOps().length });

      // ── Step 4: apply received remote ops safely ──────────────────────
      attempt.received = remoteOps.length;
      this.applyRemoteOps(remoteOps);

      // ── Success ───────────────────────────────────────────────────────
      attempt.success    = true;
      this.consecutiveFailures = 0;
      this.onStatusPatch({ lastSyncAt: Date.now(), retryCount: 0, nextRetryAt: null });
      this.transition('idle');

    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      attempt.error = msg;

      // Mark in-flight items as failed with this error
      const inFlight = this.queue.getItems()
        .filter(i => i.status === 'sending')
        .map(i => i.opId);
      if (inFlight.length > 0) this.queue.markFailed(inFlight, msg);

      // Record cycle error
      this.consecutiveFailures++;
      const se: SyncError = {
        at:         Date.now(),
        message:    msg,
        attempt:    this.attemptCounter,
        pendingOps: this.storage.getPendingOps().length,
      };
      this.errorLog.unshift(se);
      if (this.errorLog.length > MAX_ERROR_LOG) this.errorLog.pop();

      // ── Exponential backoff (cycle level) ─────────────────────────────
      // Tier 1 (HTTP): handled inside withRetry — 4 attempts, 250ms–8s
      // Tier 2 (here): 15s → 30s → 60s — fires when all HTTP retries fail
      const delayMs    = CYCLE_RETRY_DELAYS[
        Math.min(this.consecutiveFailures - 1, CYCLE_RETRY_DELAYS.length - 1)
      ];
      this.nextRetryAt = Date.now() + delayMs;
      this.onStatusPatch({ retryCount: this.consecutiveFailures, nextRetryAt: this.nextRetryAt });

      this.transition('retrying');
      this.retryTimer = setTimeout(() => {
        if (this.state === 'retrying') {
          this.transition('idle');
          void this.runCycle();
        }
      }, delayMs);

    } finally {
      attempt.completedAt = Date.now();
      attempt.durationMs  = attempt.completedAt - attempt.startedAt;
      this.syncLog.unshift(attempt);
      if (this.syncLog.length > MAX_SYNC_LOG) this.syncLog.pop();
      this.onStatusPatch({ syncing: false });
    }
  }

  // ─── Inject an external op for simulation ─────────────────────────────────

  injectRemoteOp(op: Operation): void {
    this.applyRemoteOps([op]);
  }

  // ─── Apply remote ops (conflict detection + CRDT merge) ───────────────────

  private applyRemoteOps(ops: Operation[]): void {
    for (const op of ops) {
      if (this.storage.getOpById(op.id)) continue;

      this.hlc.receive(op.hlc);
      const local = this.storage.getEntity(op.entityType, op.entityId);

      const isConcurrentConflict =
        local !== undefined &&
        !local.tombstone &&
        local.hlc.nodeId !== op.nodeId &&
        !op.nodeId.startsWith('merge:');

      if (isConcurrentConflict && local) {
        const cmp    = this.hlc.compare(op.hlc, local.hlc);
        const schema = MergeRegistry.get(op.entityType);

        if (schema) {
          // CRDT field-level merge
          const mergedData = CrdtMerger.merge(local.data, op.payload ?? {}, schema, cmp > 0);
          const mergedHlc: HLC = {
            wallTime: Math.max(local.hlc.wallTime, op.hlc.wallTime),
            logical:  Math.max(local.hlc.logical,  op.hlc.logical) + 1,
            nodeId:   'merge:' + [local.hlc.nodeId, op.nodeId].sort().join('+'),
          };
          const mergedEntity: Entity = {
            id: op.entityId, type: op.entityType,
            data: mergedData, hlc: mergedHlc, tombstone: false,
          };
          const mergedVersion: ConflictVersion = {
            hlc: mergedHlc, data: mergedData, deviceId: mergedHlc.nodeId,
          };
          const conflict: Conflict = {
            id: uuidv4(), recordId: op.entityId, tableName: op.entityType,
            localVersion:  { hlc: local.hlc, data: local.data, deviceId: local.hlc.nodeId },
            remoteVersion: { hlc: op.hlc, data: op.payload, deviceId: op.nodeId },
            mergedVersion,
            winner: 'merged', detectedAt: Date.now(), resolved: true,
          };
          this.storage.commitWrite({ ...op, synced: true }, mergedEntity);
          this.storage.logConflict(conflict);

        } else {
          // LWW fallback
          const conflict: Conflict = {
            id: uuidv4(), recordId: op.entityId, tableName: op.entityType,
            localVersion:  { hlc: local.hlc, data: local.data, deviceId: local.hlc.nodeId },
            remoteVersion: { hlc: op.hlc, data: op.payload, deviceId: op.nodeId },
            winner: cmp > 0 ? 'remote' : 'local',
            detectedAt: Date.now(), resolved: false,
          };
          const resolved = this.resolver.resolve(local, op);
          this.storage.commitWrite({ ...op, synced: true }, resolved);
          this.storage.logConflict(conflict);
        }
      } else {
        const resolved = this.resolver.resolve(local, op);
        this.storage.commitWrite({ ...op, synced: true }, resolved);
      }
    }
  }

  // ─── Private helpers ──────────────────────────────────────────────────────

  private transition(next: SyncState): void {
    this.state = next;
    if (next !== 'retrying') this.nextRetryAt = null;
    this.listeners.forEach(fn => fn(next));
  }

  private clearRetryTimer(): void {
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    this.nextRetryAt = null;
  }
}
