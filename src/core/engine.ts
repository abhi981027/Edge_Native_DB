import { v4 as uuidv4 } from 'uuid';
import { HybridLogicalClock } from './hlc';
import { ConflictResolver } from './conflict';
import { SyncManager } from './sync-manager';
import { CrdtMerger } from './crdt-merger';
import { MergeRegistry, MergeSchema } from './merge-registry';
import {
  Change,
  ChangeFilter,
  Conflict,
  ConflictVersion,
  Entity,
  HLC,
  NetworkAdapter,
  Operation,
  OperationType,
  QueueItem,
  StorageAdapter,
  SyncAttempt,
  SyncError,
  SyncState,
  SyncStatus,
} from './types';

/**
 * SyncEngine — coordinator for local writes + sync lifecycle.
 *
 * Responsibilities split between Engine and SyncManager:
 *
 *   Engine:       local writes, entity reads, crash simulations, change tracking
 *   SyncManager:  sync state machine, retry, error log, network calls
 *
 * The engine exposes a stable public API while delegating all sync-loop
 * behaviour to SyncManager so the two concerns stay independently testable.
 */
export class SyncEngine {
  private readonly hlc: HybridLogicalClock;
  private readonly resolver: ConflictResolver;
  private readonly syncManager: SyncManager;
  private _status: SyncStatus;
  private readonly statusListeners = new Set<(s: SyncStatus) => void>();

  constructor(
    private readonly nodeId: string,
    private readonly storage: StorageAdapter,
    private readonly network: NetworkAdapter
  ) {
    this.hlc      = new HybridLogicalClock(nodeId);
    this.resolver = new ConflictResolver(this.hlc);

    this._status = {
      nodeId,
      connected:   false,
      pendingOps:  this.storage.getPendingOps().length,
      lastSyncAt:  null,
      retryCount:  0,
      syncing:     false,
      syncState:   'idle',
      nextRetryAt: null,
    };

    this.syncManager = new SyncManager(
      nodeId,
      storage,
      network,
      this.hlc,
      this.resolver,
      patch => this.patchStatus(patch)
    );

    this.syncManager.onStateChange(state => {
      this.patchStatus({ syncState: state });
    });
  }

  // ─── Public write API ─────────────────────────────────────────────────────

  write(entityType: string, entityId: string, payload: Record<string, unknown>): string {
    const existing = this.storage.getEntity(entityType, entityId);
    const opType: OperationType = existing ? 'update' : 'insert';
    return this.commitLocalOp(opType, entityType, entityId, payload);
  }

  remove(entityType: string, entityId: string): string {
    return this.commitLocalOp('delete', entityType, entityId, null);
  }

  // ─── Public read API ──────────────────────────────────────────────────────

  get(entityType: string, entityId: string) {
    return this.storage.getEntity(entityType, entityId);
  }

  list(entityType: string) {
    return this.storage.listEntities(entityType);
  }

  listOps(limit = 50) {
    return this.storage.listOps(limit);
  }

  getChanges(filter: ChangeFilter = 'all', limit = 50): Change[] {
    return this.storage.getChanges(filter, limit);
  }

  // ─── Sync lifecycle ───────────────────────────────────────────────────────

  start(): void  { this.syncManager.start(); }
  stop(): void   { this.syncManager.stop(); }
  pause(): void  { this.syncManager.pause(); }
  resume(): void { this.syncManager.resume(); }

  async forceSync(): Promise<void> { await this.syncManager.forceSync(); }

  // ─── Sync observability ───────────────────────────────────────────────────

  getStatus(): SyncStatus              { return { ...this._status }; }
  getSyncState(): SyncState            { return this.syncManager.getState(); }
  getSyncErrors(): SyncError[]         { return this.syncManager.getErrors(); }
  getSyncLog(): SyncAttempt[]          { return this.syncManager.getSyncLog(); }
  getSyncQueue(): QueueItem[]          { return this.syncManager.getQueue(); }
  getNextRetryAt(): number | null      { return this.syncManager.getNextRetryAt(); }

  // ─── Conflict API ─────────────────────────────────────────────────────────

  getConflicts(resolved: boolean | null = null, limit = 50): Conflict[] {
    return this.storage.getConflicts(resolved, limit);
  }

  markConflictResolved(id: string): void {
    this.storage.markConflictResolved(id);
  }

  getUnresolvedConflictCount(): number {
    return this.storage.getUnresolvedConflictCount();
  }

  /**
   * Preview what a CRDT auto-merge would produce for a given conflict.
   * Returns null if no merge schema is registered for the entity type.
   */
  previewMerge(conflictId: string) {
    const conflicts = this.storage.getConflicts(null, 200);
    const c = conflicts.find(x => x.id === conflictId);
    if (!c) return null;

    const schema = MergeRegistry.get(c.tableName);
    if (!schema) return null;

    const remoteIsNewer = this.hlc.compare(c.remoteVersion.hlc, c.localVersion.hlc) > 0;
    const { merged, fieldDecisions } = CrdtMerger.preview(
      c.localVersion.data  ?? {},
      c.remoteVersion.data ?? {},
      schema,
      remoteIsNewer
    );

    return {
      conflictId,
      merged,
      fieldDecisions,
      schema,
      remoteIsNewer,
    };
  }

  /**
   * Apply CRDT auto-merge to an existing conflict, upsert the merged entity,
   * and mark the conflict resolved.
   */
  applyAutoMerge(conflictId: string): { ok: boolean; merged?: Record<string, unknown> } {
    const conflicts = this.storage.getConflicts(null, 200);
    const c = conflicts.find(x => x.id === conflictId);
    if (!c) return { ok: false };

    const schema = MergeRegistry.get(c.tableName);
    if (!schema) return { ok: false };

    const remoteIsNewer = this.hlc.compare(c.remoteVersion.hlc, c.localVersion.hlc) > 0;
    const merged = CrdtMerger.merge(
      c.localVersion.data  ?? {},
      c.remoteVersion.data ?? {},
      schema,
      remoteIsNewer
    );

    const mergedHlc: HLC = {
      wallTime: Math.max(c.localVersion.hlc.wallTime, c.remoteVersion.hlc.wallTime),
      logical:  Math.max(c.localVersion.hlc.logical,  c.remoteVersion.hlc.logical) + 1,
      nodeId:   'merge:' + [c.localVersion.deviceId, c.remoteVersion.deviceId].sort().join('+'),
    };

    const mergedVersion: ConflictVersion = { hlc: mergedHlc, data: merged, deviceId: mergedHlc.nodeId };
    const mergedEntity: Entity = {
      id: c.recordId, type: c.tableName, data: merged, hlc: mergedHlc, tombstone: false,
    };

    this.storage.upsertEntity(mergedEntity);
    this.storage.logConflict({ ...c, mergedVersion, winner: 'merged', resolved: true });

    return { ok: true, merged };
  }

  /**
   * Apply a user-chosen manual resolution. The `data` param is the final
   * record the user wants stored.
   */
  applyManualMerge(
    conflictId: string,
    data: Record<string, unknown>
  ): { ok: boolean } {
    const conflicts = this.storage.getConflicts(null, 200);
    const c = conflicts.find(x => x.id === conflictId);
    if (!c) return { ok: false };

    const mergedHlc: HLC = this.hlc.now();
    const mergedVersion: ConflictVersion = { hlc: mergedHlc, data, deviceId: this.nodeId };
    const mergedEntity: Entity = {
      id: c.recordId, type: c.tableName, data, hlc: mergedHlc, tombstone: false,
    };

    this.storage.upsertEntity(mergedEntity);
    this.storage.logConflict({ ...c, mergedVersion, winner: 'manual', resolved: true });

    return { ok: true };
  }

  /** Register a field-level merge schema for an entity type. */
  static registerMergeSchema(entityType: string, schema: MergeSchema): void {
    MergeRegistry.register(entityType, schema);
  }

  getMergeSchemas(): Record<string, MergeSchema> {
    return MergeRegistry.getAll();
  }

  onStatusChange(fn: (s: SyncStatus) => void): () => void {
    this.statusListeners.add(fn);
    return () => this.statusListeners.delete(fn);
  }

  // ─── Crash simulation ─────────────────────────────────────────────────────

  simulateCrashWrite(
    entityType: string,
    entityId: string,
    payload: Record<string, unknown>
  ): { opId: string; divergence: string } {
    const existing = this.storage.getEntity(entityType, entityId);
    const opType: OperationType = existing ? 'update' : 'insert';

    const op: Operation = {
      id:          uuidv4(),
      hlc:         this.hlc.now(),
      entityType,
      entityId,
      type:        opType,
      payload,
      nodeId:      this.nodeId,
      synced:      false,
    };

    this.storage.appendOp(op);
    this.storage.eraseEntityStore(entityType, entityId);
    this.patchStatus({ pendingOps: this.storage.getPendingOps().length });

    return {
      opId:      op.id,
      divergence: 'changes/oplog written, entity store wiped — divergence is now visible',
    };
  }

  simulateDuplicateOp(): {
    originalOpId: string; replayedOpId: string;
    entityBefore: unknown; entityAfter: unknown;
    duplicate: boolean; message: string;
  } {
    const recent = this.storage.listOps(1);
    if (!recent.length) {
      return { originalOpId: '', replayedOpId: '', entityBefore: null, entityAfter: null,
               duplicate: false, message: 'No ops in log — write something first' };
    }

    const op          = recent[0];
    const entityBefore = this.storage.getEntity(op.entityType, op.entityId);
    const local        = this.storage.getEntity(op.entityType, op.entityId);
    const resolved     = this.resolver.resolve(local, op);
    this.storage.commitWrite(op, resolved);

    const entityAfter = this.storage.getEntity(op.entityType, op.entityId);
    const changed     = JSON.stringify(entityBefore) !== JSON.stringify(entityAfter);

    return {
      originalOpId: op.id, replayedOpId: op.id,
      entityBefore, entityAfter,
      duplicate: !changed,
      message: changed
        ? 'WARNING: state changed after replay — idempotency broken!'
        : 'Duplicate silently ignored — state unchanged (idempotent ✓)',
    };
  }

  /**
   * Injects a fake write from a different device onto an existing record,
   * forcing the conflict detection path to fire. The fake op uses the current
   * wall time + 1 tick so it wins the LWW race and is visible in the entity
   * store (making the conflict easy to see in the UI).
   */
  simulateConflict(
    entityType: string,
    entityId: string
  ): { triggered: boolean; conflictId?: string; winner?: string; localData?: Record<string, unknown>; remoteData?: Record<string, unknown> } {
    const local = this.storage.getEntity(entityType, entityId);
    if (!local || local.tombstone) return { triggered: false };

    const fakeDevice = `sim-conflict-${uuidv4().slice(0, 6)}`;
    const fakeOp: Operation = {
      id:         uuidv4(),
      // Wall time + 1ms ensures this remote op WINS the LWW race so the conflict
      // is immediately visible in the entity store (easier to demo).
      hlc:        { wallTime: local.hlc.wallTime + 1, logical: 99, nodeId: fakeDevice },
      entityType,
      entityId,
      type:       'update',
      payload:    {
        ...local.data,
        name:             `${local.data['name'] ?? entityId} [from ${fakeDevice}]`,
        conflict_source:  fakeDevice,
        conflict_at:      new Date().toISOString(),
      },
      nodeId:     fakeDevice,
      synced:     true,
    };

    this.syncManager.injectRemoteOp(fakeOp);

    const recent = this.storage.getConflicts(false, 10);
    const c = recent.find(x => x.recordId === entityId && x.remoteVersion.deviceId === fakeDevice);

    return {
      triggered:  !!c,
      conflictId: c?.id,
      winner:     c?.winner,
      localData:  c?.localVersion.data ?? undefined,
      remoteData: c?.remoteVersion.data ?? undefined,
    };
  }

  recoverFromOplog(): { replayed: number } {
    const ops = this.storage.listAllOps();
    let replayed = 0;
    for (const op of ops) {
      const local    = this.storage.getEntity(op.entityType, op.entityId);
      const resolved = this.resolver.resolve(local, op);
      if (resolved) { this.storage.upsertEntity(resolved); replayed++; }
    }
    return { replayed };
  }

  // ─── Private ──────────────────────────────────────────────────────────────

  private commitLocalOp(
    type: OperationType,
    entityType: string,
    entityId: string,
    payload: Record<string, unknown> | null
  ): string {
    const op: Operation = {
      id:       uuidv4(),
      hlc:      this.hlc.now(),
      entityType,
      entityId,
      type,
      payload,
      nodeId:   this.nodeId,
      synced:   false,
    };

    const local    = this.storage.getEntity(entityType, entityId);
    const resolved = this.resolver.resolve(local, op);

    // Atomic: oplog + changes + entity in one BEGIN/COMMIT
    this.storage.commitWrite(op, resolved);
    this.syncManager.refreshQueue();
    this.patchStatus({ pendingOps: this.storage.getPendingOps().length });
    return op.id;
  }

  private patchStatus(patch: Partial<SyncStatus>): void {
    this._status = { ...this._status, ...patch };
    this.statusListeners.forEach(fn => fn(this._status));
  }
}
