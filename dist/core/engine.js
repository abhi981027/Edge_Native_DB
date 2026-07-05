"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SyncEngine = void 0;
const uuid_1 = require("uuid");
const hlc_1 = require("./hlc");
const conflict_1 = require("./conflict");
const sync_manager_1 = require("./sync-manager");
const crdt_merger_1 = require("./crdt-merger");
const merge_registry_1 = require("./merge-registry");
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
class SyncEngine {
    nodeId;
    storage;
    network;
    hlc;
    resolver;
    syncManager;
    _status;
    statusListeners = new Set();
    constructor(nodeId, storage, network) {
        this.nodeId = nodeId;
        this.storage = storage;
        this.network = network;
        this.hlc = new hlc_1.HybridLogicalClock(nodeId);
        this.resolver = new conflict_1.ConflictResolver(this.hlc);
        this._status = {
            nodeId,
            connected: false,
            pendingOps: this.storage.getPendingOps().length,
            lastSyncAt: null,
            retryCount: 0,
            syncing: false,
            syncState: 'idle',
            nextRetryAt: null,
        };
        this.syncManager = new sync_manager_1.SyncManager(nodeId, storage, network, this.hlc, this.resolver, patch => this.patchStatus(patch));
        this.syncManager.onStateChange(state => {
            this.patchStatus({ syncState: state });
        });
    }
    // ─── Public write API ─────────────────────────────────────────────────────
    write(entityType, entityId, payload) {
        const existing = this.storage.getEntity(entityType, entityId);
        const opType = existing ? 'update' : 'insert';
        return this.commitLocalOp(opType, entityType, entityId, payload);
    }
    remove(entityType, entityId) {
        return this.commitLocalOp('delete', entityType, entityId, null);
    }
    // ─── Public read API ──────────────────────────────────────────────────────
    get(entityType, entityId) {
        return this.storage.getEntity(entityType, entityId);
    }
    list(entityType) {
        return this.storage.listEntities(entityType);
    }
    listOps(limit = 50) {
        return this.storage.listOps(limit);
    }
    getChanges(filter = 'all', limit = 50) {
        return this.storage.getChanges(filter, limit);
    }
    // ─── Sync lifecycle ───────────────────────────────────────────────────────
    start() { this.syncManager.start(); }
    stop() { this.syncManager.stop(); }
    pause() { this.syncManager.pause(); }
    resume() { this.syncManager.resume(); }
    async forceSync() { await this.syncManager.forceSync(); }
    // ─── Sync observability ───────────────────────────────────────────────────
    getStatus() { return { ...this._status }; }
    getSyncState() { return this.syncManager.getState(); }
    getSyncErrors() { return this.syncManager.getErrors(); }
    getSyncLog() { return this.syncManager.getSyncLog(); }
    getSyncQueue() { return this.syncManager.getQueue(); }
    getNextRetryAt() { return this.syncManager.getNextRetryAt(); }
    // ─── Conflict API ─────────────────────────────────────────────────────────
    getConflicts(resolved = null, limit = 50) {
        return this.storage.getConflicts(resolved, limit);
    }
    markConflictResolved(id) {
        this.storage.markConflictResolved(id);
    }
    getUnresolvedConflictCount() {
        return this.storage.getUnresolvedConflictCount();
    }
    /**
     * Preview what a CRDT auto-merge would produce for a given conflict.
     * Returns null if no merge schema is registered for the entity type.
     */
    previewMerge(conflictId) {
        const conflicts = this.storage.getConflicts(null, 200);
        const c = conflicts.find(x => x.id === conflictId);
        if (!c)
            return null;
        const schema = merge_registry_1.MergeRegistry.get(c.tableName);
        if (!schema)
            return null;
        const remoteIsNewer = this.hlc.compare(c.remoteVersion.hlc, c.localVersion.hlc) > 0;
        const { merged, fieldDecisions } = crdt_merger_1.CrdtMerger.preview(c.localVersion.data ?? {}, c.remoteVersion.data ?? {}, schema, remoteIsNewer);
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
    applyAutoMerge(conflictId) {
        const conflicts = this.storage.getConflicts(null, 200);
        const c = conflicts.find(x => x.id === conflictId);
        if (!c)
            return { ok: false };
        const schema = merge_registry_1.MergeRegistry.get(c.tableName);
        if (!schema)
            return { ok: false };
        const remoteIsNewer = this.hlc.compare(c.remoteVersion.hlc, c.localVersion.hlc) > 0;
        const merged = crdt_merger_1.CrdtMerger.merge(c.localVersion.data ?? {}, c.remoteVersion.data ?? {}, schema, remoteIsNewer);
        const mergedHlc = {
            wallTime: Math.max(c.localVersion.hlc.wallTime, c.remoteVersion.hlc.wallTime),
            logical: Math.max(c.localVersion.hlc.logical, c.remoteVersion.hlc.logical) + 1,
            nodeId: 'merge:' + [c.localVersion.deviceId, c.remoteVersion.deviceId].sort().join('+'),
        };
        const mergedVersion = { hlc: mergedHlc, data: merged, deviceId: mergedHlc.nodeId };
        const mergedEntity = {
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
    applyManualMerge(conflictId, data) {
        const conflicts = this.storage.getConflicts(null, 200);
        const c = conflicts.find(x => x.id === conflictId);
        if (!c)
            return { ok: false };
        const mergedHlc = this.hlc.now();
        const mergedVersion = { hlc: mergedHlc, data, deviceId: this.nodeId };
        const mergedEntity = {
            id: c.recordId, type: c.tableName, data, hlc: mergedHlc, tombstone: false,
        };
        this.storage.upsertEntity(mergedEntity);
        this.storage.logConflict({ ...c, mergedVersion, winner: 'manual', resolved: true });
        return { ok: true };
    }
    /** Register a field-level merge schema for an entity type. */
    static registerMergeSchema(entityType, schema) {
        merge_registry_1.MergeRegistry.register(entityType, schema);
    }
    getMergeSchemas() {
        return merge_registry_1.MergeRegistry.getAll();
    }
    onStatusChange(fn) {
        this.statusListeners.add(fn);
        return () => this.statusListeners.delete(fn);
    }
    // ─── Crash simulation ─────────────────────────────────────────────────────
    simulateCrashWrite(entityType, entityId, payload) {
        const existing = this.storage.getEntity(entityType, entityId);
        const opType = existing ? 'update' : 'insert';
        const op = {
            id: (0, uuid_1.v4)(),
            hlc: this.hlc.now(),
            entityType,
            entityId,
            type: opType,
            payload,
            nodeId: this.nodeId,
            synced: false,
        };
        this.storage.appendOp(op);
        this.storage.eraseEntityStore(entityType, entityId);
        this.patchStatus({ pendingOps: this.storage.getPendingOps().length });
        return {
            opId: op.id,
            divergence: 'changes/oplog written, entity store wiped — divergence is now visible',
        };
    }
    simulateDuplicateOp() {
        const recent = this.storage.listOps(1);
        if (!recent.length) {
            return { originalOpId: '', replayedOpId: '', entityBefore: null, entityAfter: null,
                duplicate: false, message: 'No ops in log — write something first' };
        }
        const op = recent[0];
        const entityBefore = this.storage.getEntity(op.entityType, op.entityId);
        const local = this.storage.getEntity(op.entityType, op.entityId);
        const resolved = this.resolver.resolve(local, op);
        this.storage.commitWrite(op, resolved);
        const entityAfter = this.storage.getEntity(op.entityType, op.entityId);
        const changed = JSON.stringify(entityBefore) !== JSON.stringify(entityAfter);
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
    simulateConflict(entityType, entityId) {
        const local = this.storage.getEntity(entityType, entityId);
        if (!local || local.tombstone)
            return { triggered: false };
        const fakeDevice = `sim-conflict-${(0, uuid_1.v4)().slice(0, 6)}`;
        const fakeOp = {
            id: (0, uuid_1.v4)(),
            // Wall time + 1ms ensures this remote op WINS the LWW race so the conflict
            // is immediately visible in the entity store (easier to demo).
            hlc: { wallTime: local.hlc.wallTime + 1, logical: 99, nodeId: fakeDevice },
            entityType,
            entityId,
            type: 'update',
            payload: {
                ...local.data,
                name: `${local.data['name'] ?? entityId} [from ${fakeDevice}]`,
                conflict_source: fakeDevice,
                conflict_at: new Date().toISOString(),
            },
            nodeId: fakeDevice,
            synced: true,
        };
        this.syncManager.injectRemoteOp(fakeOp);
        const recent = this.storage.getConflicts(false, 10);
        const c = recent.find(x => x.recordId === entityId && x.remoteVersion.deviceId === fakeDevice);
        return {
            triggered: !!c,
            conflictId: c?.id,
            winner: c?.winner,
            localData: c?.localVersion.data ?? undefined,
            remoteData: c?.remoteVersion.data ?? undefined,
        };
    }
    recoverFromOplog() {
        const ops = this.storage.listAllOps();
        let replayed = 0;
        for (const op of ops) {
            const local = this.storage.getEntity(op.entityType, op.entityId);
            const resolved = this.resolver.resolve(local, op);
            if (resolved) {
                this.storage.upsertEntity(resolved);
                replayed++;
            }
        }
        return { replayed };
    }
    // ─── Private ──────────────────────────────────────────────────────────────
    commitLocalOp(type, entityType, entityId, payload) {
        const op = {
            id: (0, uuid_1.v4)(),
            hlc: this.hlc.now(),
            entityType,
            entityId,
            type,
            payload,
            nodeId: this.nodeId,
            synced: false,
        };
        const local = this.storage.getEntity(entityType, entityId);
        const resolved = this.resolver.resolve(local, op);
        // Atomic: oplog + changes + entity in one BEGIN/COMMIT
        this.storage.commitWrite(op, resolved);
        this.syncManager.refreshQueue();
        this.patchStatus({ pendingOps: this.storage.getPendingOps().length });
        return op.id;
    }
    patchStatus(patch) {
        this._status = { ...this._status, ...patch };
        this.statusListeners.forEach(fn => fn(this._status));
    }
}
exports.SyncEngine = SyncEngine;
//# sourceMappingURL=engine.js.map