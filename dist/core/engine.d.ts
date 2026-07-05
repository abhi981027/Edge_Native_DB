import { MergeSchema } from './merge-registry';
import { Change, ChangeFilter, Conflict, Entity, NetworkAdapter, Operation, QueueItem, StorageAdapter, SyncAttempt, SyncError, SyncState, SyncStatus } from './types';
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
export declare class SyncEngine {
    private readonly nodeId;
    private readonly storage;
    private readonly network;
    private readonly hlc;
    private readonly resolver;
    private readonly syncManager;
    private _status;
    private readonly statusListeners;
    constructor(nodeId: string, storage: StorageAdapter, network: NetworkAdapter);
    write(entityType: string, entityId: string, payload: Record<string, unknown>): string;
    remove(entityType: string, entityId: string): string;
    get(entityType: string, entityId: string): Entity | undefined;
    list(entityType: string): Entity[];
    listOps(limit?: number): Operation[];
    getChanges(filter?: ChangeFilter, limit?: number): Change[];
    start(): void;
    stop(): void;
    pause(): void;
    resume(): void;
    forceSync(): Promise<void>;
    getStatus(): SyncStatus;
    getSyncState(): SyncState;
    getSyncErrors(): SyncError[];
    getSyncLog(): SyncAttempt[];
    getSyncQueue(): QueueItem[];
    getNextRetryAt(): number | null;
    getConflicts(resolved?: boolean | null, limit?: number): Conflict[];
    markConflictResolved(id: string): void;
    getUnresolvedConflictCount(): number;
    /**
     * Preview what a CRDT auto-merge would produce for a given conflict.
     * Returns null if no merge schema is registered for the entity type.
     */
    previewMerge(conflictId: string): {
        conflictId: string;
        merged: Record<string, unknown>;
        fieldDecisions: import("./crdt-merger").FieldDecisions;
        schema: MergeSchema;
        remoteIsNewer: boolean;
    } | null;
    /**
     * Apply CRDT auto-merge to an existing conflict, upsert the merged entity,
     * and mark the conflict resolved.
     */
    applyAutoMerge(conflictId: string): {
        ok: boolean;
        merged?: Record<string, unknown>;
    };
    /**
     * Apply a user-chosen manual resolution. The `data` param is the final
     * record the user wants stored.
     */
    applyManualMerge(conflictId: string, data: Record<string, unknown>): {
        ok: boolean;
    };
    /** Register a field-level merge schema for an entity type. */
    static registerMergeSchema(entityType: string, schema: MergeSchema): void;
    getMergeSchemas(): Record<string, MergeSchema>;
    onStatusChange(fn: (s: SyncStatus) => void): () => void;
    simulateCrashWrite(entityType: string, entityId: string, payload: Record<string, unknown>): {
        opId: string;
        divergence: string;
    };
    simulateDuplicateOp(): {
        originalOpId: string;
        replayedOpId: string;
        entityBefore: unknown;
        entityAfter: unknown;
        duplicate: boolean;
        message: string;
    };
    /**
     * Injects a fake write from a different device onto an existing record,
     * forcing the conflict detection path to fire. The fake op uses the current
     * wall time + 1 tick so it wins the LWW race and is visible in the entity
     * store (making the conflict easy to see in the UI).
     */
    simulateConflict(entityType: string, entityId: string): {
        triggered: boolean;
        conflictId?: string;
        winner?: string;
        localData?: Record<string, unknown>;
        remoteData?: Record<string, unknown>;
    };
    recoverFromOplog(): {
        replayed: number;
    };
    private commitLocalOp;
    private patchStatus;
}
//# sourceMappingURL=engine.d.ts.map