export type HLC = {
    wallTime: number;
    logical: number;
    nodeId: string;
};
export type OperationType = 'insert' | 'update' | 'delete';
export type Operation = {
    id: string;
    hlc: HLC;
    entityType: string;
    entityId: string;
    type: OperationType;
    payload: Record<string, unknown> | null;
    nodeId: string;
    synced: boolean;
};
export type Entity = {
    id: string;
    type: string;
    data: Record<string, unknown>;
    hlc: HLC;
    tombstone: boolean;
};
export type Change = {
    id: string;
    tableName: string;
    recordId: string;
    operation: string;
    timestamp: number;
    deviceId: string;
    synced: number;
    payload?: Record<string, unknown> | null;
};
export type ChangeFilter = 'all' | 'synced' | 'unsynced';
export type QueueItemStatus = 'queued' | 'sending' | 'sent' | 'failed';
/**
 * In-memory representation of one op's journey through the sync queue.
 * The durable store is SQLite (oplog); this adds observable lifecycle state.
 */
export type QueueItem = {
    opId: string;
    entityType: string;
    entityId: string;
    operation: string;
    queuedAt: number;
    attempts: number;
    lastAttempt: number | null;
    lastError: string | null;
    status: QueueItemStatus;
};
/** A snapshot of one side of a conflict (local or remote). */
export type ConflictVersion = {
    hlc: HLC;
    data: Record<string, unknown> | null;
    deviceId: string;
};
/**
 * Logged every time a concurrent write is detected during sync.
 *
 * winner values:
 *   'local'   — LWW kept local version (remote was older)
 *   'remote'  — LWW kept remote version (remote was newer)
 *   'merged'  — CRDT field-level merge was applied (both sides partially kept)
 *   'manual'  — user applied a hand-crafted resolution from the UI
 *
 * mergedVersion is populated when winner = 'merged' | 'manual'.
 */
export type Conflict = {
    id: string;
    recordId: string;
    tableName: string;
    localVersion: ConflictVersion;
    remoteVersion: ConflictVersion;
    mergedVersion?: ConflictVersion;
    winner: 'local' | 'remote' | 'merged' | 'manual';
    detectedAt: number;
    resolved: boolean;
};
/**
 * State machine for the sync cycle:
 *
 *   idle ──── timer / forceSync ────► syncing
 *             ◄── success ──────────    │
 *             ◄── network down ────── offline
 *             ◄── retryTimer ──────── retrying ◄── all-HTTP-retries-failed ──┘
 *
 *   pause() always → paused
 *   resume() → idle
 */
export type SyncState = 'idle' | 'syncing' | 'retrying' | 'offline' | 'paused';
export type SyncAttempt = {
    id: number;
    startedAt: number;
    completedAt: number | null;
    durationMs: number | null;
    sent: number;
    received: number;
    success: boolean;
    error: string | null;
};
export type SyncError = {
    at: number;
    message: string;
    attempt: number;
    pendingOps: number;
};
export type SyncStatus = {
    nodeId: string;
    connected: boolean;
    pendingOps: number;
    lastSyncAt: number | null;
    retryCount: number;
    syncing: boolean;
    syncState: SyncState;
    nextRetryAt: number | null;
};
export interface StorageAdapter {
    getEntity(type: string, id: string): Entity | undefined;
    upsertEntity(entity: Entity): void;
    listEntities(type: string): Entity[];
    commitWrite(op: Operation, entity: Entity | null): void;
    appendOp(op: Operation): void;
    getPendingOps(): Operation[];
    markOpSynced(id: string): void;
    getOpById(id: string): Operation | undefined;
    listOps(limit?: number): Operation[];
    listAllOps(): Operation[];
    getChanges(filter: ChangeFilter, limit?: number): Change[];
    markChangeSynced(id: string): void;
    markChangesSynced(ids: string[]): void;
    logConflict(conflict: Conflict): void;
    getConflicts(resolvedFilter?: boolean | null, limit?: number): Conflict[];
    markConflictResolved(id: string): void;
    getUnresolvedConflictCount(): number;
    eraseEntityStore(type: string, id: string): void;
    getLastSeenSequence(): number;
    setLastSeenSequence(seq: number): void;
}
export interface NetworkAdapter {
    push(ops: Operation[]): Promise<PushResult>;
    pull(since: number): Promise<PullResult>;
    isAvailable(): Promise<boolean>;
    sync?(ops: Operation[]): Promise<SyncResult>;
}
export type SyncResult = {
    accepted: string[];
    remoteOps: Operation[];
};
export type PushResult = {
    accepted: string[];
    rejected: string[];
};
export type PullResult = {
    ops: Operation[];
    sequence: number;
};
//# sourceMappingURL=types.d.ts.map