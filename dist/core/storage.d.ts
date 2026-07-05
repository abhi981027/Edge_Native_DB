import { Change, ChangeFilter, Conflict, Entity, Operation, StorageAdapter } from './types';
/**
 * SQLite storage — WAL mode, atomic multi-table writes, idempotent inserts.
 *
 * Write path (happy path):
 *   BEGIN
 *     INSERT OR IGNORE INTO oplog     ← durable op record
 *     INSERT OR IGNORE INTO changes   ← audit / change-tracking log
 *     INSERT … ON CONFLICT DO UPDATE  ← entity store (LWW)
 *   COMMIT
 *
 * If the process dies between BEGIN and COMMIT, SQLite's WAL rolls the
 * partial transaction back on the next open. The DB is always consistent.
 *
 * Idempotency: both oplog and changes use INSERT OR IGNORE keyed on op.id
 * (a UUID). Re-applying the same op is always a safe no-op.
 */
export declare class SQLiteStorage implements StorageAdapter {
    private db;
    constructor(dbPath: string);
    private migrate;
    /**
     * The only correct path for local mutations.
     *
     * Wraps three writes in one transaction:
     *   1. oplog entry   (idempotent via INSERT OR IGNORE)
     *   2. changes entry (idempotent via INSERT OR IGNORE)
     *   3. entity upsert (LWW: skipped if stored HLC ≥ incoming HLC)
     *
     * Pass entity=null for delete ops (tombstone is handled by the caller
     * constructing a tombstoned entity before calling this).
     */
    commitWrite(op: Operation, entity: Entity | null): void;
    getEntity(type: string, id: string): Entity | undefined;
    /** Public upsert — used by pull path (remote ops; not wrapped in commitWrite). */
    upsertEntity(entity: Entity): void;
    listEntities(type: string): Entity[];
    appendOp(op: Operation): void;
    getPendingOps(): Operation[];
    markOpSynced(id: string): void;
    getOpById(id: string): Operation | undefined;
    listOps(limit?: number): Operation[];
    listAllOps(): Operation[];
    getChanges(filter?: ChangeFilter, limit?: number): Change[];
    markChangeSynced(id: string): void;
    markChangesSynced(ids: string[]): void;
    logConflict(conflict: Conflict): void;
    getConflicts(resolvedFilter?: boolean | null, limit?: number): Conflict[];
    markConflictResolved(id: string): void;
    getUnresolvedConflictCount(): number;
    getLastSeenSequence(): number;
    setLastSeenSequence(seq: number): void;
    eraseEntityStore(type: string, id: string): void;
    private runInsertOp;
    private runInsertChange;
    private runUpsertEntity;
    private stmtCache;
    private prepare;
}
//# sourceMappingURL=storage.d.ts.map