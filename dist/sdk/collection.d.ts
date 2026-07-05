import { SyncEngine } from '../core/engine';
/**
 * Collection<T> — typed, reactive access to one entity type.
 *
 * Wraps SyncEngine's generic read/write methods with:
 *  - A TypeScript generic so callers get full type safety
 *  - A subscribe() method that fires immediately with the current snapshot,
 *    then on every local write OR incoming sync that could change entity state
 *
 * Usage:
 *   const tasks = db.collection<Task>('tasks');
 *   tasks.upsert({ id: 't1', title: 'Buy milk', done: false });
 *   const unsub = tasks.subscribe(items => console.log(items));
 */
export declare class Collection<T extends {
    id: string;
}> {
    private readonly type;
    private readonly engine;
    private readonly subs;
    private lastStatusKey;
    constructor(type: string, engine: SyncEngine);
    /** Upsert an entity. Returns the op ID. */
    upsert(item: T): string;
    /** Soft-delete an entity (tombstone). Returns the op ID. */
    remove(id: string): string;
    /** Get a single entity by ID. Returns undefined if not found or tombstoned. */
    get(id: string): T | undefined;
    /** List all live (non-tombstoned) entities of this type. */
    all(): T[];
    /**
     * Subscribe to changes. The callback fires immediately with the current
     * snapshot, then again whenever the list could have changed.
     * Returns an unsubscribe function.
     */
    subscribe(fn: (items: T[]) => void): () => void;
    private toT;
    private notify;
}
//# sourceMappingURL=collection.d.ts.map