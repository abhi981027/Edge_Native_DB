"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Collection = void 0;
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
class Collection {
    type;
    engine;
    subs = new Set();
    lastStatusKey = '';
    constructor(type, engine) {
        this.type = type;
        this.engine = engine;
        engine.onStatusChange(s => {
            // Re-notify subscribers when anything that can change entity state changes:
            // pending op count, last sync time, or sync state machine transition
            const key = `${s.pendingOps}:${s.lastSyncAt}:${s.syncState}`;
            if (key !== this.lastStatusKey) {
                this.lastStatusKey = key;
                this.notify();
            }
        });
    }
    // ─── Write ─────────────────────────────────────────────────────────────────
    /** Upsert an entity. Returns the op ID. */
    upsert(item) {
        const { id, ...data } = item;
        const opId = this.engine.write(this.type, id, data);
        this.notify();
        return opId;
    }
    /** Soft-delete an entity (tombstone). Returns the op ID. */
    remove(id) {
        const opId = this.engine.remove(this.type, id);
        this.notify();
        return opId;
    }
    // ─── Read ───────────────────────────────────────────────────────────────────
    /** Get a single entity by ID. Returns undefined if not found or tombstoned. */
    get(id) {
        const entity = this.engine.get(this.type, id);
        if (!entity || entity.tombstone)
            return undefined;
        return this.toT(entity);
    }
    /** List all live (non-tombstoned) entities of this type. */
    all() {
        return this.engine.list(this.type)
            .filter(e => !e.tombstone)
            .map(e => this.toT(e));
    }
    // ─── Reactive ───────────────────────────────────────────────────────────────
    /**
     * Subscribe to changes. The callback fires immediately with the current
     * snapshot, then again whenever the list could have changed.
     * Returns an unsubscribe function.
     */
    subscribe(fn) {
        this.subs.add(fn);
        fn(this.all());
        return () => this.subs.delete(fn);
    }
    // ─── Private ────────────────────────────────────────────────────────────────
    toT(e) {
        return { id: e.id, ...e.data };
    }
    notify() {
        if (this.subs.size === 0)
            return;
        const items = this.all();
        this.subs.forEach(fn => fn(items));
    }
}
exports.Collection = Collection;
//# sourceMappingURL=collection.js.map