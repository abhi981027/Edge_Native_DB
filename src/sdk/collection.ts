import { SyncEngine } from '../core/engine';
import { Entity } from '../core/types';

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
export class Collection<T extends { id: string }> {
  private readonly subs = new Set<(items: T[]) => void>();
  private lastStatusKey = '';

  constructor(
    private readonly type: string,
    private readonly engine: SyncEngine,
  ) {
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
  upsert(item: T): string {
    const { id, ...data } = item;
    const opId = this.engine.write(this.type, id, data as Record<string, unknown>);
    this.notify();
    return opId;
  }

  /** Soft-delete an entity (tombstone). Returns the op ID. */
  remove(id: string): string {
    const opId = this.engine.remove(this.type, id);
    this.notify();
    return opId;
  }

  // ─── Read ───────────────────────────────────────────────────────────────────

  /** Get a single entity by ID. Returns undefined if not found or tombstoned. */
  get(id: string): T | undefined {
    const entity = this.engine.get(this.type, id);
    if (!entity || entity.tombstone) return undefined;
    return this.toT(entity);
  }

  /** List all live (non-tombstoned) entities of this type. */
  all(): T[] {
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
  subscribe(fn: (items: T[]) => void): () => void {
    this.subs.add(fn);
    fn(this.all());
    return () => this.subs.delete(fn);
  }

  // ─── Private ────────────────────────────────────────────────────────────────

  private toT(e: Entity): T {
    return { id: e.id, ...e.data } as unknown as T;
  }

  private notify(): void {
    if (this.subs.size === 0) return;
    const items = this.all();
    this.subs.forEach(fn => fn(items));
  }
}
