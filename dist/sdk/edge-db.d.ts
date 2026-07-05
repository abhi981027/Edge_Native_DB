import { SyncEngine } from '../core/engine';
import { OfflineSimulator } from '../network/adapter';
import { MergeSchema } from '../core/merge-registry';
import { Conflict, QueueItem, SyncAttempt, SyncError, SyncState, SyncStatus } from '../core/types';
import { Collection } from './collection';
import { Logger } from './logger';
export interface EdgeDBConfig {
    /**
     * Stable device identifier. Reuse the same ID across restarts so the HLC
     * remains monotonic and the server's per-device cursor is preserved.
     * Defaults to a random ID (fine for demos, not for production restarts).
     */
    nodeId?: string;
    /**
     * SQLite file path. Use ':memory:' for tests.
     * Default: './edge.db'
     */
    dbPath?: string;
    /** Full URL of the sync server POST /sync endpoint */
    upstreamUrl: string;
    /**
     * CRDT merge strategies, keyed by entity type.
     * Fields not listed fall back to Last-Write-Wins.
     *
     * @example
     * mergeSchemas: {
     *   tasks: { title: 'text-merge', done: 'max', updated_at: 'max' }
     * }
     */
    mergeSchemas?: Record<string, MergeSchema>;
    /**
     * Custom logger. Defaults to the shared in-process circular-buffer logger.
     * Pass your own Logger instance to scope log capture to this EdgeDB instance.
     */
    logger?: Logger;
}
/**
 * EdgeDB — the top-level SDK entry point.
 *
 * Wraps the sync engine, SQLite storage, and network adapter behind a
 * developer-friendly, typed API.
 *
 * @example
 * const db = new EdgeDB({
 *   nodeId: 'field-unit-42',
 *   dbPath: './data/field.db',
 *   upstreamUrl: 'https://sync.example.com',
 *   mergeSchemas: { readings: { value: 'max', unit: 'lww' } },
 * });
 *
 * db.connect();
 * const readings = db.collection<Reading>('readings');
 * readings.upsert({ id: 'r1', value: 42.1, unit: 'C', ts: Date.now() });
 *
 * const unsub = readings.subscribe(all => console.log('readings:', all.length));
 */
export declare class EdgeDB {
    readonly nodeId: string;
    readonly log: Logger;
    /** Sync lifecycle and status. */
    readonly sync: SyncController;
    /** Read-only introspection into queue, log, conflicts, errors. */
    readonly inspect: Inspector;
    private readonly engine;
    private readonly simulator;
    private readonly collections;
    constructor(config: EdgeDBConfig);
    /** Open the DB and start the background sync loop. */
    connect(): void;
    /** Stop sync loop and release timers. Safe to call multiple times. */
    disconnect(): void;
    /**
     * Get a typed collection for an entity type. Collections are singletons per
     * type — calling collection('tasks') twice returns the same instance.
     *
     * T must extend `{ id: string }`. All other fields become the entity payload.
     */
    collection<T extends {
        id: string;
    }>(type: string): Collection<T>;
    /** Subscribe to SyncStatus changes. Returns unsubscribe function. */
    onStatusChange(fn: (s: SyncStatus) => void): () => void;
}
export declare class SyncController {
    private readonly engine;
    private readonly simulator;
    private readonly log;
    constructor(engine: SyncEngine, simulator: OfflineSimulator, log: Logger);
    get status(): SyncStatus;
    get state(): SyncState;
    get nextRetryAt(): number | null;
    force(): Promise<void>;
    pause(): void;
    resume(): void;
    /** Simulate an offline network condition — ops accumulate in the queue. */
    setOffline(offline: boolean): void;
    isOffline(): boolean;
    /** Inject artificial latency in milliseconds. */
    setLatency(ms: number): void;
    getLatency(): number;
}
export declare class Inspector {
    private readonly engine;
    constructor(engine: SyncEngine);
    queue(): QueueItem[];
    syncLog(): SyncAttempt[];
    errors(): SyncError[];
    conflicts(resolved?: boolean | null): Conflict[];
    unresolvedConflicts(): number;
    previewMerge(conflictId: string): {
        conflictId: string;
        merged: Record<string, unknown>;
        fieldDecisions: import("../core/crdt-merger").FieldDecisions;
        schema: MergeSchema;
        remoteIsNewer: boolean;
    } | null;
    applyAutoMerge(conflictId: string): {
        ok: boolean;
        merged?: Record<string, unknown>;
    };
    applyManualMerge(conflictId: string, data: Record<string, unknown>): {
        ok: boolean;
    };
}
//# sourceMappingURL=edge-db.d.ts.map