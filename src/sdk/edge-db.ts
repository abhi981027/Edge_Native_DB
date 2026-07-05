import { v4 as uuidv4 } from 'uuid';
import { SyncEngine } from '../core/engine';
import { SQLiteStorage } from '../core/storage';
import { HttpNetworkAdapter } from '../network/http-adapter';
import { OfflineSimulator } from '../network/adapter';
import { MergeSchema } from '../core/merge-registry';
import {
  Conflict,
  QueueItem,
  SyncAttempt,
  SyncError,
  SyncState,
  SyncStatus,
} from '../core/types';
import { Collection } from './collection';
import { Logger, globalLogger } from './logger';

// ─── Public configuration ────────────────────────────────────────────────────

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

// ─── Main class ───────────────────────────────────────────────────────────────

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
export class EdgeDB {
  readonly nodeId: string;
  readonly log:    Logger;

  /** Sync lifecycle and status. */
  readonly sync: SyncController;

  /** Read-only introspection into queue, log, conflicts, errors. */
  readonly inspect: Inspector;

  private readonly engine:     SyncEngine;
  private readonly simulator:  OfflineSimulator;
  private readonly collections = new Map<string, Collection<any>>();

  constructor(config: EdgeDBConfig) {
    this.nodeId = config.nodeId ?? `edge-${uuidv4().slice(0, 8)}`;
    this.log    = config.logger ?? globalLogger;

    const storage = new SQLiteStorage(config.dbPath ?? './edge.db');
    const http    = new HttpNetworkAdapter(config.upstreamUrl, this.nodeId);
    const sim     = new OfflineSimulator(http);
    this.engine   = new SyncEngine(this.nodeId, storage, sim);
    this.simulator = sim;

    if (config.mergeSchemas) {
      for (const [type, schema] of Object.entries(config.mergeSchemas)) {
        SyncEngine.registerMergeSchema(type, schema);
        this.log.info('sdk', `CRDT schema registered: ${type}`);
      }
    }

    this.sync    = new SyncController(this.engine, sim, this.log);
    this.inspect = new Inspector(this.engine);

    // Wire engine events into logger
    this.engine.onStatusChange(s => {
      if (s.syncState === 'syncing')
        this.log.info('sync', 'Sync cycle started', { pending: s.pendingOps });
      else if (s.syncState === 'idle' && s.lastSyncAt != null)
        this.log.info('sync', 'Sync completed', { lastSyncAt: s.lastSyncAt });
      else if (s.syncState === 'retrying')
        this.log.warn('sync', 'Sync failed — retrying', {
          retryCount: s.retryCount,
          nextRetryAt: s.nextRetryAt,
        });
      else if (s.syncState === 'offline')
        this.log.warn('network', 'Device offline — writes will queue locally');
    });
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  /** Open the DB and start the background sync loop. */
  connect(): void {
    this.engine.start();
    this.log.info('sdk', `EdgeDB connected — nodeId: ${this.nodeId}`);
  }

  /** Stop sync loop and release timers. Safe to call multiple times. */
  disconnect(): void {
    this.engine.stop();
    this.log.info('sdk', 'EdgeDB disconnected');
  }

  // ─── Collections ───────────────────────────────────────────────────────────

  /**
   * Get a typed collection for an entity type. Collections are singletons per
   * type — calling collection('tasks') twice returns the same instance.
   *
   * T must extend `{ id: string }`. All other fields become the entity payload.
   */
  collection<T extends { id: string }>(type: string): Collection<T> {
    if (!this.collections.has(type)) {
      this.collections.set(type, new Collection<T>(type, this.engine));
      this.log.debug('sdk', `Collection created: "${type}"`);
    }
    return this.collections.get(type) as Collection<T>;
  }

  // ─── Status ─────────────────────────────────────────────────────────────────

  /** Subscribe to SyncStatus changes. Returns unsubscribe function. */
  onStatusChange(fn: (s: SyncStatus) => void): () => void {
    return this.engine.onStatusChange(fn);
  }
}

// ─── SyncController ───────────────────────────────────────────────────────────

export class SyncController {
  constructor(
    private readonly engine:    SyncEngine,
    private readonly simulator: OfflineSimulator,
    private readonly log:       Logger,
  ) {}

  get status():      SyncStatus      { return this.engine.getStatus(); }
  get state():       SyncState       { return this.engine.getSyncState(); }
  get nextRetryAt(): number | null   { return this.engine.getNextRetryAt(); }

  async force(): Promise<void> {
    this.log.info('sync', 'Force-sync triggered');
    await this.engine.forceSync();
  }

  pause():  void { this.engine.pause();  this.log.info('sync', 'Sync paused'); }
  resume(): void { this.engine.resume(); this.log.info('sync', 'Sync resumed'); }

  /** Simulate an offline network condition — ops accumulate in the queue. */
  setOffline(offline: boolean): void {
    this.simulator.setOffline(offline);
    this.log.info('network', offline ? 'Simulated offline' : 'Simulated online');
  }
  isOffline(): boolean { return this.simulator.isSimulatingOffline(); }

  /** Inject artificial latency in milliseconds. */
  setLatency(ms: number): void { this.simulator.setLatency(ms); }
  getLatency(): number          { return this.simulator.getLatency(); }
}

// ─── Inspector ────────────────────────────────────────────────────────────────

export class Inspector {
  constructor(private readonly engine: SyncEngine) {}

  queue():   QueueItem[]   { return this.engine.getSyncQueue(); }
  syncLog(): SyncAttempt[] { return this.engine.getSyncLog(); }
  errors():  SyncError[]   { return this.engine.getSyncErrors(); }

  conflicts(resolved?: boolean | null): Conflict[] {
    return this.engine.getConflicts(resolved ?? null);
  }
  unresolvedConflicts(): number { return this.engine.getUnresolvedConflictCount(); }

  previewMerge(conflictId: string) { return this.engine.previewMerge(conflictId); }
  applyAutoMerge(conflictId: string) { return this.engine.applyAutoMerge(conflictId); }
  applyManualMerge(conflictId: string, data: Record<string, unknown>) {
    return this.engine.applyManualMerge(conflictId, data);
  }
}
