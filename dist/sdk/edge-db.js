"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Inspector = exports.SyncController = exports.EdgeDB = void 0;
const uuid_1 = require("uuid");
const engine_1 = require("../core/engine");
const storage_1 = require("../core/storage");
const http_adapter_1 = require("../network/http-adapter");
const adapter_1 = require("../network/adapter");
const collection_1 = require("./collection");
const logger_1 = require("./logger");
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
class EdgeDB {
    nodeId;
    log;
    /** Sync lifecycle and status. */
    sync;
    /** Read-only introspection into queue, log, conflicts, errors. */
    inspect;
    engine;
    simulator;
    collections = new Map();
    constructor(config) {
        this.nodeId = config.nodeId ?? `edge-${(0, uuid_1.v4)().slice(0, 8)}`;
        this.log = config.logger ?? logger_1.globalLogger;
        const storage = new storage_1.SQLiteStorage(config.dbPath ?? './edge.db');
        const http = new http_adapter_1.HttpNetworkAdapter(config.upstreamUrl, this.nodeId);
        const sim = new adapter_1.OfflineSimulator(http);
        this.engine = new engine_1.SyncEngine(this.nodeId, storage, sim);
        this.simulator = sim;
        if (config.mergeSchemas) {
            for (const [type, schema] of Object.entries(config.mergeSchemas)) {
                engine_1.SyncEngine.registerMergeSchema(type, schema);
                this.log.info('sdk', `CRDT schema registered: ${type}`);
            }
        }
        this.sync = new SyncController(this.engine, sim, this.log);
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
    connect() {
        this.engine.start();
        this.log.info('sdk', `EdgeDB connected — nodeId: ${this.nodeId}`);
    }
    /** Stop sync loop and release timers. Safe to call multiple times. */
    disconnect() {
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
    collection(type) {
        if (!this.collections.has(type)) {
            this.collections.set(type, new collection_1.Collection(type, this.engine));
            this.log.debug('sdk', `Collection created: "${type}"`);
        }
        return this.collections.get(type);
    }
    // ─── Status ─────────────────────────────────────────────────────────────────
    /** Subscribe to SyncStatus changes. Returns unsubscribe function. */
    onStatusChange(fn) {
        return this.engine.onStatusChange(fn);
    }
}
exports.EdgeDB = EdgeDB;
// ─── SyncController ───────────────────────────────────────────────────────────
class SyncController {
    engine;
    simulator;
    log;
    constructor(engine, simulator, log) {
        this.engine = engine;
        this.simulator = simulator;
        this.log = log;
    }
    get status() { return this.engine.getStatus(); }
    get state() { return this.engine.getSyncState(); }
    get nextRetryAt() { return this.engine.getNextRetryAt(); }
    async force() {
        this.log.info('sync', 'Force-sync triggered');
        await this.engine.forceSync();
    }
    pause() { this.engine.pause(); this.log.info('sync', 'Sync paused'); }
    resume() { this.engine.resume(); this.log.info('sync', 'Sync resumed'); }
    /** Simulate an offline network condition — ops accumulate in the queue. */
    setOffline(offline) {
        this.simulator.setOffline(offline);
        this.log.info('network', offline ? 'Simulated offline' : 'Simulated online');
    }
    isOffline() { return this.simulator.isSimulatingOffline(); }
    /** Inject artificial latency in milliseconds. */
    setLatency(ms) { this.simulator.setLatency(ms); }
    getLatency() { return this.simulator.getLatency(); }
}
exports.SyncController = SyncController;
// ─── Inspector ────────────────────────────────────────────────────────────────
class Inspector {
    engine;
    constructor(engine) {
        this.engine = engine;
    }
    queue() { return this.engine.getSyncQueue(); }
    syncLog() { return this.engine.getSyncLog(); }
    errors() { return this.engine.getSyncErrors(); }
    conflicts(resolved) {
        return this.engine.getConflicts(resolved ?? null);
    }
    unresolvedConflicts() { return this.engine.getUnresolvedConflictCount(); }
    previewMerge(conflictId) { return this.engine.previewMerge(conflictId); }
    applyAutoMerge(conflictId) { return this.engine.applyAutoMerge(conflictId); }
    applyManualMerge(conflictId, data) {
        return this.engine.applyManualMerge(conflictId, data);
    }
}
exports.Inspector = Inspector;
//# sourceMappingURL=edge-db.js.map