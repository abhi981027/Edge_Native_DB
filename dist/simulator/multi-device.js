"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiDeviceSimulator = void 0;
const engine_1 = require("../core/engine");
const storage_1 = require("../core/storage");
const http_adapter_1 = require("../network/http-adapter");
const adapter_1 = require("../network/adapter");
const users_1 = require("../core/users");
class MultiDeviceSimulator {
    upstreamUrl;
    slots = new Map();
    eventLog = [];
    seq = 0;
    MAX_EVENTS = 300;
    subscribers = new Set();
    pollTimer = null;
    lastSyncId = new Map();
    lastConflictCount = new Map();
    constructor(deviceIds, upstreamUrl) {
        this.upstreamUrl = upstreamUrl;
        for (const id of deviceIds) {
            const storage = new storage_1.SQLiteStorage(':memory:');
            const http = new http_adapter_1.HttpNetworkAdapter(upstreamUrl, id);
            const sim = new adapter_1.OfflineSimulator(http);
            const engine = new engine_1.SyncEngine(id, storage, sim);
            const users = new users_1.UserRepository(engine);
            this.slots.set(id, { engine, simulator: sim, users });
        }
    }
    // ─── Lifecycle ─────────────────────────────────────────────────────────────
    start() {
        for (const s of this.slots.values())
            s.engine.start();
        this.pollTimer = setInterval(() => this.poll(), 600);
    }
    stop() {
        for (const s of this.slots.values())
            s.engine.stop();
        if (this.pollTimer) {
            clearInterval(this.pollTimer);
            this.pollTimer = null;
        }
    }
    // ─── Device control ────────────────────────────────────────────────────────
    deviceIds() { return [...this.slots.keys()]; }
    getEngine(id) {
        return this.slots.get(id)?.engine;
    }
    setOffline(deviceId, offline) {
        const s = this.slots.get(deviceId);
        if (!s)
            return;
        s.simulator.setOffline(offline);
        this.emit({ deviceId, kind: offline ? 'offline' : 'online',
            detail: offline ? 'went offline' : 'reconnected' });
    }
    setLatency(deviceId, ms) {
        this.slots.get(deviceId)?.simulator.setLatency(ms);
    }
    isOffline(deviceId) {
        return this.slots.get(deviceId)?.simulator.isSimulatingOffline() ?? false;
    }
    getLatency(deviceId) {
        return this.slots.get(deviceId)?.simulator.getLatency() ?? 0;
    }
    // ─── Writes ────────────────────────────────────────────────────────────────
    writeUser(deviceId, userId, name) {
        const s = this.slots.get(deviceId);
        if (!s)
            return;
        s.users.createUser({ id: userId, name, updated_at: Date.now(), device_id: deviceId });
        this.emit({ deviceId, kind: 'write', detail: `write ${userId} → "${name}"` });
    }
    updateUser(deviceId, userId, name) {
        const s = this.slots.get(deviceId);
        if (!s)
            return;
        s.users.updateUser(userId, { name, updated_at: Date.now() });
        this.emit({ deviceId, kind: 'write', detail: `update ${userId} → "${name}"` });
    }
    // ─── Sync ──────────────────────────────────────────────────────────────────
    async forceSync(deviceId) {
        await this.slots.get(deviceId)?.engine.forceSync();
    }
    async syncAll() {
        await Promise.all([...this.slots.values()].map(s => s.engine.forceSync()));
    }
    // ─── Conflict resolution ───────────────────────────────────────────────────
    autoMergeAll(deviceId) {
        const engine = this.slots.get(deviceId)?.engine;
        if (!engine)
            return 0;
        const unresolved = engine.getConflicts(false, 100);
        let merged = 0;
        for (const c of unresolved) {
            if (engine.applyAutoMerge(c.id).ok)
                merged++;
        }
        if (merged > 0)
            this.emit({ deviceId, kind: 'merge', detail: `auto-merged ${merged} conflict(s)` });
        return merged;
    }
    // ─── Snapshots ─────────────────────────────────────────────────────────────
    getSnapshot(deviceId) {
        const s = this.slots.get(deviceId);
        if (!s)
            return null;
        return {
            id: deviceId,
            status: s.engine.getStatus(),
            users: s.users.getAllUsers(),
            queue: s.engine.getSyncQueue(),
            offline: s.simulator.isSimulatingOffline(),
            latencyMs: s.simulator.getLatency(),
            unresolvedConflicts: s.engine.getUnresolvedConflictCount(),
            syncLog: s.engine.getSyncLog().slice(0, 5),
        };
    }
    getAllSnapshots() {
        return this.deviceIds().map(id => this.getSnapshot(id));
    }
    getConflicts(deviceId) {
        return this.slots.get(deviceId)?.engine.getConflicts(null, 50) ?? [];
    }
    getAllConflicts() {
        return this.deviceIds().flatMap(id => this.getConflicts(id).map(c => ({ ...c, deviceId: id })));
    }
    // ─── Events ────────────────────────────────────────────────────────────────
    getEvents(limit = 80) { return this.eventLog.slice(0, limit); }
    onEvent(fn) {
        this.subscribers.add(fn);
        return () => this.subscribers.delete(fn);
    }
    // ─── Seed ──────────────────────────────────────────────────────────────────
    async seedAll(users) {
        // Seed only one device first, then sync-all so the server has the data,
        // then sync all other devices. This avoids three "insert" conflicts.
        const [first, ...rest] = this.deviceIds();
        if (!first)
            return;
        const s = this.slots.get(first);
        for (const u of users) {
            s.users.createUser({ id: u.id, name: u.name, updated_at: Date.now(), device_id: first });
        }
        await s.engine.forceSync(); // push to server
        await Promise.all(rest.map(id => this.slots.get(id).engine.forceSync())); // pull
    }
    resetAll() {
        for (const [id, s] of this.slots) {
            s.simulator.setOffline(false);
            s.simulator.setLatency(0);
        }
        this.emit({ deviceId: 'all', kind: 'online', detail: 'simulation reset' });
    }
    // ─── Private ───────────────────────────────────────────────────────────────
    poll() {
        for (const [id, s] of this.slots) {
            // Detect new sync attempts via sync log
            const log = s.engine.getSyncLog();
            const lastId = this.lastSyncId.get(id) ?? 0;
            if (log.length && log[0].id > lastId) {
                const fresh = log.filter(a => a.id > lastId).reverse();
                this.lastSyncId.set(id, log[0].id);
                for (const a of fresh) {
                    this.emit({
                        deviceId: id,
                        kind: a.success ? 'sync_ok' : 'sync_fail',
                        detail: a.success
                            ? `↑ ${a.sent} · ↓ ${a.received}`
                            : (a.error ?? 'error').slice(0, 80),
                        sent: a.sent,
                        received: a.received,
                    });
                }
            }
            // Detect new conflicts
            const cc = s.engine.getUnresolvedConflictCount();
            const prev = this.lastConflictCount.get(id) ?? 0;
            if (cc > prev)
                this.emit({ deviceId: id, kind: 'conflict', detail: `⚔ ${cc - prev} new conflict(s)` });
            this.lastConflictCount.set(id, cc);
        }
    }
    emit(data) {
        const e = { id: ++this.seq, at: Date.now(), ...data };
        this.eventLog.unshift(e);
        if (this.eventLog.length > this.MAX_EVENTS)
            this.eventLog.pop();
        this.subscribers.forEach(fn => fn(e));
    }
}
exports.MultiDeviceSimulator = MultiDeviceSimulator;
//# sourceMappingURL=multi-device.js.map