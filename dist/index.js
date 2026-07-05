"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = __importDefault(require("path"));
const uuid_1 = require("uuid");
const engine_1 = require("./core/engine");
const storage_1 = require("./core/storage");
const users_1 = require("./core/users");
const http_adapter_1 = require("./network/http-adapter");
const adapter_1 = require("./network/adapter");
const sync_server_1 = require("./server/sync-server");
const ui_server_1 = require("./server/ui-server");
const multi_device_1 = require("./simulator/multi-device");
const logger_1 = require("./sdk/logger");
const NODE_ID = process.env['NODE_ID'] ?? `node-${(0, uuid_1.v4)().slice(0, 8)}`;
const DB_PATH = process.env['DB_PATH'] ?? path_1.default.join(process.cwd(), 'edge-data.db');
const UPSTREAM_PORT = Number(process.env['UPSTREAM_PORT'] ?? 3001);
const UI_PORT = Number(process.env['UI_PORT'] ?? 3000);
console.log('╔══════════════════════════════════════════╗');
console.log('║        EDGE-NATIVE SYNC ENGINE           ║');
console.log('╚══════════════════════════════════════════╝');
console.log(`  node id   : ${NODE_ID}`);
console.log(`  db        : ${DB_PATH}`);
console.log(`  edge ui   : http://localhost:${UI_PORT}`);
console.log(`  sync srv  : http://localhost:${UPSTREAM_PORT}`);
console.log('');
// ── 1. Sync server (replaces mock-server) ───────────────────────────────────
const syncServer = (0, sync_server_1.createSyncServer)(UPSTREAM_PORT);
// ── 2. Local edge storage ───────────────────────────────────────────────────
const storage = new storage_1.SQLiteStorage(DB_PATH);
// ── 3. Network stack (POST /sync — one round-trip per cycle) ────────────────
// deviceId is passed so the server can route changes back to OTHER devices only
const UPSTREAM_URL = `http://localhost:${UPSTREAM_PORT}`;
const httpAdapter = new http_adapter_1.HttpNetworkAdapter(UPSTREAM_URL, NODE_ID);
const simulator = new adapter_1.OfflineSimulator(httpAdapter);
// ── 4. Sync engine ──────────────────────────────────────────────────────────
const engine = new engine_1.SyncEngine(NODE_ID, storage, simulator);
// Register CRDT merge schemas: fields listed here use their strategy instead
// of LWW when a concurrent write conflict is detected during sync.
engine_1.SyncEngine.registerMergeSchema('users', {
    name: 'text-merge', // "Alice | Bob" when both devices write name concurrently
    updated_at: 'max', // keep the later timestamp
    device_id: 'lww', // LWW — last writer's device ID
});
// Wire engine events into the global logger so devtools can surface them
engine.onStatusChange(s => {
    if (s.syncState === 'syncing')
        logger_1.globalLogger.info('sync', 'Sync cycle started', { pending: s.pendingOps });
    else if (s.syncState === 'idle' && s.lastSyncAt != null)
        logger_1.globalLogger.info('sync', 'Sync completed', { lastSyncAt: s.lastSyncAt });
    else if (s.syncState === 'retrying')
        logger_1.globalLogger.warn('sync', 'Sync failed — retrying', { retryCount: s.retryCount });
    else if (s.syncState === 'offline')
        logger_1.globalLogger.warn('network', 'Device offline');
});
engine.start();
// ── 5. Domain repositories ──────────────────────────────────────────────────
const users = new users_1.UserRepository(engine);
// ── 6. Multi-device simulator ───────────────────────────────────────────────
const multiSim = new multi_device_1.MultiDeviceSimulator(['device-alpha', 'device-beta', 'device-gamma'], UPSTREAM_URL);
multiSim.start();
// ── 7. Edge UI server ───────────────────────────────────────────────────────
(0, ui_server_1.createUIServer)(UI_PORT, engine, simulator, httpAdapter, users, UPSTREAM_URL, multiSim, logger_1.globalLogger);
console.log(`  devtools   : http://localhost:${UI_PORT}/devtools`);
console.log(`  product    : http://localhost:${UI_PORT}/product`);
// ── Seed demo data on first run ─────────────────────────────────────────────
if (storage.listEntities('users').length === 0) {
    users.createUser({ id: 'user-001', name: 'Alice', updated_at: Date.now(), device_id: NODE_ID });
    users.createUser({ id: 'user-002', name: 'Bob', updated_at: Date.now(), device_id: NODE_ID });
    users.createUser({ id: 'user-003', name: 'Charlie', updated_at: Date.now(), device_id: NODE_ID });
    console.log('  [seed] created 3 demo users');
}
process.on('SIGINT', () => {
    console.log('\n[shutdown] stopping…');
    engine.stop();
    multiSim.stop();
    syncServer.close();
    process.exit(0);
});
//# sourceMappingURL=index.js.map