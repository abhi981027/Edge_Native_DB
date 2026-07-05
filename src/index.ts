import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { SyncEngine } from './core/engine';
import { SQLiteStorage } from './core/storage';
import { UserRepository } from './core/users';
import { HttpNetworkAdapter } from './network/http-adapter';
import { OfflineSimulator } from './network/adapter';
import { createSyncServer } from './server/sync-server';
import { createUIServer } from './server/ui-server';
import { MultiDeviceSimulator } from './simulator/multi-device';
import { globalLogger } from './sdk/logger';

const NODE_ID       = process.env['NODE_ID']        ?? `node-${uuidv4().slice(0, 8)}`;
const DB_PATH       = process.env['DB_PATH']         ?? path.join(process.cwd(), 'edge-data.db');
const UPSTREAM_PORT = Number(process.env['UPSTREAM_PORT'] ?? 3001);
const UI_PORT       = Number(process.env['UI_PORT']       ?? 3000);

console.log('╔══════════════════════════════════════════╗');
console.log('║        EDGE-NATIVE SYNC ENGINE           ║');
console.log('╚══════════════════════════════════════════╝');
console.log(`  node id   : ${NODE_ID}`);
console.log(`  db        : ${DB_PATH}`);
console.log(`  edge ui   : http://localhost:${UI_PORT}`);
console.log(`  sync srv  : http://localhost:${UPSTREAM_PORT}`);
console.log('');

// ── 1. Sync server (replaces mock-server) ───────────────────────────────────
const syncServer = createSyncServer(UPSTREAM_PORT);

// ── 2. Local edge storage ───────────────────────────────────────────────────
const storage = new SQLiteStorage(DB_PATH);

// ── 3. Network stack (POST /sync — one round-trip per cycle) ────────────────
// deviceId is passed so the server can route changes back to OTHER devices only
const UPSTREAM_URL = `http://localhost:${UPSTREAM_PORT}`;
const httpAdapter  = new HttpNetworkAdapter(UPSTREAM_URL, NODE_ID);
const simulator    = new OfflineSimulator(httpAdapter);

// ── 4. Sync engine ──────────────────────────────────────────────────────────
const engine = new SyncEngine(NODE_ID, storage, simulator);

// Register CRDT merge schemas: fields listed here use their strategy instead
// of LWW when a concurrent write conflict is detected during sync.
SyncEngine.registerMergeSchema('users', {
  name:       'text-merge', // "Alice | Bob" when both devices write name concurrently
  updated_at: 'max',        // keep the later timestamp
  device_id:  'lww',        // LWW — last writer's device ID
});

// Wire engine events into the global logger so devtools can surface them
engine.onStatusChange(s => {
  if (s.syncState === 'syncing')
    globalLogger.info('sync', 'Sync cycle started', { pending: s.pendingOps });
  else if (s.syncState === 'idle' && s.lastSyncAt != null)
    globalLogger.info('sync', 'Sync completed', { lastSyncAt: s.lastSyncAt });
  else if (s.syncState === 'retrying')
    globalLogger.warn('sync', 'Sync failed — retrying', { retryCount: s.retryCount });
  else if (s.syncState === 'offline')
    globalLogger.warn('network', 'Device offline');
});

engine.start();

// ── 5. Domain repositories ──────────────────────────────────────────────────
const users = new UserRepository(engine);

// ── 6. Multi-device simulator ───────────────────────────────────────────────
const multiSim = new MultiDeviceSimulator(
  ['device-alpha', 'device-beta', 'device-gamma'],
  UPSTREAM_URL,
);
multiSim.start();

// ── 7. Edge UI server ───────────────────────────────────────────────────────
createUIServer(UI_PORT, engine, simulator, httpAdapter, users, UPSTREAM_URL, multiSim, globalLogger);
console.log(`  devtools   : http://localhost:${UI_PORT}/devtools`);
console.log(`  product    : http://localhost:${UI_PORT}/product`);

// ── Seed demo data on first run ─────────────────────────────────────────────
if (storage.listEntities('users').length === 0) {
  users.createUser({ id: 'user-001', name: 'Alice',   updated_at: Date.now(), device_id: NODE_ID });
  users.createUser({ id: 'user-002', name: 'Bob',     updated_at: Date.now(), device_id: NODE_ID });
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
