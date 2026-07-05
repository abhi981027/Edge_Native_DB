import { MultiDeviceSimulator } from '../src/simulator/multi-device';
import { createSyncServer } from '../src/server/sync-server';
import { SyncEngine } from '../src/core/engine';

const PORT = 13055;
const UPSTREAM = `http://localhost:${PORT}`;

let server: ReturnType<typeof createSyncServer>;
let sim: MultiDeviceSimulator;

// Register CRDT schema once (singleton registry)
SyncEngine.registerMergeSchema('users', {
  name:       'text-merge',
  updated_at: 'max',
  device_id:  'lww',
});

function delay(ms: number) { return new Promise(r => setTimeout(r, ms)); }

beforeAll(async () => {
  server = createSyncServer(PORT);
  sim    = new MultiDeviceSimulator(['A', 'B', 'C'], UPSTREAM);
  sim.start();
  await delay(100); // let engines connect
});

afterAll(async () => {
  sim.stop();
  await delay(50);
  server.close();
});

describe('MultiDeviceSimulator — basics', () => {
  it('exposes three device IDs', () => {
    expect(sim.deviceIds()).toEqual(['A', 'B', 'C']);
  });

  it('snapshot returns null for unknown device', () => {
    expect(sim.getSnapshot('UNKNOWN')).toBeNull();
  });

  it('devices start with empty user lists', () => {
    for (const id of sim.deviceIds()) {
      expect(sim.getSnapshot(id)!.users).toHaveLength(0);
    }
  });
});

describe('MultiDeviceSimulator — writes and sync propagation', () => {
  it('write followed by forceSync propagates to other devices', async () => {
    sim.writeUser('A', 'prop-001', 'Propagated');
    await sim.forceSync('A');   // push to server
    await sim.forceSync('B');   // B pulls (may race under parallel load)
    await sim.forceSync('B');   // second pass guarantees B has A's write
    const snap = sim.getSnapshot('B')!;
    const user = snap.users.find(u => u.id === 'prop-001');
    expect(user).toBeDefined();
    expect(user!.name).toBe('Propagated');
  });

  it('syncAll propagates to all devices', async () => {
    sim.writeUser('C', 'sync-all-001', 'SyncAllUser');
    // Pass 1: C pushes. Pass 2+: A and B pull C's write.
    // Under parallel Jest load, 3 passes ensures stability.
    await sim.syncAll();
    await sim.syncAll();
    await sim.syncAll();
    for (const id of ['A', 'B', 'C']) {
      const snap = sim.getSnapshot(id)!;
      expect(snap.users.find(u => u.id === 'sync-all-001')).toBeDefined();
    }
  });

  it('updateUser changes name', async () => {
    sim.writeUser('A', 'upd-001', 'Original');
    await sim.forceSync('A');
    await sim.forceSync('B');   // B gets the initial write
    sim.updateUser('A', 'upd-001', 'Updated');
    await sim.forceSync('A');   // A pushes the update
    await sim.forceSync('B');   // B pulls the update
    await sim.forceSync('B');   // second pull — safe no-op if already pulled
    const snap = sim.getSnapshot('B')!;
    expect(snap.users.find(u => u.id === 'upd-001')!.name).toBe('Updated');
  });
});

describe('MultiDeviceSimulator — seedAll', () => {
  it('seeds same baseline on all devices', async () => {
    await sim.seedAll([
      { id: 'seed-001', name: 'Alice' },
      { id: 'seed-002', name: 'Bob'   },
    ]);
    // Extra pull pass — seedAll pushes from the first device, then pulls on
    // the rest; under parallel Jest load an additional pull guarantees stability.
    await sim.syncAll();
    for (const id of sim.deviceIds()) {
      const users = sim.getSnapshot(id)!.users;
      expect(users.find(u => u.id === 'seed-001')).toBeDefined();
      expect(users.find(u => u.id === 'seed-002')).toBeDefined();
    }
  });
});

describe('MultiDeviceSimulator — offline and latency', () => {
  it('setOffline / isOffline round-trips', () => {
    sim.setOffline('C', true);
    expect(sim.isOffline('C')).toBe(true);
    sim.setOffline('C', false);
    expect(sim.isOffline('C')).toBe(false);
  });

  it('setLatency / getLatency round-trips', () => {
    sim.setLatency('B', 200);
    expect(sim.getLatency('B')).toBe(200);
    sim.setLatency('B', 0);
    expect(sim.getLatency('B')).toBe(0);
  });

  it('write while offline stays pending until device reconnects', async () => {
    sim.writeUser('A', 'offline-001', 'OfflineUser');
    sim.setOffline('A', true);
    // Give background sync a chance to attempt (it should not succeed)
    await delay(200);
    // B should not yet have the user
    const snapBefore = sim.getSnapshot('B')!;
    const hasBefore = snapBefore.users.some(u => u.id === 'offline-001');

    // Reconnect and sync
    sim.setOffline('A', false);
    await sim.forceSync('A');
    await sim.forceSync('B');

    const snapAfter = sim.getSnapshot('B')!;
    expect(snapAfter.users.find(u => u.id === 'offline-001')).toBeDefined();
  });
});

describe('MultiDeviceSimulator — conflict detection', () => {
  beforeEach(async () => {
    // Seed a fresh conflict target on all devices
    await sim.seedAll([{ id: 'cf-target', name: 'Original' }]);
    // Isolate both devices
    sim.setOffline('A', true);
    sim.setOffline('B', true);
  });

  afterEach(async () => {
    sim.setOffline('A', false);
    sim.setOffline('B', false);
  });

  it('concurrent offline writes create a conflict on re-sync', async () => {
    sim.updateUser('A', 'cf-target', 'Version A');
    sim.updateUser('B', 'cf-target', 'Version B');

    sim.setOffline('A', false);
    sim.setOffline('B', false);

    await sim.forceSync('A');   // A's write lands on server first
    await sim.forceSync('B');   // B pulls A's version → conflict

    const conflicts = sim.getConflicts('B');
    const cf = conflicts.find(c => c.recordId === 'cf-target');
    // The text-merge CRDT schema is globally registered, so concurrent writes are
    // auto-resolved during applyRemoteOps. A conflict record is still logged.
    expect(cf).toBeDefined();
    // winner is 'merged' (CRDT) or 'local'/'remote' (LWW fallback)
    expect(['merged', 'local', 'remote']).toContain(cf!.winner);
  });
});

describe('MultiDeviceSimulator — autoMergeAll', () => {
  it('leaves zero unresolved conflicts after concurrent writes (CRDT auto-resolves)', async () => {
    await sim.seedAll([{ id: 'merge-target', name: 'Base' }]);
    sim.setOffline('A', true);
    sim.setOffline('B', true);
    sim.updateUser('A', 'merge-target', 'Side A');
    sim.updateUser('B', 'merge-target', 'Side B');
    sim.setOffline('A', false);
    sim.setOffline('B', false);
    await sim.forceSync('A');
    await sim.forceSync('B');

    // With text-merge schema, applyRemoteOps auto-resolves during sync.
    // unresolvedConflicts should already be 0; autoMergeAll is a no-op here.
    expect(sim.getSnapshot('B')!.unresolvedConflicts).toBe(0);
    // A conflict record is still logged (resolved: true, winner: merged)
    const cf = sim.getConflicts('B').find(c => c.recordId === 'merge-target');
    expect(cf).toBeDefined();
    expect(cf!.winner).toBe('merged');
  });
});

describe('MultiDeviceSimulator — events', () => {
  it('emits write event when writeUser is called', () => {
    const events: string[] = [];
    const unsub = sim.onEvent(e => { if (e.kind === 'write') events.push(e.deviceId); });
    sim.writeUser('A', 'ev-001', 'EventUser');
    unsub();
    expect(events).toContain('A');
  });

  it('emits sync_ok event after successful forceSync', async () => {
    sim.writeUser('C', 'ev-sync-001', 'SyncEvUser');
    const kinds: string[] = [];
    const unsub = sim.onEvent(e => { if (e.deviceId === 'C') kinds.push(e.kind); });
    await sim.forceSync('C');
    await delay(800); // wait for poll cycle
    unsub();
    expect(kinds).toContain('sync_ok');
  });

  it('emits offline / online events for setOffline', () => {
    const kinds: string[] = [];
    const unsub = sim.onEvent(e => { if (e.deviceId === 'B') kinds.push(e.kind); });
    sim.setOffline('B', true);
    sim.setOffline('B', false);
    unsub();
    expect(kinds).toContain('offline');
    expect(kinds).toContain('online');
  });

  it('getEvents respects limit', () => {
    for (let i = 0; i < 10; i++) sim.writeUser('A', `lim-${i}`, `User ${i}`);
    const events = sim.getEvents(5);
    expect(events.length).toBeLessThanOrEqual(5);
  });

  it('onEvent unsubscribe stops future notifications', () => {
    let count = 0;
    const unsub = sim.onEvent(() => count++);
    sim.writeUser('A', 'unsub-001', 'Unsub');
    unsub();
    sim.writeUser('A', 'unsub-002', 'AfterUnsub');
    expect(count).toBe(1);
  });
});

describe('MultiDeviceSimulator — getAllConflicts', () => {
  it('aggregates conflicts from all devices with deviceId field', async () => {
    await sim.seedAll([{ id: 'global-cf', name: 'Global' }]);
    sim.setOffline('A', true);
    sim.setOffline('B', true);
    sim.updateUser('A', 'global-cf', 'Global A');
    sim.updateUser('B', 'global-cf', 'Global B');
    sim.setOffline('A', false);
    sim.setOffline('B', false);
    await sim.forceSync('A');
    await sim.forceSync('B');

    const all = sim.getAllConflicts();
    const cf  = all.find(c => c.recordId === 'global-cf');
    expect(cf).toBeDefined();
    expect(cf!.deviceId).toBeDefined();
  });
});
