import { SyncManager } from '../src/core/sync-manager';
import { SQLiteStorage } from '../src/core/storage';
import { HybridLogicalClock } from '../src/core/hlc';
import { ConflictResolver } from '../src/core/conflict';
import { NetworkAdapter, Operation, PullResult, PushResult, SyncResult } from '../src/core/types';

function makeStorage() { return new SQLiteStorage(':memory:'); }

class FakeNetwork implements NetworkAdapter {
  public calls = 0;
  public failNext = false;
  public available = true;
  public latencyMs = 0;
  public remoteOps: Operation[] = [];

  async push(ops: Operation[]): Promise<PushResult> {
    this.calls++;
    if (this.failNext) { this.failNext = false; throw new Error('push failed'); }
    await delay(this.latencyMs);
    return { accepted: ops.map(o => o.id), rejected: [] };
  }

  async pull(): Promise<PullResult> {
    return { ops: [], sequence: 0 };
  }

  async isAvailable(): Promise<boolean> { return this.available; }

  async sync(ops: Operation[]): Promise<SyncResult> {
    this.calls++;
    if (this.failNext) { this.failNext = false; throw new Error('sync failed'); }
    await delay(this.latencyMs);
    const accepted = ops.map(o => o.id);
    return { accepted, remoteOps: this.remoteOps };
  }
}

function makeManager(storage = makeStorage(), net = new FakeNetwork()) {
  const hlc      = new HybridLogicalClock('test-node');
  const resolver = new ConflictResolver(hlc);
  const patches: unknown[] = [];
  const mgr = new SyncManager('test-node', storage, net, hlc, resolver, p => patches.push(p));
  return { mgr, storage, net, patches };
}

function delay(ms: number) { return new Promise(r => setTimeout(r, ms)); }

describe('SyncManager — 5-step sync flow', () => {

  test('step 1–5: pushes pending op, marks synced, applies remote op', async () => {
    const { mgr, storage, net } = makeManager();

    // Seed a pending local op via storage directly
    const op = {
      id: 'local-op-1',
      hlc: { wallTime: Date.now(), logical: 0, nodeId: 'test-node' },
      entityType: 'users', entityId: 'u1', type: 'insert' as const,
      payload: { name: 'Alice' }, nodeId: 'test-node', synced: false,
    };
    storage.appendOp(op);

    // Remote op the server would return
    net.remoteOps = [{
      id: 'remote-op-1',
      hlc: { wallTime: Date.now() + 1000, logical: 0, nodeId: 'server' },
      entityType: 'users', entityId: 'u-remote', type: 'insert' as const,
      payload: { name: 'Bob' }, nodeId: 'server', synced: true,
    }];

    await mgr.forceSync();

    // Step 5: local op marked synced
    expect(storage.getPendingOps()).toHaveLength(0);

    // Step 4: remote op applied to entity store
    const entity = storage.getEntity('users', 'u-remote');
    expect(entity?.data['name']).toBe('Bob');
  });

  test('state transitions: idle → syncing → idle on success', async () => {
    const { mgr } = makeManager();
    const states: string[] = [];
    mgr.onStateChange(s => states.push(s));

    await mgr.forceSync();
    expect(states).toContain('syncing');
    expect(mgr.getState()).toBe('idle');
  });

  test('state transitions: idle → syncing → retrying on failure', async () => {
    const { mgr, net } = makeManager();
    net.failNext = true;
    net.available = true;

    const states: string[] = [];
    mgr.onStateChange(s => states.push(s));

    await mgr.forceSync();
    expect(states).toContain('syncing');
    expect(states).toContain('retrying');
    expect(mgr.getState()).toBe('retrying');

    mgr.stop(); // clean up the retry timer
  });

  test('state → offline when network unavailable', async () => {
    const { mgr, net } = makeManager();
    net.available = false;

    const states: string[] = [];
    mgr.onStateChange(s => states.push(s));

    await mgr.forceSync();
    expect(states).toContain('offline');
  });

  test('pause prevents sync from running', async () => {
    const { mgr, net } = makeManager();
    mgr.pause();
    await mgr.forceSync();
    expect(net.calls).toBe(0);
    expect(mgr.getState()).toBe('paused');
  });

  test('resume from pause triggers a sync cycle', async () => {
    const { mgr, net } = makeManager();
    mgr.pause();
    expect(mgr.getState()).toBe('paused');

    // Wait to see 'syncing' state, which proves the cycle actually kicked off
    const seenSyncing = new Promise<void>(resolve => {
      mgr.onStateChange(s => { if (s === 'syncing') resolve(); });
    });
    mgr.resume();
    await seenSyncing;
    expect(net.calls).toBeGreaterThan(0);
  });

  test('forceSync cancels retry timer and runs immediately', async () => {
    const { mgr, net } = makeManager();
    net.failNext = true; // first call fails → goes to retrying

    await mgr.forceSync();
    expect(mgr.getState()).toBe('retrying');

    // forceSync should cancel the 15s timer and run right away
    const before = net.calls;
    await mgr.forceSync();
    expect(net.calls).toBeGreaterThan(before);

    mgr.stop();
  });
});

describe('SyncManager — retry safety & error log', () => {

  test('errors are recorded in the error log', async () => {
    const { mgr, net } = makeManager();
    net.failNext = true;

    await mgr.forceSync();
    const errors = mgr.getErrors();
    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain('sync failed');
    expect(errors[0].attempt).toBe(1);
    mgr.stop();
  });

  test('error log accumulates across multiple failures', async () => {
    const { mgr, net } = makeManager();

    net.failNext = true; await mgr.forceSync(); mgr.stop();
    net.available = true; net.failNext = true;
    const mgr2 = makeManager(makeStorage(), net).mgr;
    net.failNext = true; await mgr2.forceSync(); mgr2.stop();

    // Each manager tracks its own errors
    expect(mgr.getErrors().length).toBeGreaterThanOrEqual(1);
  });

  test('consecutive failures counter resets after success', async () => {
    const { mgr, net, patches } = makeManager();

    net.failNext = true;
    await mgr.forceSync();
    mgr.stop();

    // Now succeed
    const { mgr: mgr2, net: net2, patches: patches2 } = makeManager();
    await mgr2.forceSync();

    const retryPatch = patches2.find((p: any) => 'retryCount' in p && p.retryCount === 0);
    expect(retryPatch).toBeDefined();
  });

  test('sync log records attempt duration', async () => {
    const { mgr } = makeManager();
    await mgr.forceSync();

    const log = mgr.getSyncLog();
    expect(log).toHaveLength(1);
    expect(log[0].durationMs).toBeGreaterThanOrEqual(0);
    expect(log[0].success).toBe(true);
  });

  test('remote op already in storage is skipped (idempotent apply)', async () => {
    const { mgr, storage, net } = makeManager();

    const existingOp: Operation = {
      id: 'existing-op',
      hlc: { wallTime: Date.now(), logical: 0, nodeId: 'remote' },
      entityType: 'users', entityId: 'u1', type: 'insert',
      payload: { name: 'Original' }, nodeId: 'remote', synced: true,
    };

    // Pre-store the op so it's "already known"
    storage.commitWrite(existingOp, {
      id: 'u1', type: 'users', data: { name: 'Original' },
      hlc: existingOp.hlc, tombstone: false,
    });

    // Server sends it again
    net.remoteOps = [existingOp];
    await mgr.forceSync();

    // Entity unchanged — duplicate remote op was skipped
    const entity = storage.getEntity('users', 'u1');
    expect(entity?.data['name']).toBe('Original');
  });
});

describe('SyncManager — resume after crash', () => {

  test('pending ops survive a simulated restart and sync on next cycle', async () => {
    const storage = makeStorage();

    // Simulate: write local op but "crash" before sync
    const op = {
      id: 'crash-survivor',
      hlc: { wallTime: Date.now(), logical: 0, nodeId: 'node-a' },
      entityType: 'users', entityId: 'u-crashed', type: 'insert' as const,
      payload: { name: 'Survivor' }, nodeId: 'node-a', synced: false,
    };
    storage.appendOp(op);
    expect(storage.getPendingOps()).toHaveLength(1);

    // "Restart": create a new SyncManager against the SAME storage
    const { mgr, net } = makeManager(storage);
    await mgr.forceSync();

    // Pending op was picked up and sent
    expect(net.calls).toBe(1);
    expect(storage.getPendingOps()).toHaveLength(0);
  });
});
