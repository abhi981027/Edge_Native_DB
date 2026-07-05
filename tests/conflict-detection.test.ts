import { SyncEngine } from '../src/core/engine';
import { SQLiteStorage } from '../src/core/storage';
import { NetworkAdapter, Operation, PullResult, PushResult, SyncResult } from '../src/core/types';

function makeStorage() { return new SQLiteStorage(':memory:'); }

class FakeNetwork implements NetworkAdapter {
  public remoteOps: Operation[] = [];
  async push(ops: Operation[]): Promise<PushResult> {
    return { accepted: ops.map(o => o.id), rejected: [] };
  }
  async pull(): Promise<PullResult> { return { ops: [], sequence: 0 }; }
  async isAvailable(): Promise<boolean> { return true; }
  async sync(ops: Operation[]): Promise<SyncResult> {
    return { accepted: ops.map(o => o.id), remoteOps: this.remoteOps };
  }
}

function makeEngine(net = new FakeNetwork()) {
  return { engine: new SyncEngine('node-A', makeStorage(), net), net };
}

describe('Conflict detection — same record, different devices', () => {

  test('no conflict on fresh insert from remote (no local exists yet)', async () => {
    const { engine, net } = makeEngine();

    net.remoteOps = [{
      id: 'remote-insert',
      hlc: { wallTime: Date.now(), logical: 0, nodeId: 'node-B' },
      entityType: 'users', entityId: 'u1', type: 'insert',
      payload: { name: 'Bob' }, nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    expect(conflicts).toHaveLength(0);
  });

  test('no conflict when same device re-syncs (own op returned by server)', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });

    // Server echos our own op back (same nodeId)
    const ourOp = engine.listOps(1)[0];
    net.remoteOps = [{ ...ourOp, synced: true }];

    await engine.forceSync();

    // Own op returned by server = already in storage, skipped
    const conflicts = engine.getConflicts();
    expect(conflicts).toHaveLength(0);
  });

  test('conflict detected when different devices write same record', async () => {
    const { engine, net } = makeEngine();

    // Local write from node-A
    engine.write('users', 'u1', { name: 'Alice (node-A)' });

    // Remote write from node-B to the same record
    net.remoteOps = [{
      id: 'b-update-u1',
      hlc: { wallTime: Date.now() + 5000, logical: 0, nodeId: 'node-B' },
      entityType: 'users', entityId: 'u1', type: 'update',
      payload: { name: 'Alice (node-B)' }, nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].recordId).toBe('u1');
    expect(conflicts[0].tableName).toBe('users');
    expect(conflicts[0].localVersion.deviceId).toBe('node-A');
    expect(conflicts[0].remoteVersion.deviceId).toBe('node-B');
  });

  test('LWW winner recorded correctly — remote wins when newer', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    const localEntity = engine.get('users', 'u1')!;

    net.remoteOps = [{
      id: 'b-newer',
      hlc: { wallTime: localEntity.hlc.wallTime + 1000, logical: 0, nodeId: 'node-B' },
      entityType: 'users', entityId: 'u1', type: 'update',
      payload: { name: 'Alice Updated by B' }, nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    expect(conflicts[0].winner).toBe('remote');

    // Entity store has remote's version (remote won LWW)
    expect(engine.get('users', 'u1')?.data['name']).toBe('Alice Updated by B');
  });

  test('LWW winner recorded correctly — local wins when newer', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice (newer local)' });
    const localEntity = engine.get('users', 'u1')!;

    net.remoteOps = [{
      id: 'b-older',
      hlc: { wallTime: localEntity.hlc.wallTime - 5000, logical: 0, nodeId: 'node-B' },
      entityType: 'users', entityId: 'u1', type: 'update',
      payload: { name: 'Alice (older remote)' }, nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    expect(conflicts[0].winner).toBe('local');

    // Entity store unchanged — local won
    expect(engine.get('users', 'u1')?.data['name']).toBe('Alice (newer local)');
  });

  test('both versions preserved in conflict record regardless of winner', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Local Version', role: 'admin' });
    const localEntity = engine.get('users', 'u1')!;

    net.remoteOps = [{
      id: 'b-conflict',
      hlc: { wallTime: localEntity.hlc.wallTime + 1, logical: 0, nodeId: 'node-B' },
      entityType: 'users', entityId: 'u1', type: 'update',
      payload: { name: 'Remote Version', role: 'viewer' }, nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const c = engine.getConflicts()[0];
    expect(c.localVersion.data?.['name']).toBe('Local Version');
    expect(c.remoteVersion.data?.['name']).toBe('Remote Version');
  });

  test('multiple conflicts on different records are each logged', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    engine.write('users', 'u2', { name: 'Bob' });
    const ts = Date.now() + 10_000;

    net.remoteOps = [
      { id: 'c1', hlc: { wallTime: ts, logical: 0, nodeId: 'B' }, entityType: 'users', entityId: 'u1', type: 'update', payload: { name: 'Alice-B' }, nodeId: 'B', synced: true },
      { id: 'c2', hlc: { wallTime: ts, logical: 1, nodeId: 'B' }, entityType: 'users', entityId: 'u2', type: 'update', payload: { name: 'Bob-B' },   nodeId: 'B', synced: true },
    ];

    await engine.forceSync();

    expect(engine.getConflicts()).toHaveLength(2);
  });

  test('same conflict op delivered twice only logs once', async () => {
    const { engine, net } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    const ts = Date.now() + 1000;

    const op: Operation = {
      id: 'dup-conflict', hlc: { wallTime: ts, logical: 0, nodeId: 'B' },
      entityType: 'users', entityId: 'u1', type: 'update',
      payload: { name: 'Alice-B' }, nodeId: 'B', synced: true,
    };

    net.remoteOps = [op];
    await engine.forceSync(); // first delivery

    net.remoteOps = [op];
    await engine.forceSync(); // second delivery (duplicate)

    // getOpById() short-circuits the second delivery — conflict logged once only
    expect(engine.getConflicts()).toHaveLength(1);
  });
});

describe('Conflict simulation', () => {

  test('simulateConflict triggers a detectable conflict', () => {
    const { engine } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });

    const result = engine.simulateConflict('users', 'u1');

    expect(result.triggered).toBe(true);
    expect(result.winner).toBeDefined();
    expect(result.localData?.['name']).toBe('Alice');
    expect(result.remoteData?.['name']).toContain('Alice');
  });

  test('simulateConflict on non-existent entity returns triggered:false', () => {
    const { engine } = makeEngine();
    const result = engine.simulateConflict('users', 'no-such-user');
    expect(result.triggered).toBe(false);
  });

  test('simulated conflict appears in getConflicts()', () => {
    const { engine } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    engine.simulateConflict('users', 'u1');

    const conflicts = engine.getConflicts();
    expect(conflicts.length).toBeGreaterThanOrEqual(1);
    const c = conflicts.find(x => x.recordId === 'u1');
    expect(c).toBeDefined();
    expect(c!.resolved).toBe(false);
  });
});

describe('Conflict resolution (acknowledge)', () => {

  test('markConflictResolved marks it resolved', () => {
    const { engine } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    engine.simulateConflict('users', 'u1');

    const [c] = engine.getConflicts();
    expect(c.resolved).toBe(false);

    engine.markConflictResolved(c.id);

    const updated = engine.getConflicts()[0];
    expect(updated.resolved).toBe(true);
  });

  test('getUnresolvedConflictCount decrements after resolve', () => {
    const { engine } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    engine.write('users', 'u2', { name: 'Bob' });
    engine.simulateConflict('users', 'u1');
    engine.simulateConflict('users', 'u2');

    expect(engine.getUnresolvedConflictCount()).toBe(2);
    engine.markConflictResolved(engine.getConflicts()[0].id);
    expect(engine.getUnresolvedConflictCount()).toBe(1);
  });

  test('filter: getConflicts(false) returns only unresolved', () => {
    const { engine } = makeEngine();
    engine.write('users', 'u1', { name: 'Alice' });
    engine.write('users', 'u2', { name: 'Bob' });
    engine.simulateConflict('users', 'u1');
    engine.simulateConflict('users', 'u2');

    engine.markConflictResolved(engine.getConflicts()[0].id);

    expect(engine.getConflicts(false)).toHaveLength(1);
    expect(engine.getConflicts(true)).toHaveLength(1);
    expect(engine.getConflicts(null)).toHaveLength(2);
  });
});
