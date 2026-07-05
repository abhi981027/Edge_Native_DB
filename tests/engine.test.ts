import { SyncEngine } from '../src/core/engine';
import { SQLiteStorage } from '../src/core/storage';
import { UserRepository } from '../src/core/users';
import { NetworkAdapter, PullResult, PushResult, Operation } from '../src/core/types';

// In-memory storage for test isolation
function makeStorage() {
  return new SQLiteStorage(':memory:');
}

// Controllable fake network
class FakeNetwork implements NetworkAdapter {
  public pushLog: Operation[][] = [];
  public pullResults: PullResult[] = [];
  public available = true;
  public failNext = false;

  async push(ops: Operation[]): Promise<PushResult> {
    if (this.failNext) { this.failNext = false; throw new Error('push failed'); }
    this.pushLog.push(ops);
    return { accepted: ops.map(o => o.id), rejected: [] };
  }

  async pull(): Promise<PullResult> {
    if (this.failNext) { this.failNext = false; throw new Error('pull failed'); }
    return this.pullResults.shift() ?? { ops: [], sequence: 0 };
  }

  async isAvailable(): Promise<boolean> {
    return this.available;
  }
}

describe('SyncEngine — local writes', () => {
  test('write creates an entity', () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    engine.write('users', 'u1', { name: 'Alice' });
    const entity = engine.get('users', 'u1');
    expect(entity?.data['name']).toBe('Alice');
    expect(entity?.tombstone).toBe(false);
  });

  test('write returns a unique op id', () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    const id1 = engine.write('users', 'u1', { name: 'Alice' });
    const id2 = engine.write('users', 'u2', { name: 'Bob' });
    expect(id1).not.toBe(id2);
  });

  test('second write upgrades insert to update (merges fields)', () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    engine.write('users', 'u1', { name: 'Alice', role: 'viewer' });
    engine.write('users', 'u1', { name: 'Alice Pro' });
    const entity = engine.get('users', 'u1');
    expect(entity?.data['name']).toBe('Alice Pro');
    expect(entity?.data['role']).toBe('viewer'); // not wiped
  });

  test('remove soft-deletes the entity', () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    engine.write('users', 'u1', { name: 'Alice' });
    engine.remove('users', 'u1');
    expect(engine.get('users', 'u1')?.tombstone).toBe(true);
    expect(engine.list('users')).toHaveLength(0); // deleted excluded from list
  });

  test('writes queue as pending ops', () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    engine.write('users', 'u1', { name: 'Alice' });
    engine.write('users', 'u2', { name: 'Bob' });
    expect(engine.getStatus().pendingOps).toBe(2);
  });
});

describe('SyncEngine — sync push', () => {
  test('force sync pushes pending ops and marks them synced', async () => {
    const net    = new FakeNetwork();
    const engine = new SyncEngine('A', makeStorage(), net);
    engine.write('users', 'u1', { name: 'Alice' });

    expect(engine.getStatus().pendingOps).toBe(1);
    await engine.forceSync();
    expect(engine.getStatus().pendingOps).toBe(0);
    expect(net.pushLog).toHaveLength(1);
    expect(net.pushLog[0]).toHaveLength(1);
  });

  test('status shows connected after successful sync', async () => {
    const engine = new SyncEngine('A', makeStorage(), new FakeNetwork());
    await engine.forceSync();
    expect(engine.getStatus().connected).toBe(true);
    expect(engine.getStatus().lastSyncAt).not.toBeNull();
  });

  test('status shows disconnected when network unavailable', async () => {
    const net  = new FakeNetwork();
    net.available = false;
    const engine = new SyncEngine('A', makeStorage(), net);
    await engine.forceSync();
    expect(engine.getStatus().connected).toBe(false);
  });
});

describe('SyncEngine — pull and conflict resolution', () => {
  test('pulls remote ops and applies them locally', async () => {
    const net = new FakeNetwork();
    const engine = new SyncEngine('node-B', makeStorage(), net);

    const remoteOp: Operation = {
      id: 'remote-op-1',
      hlc: { wallTime: Date.now() + 5000, logical: 0, nodeId: 'node-A' },
      entityType: 'users',
      entityId: 'u-remote',
      type: 'insert',
      payload: { name: 'Remote User' },
      nodeId: 'node-A',
      synced: true,
    };
    net.pullResults.push({ ops: [remoteOp], sequence: 1 });

    await engine.forceSync();
    const entity = engine.get('users', 'u-remote');
    expect(entity?.data['name']).toBe('Remote User');
  });

  test('remote op does not overwrite newer local write (LWW)', async () => {
    const net    = new FakeNetwork();
    const engine = new SyncEngine('node-B', makeStorage(), net);

    // Local write is newer (wall clock in the future relative to remote)
    engine.write('users', 'u1', { name: 'Local (newer)' });
    const localEntity = engine.get('users', 'u1')!;

    const staleRemoteOp: Operation = {
      id: 'remote-op-stale',
      hlc: { wallTime: localEntity.hlc.wallTime - 1000, logical: 0, nodeId: 'node-A' },
      entityType: 'users',
      entityId: 'u1',
      type: 'update',
      payload: { name: 'Remote (older)' },
      nodeId: 'node-A',
      synced: true,
    };
    net.pullResults.push({ ops: [staleRemoteOp], sequence: 1 });

    await engine.forceSync();
    expect(engine.get('users', 'u1')?.data['name']).toBe('Local (newer)');
  });

  test('duplicate remote op is idempotent (no double-apply)', async () => {
    const net    = new FakeNetwork();
    const engine = new SyncEngine('node-B', makeStorage(), net);

    const op: Operation = {
      id: 'dup-op',
      hlc: { wallTime: Date.now() + 5000, logical: 0, nodeId: 'A' },
      entityType: 'users',
      entityId: 'u-dup',
      type: 'insert',
      payload: { name: 'Dup User', count: 1 },
      nodeId: 'A',
      synced: true,
    };
    // Server sends same op twice (e.g., cursor not advanced)
    net.pullResults.push({ ops: [op], sequence: 1 });
    net.pullResults.push({ ops: [op], sequence: 1 });

    await engine.forceSync();
    await engine.forceSync();

    const all = engine.list('users');
    expect(all.filter(e => e.id === 'u-dup')).toHaveLength(1);
  });
});

describe('UserRepository', () => {
  function makeRepo() {
    const engine = new SyncEngine('node-A', makeStorage(), new FakeNetwork());
    return { repo: new UserRepository(engine), engine };
  }

  test('createUser and getUser', () => {
    const { repo } = makeRepo();
    repo.createUser({ id: 'u1', name: 'Alice', updated_at: 1000, device_id: 'dev-1' });
    const user = repo.getUser('u1');
    expect(user?.name).toBe('Alice');
    expect(user?.device_id).toBe('dev-1');
    expect(user?.updated_at).toBe(1000);
  });

  test('updateUser merges fields', () => {
    const { repo } = makeRepo();
    repo.createUser({ id: 'u1', name: 'Alice', updated_at: 1000, device_id: 'dev-1' });
    repo.updateUser('u1', { name: 'Alice V2' });
    const user = repo.getUser('u1');
    expect(user?.name).toBe('Alice V2');
    expect(user?.device_id).toBe('dev-1'); // preserved
  });

  test('getAllUsers returns all non-deleted users', () => {
    const { repo } = makeRepo();
    repo.createUser({ id: 'u1', name: 'Alice',   updated_at: 1, device_id: '' });
    repo.createUser({ id: 'u2', name: 'Bob',     updated_at: 2, device_id: '' });
    repo.createUser({ id: 'u3', name: 'Charlie', updated_at: 3, device_id: '' });
    repo.deleteUser('u2');
    const users = repo.getAllUsers();
    expect(users).toHaveLength(2);
    expect(users.map(u => u.id)).not.toContain('u2');
  });

  test('deleteUser removes from list', () => {
    const { repo } = makeRepo();
    repo.createUser({ id: 'u1', name: 'Alice', updated_at: 1, device_id: '' });
    repo.deleteUser('u1');
    expect(repo.getAllUsers()).toHaveLength(0);
    expect(repo.getUser('u1')).toBeUndefined(); // not in list
  });
});
