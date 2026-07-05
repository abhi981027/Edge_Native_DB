import { HybridLogicalClock } from '../src/core/hlc';
import { ConflictResolver } from '../src/core/conflict';
import { Entity, Operation } from '../src/core/types';

function makeOp(overrides: Partial<Operation> = {}): Operation {
  return {
    id: 'op-1',
    hlc: { wallTime: 1000, logical: 0, nodeId: 'A' },
    entityType: 'users',
    entityId: 'u1',
    type: 'insert',
    payload: { name: 'Alice' },
    nodeId: 'A',
    synced: false,
    ...overrides,
  };
}

function makeEntity(overrides: Partial<Entity> = {}): Entity {
  return {
    id: 'u1',
    type: 'users',
    data: { name: 'Alice' },
    hlc: { wallTime: 1000, logical: 0, nodeId: 'A' },
    tombstone: false,
    ...overrides,
  };
}

describe('ConflictResolver', () => {
  let resolver: ConflictResolver;

  beforeEach(() => {
    resolver = new ConflictResolver(new HybridLogicalClock('test'));
  });

  test('applies op when no local entity exists', () => {
    const op = makeOp({ type: 'insert', payload: { name: 'Alice' } });
    const result = resolver.resolve(undefined, op);
    expect(result).not.toBeNull();
    expect(result?.data['name']).toBe('Alice');
  });

  test('remote wins when remote HLC is newer', () => {
    const local  = makeEntity({ hlc: { wallTime: 500, logical: 0, nodeId: 'A' } });
    const remote = makeOp({
      type: 'update',
      payload: { name: 'Alice Updated' },
      hlc: { wallTime: 1000, logical: 0, nodeId: 'B' },
    });
    const result = resolver.resolve(local, remote);
    expect(result?.data['name']).toBe('Alice Updated');
  });

  test('local wins when remote HLC is older', () => {
    const local  = makeEntity({ hlc: { wallTime: 2000, logical: 0, nodeId: 'A' } });
    const remote = makeOp({ hlc: { wallTime: 500, logical: 0, nodeId: 'B' } });
    const result = resolver.resolve(local, remote);
    expect(result).toBeNull(); // local wins → no update
  });

  test('local wins when remote HLC equals local HLC but nodeId is less', () => {
    const ts = { wallTime: 1000, logical: 0 };
    const local  = makeEntity({ hlc: { ...ts, nodeId: 'Z' } });
    const remote = makeOp({ hlc: { ...ts, nodeId: 'A' }, type: 'update', payload: { name: 'Overwrite' } });
    const result = resolver.resolve(local, remote);
    expect(result).toBeNull(); // A < Z → local (Z) wins
  });

  test('update merges partial fields (does not wipe existing)', () => {
    const local  = makeEntity({ data: { name: 'Alice', role: 'admin' } });
    const remote = makeOp({
      type: 'update',
      payload: { name: 'Alice V2' }, // only name, not role
      hlc: { wallTime: 2000, logical: 0, nodeId: 'B' },
    });
    const result = resolver.resolve(local, remote);
    expect(result?.data['name']).toBe('Alice V2');
    expect(result?.data['role']).toBe('admin'); // preserved
  });

  test('delete sets tombstone and preserves entity id', () => {
    const local  = makeEntity();
    const remote = makeOp({
      type: 'delete',
      payload: null,
      hlc: { wallTime: 3000, logical: 0, nodeId: 'B' },
    });
    const result = resolver.resolve(local, remote);
    expect(result?.tombstone).toBe(true);
    expect(result?.id).toBe('u1');
  });

  test('delete on non-existent entity returns null', () => {
    const op = makeOp({ type: 'delete', payload: null });
    expect(resolver.resolve(undefined, op)).toBeNull();
  });
});
