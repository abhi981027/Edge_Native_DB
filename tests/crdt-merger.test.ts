import { CrdtMerger } from '../src/core/crdt-merger';
import { MergeRegistry, MergeSchema } from '../src/core/merge-registry';
import { SyncEngine } from '../src/core/engine';
import { SQLiteStorage } from '../src/core/storage';
import { NetworkAdapter, Operation, PullResult, PushResult, SyncResult } from '../src/core/types';

// ─── Field strategy unit tests ────────────────────────────────────────────────

describe('CrdtMerger — text-merge', () => {
  const schema: MergeSchema = { name: 'text-merge' };

  test('identical values produce a single copy', () => {
    const { merged } = CrdtMerger.preview({ name: 'Alice' }, { name: 'Alice' }, schema, true);
    expect(merged['name']).toBe('Alice');
  });

  test('different values are concatenated with | separator', () => {
    const { merged } = CrdtMerger.preview({ name: 'Alice' }, { name: 'Bob' }, schema, true);
    expect(merged['name']).toBe('Alice | Bob');
  });

  test('deterministic: same result regardless of which is local vs remote', () => {
    const r1 = CrdtMerger.merge({ name: 'Alice' }, { name: 'Bob' }, schema, true);
    const r2 = CrdtMerger.merge({ name: 'Bob' }, { name: 'Alice' }, schema, false);
    expect(r1['name']).toBe(r2['name']); // commutative
  });

  test('empty/null remote falls back to local', () => {
    const { merged } = CrdtMerger.preview({ name: 'Alice' }, { name: '' }, schema, true);
    expect(merged['name']).toBe('Alice');
  });

  test('only remote value kept when local is empty', () => {
    const { merged } = CrdtMerger.preview({ name: '' }, { name: 'Bob' }, schema, true);
    expect(merged['name']).toBe('Bob');
  });
});

describe('CrdtMerger — counter (monotone max)', () => {
  const schema: MergeSchema = { views: 'counter' };

  test('takes the higher value', () => {
    const { merged } = CrdtMerger.preview({ views: 10 }, { views: 15 }, schema, true);
    expect(merged['views']).toBe(15);
  });

  test('takes local when local is higher', () => {
    const { merged } = CrdtMerger.preview({ views: 20 }, { views: 15 }, schema, false);
    expect(merged['views']).toBe(20);
  });

  test('equal values produce the same result', () => {
    const { merged } = CrdtMerger.preview({ views: 7 }, { views: 7 }, schema, true);
    expect(merged['views']).toBe(7);
  });

  test('counter is monotone — never decreases', () => {
    // Even when remote is "newer" (remoteIsNewer=true) but has lower value, we take max
    const r1 = CrdtMerger.merge({ views: 50 }, { views: 10 }, schema, true);
    expect(r1['views']).toBe(50);
  });
});

describe('CrdtMerger — max', () => {
  const schema: MergeSchema = { updated_at: 'max' };

  test('takes the larger timestamp', () => {
    const { merged } = CrdtMerger.preview({ updated_at: 1000 }, { updated_at: 2000 }, schema, false);
    expect(merged['updated_at']).toBe(2000);
  });

  test('equal values — returns one of them', () => {
    const { merged } = CrdtMerger.preview({ updated_at: 999 }, { updated_at: 999 }, schema, true);
    expect(merged['updated_at']).toBe(999);
  });
});

describe('CrdtMerger — set-union', () => {
  const schema: MergeSchema = { tags: 'set-union' };

  test('unions two non-overlapping arrays', () => {
    const { merged } = CrdtMerger.preview({ tags: ['a'] }, { tags: ['b'] }, schema, true);
    expect(merged['tags']).toEqual(['a', 'b']);
  });

  test('deduplicates overlapping items', () => {
    const { merged } = CrdtMerger.preview({ tags: ['x', 'y'] }, { tags: ['y', 'z'] }, schema, true);
    expect((merged['tags'] as string[]).sort()).toEqual(['x', 'y', 'z']);
  });

  test('result is stably sorted (deterministic)', () => {
    const r1 = CrdtMerger.merge({ tags: ['c', 'a'] }, { tags: ['b'] }, schema, true);
    const r2 = CrdtMerger.merge({ tags: ['b'] }, { tags: ['c', 'a'] }, schema, false);
    expect(r1['tags']).toEqual(r2['tags']); // commutative
  });
});

describe('CrdtMerger — lww fallback inside mixed schema', () => {
  const schema: MergeSchema = { name: 'text-merge', role: 'lww' };

  test('lww field: remote wins when remoteIsNewer=true', () => {
    const { merged } = CrdtMerger.preview(
      { name: 'Alice', role: 'viewer' },
      { name: 'Bob',   role: 'admin' },
      schema, true
    );
    expect(merged['role']).toBe('admin');
    expect(merged['name']).toBe('Alice | Bob'); // text-merge still applies
  });

  test('lww field: local wins when remoteIsNewer=false', () => {
    const { merged } = CrdtMerger.preview(
      { name: 'Alice', role: 'viewer' },
      { name: 'Bob',   role: 'admin' },
      schema, false
    );
    expect(merged['role']).toBe('viewer');
  });
});

describe('CrdtMerger — fields only in one version', () => {
  const schema: MergeSchema = { name: 'text-merge' };

  test('field only in local is preserved', () => {
    const { merged } = CrdtMerger.preview({ name: 'Alice', local_only: 42 }, { name: 'Bob' }, schema, true);
    expect(merged['local_only']).toBe(42);
  });

  test('field only in remote is included (lww default)', () => {
    const { merged } = CrdtMerger.preview({ name: 'Alice' }, { name: 'Bob', remote_only: 99 }, schema, true);
    expect(merged['remote_only']).toBe(99);
  });
});

describe('CrdtMerger — preview field decisions', () => {
  test('preview identifies which fields conflicted', () => {
    const schema: MergeSchema = { name: 'text-merge', role: 'lww' };
    const { fieldDecisions } = CrdtMerger.preview(
      { name: 'Alice', role: 'admin' },
      { name: 'Bob',   role: 'admin' },
      schema, true
    );
    expect(fieldDecisions['name'].conflicted).toBe(true);  // values differ
    expect(fieldDecisions['role'].conflicted).toBe(false); // same value
  });
});

// ─── Integration: CRDT merge in sync path ─────────────────────────────────────

class FakeNet implements NetworkAdapter {
  remoteOps: Operation[] = [];
  async push(ops: Operation[]): Promise<PushResult> { return { accepted: ops.map(o => o.id), rejected: [] }; }
  async pull(): Promise<PullResult> { return { ops: [], sequence: 0 }; }
  async isAvailable() { return true; }
  async sync(ops: Operation[]): Promise<SyncResult> {
    return { accepted: ops.map(o => o.id), remoteOps: this.remoteOps };
  }
}

describe('CRDT merge — integration through SyncEngine', () => {
  beforeEach(() => {
    // Register merge schema for each test
    SyncEngine.registerMergeSchema('products', { name: 'text-merge', stock: 'counter', tags: 'set-union' });
  });

  function makeEngine(net = new FakeNet()) {
    return { engine: new SyncEngine('node-A', new SQLiteStorage(':memory:'), net), net };
  }

  test('CRDT merge: text fields from two devices are combined', async () => {
    const { engine, net } = makeEngine();

    // Device A writes
    engine.write('products', 'p1', { name: 'Widget', stock: 10 });
    const local = engine.get('products', 'p1')!;

    // Device B concurrently writes (newer HLC)
    net.remoteOps = [{
      id: 'b-op',
      hlc: { wallTime: local.hlc.wallTime + 1, logical: 0, nodeId: 'node-B' },
      entityType: 'products', entityId: 'p1', type: 'update',
      payload: { name: 'Super Widget', stock: 15 },
      nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const entity = engine.get('products', 'p1')!;
    // text-merge: both names preserved
    expect(entity.data['name']).toContain('Widget');
    expect(entity.data['name']).toContain('Super Widget');
    // counter: max(10, 15) = 15
    expect(entity.data['stock']).toBe(15);
  });

  test('CRDT merge is auto-resolved (conflict.resolved=true)', async () => {
    const { engine, net } = makeEngine();
    engine.write('products', 'p1', { name: 'Widget' });
    const local = engine.get('products', 'p1')!;

    net.remoteOps = [{
      id: 'b-op2',
      hlc: { wallTime: local.hlc.wallTime + 1, logical: 0, nodeId: 'node-B' },
      entityType: 'products', entityId: 'p1', type: 'update',
      payload: { name: 'Super Widget' },
      nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    expect(conflicts[0].winner).toBe('merged');
    expect(conflicts[0].resolved).toBe(true);
    expect(conflicts[0].mergedVersion).toBeDefined();
  });

  test('LWW used when no schema registered', async () => {
    const { engine, net } = makeEngine();
    engine.write('orders', 'o1', { status: 'pending' });
    const local = engine.get('orders', 'o1')!;

    net.remoteOps = [{
      id: 'o-op',
      hlc: { wallTime: local.hlc.wallTime + 1, logical: 0, nodeId: 'node-B' },
      entityType: 'orders', entityId: 'o1', type: 'update',
      payload: { status: 'shipped' },
      nodeId: 'node-B', synced: true,
    }];

    await engine.forceSync();

    const conflicts = engine.getConflicts();
    // No schema → LWW used
    expect(['local', 'remote']).toContain(conflicts[0].winner);
    expect(conflicts[0].resolved).toBe(false); // not auto-resolved
  });
});

describe('applyAutoMerge / applyManualMerge', () => {
  function makeEngine() {
    SyncEngine.registerMergeSchema('notes', { body: 'text-merge' });
    return new SyncEngine('node-A', new SQLiteStorage(':memory:'), new FakeNet());
  }

  test('applyAutoMerge merges using registered schema', () => {
    const engine = makeEngine();
    engine.write('notes', 'n1', { body: 'Hello' });
    engine.simulateConflict('notes', 'n1');

    // 'notes' has a CRDT schema → simulateConflict auto-merges (resolved=true)
    const [c] = engine.getConflicts(null); // null = all, including auto-resolved
    expect(c).toBeDefined();

    const result = engine.applyAutoMerge(c.id);
    expect(result.ok).toBe(true);
    expect(result.merged?.['body']).toBeDefined();
    expect(engine.getConflicts(false)).toHaveLength(0); // no unresolved
  });

  test('applyAutoMerge returns ok:false when no schema', () => {
    const engine = makeEngine();
    // 'items' has no registered schema → LWW → unresolved conflict
    engine.write('items', 'i1', { qty: 1 });
    engine.simulateConflict('items', 'i1');

    const [c] = engine.getConflicts(false); // unresolved because LWW was used
    expect(c).toBeDefined();
    const result = engine.applyAutoMerge(c.id);
    expect(result.ok).toBe(false); // no schema → can't auto-merge
  });

  test('applyManualMerge stores user-provided data', () => {
    const engine = makeEngine();
    engine.write('notes', 'n1', { body: 'Hello' });
    engine.simulateConflict('notes', 'n1');

    const [c] = engine.getConflicts(null); // all (auto-merged by CRDT schema)
    expect(c).toBeDefined();
    engine.applyManualMerge(c.id, { body: 'Manually merged text', edited: true });

    const entity = engine.get('notes', 'n1')!;
    expect(entity.data['body']).toBe('Manually merged text');
    expect(entity.data['edited']).toBe(true);

    const resolved = engine.getConflicts(true);
    const manual = resolved.find(x => x.winner === 'manual');
    expect(manual).toBeDefined();
  });

  test('previewMerge shows field-level decisions', () => {
    const engine = makeEngine();
    engine.write('notes', 'n1', { body: 'Hello' });
    engine.simulateConflict('notes', 'n1');

    const [c] = engine.getConflicts(null); // all (auto-merged)
    expect(c).toBeDefined();
    const preview = engine.previewMerge(c.id);

    expect(preview).not.toBeNull();
    expect(preview!.merged['body']).toBeDefined();
    expect(preview!.fieldDecisions['body']).toBeDefined();
    expect(preview!.schema).toMatchObject({ body: 'text-merge' });
  });
});
