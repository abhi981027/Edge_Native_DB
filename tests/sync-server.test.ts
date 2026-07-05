import { createSyncServer } from '../src/server/sync-server';
import { Change } from '../src/core/types';

const PORT = 13_001; // isolated test port

function makeChange(overrides: Partial<Change> = {}): Change {
  return {
    id:        overrides.id        ?? `chg-${Math.random().toString(36).slice(2)}`,
    tableName: overrides.tableName ?? 'users',
    recordId:  overrides.recordId  ?? 'u1',
    operation: overrides.operation ?? 'insert',
    timestamp: overrides.timestamp ?? Date.now(),
    deviceId:  overrides.deviceId  ?? 'device-A',
    payload:   overrides.payload   ?? { name: 'Alice' },
    synced:    0,
  };
}

async function sync(deviceId: string, changes: Change[] = []) {
  const res = await fetch(`http://localhost:${PORT}/sync`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ device_id: deviceId, changes }),
  });
  return res.json() as Promise<{ changes: Change[]; meta: Record<string, number> }>;
}

describe('Sync Server — POST /sync', () => {
  let srv: { close: () => void };

  beforeAll(() => { srv = createSyncServer(PORT, ':memory:'); });
  afterAll(()  => { srv.close(); });

  // ── Idempotency ──────────────────────────────────────────────────────────

  test('stores a change and returns it to other devices', async () => {
    const c = makeChange({ deviceId: 'device-A' });
    await sync('device-A', [c]);

    const { changes } = await sync('device-B', []);
    expect(changes.find(x => x.id === c.id)).toBeDefined();
  });

  test('same change sent twice is stored exactly once', async () => {
    const c = makeChange({ deviceId: 'device-A', id: 'dup-test-1' });
    const r1 = await sync('device-A', [c]);
    const r2 = await sync('device-A', [c]);

    expect(r1.meta['accepted']).toBe(1);
    expect(r2.meta['accepted']).toBe(0);  // duplicate — ignored
    expect(r2.meta['duplicates']).toBe(1);
  });

  test('duplicate is logged', async () => {
    const c = makeChange({ id: 'dup-log-test', deviceId: 'device-A' });
    await sync('device-A', [c]);
    await sync('device-A', [c]); // second delivery

    const res = await fetch(`http://localhost:${PORT}/api/duplicates`);
    const dups = await res.json() as Array<{ id: string; count: number }>;
    const entry = dups.find(d => d.id === 'dup-log-test');
    expect(entry).toBeDefined();
    expect(entry!.count).toBeGreaterThanOrEqual(1);
  });

  test('re-sending the same change never corrupts stored data', async () => {
    const c = makeChange({ id: 'safe-dup', deviceId: 'A', payload: { val: 1 } });
    await sync('A', [c]);

    const mutated = { ...c, payload: { val: 999 } };
    await sync('A', [mutated]); // re-deliver with different payload → IGNORED

    // Device-B should only see the original payload (val: 1)
    const { changes } = await sync('B', []);
    const received = changes.find(x => x.id === 'safe-dup');
    expect(received?.payload).toMatchObject({ val: 1 });
  });

  // ── Per-device cursor ────────────────────────────────────────────────────

  test('device only receives changes from OTHER devices', async () => {
    const own    = makeChange({ deviceId: 'device-X', id: 'own-change' });
    const foreign = makeChange({ deviceId: 'device-Y', id: 'foreign-change' });

    await sync('device-X', [own]);
    await sync('device-Y', [foreign]);

    const { changes } = await sync('device-X', []);
    const ids = changes.map((c: Change) => c.id);
    expect(ids).toContain('foreign-change');
    expect(ids).not.toContain('own-change');
  });

  test('cursor advances: device does not receive same batch twice', async () => {
    const c1 = makeChange({ deviceId: 'sender', id: 'cursor-test-1' });
    const c2 = makeChange({ deviceId: 'sender', id: 'cursor-test-2' });
    await sync('sender', [c1, c2]);

    const r1 = await sync('receiver', []);
    const r2 = await sync('receiver', []);

    const ids1 = r1.changes.map((c: Change) => c.id);
    const ids2 = r2.changes.map((c: Change) => c.id);

    // First sync gets the changes, second sync gets nothing new
    expect(ids1).toEqual(expect.arrayContaining(['cursor-test-1', 'cursor-test-2']));
    expect(ids2).not.toContain('cursor-test-1');
    expect(ids2).not.toContain('cursor-test-2');
  });

  // ── No ordering assumption ────────────────────────────────────────────────

  test('changes arriving out of client-timestamp order are all delivered', async () => {
    const now = Date.now();
    // Send future timestamp first, past timestamp second — should both land
    const late  = makeChange({ deviceId: 'out-of-order', id: 'oot-late',  timestamp: now + 10_000 });
    const early = makeChange({ deviceId: 'out-of-order', id: 'oot-early', timestamp: now - 10_000 });

    await sync('out-of-order', [late]);
    await sync('out-of-order', [early]);

    const { changes } = await sync('receiver-oot', []);
    const ids = changes.map((c: Change) => c.id);
    expect(ids).toContain('oot-late');
    expect(ids).toContain('oot-early');
  });

  // ── Partial sync ──────────────────────────────────────────────────────────

  test('partial batch: missing changes arrive on next sync', async () => {
    const c1 = makeChange({ deviceId: 'partial-src', id: 'partial-1' });
    const c2 = makeChange({ deviceId: 'partial-src', id: 'partial-2' });
    const c3 = makeChange({ deviceId: 'partial-src', id: 'partial-3' });

    // Send only c1, c2 first (c3 "lost in transit")
    await sync('partial-src', [c1, c2]);

    // Send all three later (c1, c2 are duplicates, c3 is new)
    const r = await sync('partial-src', [c1, c2, c3]);
    expect(r.meta['accepted']).toBe(1);    // only c3 is new
    expect(r.meta['duplicates']).toBe(2);  // c1, c2 are dups
  });

  // ── Payload forwarding ────────────────────────────────────────────────────

  test('payload is preserved through server and delivered to peer', async () => {
    const c = makeChange({
      deviceId: 'payload-src',
      id: 'payload-test',
      tableName: 'users',
      recordId: 'u-payload',
      operation: 'insert',
      payload: { name: 'Payload User', role: 'admin', score: 42 },
    });

    await sync('payload-src', [c]);
    const { changes } = await sync('payload-rcv', []);
    const received = changes.find(x => x.id === 'payload-test');

    expect(received?.payload).toMatchObject({ name: 'Payload User', role: 'admin', score: 42 });
  });

  // ── Admin API ─────────────────────────────────────────────────────────────

  test('/api/stats reports accurate counts', async () => {
    const res  = await fetch(`http://localhost:${PORT}/api/stats`);
    const stats = await res.json() as Record<string, number>;
    expect(stats['total']).toBeGreaterThan(0);
    expect(stats['devices']).toBeGreaterThan(0);
  });

  test('/api/devices returns sync metadata per device', async () => {
    const res     = await fetch(`http://localhost:${PORT}/api/devices`);
    const devices = await res.json() as Array<{ deviceId: string; syncCount: number }>;
    expect(devices.length).toBeGreaterThan(0);
    expect(devices[0].syncCount).toBeGreaterThan(0);
  });

  // ── Input validation ──────────────────────────────────────────────────────

  test('missing device_id returns 400', async () => {
    const res = await fetch(`http://localhost:${PORT}/sync`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ changes: [] }),
    });
    expect(res.status).toBe(400);
  });

  test('malformed changes array is tolerated (invalid entries skipped)', async () => {
    const res = await fetch(`http://localhost:${PORT}/sync`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // Send a mix of valid and invalid change objects
      body: JSON.stringify({
        device_id: 'validator-test',
        changes: [
          { id: 'valid-1', tableName: 'users', recordId: 'r1', operation: 'insert', timestamp: Date.now(), deviceId: 'validator-test' },
          { broken: true },              // missing required fields — skipped
          null,                          // null — skipped
          { id: '', tableName: 'x' },    // empty id — skipped
        ],
      }),
    });
    expect(res.status).toBe(200);
    const data = await res.json() as { meta: Record<string, number> };
    expect(data.meta['accepted']).toBe(1); // only the valid entry
  });
});
