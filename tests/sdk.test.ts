import { EdgeDB, Inspector, SyncController } from '../src/sdk/edge-db';
import { Collection } from '../src/sdk/collection';
import { Logger, globalLogger } from '../src/sdk/logger';
import { createSyncServer } from '../src/server/sync-server';

const PORT     = 13066;
const UPSTREAM = `http://localhost:${PORT}`;

type Task = { id: string; title: string; done: boolean; updated_at: number };

let server: ReturnType<typeof createSyncServer>;
let db: EdgeDB;

function delay(ms: number) { return new Promise(r => setTimeout(r, ms)); }

beforeAll(async () => {
  server = createSyncServer(PORT);
  db = new EdgeDB({
    nodeId:       'sdk-test-node',
    dbPath:       ':memory:',
    upstreamUrl:  UPSTREAM,
    mergeSchemas: {
      tasks: { title: 'text-merge', done: 'max', updated_at: 'max' },
    },
    logger: new Logger(),   // isolated logger per test run
  });
  db.connect();
  await delay(100);
});

afterAll(async () => {
  db.disconnect();
  await delay(50);
  server.close();
});

// ─── EdgeDB class ─────────────────────────────────────────────────────────────

describe('EdgeDB', () => {
  it('exposes nodeId from config', () => {
    expect(db.nodeId).toBe('sdk-test-node');
  });

  it('exposes sync controller', () => {
    expect(db.sync).toBeInstanceOf(SyncController);
  });

  it('exposes inspector', () => {
    expect(db.inspect).toBeInstanceOf(Inspector);
  });

  it('collection() returns same instance for the same type', () => {
    const a = db.collection<Task>('tasks');
    const b = db.collection<Task>('tasks');
    expect(a).toBe(b);
  });

  it('collection() returns different instances for different types', () => {
    const tasks  = db.collection<Task>('tasks');
    const events = db.collection<{ id: string; name: string }>('events');
    expect(tasks).not.toBe(events);
  });
});

// ─── Collection ────────────────────────────────────────────────────────────────

describe('Collection<T>', () => {
  let tasks: Collection<Task>;

  beforeEach(() => {
    tasks = db.collection<Task>('tasks');
  });

  it('upsert returns a string op ID', () => {
    const opId = tasks.upsert({ id: 'c-t1', title: 'Test', done: false, updated_at: Date.now() });
    expect(typeof opId).toBe('string');
    expect(opId.length).toBeGreaterThan(0);
  });

  it('get() returns the upserted entity', () => {
    tasks.upsert({ id: 'c-t2', title: 'Hello', done: false, updated_at: 1000 });
    const t = tasks.get('c-t2');
    expect(t).toBeDefined();
    expect(t!.title).toBe('Hello');
    expect(t!.done).toBe(false);
  });

  it('get() returns undefined for unknown id', () => {
    expect(tasks.get('nonexistent-999')).toBeUndefined();
  });

  it('all() includes the upserted entity', () => {
    tasks.upsert({ id: 'c-t3', title: 'Listed', done: false, updated_at: 2000 });
    const all = tasks.all();
    expect(all.find(t => t.id === 'c-t3')).toBeDefined();
  });

  it('remove() makes entity disappear from get() and all()', () => {
    tasks.upsert({ id: 'c-t4', title: 'ToDelete', done: false, updated_at: 3000 });
    expect(tasks.get('c-t4')).toBeDefined();
    tasks.remove('c-t4');
    expect(tasks.get('c-t4')).toBeUndefined();
    expect(tasks.all().find(t => t.id === 'c-t4')).toBeUndefined();
  });

  it('upsert with same id overwrites previous value', () => {
    tasks.upsert({ id: 'c-t5', title: 'First',  done: false, updated_at: 1000 });
    tasks.upsert({ id: 'c-t5', title: 'Second', done: true,  updated_at: 2000 });
    const t = tasks.get('c-t5');
    expect(t!.title).toBe('Second');
    expect(t!.done).toBe(true);
  });

  it('subscribe() fires immediately with current snapshot', () => {
    tasks.upsert({ id: 'c-sub1', title: 'SubTest', done: false, updated_at: 4000 });
    let received: Task[] | null = null;
    const unsub = tasks.subscribe(items => { received = items; });
    expect(received).not.toBeNull();
    expect(Array.isArray(received)).toBe(true);
    unsub();
  });

  it('subscribe() fires after upsert', () => {
    let callCount = 0;
    const unsub = tasks.subscribe(() => { callCount++; });
    const before = callCount;
    tasks.upsert({ id: 'c-sub2', title: 'AfterSub', done: false, updated_at: 5000 });
    expect(callCount).toBeGreaterThan(before);
    unsub();
  });

  it('unsubscribe stops notifications', () => {
    let count = 0;
    const unsub = tasks.subscribe(() => count++);
    const snap = count;
    unsub();
    tasks.upsert({ id: 'c-unsub', title: 'AfterUnsub', done: false, updated_at: 6000 });
    expect(count).toBe(snap); // no new calls after unsubscribe
  });
});

// ─── SyncController ───────────────────────────────────────────────────────────

describe('SyncController', () => {
  it('status returns a SyncStatus object', () => {
    const s = db.sync.status;
    expect(typeof s.pendingOps).toBe('number');
    expect(typeof s.syncState).toBe('string');
    expect(typeof s.connected).toBe('boolean');
  });

  it('state is a valid SyncState string', () => {
    const valid = ['idle', 'syncing', 'retrying', 'offline', 'paused'];
    expect(valid).toContain(db.sync.state);
  });

  it('pause() transitions to paused', () => {
    db.sync.pause();
    expect(db.sync.state).toBe('paused');
    db.sync.resume();
  });

  it('resume() after pause transitions away from paused', async () => {
    db.sync.pause();
    db.sync.resume();
    // After resume the state immediately becomes 'idle' then transitions
    await delay(50);
    expect(db.sync.state).not.toBe('paused');
  });

  it('setOffline / isOffline round-trips', () => {
    db.sync.setOffline(true);
    expect(db.sync.isOffline()).toBe(true);
    db.sync.setOffline(false);
    expect(db.sync.isOffline()).toBe(false);
  });

  it('setLatency / getLatency round-trips', () => {
    db.sync.setLatency(300);
    expect(db.sync.getLatency()).toBe(300);
    db.sync.setLatency(0);
    expect(db.sync.getLatency()).toBe(0);
  });

  it('force() resolves without error when online', async () => {
    await expect(db.sync.force()).resolves.toBeUndefined();
  });
});

// ─── Inspector ────────────────────────────────────────────────────────────────

describe('Inspector', () => {
  it('queue() returns an array', () => {
    expect(Array.isArray(db.inspect.queue())).toBe(true);
  });

  it('syncLog() returns an array', () => {
    expect(Array.isArray(db.inspect.syncLog())).toBe(true);
  });

  it('errors() returns an array', () => {
    expect(Array.isArray(db.inspect.errors())).toBe(true);
  });

  it('conflicts() returns an array', () => {
    expect(Array.isArray(db.inspect.conflicts())).toBe(true);
  });

  it('unresolvedConflicts() returns a number', () => {
    expect(typeof db.inspect.unresolvedConflicts()).toBe('number');
  });

  it('syncLog() has entries after force()', async () => {
    await db.sync.force();
    expect(db.inspect.syncLog().length).toBeGreaterThan(0);
  });

  it('queue() has items while ops are pending', async () => {
    db.sync.setOffline(true);
    const tasks = db.collection<Task>('tasks');
    tasks.upsert({ id: 'insp-q1', title: 'Pending', done: false, updated_at: Date.now() });
    expect(db.inspect.queue().length).toBeGreaterThan(0);
    db.sync.setOffline(false);
    await db.sync.force();
  });
});

// ─── Logger ───────────────────────────────────────────────────────────────────

describe('Logger', () => {
  let log: Logger;

  beforeEach(() => { log = new Logger(); });

  it('info() stores entry retrievable via getEntries()', () => {
    log.info('sync', 'test message', { key: 'val' });
    const entries = log.getEntries();
    expect(entries.length).toBe(1);
    expect(entries[0].message).toBe('test message');
    expect(entries[0].level).toBe('info');
    expect(entries[0].category).toBe('sync');
    expect(entries[0].meta).toEqual({ key: 'val' });
  });

  it('getEntries() filters by level', () => {
    log.info('sync',    'info msg');
    log.warn('network', 'warn msg');
    log.error('sdk',    'err msg');
    expect(log.getEntries({ level: 'warn' })).toHaveLength(1);
    expect(log.getEntries({ level: 'error' })).toHaveLength(1);
  });

  it('getEntries() filters by category', () => {
    log.info('sync',    'a');
    log.info('network', 'b');
    log.info('sdk',     'c');
    expect(log.getEntries({ category: 'network' })).toHaveLength(1);
  });

  it('getEntries() filters by search', () => {
    log.info('sync', 'needle in haystack');
    log.info('sync', 'nothing here');
    expect(log.getEntries({ search: 'needle' })).toHaveLength(1);
  });

  it('getEntries() respects limit', () => {
    for (let i = 0; i < 10; i++) log.info('sync', `msg ${i}`);
    expect(log.getEntries({ limit: 3 })).toHaveLength(3);
  });

  it('clear() empties the log', () => {
    log.info('sdk', 'before clear');
    log.clear();
    expect(log.getEntries()).toHaveLength(0);
  });

  it('onEntry() fires for each new entry and unsubscribes cleanly', () => {
    const received: string[] = [];
    const unsub = log.onEntry(e => received.push(e.message));
    log.info('sync', 'first');
    unsub();
    log.info('sync', 'second');
    expect(received).toEqual(['first']);
  });

  it('entries are ordered newest-first', () => {
    log.info('sdk', 'old');
    log.info('sdk', 'new');
    const entries = log.getEntries();
    expect(entries[0].message).toBe('new');
    expect(entries[1].message).toBe('old');
  });

  it('caps at 500 entries', () => {
    for (let i = 0; i < 510; i++) log.info('system', `msg ${i}`);
    expect(log.size()).toBe(500);
  });
});

// ─── EdgeDB integration ───────────────────────────────────────────────────────

describe('EdgeDB — end-to-end sync', () => {
  let db2: EdgeDB;

  beforeAll(async () => {
    db2 = new EdgeDB({
      nodeId:      'sdk-test-node-2',
      dbPath:      ':memory:',
      upstreamUrl: UPSTREAM,
    });
    db2.connect();
    await delay(100);
  });

  afterAll(() => db2.disconnect());

  it('write on db1, sync, then db2 sees it after its own sync', async () => {
    const tasks1 = db.collection<Task>('tasks');
    const tasks2 = db2.collection<Task>('tasks');

    tasks1.upsert({ id: 'e2e-1', title: 'CrossDevice', done: false, updated_at: Date.now() });
    await db.sync.force();   // db1 pushes to server
    await db2.sync.force();  // db2 pulls from server

    const found = tasks2.get('e2e-1');
    expect(found).toBeDefined();
    expect(found!.title).toBe('CrossDevice');
  });

  it('onStatusChange fires on status transitions', async () => {
    const states: string[] = [];
    const unsub = db.onStatusChange(s => states.push(s.syncState));
    await db.sync.force();
    unsub();
    expect(states.length).toBeGreaterThan(0);
  });
});
