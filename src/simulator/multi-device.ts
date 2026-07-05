import { SyncEngine } from '../core/engine';
import { SQLiteStorage } from '../core/storage';
import { HttpNetworkAdapter } from '../network/http-adapter';
import { OfflineSimulator } from '../network/adapter';
import { UserRepository } from '../core/users';
import { QueueItem, SyncAttempt, SyncStatus } from '../core/types';

export type DeviceEventKind =
  | 'sync_ok' | 'sync_fail' | 'write' | 'conflict' | 'merge'
  | 'offline'  | 'online';

export type DeviceEvent = {
  id:       number;
  at:       number;
  deviceId: string;
  kind:     DeviceEventKind;
  detail:   string;
  sent?:    number;
  received?: number;
};

export type DeviceSnapshot = {
  id:                  string;
  status:              SyncStatus;
  users:               { id: string; name: string; updated_at: number; device_id: string }[];
  queue:               QueueItem[];
  offline:             boolean;
  latencyMs:           number;
  unresolvedConflicts: number;
  syncLog:             SyncAttempt[];
};

type Slot = {
  engine:    SyncEngine;
  simulator: OfflineSimulator;
  users:     UserRepository;
};

export class MultiDeviceSimulator {
  private readonly slots = new Map<string, Slot>();

  private eventLog: DeviceEvent[]    = [];
  private seq                        = 0;
  private readonly MAX_EVENTS        = 300;
  private readonly subscribers       = new Set<(e: DeviceEvent) => void>();

  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private readonly lastSyncId        = new Map<string, number>();
  private readonly lastConflictCount = new Map<string, number>();

  constructor(
    deviceIds: string[],
    private readonly upstreamUrl: string,
  ) {
    for (const id of deviceIds) {
      const storage  = new SQLiteStorage(':memory:');
      const http     = new HttpNetworkAdapter(upstreamUrl, id);
      const sim      = new OfflineSimulator(http);
      const engine   = new SyncEngine(id, storage, sim);
      const users    = new UserRepository(engine);
      this.slots.set(id, { engine, simulator: sim, users });
    }
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  start(): void {
    for (const s of this.slots.values()) s.engine.start();
    this.pollTimer = setInterval(() => this.poll(), 600);
  }

  stop(): void {
    for (const s of this.slots.values()) s.engine.stop();
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
  }

  // ─── Device control ────────────────────────────────────────────────────────

  deviceIds(): string[] { return [...this.slots.keys()]; }

  getEngine(id: string): SyncEngine | undefined {
    return this.slots.get(id)?.engine;
  }

  setOffline(deviceId: string, offline: boolean): void {
    const s = this.slots.get(deviceId);
    if (!s) return;
    s.simulator.setOffline(offline);
    this.emit({ deviceId, kind: offline ? 'offline' : 'online',
      detail: offline ? 'went offline' : 'reconnected' });
  }

  setLatency(deviceId: string, ms: number): void {
    this.slots.get(deviceId)?.simulator.setLatency(ms);
  }

  isOffline(deviceId: string): boolean {
    return this.slots.get(deviceId)?.simulator.isSimulatingOffline() ?? false;
  }

  getLatency(deviceId: string): number {
    return this.slots.get(deviceId)?.simulator.getLatency() ?? 0;
  }

  // ─── Writes ────────────────────────────────────────────────────────────────

  writeUser(deviceId: string, userId: string, name: string): void {
    const s = this.slots.get(deviceId);
    if (!s) return;
    s.users.createUser({ id: userId, name, updated_at: Date.now(), device_id: deviceId });
    this.emit({ deviceId, kind: 'write', detail: `write ${userId} → "${name}"` });
  }

  updateUser(deviceId: string, userId: string, name: string): void {
    const s = this.slots.get(deviceId);
    if (!s) return;
    s.users.updateUser(userId, { name, updated_at: Date.now() });
    this.emit({ deviceId, kind: 'write', detail: `update ${userId} → "${name}"` });
  }

  // ─── Sync ──────────────────────────────────────────────────────────────────

  async forceSync(deviceId: string): Promise<void> {
    await this.slots.get(deviceId)?.engine.forceSync();
  }

  async syncAll(): Promise<void> {
    await Promise.all([...this.slots.values()].map(s => s.engine.forceSync()));
  }

  // ─── Conflict resolution ───────────────────────────────────────────────────

  autoMergeAll(deviceId: string): number {
    const engine = this.slots.get(deviceId)?.engine;
    if (!engine) return 0;
    const unresolved = engine.getConflicts(false, 100);
    let merged = 0;
    for (const c of unresolved) {
      if (engine.applyAutoMerge(c.id).ok) merged++;
    }
    if (merged > 0)
      this.emit({ deviceId, kind: 'merge', detail: `auto-merged ${merged} conflict(s)` });
    return merged;
  }

  // ─── Snapshots ─────────────────────────────────────────────────────────────

  getSnapshot(deviceId: string): DeviceSnapshot | null {
    const s = this.slots.get(deviceId);
    if (!s) return null;
    return {
      id:                  deviceId,
      status:              s.engine.getStatus(),
      users:               s.users.getAllUsers(),
      queue:               s.engine.getSyncQueue(),
      offline:             s.simulator.isSimulatingOffline(),
      latencyMs:           s.simulator.getLatency(),
      unresolvedConflicts: s.engine.getUnresolvedConflictCount(),
      syncLog:             s.engine.getSyncLog().slice(0, 5),
    };
  }

  getAllSnapshots(): DeviceSnapshot[] {
    return this.deviceIds().map(id => this.getSnapshot(id)!);
  }

  getConflicts(deviceId: string) {
    return this.slots.get(deviceId)?.engine.getConflicts(null, 50) ?? [];
  }

  getAllConflicts(): (ReturnType<SyncEngine['getConflicts']>[0] & { deviceId: string })[] {
    return this.deviceIds().flatMap(id =>
      this.getConflicts(id).map(c => ({ ...c, deviceId: id }))
    );
  }

  // ─── Events ────────────────────────────────────────────────────────────────

  getEvents(limit = 80): DeviceEvent[] { return this.eventLog.slice(0, limit); }

  onEvent(fn: (e: DeviceEvent) => void): () => void {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  // ─── Seed ──────────────────────────────────────────────────────────────────

  async seedAll(users: { id: string; name: string }[]): Promise<void> {
    // Seed only one device first, then sync-all so the server has the data,
    // then sync all other devices. This avoids three "insert" conflicts.
    const [first, ...rest] = this.deviceIds();
    if (!first) return;
    const s = this.slots.get(first)!;
    for (const u of users) {
      s.users.createUser({ id: u.id, name: u.name, updated_at: Date.now(), device_id: first });
    }
    await s.engine.forceSync();                // push to server
    await Promise.all(rest.map(id => this.slots.get(id)!.engine.forceSync())); // pull
  }

  resetAll(): void {
    for (const [id, s] of this.slots) {
      s.simulator.setOffline(false);
      s.simulator.setLatency(0);
    }
    this.emit({ deviceId: 'all', kind: 'online', detail: 'simulation reset' });
  }

  // ─── Private ───────────────────────────────────────────────────────────────

  private poll(): void {
    for (const [id, s] of this.slots) {
      // Detect new sync attempts via sync log
      const log    = s.engine.getSyncLog();
      const lastId = this.lastSyncId.get(id) ?? 0;
      if (log.length && log[0].id > lastId) {
        const fresh = log.filter(a => a.id > lastId).reverse();
        this.lastSyncId.set(id, log[0].id);
        for (const a of fresh) {
          this.emit({
            deviceId: id,
            kind:     a.success ? 'sync_ok' : 'sync_fail',
            detail:   a.success
              ? `↑ ${a.sent} · ↓ ${a.received}`
              : (a.error ?? 'error').slice(0, 80),
            sent:     a.sent,
            received: a.received,
          });
        }
      }

      // Detect new conflicts
      const cc   = s.engine.getUnresolvedConflictCount();
      const prev = this.lastConflictCount.get(id) ?? 0;
      if (cc > prev)
        this.emit({ deviceId: id, kind: 'conflict', detail: `⚔ ${cc - prev} new conflict(s)` });
      this.lastConflictCount.set(id, cc);
    }
  }

  private emit(data: Omit<DeviceEvent, 'id' | 'at'>): void {
    const e: DeviceEvent = { id: ++this.seq, at: Date.now(), ...data };
    this.eventLog.unshift(e);
    if (this.eventLog.length > this.MAX_EVENTS) this.eventLog.pop();
    this.subscribers.forEach(fn => fn(e));
  }
}
