// ─── Hybrid Logical Clock ────────────────────────────────────────────────────

export type HLC = {
  wallTime: number; // ms since epoch (physical)
  logical: number;  // counter for same-millisecond events
  nodeId: string;   // tiebreaker — disambiguates equal HLCs from different nodes
};

// ─── Operations ──────────────────────────────────────────────────────────────

export type OperationType = 'insert' | 'update' | 'delete';

export type Operation = {
  id: string;           // UUID — doubles as idempotency key on the wire
  hlc: HLC;             // causal timestamp
  entityType: string;   // e.g. 'sensor_reading', 'config', 'order'
  entityId: string;     // stable identifier of the target entity
  type: OperationType;
  payload: Record<string, unknown> | null; // null on delete
  nodeId: string;       // which device created this op
  synced: boolean;      // has this been acknowledged by upstream?
};

// ─── Entities ────────────────────────────────────────────────────────────────

export type Entity = {
  id: string;
  type: string;
  data: Record<string, unknown>;
  hlc: HLC;          // timestamp of last successful write
  tombstone: boolean; // soft-delete flag; physically prunable later
};

// ─── Change tracking ─────────────────────────────────────────────────────────

export type Change = {
  id: string;         // same UUID as the originating Operation (idempotency key)
  tableName: string;  // entity type / table name
  recordId: string;   // which record changed
  operation: string;  // insert | update | delete
  timestamp: number;  // epoch ms (op HLC wall time)
  deviceId: string;   // which device made the change
  synced: number;     // 0 = pending upload, 1 = acknowledged by upstream
  payload?: Record<string, unknown> | null; // data delta, included on the sync wire
};

export type ChangeFilter = 'all' | 'synced' | 'unsynced';

// ─── Sync queue (per-item lifecycle) ────────────────────────────────────────

export type QueueItemStatus = 'queued' | 'sending' | 'sent' | 'failed';

/**
 * In-memory representation of one op's journey through the sync queue.
 * The durable store is SQLite (oplog); this adds observable lifecycle state.
 */
export type QueueItem = {
  opId:        string;
  entityType:  string;
  entityId:    string;
  operation:   string;         // insert | update | delete
  queuedAt:    number;         // HLC wall time of the original write
  attempts:    number;         // how many send attempts have been made
  lastAttempt: number | null;  // epoch ms of last attempt
  lastError:   string | null;  // message from the last failed attempt
  status:      QueueItemStatus;
};

// ─── Conflict tracking ────────────────────────────────────────────────────────

/** A snapshot of one side of a conflict (local or remote). */
export type ConflictVersion = {
  hlc:      HLC;
  data:     Record<string, unknown> | null;
  deviceId: string;
};

/**
 * Logged every time a concurrent write is detected during sync.
 *
 * winner values:
 *   'local'   — LWW kept local version (remote was older)
 *   'remote'  — LWW kept remote version (remote was newer)
 *   'merged'  — CRDT field-level merge was applied (both sides partially kept)
 *   'manual'  — user applied a hand-crafted resolution from the UI
 *
 * mergedVersion is populated when winner = 'merged' | 'manual'.
 */
export type Conflict = {
  id:             string;
  recordId:       string;
  tableName:      string;
  localVersion:   ConflictVersion;
  remoteVersion:  ConflictVersion;
  mergedVersion?: ConflictVersion;              // set when CRDT or manual merge applied
  winner:         'local' | 'remote' | 'merged' | 'manual';
  detectedAt:     number;                       // epoch ms
  resolved:       boolean;
};

// ─── Sync state ───────────────────────────────────────────────────────────────

/**
 * State machine for the sync cycle:
 *
 *   idle ──── timer / forceSync ────► syncing
 *             ◄── success ──────────    │
 *             ◄── network down ────── offline
 *             ◄── retryTimer ──────── retrying ◄── all-HTTP-retries-failed ──┘
 *
 *   pause() always → paused
 *   resume() → idle
 */
export type SyncState = 'idle' | 'syncing' | 'retrying' | 'offline' | 'paused';

export type SyncAttempt = {
  id: number;            // monotonic counter
  startedAt: number;     // epoch ms
  completedAt: number | null;
  durationMs: number | null;
  sent: number;          // ops pushed upstream
  received: number;      // remote ops applied
  success: boolean;
  error: string | null;
};

export type SyncError = {
  at: number;        // epoch ms
  message: string;
  attempt: number;   // which cycle attempt number
  pendingOps: number;
};

export type SyncStatus = {
  nodeId: string;
  connected: boolean;
  pendingOps: number;
  lastSyncAt: number | null; // epoch ms
  retryCount: number;        // consecutive cycle-level failures
  syncing: boolean;
  syncState: SyncState;
  nextRetryAt: number | null; // epoch ms when retry fires, null if not retrying
};

// ─── Storage interface ───────────────────────────────────────────────────────
// Abstraction lets us swap SQLite for IndexedDB, LevelDB, etc.

export interface StorageAdapter {
  // Entity store
  getEntity(type: string, id: string): Entity | undefined;
  upsertEntity(entity: Entity): void;
  listEntities(type: string): Entity[];

  // Atomic write — oplog + changes + entity in one SQLite transaction
  commitWrite(op: Operation, entity: Entity | null): void;

  // Operation log (append-only, idempotent inserts)
  appendOp(op: Operation): void;
  getPendingOps(): Operation[];
  markOpSynced(id: string): void;
  getOpById(id: string): Operation | undefined;
  listOps(limit?: number): Operation[];
  listAllOps(): Operation[];

  // Change tracking
  getChanges(filter: ChangeFilter, limit?: number): Change[];
  markChangeSynced(id: string): void;
  markChangesSynced(ids: string[]): void;

  // Conflict log
  logConflict(conflict: Conflict): void;
  getConflicts(resolvedFilter?: boolean | null, limit?: number): Conflict[];
  markConflictResolved(id: string): void;
  getUnresolvedConflictCount(): number;

  // Crash/demo helpers (not part of normal write path)
  eraseEntityStore(type: string, id: string): void;

  // Sync cursor — tracks how far we've read from upstream
  getLastSeenSequence(): number;
  setLastSeenSequence(seq: number): void;
}

// ─── Network interface ───────────────────────────────────────────────────────
// Adapter pattern — swap HTTP for WebSocket, MQTT, BLE without changing engine

export interface NetworkAdapter {
  push(ops: Operation[]): Promise<PushResult>;
  pull(since: number): Promise<PullResult>;
  isAvailable(): Promise<boolean>;
  // Optional combined push+pull in one round-trip (more efficient for constrained links).
  // When present, the engine uses this instead of separate push() + pull() calls.
  sync?(ops: Operation[]): Promise<SyncResult>;
}

export type SyncResult = {
  accepted: string[];    // op IDs acknowledged by server (all sent ops on HTTP 200)
  remoteOps: Operation[]; // ops from other devices to apply locally
};

export type PushResult = {
  accepted: string[]; // op IDs the server successfully stored
  rejected: string[]; // op IDs the server rejected (validation failures, etc.)
};

export type PullResult = {
  ops: Operation[];
  sequence: number; // highest sequence number in this batch
};
