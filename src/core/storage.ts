import { DatabaseSync, StatementSync } from 'node:sqlite';
import { Change, ChangeFilter, Conflict, ConflictVersion, Entity, Operation, StorageAdapter } from './types';

/**
 * SQLite storage — WAL mode, atomic multi-table writes, idempotent inserts.
 *
 * Write path (happy path):
 *   BEGIN
 *     INSERT OR IGNORE INTO oplog     ← durable op record
 *     INSERT OR IGNORE INTO changes   ← audit / change-tracking log
 *     INSERT … ON CONFLICT DO UPDATE  ← entity store (LWW)
 *   COMMIT
 *
 * If the process dies between BEGIN and COMMIT, SQLite's WAL rolls the
 * partial transaction back on the next open. The DB is always consistent.
 *
 * Idempotency: both oplog and changes use INSERT OR IGNORE keyed on op.id
 * (a UUID). Re-applying the same op is always a safe no-op.
 */
export class SQLiteStorage implements StorageAdapter {
  private db: DatabaseSync;

  constructor(dbPath: string) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA foreign_keys = ON');
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        id          TEXT NOT NULL,
        type        TEXT NOT NULL,
        data        TEXT NOT NULL,
        hlc_wall    INTEGER NOT NULL,
        hlc_logical INTEGER NOT NULL,
        hlc_node    TEXT NOT NULL,
        tombstone   INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (type, id)
      );

      CREATE TABLE IF NOT EXISTS oplog (
        id          TEXT PRIMARY KEY,
        hlc_wall    INTEGER NOT NULL,
        hlc_logical INTEGER NOT NULL,
        hlc_node    TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id   TEXT NOT NULL,
        op_type     TEXT NOT NULL,
        payload     TEXT,
        node_id     TEXT NOT NULL,
        synced      INTEGER NOT NULL DEFAULT 0,
        created_at  INTEGER NOT NULL DEFAULT (unixepoch() * 1000)
      );

      CREATE TABLE IF NOT EXISTS changes (
        id          TEXT PRIMARY KEY,
        table_name  TEXT NOT NULL,
        record_id   TEXT NOT NULL,
        operation   TEXT NOT NULL,
        timestamp   INTEGER NOT NULL,
        device_id   TEXT NOT NULL,
        payload     TEXT,
        synced      INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS conflicts (
        id              TEXT PRIMARY KEY,
        record_id       TEXT NOT NULL,
        table_name      TEXT NOT NULL,
        local_version   TEXT NOT NULL,   -- JSON: { hlc, data, deviceId }
        remote_version  TEXT NOT NULL,   -- JSON: { hlc, data, deviceId }
        merged_version  TEXT,            -- JSON: set when winner='merged'|'manual'
        winner          TEXT NOT NULL,   -- 'local' | 'remote' | 'merged' | 'manual'
        detected_at     INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
        resolved        INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS sync_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_oplog_pending    ON oplog (synced, created_at);
      CREATE INDEX IF NOT EXISTS idx_oplog_hlc        ON oplog (hlc_wall, hlc_logical);
      CREATE INDEX IF NOT EXISTS idx_entities_type    ON entities (type, tombstone);
      CREATE INDEX IF NOT EXISTS idx_changes_synced   ON changes (synced, timestamp);
      CREATE INDEX IF NOT EXISTS idx_changes_table    ON changes (table_name, record_id);
      CREATE INDEX IF NOT EXISTS idx_conflicts_record ON conflicts (record_id, table_name);
      CREATE INDEX IF NOT EXISTS idx_conflicts_unres  ON conflicts (resolved, detected_at);
    `);

    // Additive migrations for existing DBs
    try { this.db.exec('ALTER TABLE changes   ADD COLUMN payload        TEXT'); } catch { /* already exists */ }
    try { this.db.exec('ALTER TABLE conflicts ADD COLUMN merged_version TEXT'); } catch { /* already exists */ }
  }

  // ─── Atomic write ─────────────────────────────────────────────────────────

  /**
   * The only correct path for local mutations.
   *
   * Wraps three writes in one transaction:
   *   1. oplog entry   (idempotent via INSERT OR IGNORE)
   *   2. changes entry (idempotent via INSERT OR IGNORE)
   *   3. entity upsert (LWW: skipped if stored HLC ≥ incoming HLC)
   *
   * Pass entity=null for delete ops (tombstone is handled by the caller
   * constructing a tombstoned entity before calling this).
   */
  commitWrite(op: Operation, entity: Entity | null): void {
    this.db.exec('BEGIN');
    try {
      this.runInsertOp(op);
      this.runInsertChange(op);
      if (entity) {
        this.runUpsertEntity(entity);
      }
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  // ─── Entity store ─────────────────────────────────────────────────────────

  getEntity(type: string, id: string): Entity | undefined {
    const row = this.prepare(
      'SELECT * FROM entities WHERE type = ? AND id = ?'
    ).get(type, id) as RawEntity | undefined;
    return row ? rowToEntity(row) : undefined;
  }

  /** Public upsert — used by pull path (remote ops; not wrapped in commitWrite). */
  upsertEntity(entity: Entity): void {
    this.runUpsertEntity(entity);
  }

  listEntities(type: string): Entity[] {
    const rows = this.prepare(
      'SELECT * FROM entities WHERE type = ? AND tombstone = 0'
    ).all(type) as RawEntity[];
    return rows.map(rowToEntity);
  }

  // ─── Operation log ────────────────────────────────────────────────────────

  appendOp(op: Operation): void {
    this.runInsertOp(op);
  }

  getPendingOps(): Operation[] {
    const rows = this.prepare(
      'SELECT * FROM oplog WHERE synced = 0 ORDER BY hlc_wall ASC, hlc_logical ASC'
    ).all() as RawOp[];
    return rows.map(rowToOp);
  }

  markOpSynced(id: string): void {
    this.prepare('UPDATE oplog SET synced = 1 WHERE id = ?').run(id);
  }

  getOpById(id: string): Operation | undefined {
    const row = this.prepare(
      'SELECT * FROM oplog WHERE id = ?'
    ).get(id) as RawOp | undefined;
    return row ? rowToOp(row) : undefined;
  }

  listOps(limit = 50): Operation[] {
    const rows = this.prepare(
      'SELECT * FROM oplog ORDER BY hlc_wall DESC, hlc_logical DESC LIMIT ?'
    ).all(limit) as RawOp[];
    return rows.map(rowToOp);
  }

  listAllOps(): Operation[] {
    const rows = this.prepare(
      'SELECT * FROM oplog ORDER BY hlc_wall ASC, hlc_logical ASC'
    ).all() as RawOp[];
    return rows.map(rowToOp);
  }

  // ─── Change tracking ──────────────────────────────────────────────────────

  getChanges(filter: ChangeFilter = 'all', limit = 50): Change[] {
    const sql =
      filter === 'synced'
        ? 'SELECT * FROM changes WHERE synced = 1 ORDER BY timestamp DESC LIMIT ?'
        : filter === 'unsynced'
        ? 'SELECT * FROM changes WHERE synced = 0 ORDER BY timestamp DESC LIMIT ?'
        : 'SELECT * FROM changes ORDER BY timestamp DESC LIMIT ?';

    const rows = this.prepare(sql).all(limit) as RawChange[];
    return rows.map(rowToChange);
  }

  markChangeSynced(id: string): void {
    this.prepare('UPDATE changes SET synced = 1 WHERE id = ?').run(id);
  }

  markChangesSynced(ids: string[]): void {
    if (ids.length === 0) return;
    // SQLite has no array binding; batch via individual updates in one transaction
    this.db.exec('BEGIN');
    try {
      const stmt = this.prepare('UPDATE changes SET synced = 1 WHERE id = ?');
      for (const id of ids) stmt.run(id);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  // ─── Conflict log ─────────────────────────────────────────────────────────

  logConflict(conflict: Conflict): void {
    this.prepare(
      `INSERT OR REPLACE INTO conflicts
         (id, record_id, table_name, local_version, remote_version, merged_version,
          winner, detected_at, resolved)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      conflict.id,
      conflict.recordId,
      conflict.tableName,
      JSON.stringify(conflict.localVersion),
      JSON.stringify(conflict.remoteVersion),
      conflict.mergedVersion ? JSON.stringify(conflict.mergedVersion) : null,
      conflict.winner,
      conflict.detectedAt,
      conflict.resolved ? 1 : 0
    );
  }

  getConflicts(resolvedFilter: boolean | null = null, limit = 50): Conflict[] {
    let sql = 'SELECT * FROM conflicts';
    const params: (number | string)[] = [];
    if (resolvedFilter === true)  { sql += ' WHERE resolved = 1'; }
    if (resolvedFilter === false) { sql += ' WHERE resolved = 0'; }
    sql += ' ORDER BY detected_at DESC LIMIT ?';
    params.push(limit);

    const rows = this.prepare(sql).all(...params) as RawConflict[];
    return rows.map(rowToConflict);
  }

  markConflictResolved(id: string): void {
    this.prepare('UPDATE conflicts SET resolved = 1 WHERE id = ?').run(id);
  }

  getUnresolvedConflictCount(): number {
    const row = this.prepare('SELECT COUNT(*) AS c FROM conflicts WHERE resolved = 0').get() as { c: number };
    return Number(row.c);
  }

  // ─── Sync cursor ──────────────────────────────────────────────────────────

  getLastSeenSequence(): number {
    const row = this.prepare(
      "SELECT value FROM sync_meta WHERE key = 'last_seen_seq'"
    ).get() as { value: string } | undefined;
    return row ? parseInt(row.value, 10) : 0;
  }

  setLastSeenSequence(seq: number): void {
    this.prepare(
      `INSERT INTO sync_meta (key, value) VALUES ('last_seen_seq', ?)
       ON CONFLICT (key) DO UPDATE SET value = excluded.value`
    ).run(String(seq));
  }

  // ─── Crash/demo helpers ───────────────────────────────────────────────────

  eraseEntityStore(type: string, id: string): void {
    this.prepare('DELETE FROM entities WHERE type = ? AND id = ?').run(type, id);
  }

  // ─── Private SQL primitives ───────────────────────────────────────────────

  private runInsertOp(op: Operation): void {
    this.prepare(
      `INSERT OR IGNORE INTO oplog
         (id, hlc_wall, hlc_logical, hlc_node, entity_type, entity_id,
          op_type, payload, node_id, synced)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      op.id,
      op.hlc.wallTime, op.hlc.logical, op.hlc.nodeId,
      op.entityType, op.entityId, op.type,
      op.payload ? JSON.stringify(op.payload) : null,
      op.nodeId,
      op.synced ? 1 : 0
    );
  }

  private runInsertChange(op: Operation): void {
    this.prepare(
      `INSERT OR IGNORE INTO changes
         (id, table_name, record_id, operation, timestamp, device_id, payload, synced)
       VALUES (?, ?, ?, ?, ?, ?, ?, 0)`
    ).run(
      op.id,
      op.entityType,
      op.entityId,
      op.type,
      op.hlc.wallTime,
      op.nodeId,
      op.payload ? JSON.stringify(op.payload) : null
    );
  }

  private runUpsertEntity(entity: Entity): void {
    this.prepare(
      `INSERT INTO entities (id, type, data, hlc_wall, hlc_logical, hlc_node, tombstone)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (type, id) DO UPDATE SET
         data        = excluded.data,
         hlc_wall    = excluded.hlc_wall,
         hlc_logical = excluded.hlc_logical,
         hlc_node    = excluded.hlc_node,
         tombstone   = excluded.tombstone
       WHERE excluded.hlc_wall > hlc_wall
          OR (excluded.hlc_wall = hlc_wall AND excluded.hlc_logical > hlc_logical)
          OR (excluded.hlc_wall = hlc_wall AND excluded.hlc_logical = hlc_logical
              AND excluded.hlc_node > hlc_node)`
    ).run(
      entity.id,
      entity.type,
      JSON.stringify(entity.data),
      entity.hlc.wallTime, entity.hlc.logical, entity.hlc.nodeId,
      entity.tombstone ? 1 : 0
    );
  }

  private stmtCache = new Map<string, StatementSync>();

  private prepare(sql: string): StatementSync {
    let stmt = this.stmtCache.get(sql);
    if (!stmt) {
      stmt = this.db.prepare(sql);
      this.stmtCache.set(sql, stmt);
    }
    return stmt;
  }
}

// ─── Row mappers ─────────────────────────────────────────────────────────────

type RawEntity = {
  id: string; type: string; data: string;
  hlc_wall: number; hlc_logical: number; hlc_node: string;
  tombstone: number;
};

type RawOp = {
  id: string;
  hlc_wall: number; hlc_logical: number; hlc_node: string;
  entity_type: string; entity_id: string; op_type: string;
  payload: string | null; node_id: string; synced: number;
};

type RawChange = {
  id: string; table_name: string; record_id: string;
  operation: string; timestamp: number; device_id: string;
  payload: string | null; synced: number;
};

function rowToEntity(r: RawEntity): Entity {
  return {
    id: r.id,
    type: r.type,
    data: JSON.parse(r.data) as Record<string, unknown>,
    hlc: { wallTime: Number(r.hlc_wall), logical: Number(r.hlc_logical), nodeId: r.hlc_node },
    tombstone: Number(r.tombstone) === 1,
  };
}

function rowToOp(r: RawOp): Operation {
  return {
    id: r.id,
    hlc: { wallTime: Number(r.hlc_wall), logical: Number(r.hlc_logical), nodeId: r.hlc_node },
    entityType: r.entity_type,
    entityId: r.entity_id,
    type: r.op_type as Operation['type'],
    payload: r.payload ? (JSON.parse(r.payload) as Record<string, unknown>) : null,
    nodeId: r.node_id,
    synced: Number(r.synced) === 1,
  };
}

function rowToChange(r: RawChange): Change {
  return {
    id: r.id,
    tableName: r.table_name,
    recordId: r.record_id,
    operation: r.operation,
    timestamp: Number(r.timestamp),
    deviceId: r.device_id,
    payload: r.payload ? (JSON.parse(r.payload) as Record<string, unknown>) : undefined,
    synced: Number(r.synced),
  };
}

type RawConflict = {
  id: string; record_id: string; table_name: string;
  local_version: string; remote_version: string;
  merged_version: string | null;
  winner: string; detected_at: number; resolved: number;
};

function rowToConflict(r: RawConflict): Conflict {
  return {
    id:             r.id,
    recordId:       r.record_id,
    tableName:      r.table_name,
    localVersion:   JSON.parse(r.local_version)  as ConflictVersion,
    remoteVersion:  JSON.parse(r.remote_version) as ConflictVersion,
    mergedVersion:  r.merged_version ? JSON.parse(r.merged_version) as ConflictVersion : undefined,
    winner:         r.winner as Conflict['winner'],
    detectedAt:     Number(r.detected_at),
    resolved:       Number(r.resolved) === 1,
  };
}
