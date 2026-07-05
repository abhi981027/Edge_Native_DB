import express from 'express';
import path from 'path';
import { DatabaseSync } from 'node:sqlite';
import { Change } from '../core/types';

/**
 * Sync Server — receives changes from edge devices, routes them to peers.
 *
 * ─── Idempotency ──────────────────────────────────────────────────────────────
 * Every change has a UUID (id). The server_changes table has UNIQUE(id).
 * INSERT OR IGNORE guarantees that re-delivering the same change is a silent
 * no-op. The duplicate_log captures occurrences for observability.
 *
 * ─── No ordering assumption ───────────────────────────────────────────────────
 * The server assigns its own monotonic seq (INTEGER PRIMARY KEY = SQLite rowid).
 * Client timestamps are stored for display and conflict resolution, but
 * delivery order uses server seq. A change timestamped T=100 can arrive after
 * T=200; seq ensures stable, total ordering for all receivers.
 *
 * ─── Per-device cursor ────────────────────────────────────────────────────────
 * device_cursors.last_sent_seq tracks the highest seq already delivered to each
 * device. On each POST /sync the server returns changes WHERE seq > cursor AND
 * device_id != requester. Cursor advances AFTER the response is built. If the
 * client crashes before applying, the next sync re-delivers the same batch —
 * safe because the client deduplicates on change.id.
 *
 * ─── Partial sync ─────────────────────────────────────────────────────────────
 * Each change is independently idempotent. Sending 3 of 10 pending changes is
 * fine — the remaining 7 arrive on the next sync cycle.
 */

type SyncRequest  = { device_id: string; changes: Change[] };
type SyncResponse = { changes: (Change & { seq: number })[]; meta: SyncMeta };
type SyncMeta     = { accepted: number; duplicates: number; outgoing: number; cursor: number };

export function createSyncServer(
  port: number,
  dbPath = ':memory:'
): { close: () => void } {
  const app = express();
  const db  = new DatabaseSync(dbPath);

  db.exec('PRAGMA journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS server_changes (
      seq         INTEGER PRIMARY KEY,         -- server-assigned delivery order
      id          TEXT    NOT NULL UNIQUE,      -- change UUID — idempotency key
      table_name  TEXT    NOT NULL,
      record_id   TEXT    NOT NULL,
      operation   TEXT    NOT NULL,
      timestamp   INTEGER NOT NULL,             -- client HLC wall time
      device_id   TEXT    NOT NULL,
      payload     TEXT,                         -- JSON delta, null for deletes
      received_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000)
    );

    CREATE TABLE IF NOT EXISTS device_cursors (
      device_id     TEXT    PRIMARY KEY,
      last_sent_seq INTEGER NOT NULL DEFAULT 0,
      first_seen    INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
      last_seen     INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
      sync_count    INTEGER NOT NULL DEFAULT 0
    );

    -- Captures every re-delivery of the same change id for observability.
    CREATE TABLE IF NOT EXISTS duplicate_log (
      id         TEXT    PRIMARY KEY,
      device_id  TEXT    NOT NULL,
      first_seen INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
      last_seen  INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
      count      INTEGER NOT NULL DEFAULT 1
    );

    CREATE INDEX IF NOT EXISTS idx_sc_device ON server_changes (device_id, seq);
    CREATE INDEX IF NOT EXISTS idx_sc_seq    ON server_changes (seq);
  `);

  // ─── Prepared statements ──────────────────────────────────────────────────

  const stmtInsertChange = db.prepare(`
    INSERT OR IGNORE INTO server_changes
      (id, table_name, record_id, operation, timestamp, device_id, payload)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const stmtCheckExists = db.prepare(
    'SELECT seq FROM server_changes WHERE id = ?'
  );

  const stmtLogDuplicate = db.prepare(`
    INSERT INTO duplicate_log (id, device_id, first_seen, last_seen, count)
    VALUES (?, ?, unixepoch() * 1000, unixepoch() * 1000, 1)
    ON CONFLICT (id) DO UPDATE SET
      last_seen = unixepoch() * 1000,
      count     = count + 1
  `);

  const stmtUpsertCursor = db.prepare(`
    INSERT INTO device_cursors (device_id, last_sent_seq, first_seen, last_seen, sync_count)
    VALUES (?, 0, unixepoch() * 1000, unixepoch() * 1000, 1)
    ON CONFLICT (device_id) DO UPDATE SET
      last_seen  = unixepoch() * 1000,
      sync_count = sync_count + 1
  `);

  const stmtGetCursor   = db.prepare('SELECT last_sent_seq FROM device_cursors WHERE device_id = ?');
  const stmtUpdateCursor = db.prepare('UPDATE device_cursors SET last_sent_seq = ? WHERE device_id = ?');
  const stmtMaxSeq       = db.prepare('SELECT MAX(seq) AS max_seq FROM server_changes');

  const stmtOutgoing = db.prepare(`
    SELECT * FROM server_changes
    WHERE seq > ? AND device_id != ?
    ORDER BY seq ASC
    LIMIT 500
  `);

  app.use(express.json({ limit: '8mb' }));

  // ─── Serve server dashboard ───────────────────────────────────────────────
  app.get('/', (_, res) => {
    res.sendFile(path.join(process.cwd(), 'public', 'server.html'));
  });

  // ─── Liveness probe ───────────────────────────────────────────────────────
  app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));

  // ─── POST /sync ───────────────────────────────────────────────────────────
  app.post('/sync', (req, res) => {
    const body = req.body as SyncRequest;

    if (!body.device_id || typeof body.device_id !== 'string') {
      res.status(400).json({ error: 'device_id required' });
      return;
    }
    if (!Array.isArray(body.changes)) {
      res.status(400).json({ error: 'changes must be an array' });
      return;
    }

    const { device_id, changes } = body;
    const stats: SyncMeta = { accepted: 0, duplicates: 0, outgoing: 0, cursor: 0 };

    // ── Step 1: register / touch device cursor ──────────────────────────────
    stmtUpsertCursor.run(device_id);

    // ── Step 2: store incoming changes atomically ───────────────────────────
    // All-or-nothing: if any insert fails due to a real error, roll back.
    // INSERT OR IGNORE handles duplicates silently; they are separately logged.
    db.exec('BEGIN');
    try {
      for (const c of changes) {
        if (!c || typeof c !== 'object') continue;
        if (!c.id || !c.tableName || !c.recordId || !c.operation) continue;

        const exists = stmtCheckExists.get(c.id);
        if (exists) {
          stmtLogDuplicate.run(c.id, device_id);
          stats.duplicates++;
        } else {
          stmtInsertChange.run(
            c.id,
            c.tableName,
            c.recordId,
            c.operation,
            c.timestamp ?? Date.now(),
            c.deviceId ?? device_id,
            c.payload != null ? JSON.stringify(c.payload) : null
          );
          stats.accepted++;
        }
      }
      db.exec('COMMIT');
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }

    // ── Step 3: build outgoing batch ────────────────────────────────────────
    const cursorRow = stmtGetCursor.get(device_id) as { last_sent_seq: number } | undefined;
    const cursor    = Number(cursorRow?.last_sent_seq ?? 0);

    const rows    = stmtOutgoing.all(cursor, device_id) as RawServerChange[];
    const outgoing = rows.map(rowToWireChange);
    stats.outgoing = outgoing.length;

    // ── Step 4: advance cursor to current server max ─────────────────────────
    // Advancing AFTER building the response means a crash before we send still
    // re-delivers the same batch next time (safe — client deduplicates on id).
    const maxSeqRow  = stmtMaxSeq.get() as { max_seq: number | null };
    const newCursor  = maxSeqRow.max_seq ?? cursor;
    stats.cursor     = newCursor;
    stmtUpdateCursor.run(newCursor, device_id);

    const response: SyncResponse = { changes: outgoing, meta: stats };
    res.json(response);
  });

  // ─── Admin API (server dashboard) ─────────────────────────────────────────

  app.get('/api/stats', (_, res) => {
    const total      = num(db.prepare('SELECT COUNT(*) AS c FROM server_changes').get());
    const devices    = num(db.prepare('SELECT COUNT(*) AS c FROM device_cursors').get());
    const duplicates = num(db.prepare('SELECT COUNT(*) AS c FROM duplicate_log').get());
    const dupEvents  = num(db.prepare('SELECT SUM(count) AS c FROM duplicate_log').get());

    // Count changes that haven't been delivered to at least one device
    const pendingRow = db.prepare(`
      SELECT COUNT(DISTINCT sc.id) AS c
      FROM server_changes sc
      JOIN device_cursors dc ON dc.device_id != sc.device_id
      WHERE sc.seq > dc.last_sent_seq
    `).get() as { c: number } | undefined;

    res.json({
      total,
      devices,
      duplicateChangeIds: duplicates,
      duplicateEvents: dupEvents,
      pendingDeliveries: pendingRow ? Number(pendingRow.c) : 0,
    });
  });

  app.get('/api/changes', (req, res) => {
    const limit    = Math.min(parseInt(String(req.query['limit']  ?? '100'), 10), 500);
    const device   = req.query['device']    as string | undefined;
    const op       = req.query['operation'] as string | undefined;
    const table    = req.query['table']     as string | undefined;

    let sql  = 'SELECT * FROM server_changes WHERE 1=1';
    const params: (string | number)[] = [];
    if (device) { sql += ' AND device_id = ?'; params.push(device); }
    if (op)     { sql += ' AND operation = ?'; params.push(op); }
    if (table)  { sql += ' AND table_name = ?'; params.push(table); }
    sql += ' ORDER BY seq DESC LIMIT ?';
    params.push(limit);

    const rows = db.prepare(sql).all(...params) as RawServerChange[];
    res.json(rows.map(rowToWireChange));
  });

  app.get('/api/devices', (_, res) => {
    const rows = db.prepare(`
      SELECT dc.*,
        (SELECT COUNT(*) FROM server_changes WHERE device_id = dc.device_id) AS total_changes,
        (SELECT COUNT(*) FROM server_changes WHERE seq > dc.last_sent_seq
                            AND device_id != dc.device_id)                    AS pending_inbound
      FROM device_cursors dc
      ORDER BY last_seen DESC
    `).all() as unknown[];
    res.json(rows.map(r => {
      const d = r as Record<string, unknown>;
      return {
        deviceId:       d['device_id'],
        lastSentSeq:    Number(d['last_sent_seq']),
        firstSeen:      Number(d['first_seen']),
        lastSeen:       Number(d['last_seen']),
        syncCount:      Number(d['sync_count']),
        totalChanges:   Number(d['total_changes']),
        pendingInbound: Number(d['pending_inbound']),
      };
    }));
  });

  app.get('/api/duplicates', (req, res) => {
    const limit = Math.min(parseInt(String(req.query['limit'] ?? '50'), 10), 200);
    const rows  = db.prepare(
      'SELECT * FROM duplicate_log ORDER BY last_seen DESC LIMIT ?'
    ).all(limit) as unknown[];
    res.json(rows.map(r => {
      const d = r as Record<string, unknown>;
      return {
        id:         d['id'],
        deviceId:   d['device_id'],
        firstSeen:  Number(d['first_seen']),
        lastSeen:   Number(d['last_seen']),
        count:      Number(d['count']),
      };
    }));
  });

  const server = app.listen(port, () => {
    console.log(`[sync-server] :${port}  dashboard → http://localhost:${port}`);
  });

  return { close: () => server.close() };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function num(row: unknown): number {
  return row ? Number((row as Record<string, unknown>)['c'] ?? 0) : 0;
}

type RawServerChange = {
  seq: number; id: string;
  table_name: string; record_id: string; operation: string;
  timestamp: number; device_id: string;
  payload: string | null; received_at: number;
};

function rowToWireChange(r: RawServerChange) {
  return {
    seq:        Number(r.seq),
    id:         r.id,
    tableName:  r.table_name,
    recordId:   r.record_id,
    operation:  r.operation,
    timestamp:  Number(r.timestamp),
    deviceId:   r.device_id,
    payload:    r.payload ? JSON.parse(r.payload) as Record<string, unknown> : null,
    receivedAt: Number(r.received_at),
    synced:     1,
  };
}
