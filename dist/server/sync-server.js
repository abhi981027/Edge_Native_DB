"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSyncServer = createSyncServer;
const express_1 = __importDefault(require("express"));
const path_1 = __importDefault(require("path"));
const node_sqlite_1 = require("node:sqlite");
function createSyncServer(port, dbPath = ':memory:') {
    const app = (0, express_1.default)();
    const db = new node_sqlite_1.DatabaseSync(dbPath);
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
    const stmtCheckExists = db.prepare('SELECT seq FROM server_changes WHERE id = ?');
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
    const stmtGetCursor = db.prepare('SELECT last_sent_seq FROM device_cursors WHERE device_id = ?');
    const stmtUpdateCursor = db.prepare('UPDATE device_cursors SET last_sent_seq = ? WHERE device_id = ?');
    const stmtMaxSeq = db.prepare('SELECT MAX(seq) AS max_seq FROM server_changes');
    const stmtOutgoing = db.prepare(`
    SELECT * FROM server_changes
    WHERE seq > ? AND device_id != ?
    ORDER BY seq ASC
    LIMIT 500
  `);
    app.use(express_1.default.json({ limit: '8mb' }));
    // ─── Serve server dashboard ───────────────────────────────────────────────
    app.get('/', (_, res) => {
        res.sendFile(path_1.default.join(process.cwd(), 'public', 'server.html'));
    });
    // ─── Liveness probe ───────────────────────────────────────────────────────
    app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));
    // ─── POST /sync ───────────────────────────────────────────────────────────
    app.post('/sync', (req, res) => {
        const body = req.body;
        if (!body.device_id || typeof body.device_id !== 'string') {
            res.status(400).json({ error: 'device_id required' });
            return;
        }
        if (!Array.isArray(body.changes)) {
            res.status(400).json({ error: 'changes must be an array' });
            return;
        }
        const { device_id, changes } = body;
        const stats = { accepted: 0, duplicates: 0, outgoing: 0, cursor: 0 };
        // ── Step 1: register / touch device cursor ──────────────────────────────
        stmtUpsertCursor.run(device_id);
        // ── Step 2: store incoming changes atomically ───────────────────────────
        // All-or-nothing: if any insert fails due to a real error, roll back.
        // INSERT OR IGNORE handles duplicates silently; they are separately logged.
        db.exec('BEGIN');
        try {
            for (const c of changes) {
                if (!c || typeof c !== 'object')
                    continue;
                if (!c.id || !c.tableName || !c.recordId || !c.operation)
                    continue;
                const exists = stmtCheckExists.get(c.id);
                if (exists) {
                    stmtLogDuplicate.run(c.id, device_id);
                    stats.duplicates++;
                }
                else {
                    stmtInsertChange.run(c.id, c.tableName, c.recordId, c.operation, c.timestamp ?? Date.now(), c.deviceId ?? device_id, c.payload != null ? JSON.stringify(c.payload) : null);
                    stats.accepted++;
                }
            }
            db.exec('COMMIT');
        }
        catch (err) {
            db.exec('ROLLBACK');
            throw err;
        }
        // ── Step 3: build outgoing batch ────────────────────────────────────────
        const cursorRow = stmtGetCursor.get(device_id);
        const cursor = Number(cursorRow?.last_sent_seq ?? 0);
        const rows = stmtOutgoing.all(cursor, device_id);
        const outgoing = rows.map(rowToWireChange);
        stats.outgoing = outgoing.length;
        // ── Step 4: advance cursor to current server max ─────────────────────────
        // Advancing AFTER building the response means a crash before we send still
        // re-delivers the same batch next time (safe — client deduplicates on id).
        const maxSeqRow = stmtMaxSeq.get();
        const newCursor = maxSeqRow.max_seq ?? cursor;
        stats.cursor = newCursor;
        stmtUpdateCursor.run(newCursor, device_id);
        const response = { changes: outgoing, meta: stats };
        res.json(response);
    });
    // ─── Admin API (server dashboard) ─────────────────────────────────────────
    app.get('/api/stats', (_, res) => {
        const total = num(db.prepare('SELECT COUNT(*) AS c FROM server_changes').get());
        const devices = num(db.prepare('SELECT COUNT(*) AS c FROM device_cursors').get());
        const duplicates = num(db.prepare('SELECT COUNT(*) AS c FROM duplicate_log').get());
        const dupEvents = num(db.prepare('SELECT SUM(count) AS c FROM duplicate_log').get());
        // Count changes that haven't been delivered to at least one device
        const pendingRow = db.prepare(`
      SELECT COUNT(DISTINCT sc.id) AS c
      FROM server_changes sc
      JOIN device_cursors dc ON dc.device_id != sc.device_id
      WHERE sc.seq > dc.last_sent_seq
    `).get();
        res.json({
            total,
            devices,
            duplicateChangeIds: duplicates,
            duplicateEvents: dupEvents,
            pendingDeliveries: pendingRow ? Number(pendingRow.c) : 0,
        });
    });
    app.get('/api/changes', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '100'), 10), 500);
        const device = req.query['device'];
        const op = req.query['operation'];
        const table = req.query['table'];
        let sql = 'SELECT * FROM server_changes WHERE 1=1';
        const params = [];
        if (device) {
            sql += ' AND device_id = ?';
            params.push(device);
        }
        if (op) {
            sql += ' AND operation = ?';
            params.push(op);
        }
        if (table) {
            sql += ' AND table_name = ?';
            params.push(table);
        }
        sql += ' ORDER BY seq DESC LIMIT ?';
        params.push(limit);
        const rows = db.prepare(sql).all(...params);
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
    `).all();
        res.json(rows.map(r => {
            const d = r;
            return {
                deviceId: d['device_id'],
                lastSentSeq: Number(d['last_sent_seq']),
                firstSeen: Number(d['first_seen']),
                lastSeen: Number(d['last_seen']),
                syncCount: Number(d['sync_count']),
                totalChanges: Number(d['total_changes']),
                pendingInbound: Number(d['pending_inbound']),
            };
        }));
    });
    app.get('/api/duplicates', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '50'), 10), 200);
        const rows = db.prepare('SELECT * FROM duplicate_log ORDER BY last_seen DESC LIMIT ?').all(limit);
        res.json(rows.map(r => {
            const d = r;
            return {
                id: d['id'],
                deviceId: d['device_id'],
                firstSeen: Number(d['first_seen']),
                lastSeen: Number(d['last_seen']),
                count: Number(d['count']),
            };
        }));
    });
    const server = app.listen(port, () => {
        console.log(`[sync-server] :${port}  dashboard → http://localhost:${port}`);
    });
    return { close: () => server.close() };
}
// ─── Helpers ──────────────────────────────────────────────────────────────────
function num(row) {
    return row ? Number(row['c'] ?? 0) : 0;
}
function rowToWireChange(r) {
    return {
        seq: Number(r.seq),
        id: r.id,
        tableName: r.table_name,
        recordId: r.record_id,
        operation: r.operation,
        timestamp: Number(r.timestamp),
        deviceId: r.device_id,
        payload: r.payload ? JSON.parse(r.payload) : null,
        receivedAt: Number(r.received_at),
        synced: 1,
    };
}
//# sourceMappingURL=sync-server.js.map