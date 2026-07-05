"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMockServer = createMockServer;
const express_1 = __importDefault(require("express"));
const node_sqlite_1 = require("node:sqlite");
/**
 * Minimal upstream sync server.
 *
 * Stores a sequence-ordered oplog. Edge nodes push their local ops here and
 * pull remote ops by cursor. The server is intentionally dumb — it stores
 * everything and lets edge nodes resolve conflicts locally.
 *
 * In production this would be replaced by a real backend. For development and
 * testing it lets you run the full sync flow on one machine.
 */
function createMockServer(port, dbPath = ':memory:') {
    const app = (0, express_1.default)();
    const db = new node_sqlite_1.DatabaseSync(dbPath);
    db.exec('PRAGMA journal_mode = WAL');
    db.exec(`
    CREATE TABLE IF NOT EXISTS server_ops (
      seq    INTEGER PRIMARY KEY AUTOINCREMENT,
      op_id  TEXT UNIQUE NOT NULL,
      data   TEXT NOT NULL
    );
  `);
    const insertOp = db.prepare('INSERT OR IGNORE INTO server_ops (op_id, data) VALUES (?, ?)');
    const pullOps = db.prepare('SELECT seq, data FROM server_ops WHERE seq > ? ORDER BY seq ASC LIMIT 200');
    const listOps = db.prepare('SELECT seq, data FROM server_ops ORDER BY seq DESC LIMIT ?');
    app.use(express_1.default.json({ limit: '4mb' }));
    // Liveness probe
    app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));
    // Edge → Server: push local ops
    app.post('/sync/push', (req, res) => {
        const { ops } = req.body;
        if (!Array.isArray(ops)) {
            res.status(400).json({ error: 'ops must be an array' });
            return;
        }
        const accepted = [];
        for (const op of ops) {
            try {
                insertOp.run(op.id, JSON.stringify(op));
                accepted.push(op.id);
            }
            catch {
                // duplicate op id — silently skip (idempotent)
            }
        }
        res.json({ accepted, rejected: [] });
    });
    // Server → Edge: pull ops since cursor
    app.get('/sync/pull', (req, res) => {
        const since = parseInt(String(req.query['since'] ?? '0'), 10);
        const rows = pullOps.all(since);
        const ops = rows.map(r => JSON.parse(r.data));
        const sequence = rows.length > 0 ? Number(rows[rows.length - 1].seq) : since;
        res.json({ ops, sequence });
    });
    // Debug — list recent server ops
    app.get('/sync/ops', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '50'), 10), 500);
        const rows = listOps.all(limit);
        res.json(rows.map(r => ({ seq: Number(r.seq), ...JSON.parse(r.data) })));
    });
    const server = app.listen(port, () => {
        console.log(`[upstream] mock server listening on :${port}`);
    });
    return { close: () => server.close() };
}
//# sourceMappingURL=mock-server.js.map