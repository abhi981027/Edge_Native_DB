import express from 'express';
import { DatabaseSync } from 'node:sqlite';
import { Operation } from '../core/types';

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
export function createMockServer(
  port: number,
  dbPath = ':memory:'
): { close: () => void } {
  const app = express();
  const db = new DatabaseSync(dbPath);

  db.exec('PRAGMA journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS server_ops (
      seq    INTEGER PRIMARY KEY AUTOINCREMENT,
      op_id  TEXT UNIQUE NOT NULL,
      data   TEXT NOT NULL
    );
  `);

  const insertOp = db.prepare('INSERT OR IGNORE INTO server_ops (op_id, data) VALUES (?, ?)');
  const pullOps  = db.prepare('SELECT seq, data FROM server_ops WHERE seq > ? ORDER BY seq ASC LIMIT 200');
  const listOps  = db.prepare('SELECT seq, data FROM server_ops ORDER BY seq DESC LIMIT ?');

  app.use(express.json({ limit: '4mb' }));

  // Liveness probe
  app.get('/health', (_, res) => res.json({ ok: true, ts: Date.now() }));

  // Edge → Server: push local ops
  app.post('/sync/push', (req, res) => {
    const { ops } = req.body as { ops: Operation[] };

    if (!Array.isArray(ops)) {
      res.status(400).json({ error: 'ops must be an array' });
      return;
    }

    const accepted: string[] = [];

    for (const op of ops) {
      try {
        insertOp.run(op.id, JSON.stringify(op));
        accepted.push(op.id);
      } catch {
        // duplicate op id — silently skip (idempotent)
      }
    }

    res.json({ accepted, rejected: [] });
  });

  // Server → Edge: pull ops since cursor
  app.get('/sync/pull', (req, res) => {
    const since = parseInt(String(req.query['since'] ?? '0'), 10);
    const rows = pullOps.all(since) as Array<{ seq: number | bigint; data: string }>;

    const ops = rows.map(r => JSON.parse(r.data) as Operation);
    const sequence = rows.length > 0 ? Number(rows[rows.length - 1].seq) : since;

    res.json({ ops, sequence });
  });

  // Debug — list recent server ops
  app.get('/sync/ops', (req, res) => {
    const limit = Math.min(parseInt(String(req.query['limit'] ?? '50'), 10), 500);
    const rows = listOps.all(limit) as Array<{ seq: number | bigint; data: string }>;
    res.json(rows.map(r => ({ seq: Number(r.seq), ...JSON.parse(r.data) })));
  });

  const server = app.listen(port, () => {
    console.log(`[upstream] mock server listening on :${port}`);
  });

  return { close: () => server.close() };
}
