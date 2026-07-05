"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createUIServer = createUIServer;
const express_1 = __importDefault(require("express"));
const path_1 = __importDefault(require("path"));
const uuid_1 = require("uuid");
const DEVICE_B_ID = 'node-device-b';
function createUIServer(port, engine, simulator, httpAdapter, users, upstreamUrl, multiSim, logger) {
    const app = (0, express_1.default)();
    app.use(express_1.default.json());
    // ─── Static dashboards ───────────────────────────────────────────────────
    app.get('/', (_, res) => res.sendFile(path_1.default.join(process.cwd(), 'public', 'demo.html')));
    app.get('/dashboard', (_, res) => res.sendFile(path_1.default.join(process.cwd(), 'public', 'index.html')));
    app.get('/multi', (_, res) => res.sendFile(path_1.default.join(process.cwd(), 'public', 'multi-device.html')));
    app.get('/devtools', (_, res) => res.sendFile(path_1.default.join(process.cwd(), 'public', 'devtools.html')));
    app.get('/product', (_, res) => res.sendFile(path_1.default.join(process.cwd(), 'public', 'product.html')));
    // ─── DevTools API ─────────────────────────────────────────────────────────
    if (logger) {
        // Log entries with optional filtering
        app.get('/api/devtools/logs', (req, res) => {
            const { level, category, search, since, limit } = req.query;
            res.json(logger.getEntries({
                level: level ?? undefined,
                category: category ?? undefined,
                search: search ?? undefined,
                since: since ? Number(since) : undefined,
                limit: limit ? Math.min(Number(limit), 500) : 200,
            }));
        });
        // SSE stream for live log entries
        app.get('/api/devtools/log-stream', (req, res) => {
            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.flushHeaders();
            // Send current tail immediately
            res.write(`data: ${JSON.stringify({ type: 'snapshot', entries: logger.getEntries({ limit: 80 }) })}\n\n`);
            const unsub = logger.onEntry(e => {
                res.write(`data: ${JSON.stringify({ type: 'entry', entry: e })}\n\n`);
            });
            req.on('close', unsub);
        });
        app.delete('/api/devtools/logs', (_, res) => {
            logger.clear();
            res.json({ ok: true });
        });
    }
    // ─── DevTools: engine-level introspection (always available) ─────────────
    app.get('/api/devtools/ops', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '100'), 10), 500);
        res.json(engine.listOps(limit));
    });
    app.get('/api/devtools/queue', (_, res) => {
        res.json({
            items: engine.getSyncQueue(),
            syncState: engine.getSyncState(),
            nextRetryAt: engine.getNextRetryAt(),
        });
    });
    app.get('/api/devtools/sync-log', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '30'), 10), 100);
        res.json(engine.getSyncLog().slice(0, limit));
    });
    app.get('/api/devtools/conflicts', (req, res) => {
        const { filter, limit } = req.query;
        const resolved = filter === 'resolved' ? true : filter === 'unresolved' ? false : null;
        res.json({
            conflicts: engine.getConflicts(resolved, Math.min(Number(limit ?? 50), 200)),
            unresolvedCount: engine.getUnresolvedConflictCount(),
            mergeSchemas: engine.getMergeSchemas(),
        });
    });
    app.get('/api/devtools/errors', (_, res) => {
        res.json(engine.getSyncErrors());
    });
    // ─── Status ───────────────────────────────────────────────────────────────
    app.get('/api/status', (_, res) => {
        res.json({
            ...engine.getStatus(),
            circuit: httpAdapter.getCircuitStats(),
            simulation: {
                offline: simulator.isSimulatingOffline(),
                latencyMs: simulator.getLatency(),
            },
        });
    });
    // ─── Users API ────────────────────────────────────────────────────────────
    app.get('/api/users', (_, res) => {
        res.json(users.getAllUsers());
    });
    app.get('/api/users/:id', (req, res) => {
        const user = users.getUser(req.params['id']);
        if (!user) {
            res.status(404).json({ error: 'not found' });
            return;
        }
        res.json(user);
    });
    app.post('/api/users', (req, res) => {
        const { id, name, device_id } = req.body;
        if (!id || !name) {
            res.status(400).json({ error: 'id and name required' });
            return;
        }
        const opId = users.createUser({ id, name, updated_at: Date.now(), device_id: device_id ?? '' });
        res.status(201).json({ opId, user: users.getUser(id) });
    });
    app.patch('/api/users/:id', (req, res) => {
        const opId = users.updateUser(req.params['id'], req.body);
        res.json({ opId, user: users.getUser(req.params['id']) });
    });
    app.delete('/api/users/:id', (req, res) => {
        const opId = users.deleteUser(req.params['id']);
        res.json({ opId });
    });
    // ─── Generic entity API ───────────────────────────────────────────────────
    app.get('/api/entities/:type', (req, res) => {
        res.json(engine.list(req.params['type']));
    });
    app.post('/api/entities/:type', (req, res) => {
        const { id, data } = req.body;
        if (!id) {
            res.status(400).json({ error: 'id required' });
            return;
        }
        const opId = engine.write(req.params['type'], id, data);
        res.status(201).json({ opId });
    });
    app.delete('/api/entities/:type/:id', (req, res) => {
        const opId = engine.remove(req.params['type'], req.params['id']);
        res.json({ opId });
    });
    // ─── Operations log ───────────────────────────────────────────────────────
    app.get('/api/ops', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '20'), 10), 100);
        res.json(engine.listOps(limit));
    });
    // ─── Change tracking ──────────────────────────────────────────────────────
    app.get('/api/changes', (req, res) => {
        const filter = req.query['filter'] ?? 'all';
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '50'), 10), 200);
        const valid = ['all', 'synced', 'unsynced'];
        res.json(engine.getChanges(valid.includes(filter) ? filter : 'all', limit));
    });
    // ─── Sync control & observability ────────────────────────────────────────
    app.post('/api/sync', async (_, res) => {
        await engine.forceSync();
        res.json({ ok: true, status: engine.getStatus() });
    });
    app.post('/api/sync/pause', (_, res) => { engine.pause(); res.json({ syncState: engine.getSyncState() }); });
    app.post('/api/sync/resume', (_, res) => { engine.resume(); res.json({ syncState: engine.getSyncState() }); });
    app.get('/api/sync/errors', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '20'), 10), 50);
        res.json(engine.getSyncErrors().slice(0, limit));
    });
    app.get('/api/sync/log', (req, res) => {
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '10'), 10), 30);
        res.json(engine.getSyncLog().slice(0, limit));
    });
    app.get('/api/sync/queue', (_, res) => {
        res.json({
            items: engine.getSyncQueue(),
            nextRetryAt: engine.getNextRetryAt(),
            syncState: engine.getSyncState(),
        });
    });
    // ─── Conflict API ─────────────────────────────────────────────────────────
    app.get('/api/conflicts', (req, res) => {
        const filter = req.query['filter'];
        const limit = Math.min(parseInt(String(req.query['limit'] ?? '30'), 10), 100);
        const resolved = filter === 'resolved' ? true :
            filter === 'unresolved' ? false : null;
        res.json({
            conflicts: engine.getConflicts(resolved, limit),
            unresolvedCount: engine.getUnresolvedConflictCount(),
        });
    });
    // Acknowledge without changing entity store (just mark UI-resolved)
    app.post('/api/conflicts/:id/resolve', (req, res) => {
        engine.markConflictResolved(req.params['id']);
        res.json({ ok: true });
    });
    // Preview CRDT auto-merge without applying
    app.get('/api/conflicts/:id/preview', (req, res) => {
        const preview = engine.previewMerge(req.params['id']);
        if (!preview) {
            res.status(404).json({ error: 'conflict not found or no schema' });
            return;
        }
        res.json(preview);
    });
    // Apply CRDT auto-merge
    app.post('/api/conflicts/:id/auto-merge', (req, res) => {
        const result = engine.applyAutoMerge(req.params['id']);
        if (!result.ok) {
            res.status(400).json({ error: 'no merge schema for this entity type' });
            return;
        }
        res.json(result);
    });
    // Apply manual resolution (user-supplied data)
    app.post('/api/conflicts/:id/manual-merge', (req, res) => {
        const { data } = req.body;
        if (!data || typeof data !== 'object') {
            res.status(400).json({ error: 'data object required' });
            return;
        }
        const result = engine.applyManualMerge(req.params['id'], data);
        if (!result.ok) {
            res.status(404).json({ error: 'conflict not found' });
            return;
        }
        res.json(result);
    });
    // Get registered merge schemas
    app.get('/api/merge-schemas', (_, res) => {
        res.json(engine.getMergeSchemas());
    });
    // ─── Crash & duplicate simulation ────────────────────────────────────────
    app.post('/api/simulate/crash-write', (req, res) => {
        const { entityType, entityId, payload } = req.body;
        if (!entityType || !entityId || !payload) {
            res.status(400).json({ error: 'entityType, entityId, and payload required' });
            return;
        }
        const result = engine.simulateCrashWrite(entityType, entityId, payload);
        res.json({
            ...result,
            entityNow: engine.get(entityType, entityId) ?? null,
            changesNow: engine.getChanges('all', 5),
        });
    });
    app.post('/api/simulate/recover', (_, res) => {
        const result = engine.recoverFromOplog();
        res.json({ ...result, message: `${result.replayed} op(s) replayed from oplog` });
    });
    app.post('/api/simulate/duplicate-op', (_, res) => {
        const result = engine.simulateDuplicateOp();
        res.json(result);
    });
    app.post('/api/simulate/conflict', (req, res) => {
        const { entityType, entityId } = req.body;
        if (!entityType || !entityId) {
            res.status(400).json({ error: 'entityType and entityId required' });
            return;
        }
        const result = engine.simulateConflict(entityType, entityId);
        res.json(result);
    });
    // ─── Multi-device simulation ─────────────────────────────────────────────
    /**
     * Write a change directly to the sync server as "Device B".
     * Bypasses the local edge engine so the change is unknown locally until
     * the next sync cycle — triggering a genuine conflict when it lands.
     */
    app.post('/api/simulate/device-b-write', async (req, res) => {
        const { entityType = 'users', entityId, payload } = req.body;
        if (!entityId || !payload) {
            res.status(400).json({ error: 'entityId and payload required' });
            return;
        }
        const change = {
            id: (0, uuid_1.v4)(),
            tableName: entityType ?? 'users',
            recordId: entityId,
            operation: 'update',
            timestamp: Date.now(),
            deviceId: DEVICE_B_ID,
            payload,
            synced: 0,
        };
        try {
            const r = await fetch(`${upstreamUrl}/sync`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device_id: DEVICE_B_ID, changes: [change] }),
            });
            const serverResp = await r.json();
            res.json({ ok: true, change, deviceId: DEVICE_B_ID, serverResponse: serverResp });
        }
        catch (err) {
            res.status(502).json({ error: 'sync server unreachable', detail: String(err) });
        }
    });
    // ─── Demo reset ───────────────────────────────────────────────────────────
    app.post('/api/demo/reset', (_, res) => {
        const nodeId = engine.getStatus().nodeId;
        const now = Date.now();
        // Overwrite seed users — higher timestamp always wins LWW
        users.createUser({ id: 'user-001', name: 'Alice', updated_at: now, device_id: nodeId });
        users.createUser({ id: 'user-002', name: 'Bob', updated_at: now, device_id: nodeId });
        users.createUser({ id: 'user-003', name: 'Charlie', updated_at: now, device_id: nodeId });
        // Clear simulation state
        simulator.setOffline(false);
        simulator.setLatency(0);
        simulator.setTimeoutMs(0);
        res.json({ ok: true, seeded: ['user-001', 'user-002', 'user-003'] });
    });
    // ─── Failure simulation ───────────────────────────────────────────────────
    app.post('/api/simulate/offline', (req, res) => {
        const { offline } = req.body;
        simulator.setOffline(!!offline);
        res.json({ offline: simulator.isSimulatingOffline() });
    });
    app.post('/api/simulate/latency', (req, res) => {
        const { ms } = req.body;
        simulator.setLatency(Number(ms) || 0);
        res.json({ latencyMs: simulator.getLatency() });
    });
    app.post('/api/simulate/timeout', (req, res) => {
        const { ms } = req.body;
        simulator.setTimeoutMs(Number(ms) || 0);
        res.json({ timeoutMs: simulator.getTimeoutMs() });
    });
    // ─── SSE — real-time status push ─────────────────────────────────────────
    app.get('/api/events', (req, res) => {
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.flushHeaders();
        const snapshot = () => ({
            ...engine.getStatus(),
            circuit: httpAdapter.getCircuitStats(),
            simulation: {
                offline: simulator.isSimulatingOffline(),
                latencyMs: simulator.getLatency(),
                timeoutMs: simulator.getTimeoutMs(),
            },
        });
        res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
        const unsub = engine.onStatusChange(() => {
            res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
        });
        req.on('close', unsub);
    });
    // ─── Multi-device simulation API ─────────────────────────────────────────
    if (multiSim) {
        // Snapshots
        app.get('/api/multi/devices', (_, res) => {
            res.json(multiSim.getAllSnapshots());
        });
        app.get('/api/multi/devices/:id', (req, res) => {
            const snap = multiSim.getSnapshot(req.params['id']);
            if (!snap) {
                res.status(404).json({ error: 'device not found' });
                return;
            }
            res.json(snap);
        });
        // Per-device write
        app.post('/api/multi/devices/:id/write-user', (req, res) => {
            const { userId, name } = req.body;
            if (!userId || !name) {
                res.status(400).json({ error: 'userId and name required' });
                return;
            }
            multiSim.writeUser(req.params['id'], userId, name);
            res.json({ ok: true, snapshot: multiSim.getSnapshot(req.params['id']) });
        });
        app.post('/api/multi/devices/:id/update-user', (req, res) => {
            const { userId, name } = req.body;
            if (!userId || !name) {
                res.status(400).json({ error: 'userId and name required' });
                return;
            }
            multiSim.updateUser(req.params['id'], userId, name);
            res.json({ ok: true, snapshot: multiSim.getSnapshot(req.params['id']) });
        });
        // Per-device sync
        app.post('/api/multi/devices/:id/sync', async (req, res) => {
            await multiSim.forceSync(req.params['id']);
            res.json({ ok: true, snapshot: multiSim.getSnapshot(req.params['id']) });
        });
        // Per-device offline / latency
        app.post('/api/multi/devices/:id/offline', (req, res) => {
            const { offline } = req.body;
            multiSim.setOffline(req.params['id'], !!offline);
            res.json({ ok: true, offline: multiSim.isOffline(req.params['id']) });
        });
        app.post('/api/multi/devices/:id/latency', (req, res) => {
            const { ms } = req.body;
            multiSim.setLatency(req.params['id'], Number(ms) || 0);
            res.json({ ok: true, latencyMs: multiSim.getLatency(req.params['id']) });
        });
        // Per-device auto-merge all unresolved conflicts
        app.post('/api/multi/devices/:id/merge-all', (req, res) => {
            const merged = multiSim.autoMergeAll(req.params['id']);
            res.json({ ok: true, merged });
        });
        // Per-device conflicts
        app.get('/api/multi/devices/:id/conflicts', (req, res) => {
            res.json(multiSim.getConflicts(req.params['id']));
        });
        // Conflict resolution on a specific device
        app.post('/api/multi/devices/:deviceId/conflicts/:conflictId/auto-merge', (req, res) => {
            const engine = multiSim.getEngine(req.params['deviceId']);
            if (!engine) {
                res.status(404).json({ error: 'device not found' });
                return;
            }
            const result = engine.applyAutoMerge(req.params['conflictId']);
            res.json(result);
        });
        app.post('/api/multi/devices/:deviceId/conflicts/:conflictId/manual-merge', (req, res) => {
            const engine = multiSim.getEngine(req.params['deviceId']);
            if (!engine) {
                res.status(404).json({ error: 'device not found' });
                return;
            }
            const { data } = req.body;
            if (!data) {
                res.status(400).json({ error: 'data required' });
                return;
            }
            res.json(engine.applyManualMerge(req.params['conflictId'], data));
        });
        // Global: all conflicts across all devices
        app.get('/api/multi/conflicts', (_, res) => {
            res.json(multiSim.getAllConflicts());
        });
        // Global: sync all devices
        app.post('/api/multi/sync-all', async (_, res) => {
            await multiSim.syncAll();
            res.json({ ok: true, snapshots: multiSim.getAllSnapshots() });
        });
        // Seed all devices with baseline users and sync
        app.post('/api/multi/seed', async (req, res) => {
            const { users: seedUsers } = req.body;
            if (!Array.isArray(seedUsers)) {
                res.status(400).json({ error: 'users array required' });
                return;
            }
            await multiSim.seedAll(seedUsers);
            res.json({ ok: true, snapshots: multiSim.getAllSnapshots() });
        });
        // Reset all simulation state (offline flags, latency)
        app.post('/api/multi/reset', async (req, res) => {
            const { users: seedUsers } = req.body;
            multiSim.resetAll();
            if (Array.isArray(seedUsers) && seedUsers.length) {
                await multiSim.seedAll(seedUsers);
            }
            res.json({ ok: true, snapshots: multiSim.getAllSnapshots() });
        });
        // Events log
        app.get('/api/multi/events-log', (req, res) => {
            const limit = Math.min(parseInt(String(req.query['limit'] ?? '60'), 10), 200);
            res.json(multiSim.getEvents(limit));
        });
        // SSE — push multi-device events in real-time
        app.get('/api/multi/events', (req, res) => {
            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.flushHeaders();
            // Send current state immediately
            const snapshot = () => ({ snapshots: multiSim.getAllSnapshots(), events: multiSim.getEvents(30) });
            res.write(`data: ${JSON.stringify({ type: 'snapshot', ...snapshot() })}\n\n`);
            // Forward live events
            const unsub = multiSim.onEvent(e => {
                res.write(`data: ${JSON.stringify({ type: 'event', event: e })}\n\n`);
            });
            // Heartbeat every 3s to keep the connection alive and send fresh snapshots
            const heartbeat = setInterval(() => {
                res.write(`data: ${JSON.stringify({ type: 'snapshot', ...snapshot() })}\n\n`);
            }, 3000);
            req.on('close', () => { unsub(); clearInterval(heartbeat); });
        });
    }
    const server = app.listen(port, () => {
        console.log(`[ui] dashboard → http://localhost:${port}`);
    });
    return { close: () => server.close() };
}
//# sourceMappingURL=ui-server.js.map