"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HttpNetworkAdapter = void 0;
const retry_1 = require("./retry");
/**
 * HTTP adapter targeting the real sync server's POST /sync endpoint.
 *
 * Protocol:
 *   POST /sync  { device_id, changes[] }
 *   ← 200       { changes[], meta }
 *
 * A single HTTP call per sync cycle exchanges both local and remote changes.
 * The engine prefers sync() over push()+pull() when it's available.
 *
 * push() and pull() are kept for interface compliance and used by the
 * FakeNetwork in tests via the legacy fallback path.
 */
class HttpNetworkAdapter {
    baseUrl;
    deviceId;
    retryOpts;
    circuit;
    constructor(baseUrl, deviceId, retryOpts = retry_1.DEFAULT_RETRY) {
        this.baseUrl = baseUrl;
        this.deviceId = deviceId;
        this.retryOpts = retryOpts;
        this.circuit = new retry_1.CircuitBreaker(3, 15_000);
    }
    // ─── Combined push + pull (single round-trip) ─────────────────────────────
    async sync(ops) {
        return (0, retry_1.withRetry)(async () => {
            const changes = ops.map(opToChange);
            const res = await fetch(`${this.baseUrl}/sync`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device_id: this.deviceId, changes }),
            });
            if (!res.ok)
                throw new Error(`Sync HTTP ${res.status}`);
            const data = await res.json();
            return {
                // On HTTP 200 the server has stored all our changes (INSERT OR IGNORE)
                accepted: ops.map(o => o.id),
                remoteOps: data.changes.map(changeToOp),
            };
        }, this.retryOpts, this.circuit);
    }
    // ─── Legacy interface (used by tests / FakeNetwork fallback) ─────────────
    async push(ops) {
        const { accepted } = await this.sync(ops);
        return { accepted, rejected: [] };
    }
    async pull(_since) {
        const { remoteOps } = await this.sync([]);
        return { ops: remoteOps, sequence: Date.now() };
    }
    // ─── Health check ─────────────────────────────────────────────────────────
    async isAvailable() {
        try {
            const res = await fetch(`${this.baseUrl}/health`, {
                signal: AbortSignal.timeout(2_000),
            });
            return res.ok;
        }
        catch {
            return false;
        }
    }
    getCircuitStats() {
        return this.circuit.getStats();
    }
}
exports.HttpNetworkAdapter = HttpNetworkAdapter;
// ─── Wire format converters ───────────────────────────────────────────────────
function opToChange(op) {
    return {
        id: op.id,
        tableName: op.entityType,
        recordId: op.entityId,
        operation: op.type,
        timestamp: op.hlc.wallTime,
        deviceId: op.nodeId,
        payload: op.payload ?? undefined,
        synced: 0,
    };
}
function changeToOp(c) {
    return {
        id: c.id,
        // Reconstruct a minimal HLC from the stored timestamp.
        // The logical counter is lost in the Change wire format; 0 is safe because
        // cross-device ordering only needs wall-time precision and the engine's own
        // HLC provides intra-device ordering.
        hlc: { wallTime: c.timestamp, logical: 0, nodeId: c.deviceId },
        entityType: c.tableName,
        entityId: c.recordId,
        type: c.operation,
        payload: c.payload ?? null,
        nodeId: c.deviceId,
        synced: true,
    };
}
//# sourceMappingURL=http-adapter.js.map