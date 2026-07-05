import {
  Change,
  NetworkAdapter,
  Operation,
  PullResult,
  PushResult,
  SyncResult,
} from '../core/types';
import { CircuitBreaker, DEFAULT_RETRY, RetryOptions, withRetry } from './retry';

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
export class HttpNetworkAdapter implements NetworkAdapter {
  private readonly circuit: CircuitBreaker;

  constructor(
    private readonly baseUrl: string,
    private readonly deviceId: string,
    private readonly retryOpts: RetryOptions = DEFAULT_RETRY
  ) {
    this.circuit = new CircuitBreaker(3, 15_000);
  }

  // ─── Combined push + pull (single round-trip) ─────────────────────────────

  async sync(ops: Operation[]): Promise<SyncResult> {
    return withRetry(async () => {
      const changes = ops.map(opToChange);
      const res = await fetch(`${this.baseUrl}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ device_id: this.deviceId, changes }),
      });
      if (!res.ok) throw new Error(`Sync HTTP ${res.status}`);

      const data = await res.json() as { changes: Change[] };

      return {
        // On HTTP 200 the server has stored all our changes (INSERT OR IGNORE)
        accepted: ops.map(o => o.id),
        remoteOps: data.changes.map(changeToOp),
      };
    }, this.retryOpts, this.circuit);
  }

  // ─── Legacy interface (used by tests / FakeNetwork fallback) ─────────────

  async push(ops: Operation[]): Promise<PushResult> {
    const { accepted } = await this.sync(ops);
    return { accepted, rejected: [] };
  }

  async pull(_since: number): Promise<PullResult> {
    const { remoteOps } = await this.sync([]);
    return { ops: remoteOps, sequence: Date.now() };
  }

  // ─── Health check ─────────────────────────────────────────────────────────

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/health`, {
        signal: AbortSignal.timeout(2_000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  getCircuitStats() {
    return this.circuit.getStats();
  }
}

// ─── Wire format converters ───────────────────────────────────────────────────

function opToChange(op: Operation): Change {
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

function changeToOp(c: Change): Operation {
  return {
    id: c.id,
    // Reconstruct a minimal HLC from the stored timestamp.
    // The logical counter is lost in the Change wire format; 0 is safe because
    // cross-device ordering only needs wall-time precision and the engine's own
    // HLC provides intra-device ordering.
    hlc: { wallTime: c.timestamp, logical: 0, nodeId: c.deviceId },
    entityType: c.tableName,
    entityId: c.recordId,
    type: c.operation as Operation['type'],
    payload: c.payload ?? null,
    nodeId: c.deviceId,
    synced: true,
  };
}
