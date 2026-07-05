import { NetworkAdapter, Operation, PullResult, PushResult, SyncResult } from '../core/types';
import { RetryOptions } from './retry';
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
export declare class HttpNetworkAdapter implements NetworkAdapter {
    private readonly baseUrl;
    private readonly deviceId;
    private readonly retryOpts;
    private readonly circuit;
    constructor(baseUrl: string, deviceId: string, retryOpts?: RetryOptions);
    sync(ops: Operation[]): Promise<SyncResult>;
    push(ops: Operation[]): Promise<PushResult>;
    pull(_since: number): Promise<PullResult>;
    isAvailable(): Promise<boolean>;
    getCircuitStats(): {
        state: "closed" | "open" | "half-open";
        consecutiveFailures: number;
        openedAt: number | null;
        recoveryAt: number | null;
    };
}
//# sourceMappingURL=http-adapter.d.ts.map