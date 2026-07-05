import { SyncEngine } from '../core/engine';
import { QueueItem, SyncAttempt, SyncStatus } from '../core/types';
export type DeviceEventKind = 'sync_ok' | 'sync_fail' | 'write' | 'conflict' | 'merge' | 'offline' | 'online';
export type DeviceEvent = {
    id: number;
    at: number;
    deviceId: string;
    kind: DeviceEventKind;
    detail: string;
    sent?: number;
    received?: number;
};
export type DeviceSnapshot = {
    id: string;
    status: SyncStatus;
    users: {
        id: string;
        name: string;
        updated_at: number;
        device_id: string;
    }[];
    queue: QueueItem[];
    offline: boolean;
    latencyMs: number;
    unresolvedConflicts: number;
    syncLog: SyncAttempt[];
};
export declare class MultiDeviceSimulator {
    private readonly upstreamUrl;
    private readonly slots;
    private eventLog;
    private seq;
    private readonly MAX_EVENTS;
    private readonly subscribers;
    private pollTimer;
    private readonly lastSyncId;
    private readonly lastConflictCount;
    constructor(deviceIds: string[], upstreamUrl: string);
    start(): void;
    stop(): void;
    deviceIds(): string[];
    getEngine(id: string): SyncEngine | undefined;
    setOffline(deviceId: string, offline: boolean): void;
    setLatency(deviceId: string, ms: number): void;
    isOffline(deviceId: string): boolean;
    getLatency(deviceId: string): number;
    writeUser(deviceId: string, userId: string, name: string): void;
    updateUser(deviceId: string, userId: string, name: string): void;
    forceSync(deviceId: string): Promise<void>;
    syncAll(): Promise<void>;
    autoMergeAll(deviceId: string): number;
    getSnapshot(deviceId: string): DeviceSnapshot | null;
    getAllSnapshots(): DeviceSnapshot[];
    getConflicts(deviceId: string): import("../core/types").Conflict[];
    getAllConflicts(): (ReturnType<SyncEngine['getConflicts']>[0] & {
        deviceId: string;
    })[];
    getEvents(limit?: number): DeviceEvent[];
    onEvent(fn: (e: DeviceEvent) => void): () => void;
    seedAll(users: {
        id: string;
        name: string;
    }[]): Promise<void>;
    resetAll(): void;
    private poll;
    private emit;
}
//# sourceMappingURL=multi-device.d.ts.map