export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LogCategory = 'sync' | 'network' | 'storage' | 'conflict' | 'sdk' | 'system';
export type LogEntry = {
    id: number;
    at: number;
    level: LogLevel;
    category: LogCategory;
    message: string;
    meta?: Record<string, unknown>;
};
export declare class Logger {
    private entries;
    private seq;
    private readonly MAX;
    private readonly subs;
    log(level: LogLevel, category: LogCategory, msg: string, meta?: Record<string, unknown>): void;
    debug(cat: LogCategory, msg: string, meta?: Record<string, unknown>): void;
    info(cat: LogCategory, msg: string, meta?: Record<string, unknown>): void;
    warn(cat: LogCategory, msg: string, meta?: Record<string, unknown>): void;
    error(cat: LogCategory, msg: string, meta?: Record<string, unknown>): void;
    getEntries(opts?: {
        level?: LogLevel;
        category?: LogCategory;
        search?: string;
        since?: number;
        limit?: number;
    }): LogEntry[];
    onEntry(fn: (e: LogEntry) => void): () => void;
    clear(): void;
    size(): number;
}
export declare const globalLogger: Logger;
//# sourceMappingURL=logger.d.ts.map