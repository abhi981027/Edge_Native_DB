export type LogLevel    = 'debug' | 'info' | 'warn' | 'error';
export type LogCategory = 'sync' | 'network' | 'storage' | 'conflict' | 'sdk' | 'system';

export type LogEntry = {
  id:       number;
  at:       number;
  level:    LogLevel;
  category: LogCategory;
  message:  string;
  meta?:    Record<string, unknown>;
};

export class Logger {
  private entries: LogEntry[] = [];
  private seq                 = 0;
  private readonly MAX        = 500;
  private readonly subs       = new Set<(e: LogEntry) => void>();

  log(level: LogLevel, category: LogCategory, msg: string, meta?: Record<string, unknown>): void {
    const e: LogEntry = { id: ++this.seq, at: Date.now(), level, category, message: msg, meta };
    this.entries.unshift(e);
    if (this.entries.length > this.MAX) this.entries.pop();
    this.subs.forEach(fn => fn(e));
  }

  debug(cat: LogCategory, msg: string, meta?: Record<string, unknown>) { this.log('debug', cat, msg, meta); }
  info (cat: LogCategory, msg: string, meta?: Record<string, unknown>) { this.log('info',  cat, msg, meta); }
  warn (cat: LogCategory, msg: string, meta?: Record<string, unknown>) { this.log('warn',  cat, msg, meta); }
  error(cat: LogCategory, msg: string, meta?: Record<string, unknown>) { this.log('error', cat, msg, meta); }

  getEntries(opts?: {
    level?:    LogLevel;
    category?: LogCategory;
    search?:   string;
    since?:    number;
    limit?:    number;
  }): LogEntry[] {
    let r = this.entries;
    if (opts?.level)    r = r.filter(e => e.level    === opts.level);
    if (opts?.category) r = r.filter(e => e.category === opts.category);
    if (opts?.since)    r = r.filter(e => e.id > opts.since!);
    if (opts?.search) {
      const q = opts.search.toLowerCase();
      r = r.filter(e => e.message.toLowerCase().includes(q) ||
                         JSON.stringify(e.meta ?? {}).toLowerCase().includes(q));
    }
    return r.slice(0, opts?.limit ?? 100);
  }

  onEntry(fn: (e: LogEntry) => void): () => void {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  clear(): void { this.entries = []; }

  size(): number { return this.entries.length; }
}

export const globalLogger = new Logger();
