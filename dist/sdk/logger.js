"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.globalLogger = exports.Logger = void 0;
class Logger {
    entries = [];
    seq = 0;
    MAX = 500;
    subs = new Set();
    log(level, category, msg, meta) {
        const e = { id: ++this.seq, at: Date.now(), level, category, message: msg, meta };
        this.entries.unshift(e);
        if (this.entries.length > this.MAX)
            this.entries.pop();
        this.subs.forEach(fn => fn(e));
    }
    debug(cat, msg, meta) { this.log('debug', cat, msg, meta); }
    info(cat, msg, meta) { this.log('info', cat, msg, meta); }
    warn(cat, msg, meta) { this.log('warn', cat, msg, meta); }
    error(cat, msg, meta) { this.log('error', cat, msg, meta); }
    getEntries(opts) {
        let r = this.entries;
        if (opts?.level)
            r = r.filter(e => e.level === opts.level);
        if (opts?.category)
            r = r.filter(e => e.category === opts.category);
        if (opts?.since)
            r = r.filter(e => e.id > opts.since);
        if (opts?.search) {
            const q = opts.search.toLowerCase();
            r = r.filter(e => e.message.toLowerCase().includes(q) ||
                JSON.stringify(e.meta ?? {}).toLowerCase().includes(q));
        }
        return r.slice(0, opts?.limit ?? 100);
    }
    onEntry(fn) {
        this.subs.add(fn);
        return () => this.subs.delete(fn);
    }
    clear() { this.entries = []; }
    size() { return this.entries.length; }
}
exports.Logger = Logger;
exports.globalLogger = new Logger();
//# sourceMappingURL=logger.js.map