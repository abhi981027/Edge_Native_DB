"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConflictResolver = void 0;
/**
 * Last-Write-Wins conflict resolver using HLC timestamps.
 *
 * Rules:
 *   - Remote op HLC > local entity HLC  → remote wins, apply
 *   - Remote op HLC ≤ local entity HLC  → local wins, discard remote
 *   - No local entity exists            → always apply
 *   - Delete beats any same-HLC update  → tombstone is sticky
 *
 * Why LWW? It's deterministic, simple to implement correctly, and good enough
 * for most edge telemetry / config use cases. For richer semantics (e.g. CRDT
 * counters), swap this class out — the engine doesn't care about the strategy.
 */
class ConflictResolver {
    hlc;
    constructor(hlc) {
        this.hlc = hlc;
    }
    /**
     * Decide whether to apply `incoming` op to `local` entity.
     * Returns the resulting entity to store, or null if local wins.
     */
    resolve(local, incoming) {
        if (!local) {
            return this.apply(undefined, incoming);
        }
        const cmp = this.hlc.compare(incoming.hlc, local.hlc);
        if (cmp <= 0) {
            // Incoming is causally older or equal — local wins
            return null;
        }
        return this.apply(local, incoming);
    }
    apply(local, op) {
        switch (op.type) {
            case 'delete':
                if (!local)
                    return null;
                return { ...local, tombstone: true, hlc: op.hlc };
            case 'insert':
                if (!op.payload)
                    return null;
                return {
                    id: op.entityId,
                    type: op.entityType,
                    data: op.payload,
                    hlc: op.hlc,
                    tombstone: false,
                };
            case 'update':
                if (!op.payload)
                    return null;
                return {
                    id: op.entityId,
                    type: op.entityType,
                    // Merge with existing data so partial updates don't wipe fields
                    data: local ? { ...local.data, ...op.payload } : op.payload,
                    hlc: op.hlc,
                    tombstone: false,
                };
        }
    }
}
exports.ConflictResolver = ConflictResolver;
//# sourceMappingURL=conflict.js.map