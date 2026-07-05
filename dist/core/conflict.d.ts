import { Entity, Operation } from './types';
import { HybridLogicalClock } from './hlc';
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
export declare class ConflictResolver {
    private readonly hlc;
    constructor(hlc: HybridLogicalClock);
    /**
     * Decide whether to apply `incoming` op to `local` entity.
     * Returns the resulting entity to store, or null if local wins.
     */
    resolve(local: Entity | undefined, incoming: Operation): Entity | null;
    private apply;
}
//# sourceMappingURL=conflict.d.ts.map