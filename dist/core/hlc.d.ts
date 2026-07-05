import { HLC } from './types';
/**
 * Hybrid Logical Clock (HLC) — Kulkarni & Demirbas, 2014
 *
 * Combines physical wall time with a logical counter:
 *   - Advances physical time when wall clock moves forward
 *   - Increments counter when wall clock is stale (drift, NTP step-back)
 *   - Merges with remote HLCs on receive to maintain causal order
 *
 * Why not pure Lamport? No wall-time readability, bad for debugging.
 * Why not pure wall clock? Clock drift on edge devices causes ordering violations.
 * HLC gives us both: causality + physical-time proximity.
 */
export declare class HybridLogicalClock {
    private readonly nodeId;
    private l;
    private c;
    constructor(nodeId: string);
    get node(): string;
    /** Generate a new timestamp for a local event. */
    now(): HLC;
    /**
     * Receive a remote timestamp and advance our clock past it.
     * Must be called before applying any remote operation.
     */
    receive(remote: HLC): HLC;
    /**
     * Total order comparison. Returns:
     *   negative  → a happened before b
     *   0         → identical (same node, same instant)
     *   positive  → a happened after b
     */
    compare(a: HLC, b: HLC): number;
    serialize(hlc: HLC): string;
    deserialize(s: string): HLC;
}
//# sourceMappingURL=hlc.d.ts.map