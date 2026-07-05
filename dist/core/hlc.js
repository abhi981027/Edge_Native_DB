"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HybridLogicalClock = void 0;
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
class HybridLogicalClock {
    nodeId;
    l = 0; // max physical time seen (ms)
    c = 0; // logical counter
    constructor(nodeId) {
        this.nodeId = nodeId;
    }
    get node() { return this.nodeId; }
    /** Generate a new timestamp for a local event. */
    now() {
        const wallNow = Date.now();
        if (wallNow > this.l) {
            this.l = wallNow;
            this.c = 0;
        }
        else {
            // Wall clock hasn't advanced — bump logical counter to preserve ordering
            this.c += 1;
        }
        return { wallTime: this.l, logical: this.c, nodeId: this.nodeId };
    }
    /**
     * Receive a remote timestamp and advance our clock past it.
     * Must be called before applying any remote operation.
     */
    receive(remote) {
        const wallNow = Date.now();
        const prevL = this.l;
        this.l = Math.max(wallNow, this.l, remote.wallTime);
        if (this.l === prevL && this.l === remote.wallTime) {
            // All three agree — take the max logical and bump
            this.c = Math.max(this.c, remote.logical) + 1;
        }
        else if (this.l === prevL) {
            // Our clock was already furthest ahead
            this.c += 1;
        }
        else if (this.l === remote.wallTime) {
            // Remote was furthest ahead
            this.c = remote.logical + 1;
        }
        else {
            // Wall clock jumped ahead — reset counter
            this.c = 0;
        }
        return { wallTime: this.l, logical: this.c, nodeId: this.nodeId };
    }
    /**
     * Total order comparison. Returns:
     *   negative  → a happened before b
     *   0         → identical (same node, same instant)
     *   positive  → a happened after b
     */
    compare(a, b) {
        if (a.wallTime !== b.wallTime)
            return a.wallTime - b.wallTime;
        if (a.logical !== b.logical)
            return a.logical - b.logical;
        return a.nodeId.localeCompare(b.nodeId);
    }
    serialize(hlc) {
        return `${hlc.wallTime}:${hlc.logical}:${hlc.nodeId}`;
    }
    deserialize(s) {
        const colon1 = s.indexOf(':');
        const colon2 = s.indexOf(':', colon1 + 1);
        return {
            wallTime: parseInt(s.slice(0, colon1), 10),
            logical: parseInt(s.slice(colon1 + 1, colon2), 10),
            nodeId: s.slice(colon2 + 1),
        };
    }
}
exports.HybridLogicalClock = HybridLogicalClock;
//# sourceMappingURL=hlc.js.map