import { FieldStrategy, MergeSchema } from './merge-registry';
/**
 * CRDT-lite field merger.
 *
 * All merge functions are PURE and DETERMINISTIC:
 *   - Same two input values always produce the same output.
 *   - Output does not depend on which value is "local" vs "remote".
 *   - Commutative: merge(A, B) === merge(B, A) for all strategies except lww.
 *
 * This means every edge device that sees the same pair of conflicting values
 * will compute the identical merged result, converging without coordination.
 *
 * ─── Strategy details ────────────────────────────────────────────────────────
 *
 * text-merge:
 *   Preserves both writers' intent when text diverges. Useful for names,
 *   descriptions, notes — anything where "last write" would silently discard
 *   information. Deterministic: values are sorted before joining.
 *   "Alice" + "Alice (CEO)" → "Alice | Alice (CEO)"
 *
 * counter:
 *   Takes max(local, remote). Correct for grow-only counters (view counts,
 *   download counts, likes) where values are only ever incremented.
 *   Note: this is NOT a proper PN-counter (which needs per-node deltas).
 *   For additive merge across concurrent increments, store delta payloads
 *   and sum them here. The max strategy is a safe approximation when
 *   simultaneous concurrent increments are rare.
 *
 * max:
 *   Takes the numerically larger value. Useful for timestamps, scores,
 *   or any monotone field that should never decrease.
 *
 * set-union:
 *   Unions both arrays, deduplicates, sorts stably. Useful for tags, roles,
 *   permissions — append-only sets that grow over time.
 *   ["admin"] + ["viewer"] → ["admin", "viewer"]
 *
 * lww:
 *   Falls back to last-write-wins for this field specifically. The
 *   `remoteIsNewer` parameter (derived from HLC comparison) determines
 *   which version to keep.
 */
export declare class CrdtMerger {
    /**
     * Merge two record versions field-by-field.
     * Fields not in the schema use `lww`.
     * Fields only in one version are kept from that version.
     */
    static merge(local: Record<string, unknown>, remote: Record<string, unknown> | null, schema: MergeSchema, remoteIsNewer: boolean): Record<string, unknown>;
    /**
     * Preview the merge result field-by-field without applying it.
     * Used by the UI to show the "after" state before the user commits.
     */
    static preview(local: Record<string, unknown>, remote: Record<string, unknown> | null, schema: MergeSchema, remoteIsNewer: boolean): {
        merged: Record<string, unknown>;
        fieldDecisions: FieldDecisions;
    };
}
export type FieldDecisions = Record<string, {
    strategy: FieldStrategy;
    localVal: unknown;
    remoteVal: unknown;
    mergedVal: unknown;
    conflicted: boolean;
}>;
//# sourceMappingURL=crdt-merger.d.ts.map