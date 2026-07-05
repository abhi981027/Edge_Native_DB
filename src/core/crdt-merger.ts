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
export class CrdtMerger {

  /**
   * Merge two record versions field-by-field.
   * Fields not in the schema use `lww`.
   * Fields only in one version are kept from that version.
   */
  static merge(
    local:         Record<string, unknown>,
    remote:        Record<string, unknown> | null,
    schema:        MergeSchema,
    remoteIsNewer: boolean
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const allFields = new Set([
      ...Object.keys(local),
      ...Object.keys(remote ?? {}),
    ]);

    for (const field of allFields) {
      const strategy: FieldStrategy = schema[field] ?? 'lww';
      result[field] = applyStrategy(local[field], remote?.[field], strategy, remoteIsNewer);
    }

    return result;
  }

  /**
   * Preview the merge result field-by-field without applying it.
   * Used by the UI to show the "after" state before the user commits.
   */
  static preview(
    local:         Record<string, unknown>,
    remote:        Record<string, unknown> | null,
    schema:        MergeSchema,
    remoteIsNewer: boolean
  ): {
    merged:         Record<string, unknown>;
    fieldDecisions: FieldDecisions;
  } {
    const merged: Record<string, unknown>      = {};
    const fieldDecisions: FieldDecisions       = {};
    const allFields = new Set([
      ...Object.keys(local),
      ...Object.keys(remote ?? {}),
    ]);

    for (const field of allFields) {
      const strategy: FieldStrategy   = schema[field] ?? 'lww';
      const localVal                  = local[field];
      const remoteVal                 = remote?.[field];
      const mergedVal                 = applyStrategy(localVal, remoteVal, strategy, remoteIsNewer);
      const conflict                  = JSON.stringify(localVal) !== JSON.stringify(remoteVal);

      merged[field] = mergedVal;
      fieldDecisions[field] = {
        strategy,
        localVal,
        remoteVal,
        mergedVal,
        conflicted: conflict && strategy !== 'lww',
      };
    }

    return { merged, fieldDecisions };
  }
}

export type FieldDecisions = Record<string, {
  strategy:  FieldStrategy;
  localVal:  unknown;
  remoteVal: unknown;
  mergedVal: unknown;
  conflicted: boolean; // true if the two values differed
}>;

// ─── Strategy implementations ─────────────────────────────────────────────────

function applyStrategy(
  local:         unknown,
  remote:        unknown,
  strategy:      FieldStrategy,
  remoteIsNewer: boolean
): unknown {
  switch (strategy) {

    case 'lww':
      if (remote === undefined) return local;
      if (local  === undefined) return remote;
      return remoteIsNewer ? remote : local;

    case 'text-merge': {
      const l = local  != null ? String(local)  : null;
      const r = remote != null ? String(remote) : null;
      if (l === r)              return l;
      if (l == null || l === '') return r;
      if (r == null || r === '') return l;
      // Sort then join — produces the same string regardless of argument order
      return [l, r].sort().join(' | ');
    }

    case 'counter': {
      // Monotone max: correct for grow-only counters without delta tracking.
      // If both devices incremented from the same baseline (e.g. 5 → 8 and 5 → 7),
      // max gives 8 (loses the +2 delta). For true additive merge, use a
      // delta payload format: { __inc: 3 } instead of an absolute value.
      const lNum = Number(local  ?? 0);
      const rNum = Number(remote ?? 0);
      return Math.max(lNum, rNum);
    }

    case 'max': {
      if (local  === undefined) return remote;
      if (remote === undefined) return local;
      const lCmp = Number(local);
      const rCmp = Number(remote);
      return lCmp >= rCmp ? local : remote;
    }

    case 'set-union': {
      const lArr = Array.isArray(local)  ? local  : (local  != null ? [local]  : []);
      const rArr = Array.isArray(remote) ? remote : (remote != null ? [remote] : []);
      const seen = new Set<string>();
      const union: unknown[] = [];
      for (const item of [...lArr, ...rArr]) {
        const key = JSON.stringify(item);
        if (!seen.has(key)) { seen.add(key); union.push(item); }
      }
      // Stable sort ensures determinism
      return union.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    }
  }
}
