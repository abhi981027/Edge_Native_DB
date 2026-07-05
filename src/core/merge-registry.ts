/**
 * Field-level merge strategies for CRDT-lite resolution.
 *
 * Registered globally at app startup; consulted at sync time when a
 * conflict is detected for a given entity type.
 *
 * LWW (default, unregistered types):
 *   Picks the record with the higher HLC timestamp wholesale.
 *   Fast, simple, correct for entities where staleness is acceptable.
 *
 * CRDT-lite (registered types):
 *   Resolves each field independently. Different fields from concurrent
 *   writes can co-exist in the final merged record.
 */

export type FieldStrategy =
  | 'lww'        // default: higher HLC timestamp wins the whole field
  | 'text-merge' // concatenate distinct text values: "A | B" (deterministic: sorted)
  | 'counter'    // monotone max — safe for grow-only counters (views, likes, scores)
  | 'max'        // take the numerically or temporally larger value
  | 'set-union'; // union of array values, stable-sorted

/** Maps field names to their merge strategy for an entity type. */
export type MergeSchema = Record<string, FieldStrategy>;

/**
 * Global registry. Registered schemas are used by SyncManager at sync time.
 * Unregistered entity types fall back to LWW.
 */
export class MergeRegistry {
  private static readonly registry = new Map<string, MergeSchema>();

  static register(entityType: string, schema: MergeSchema): void {
    this.registry.set(entityType, schema);
  }

  static get(entityType: string): MergeSchema | undefined {
    return this.registry.get(entityType);
  }

  static has(entityType: string): boolean {
    return this.registry.has(entityType);
  }

  static getAll(): Record<string, MergeSchema> {
    return Object.fromEntries(this.registry.entries());
  }
}
