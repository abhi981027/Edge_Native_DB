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
export type FieldStrategy = 'lww' | 'text-merge' | 'counter' | 'max' | 'set-union';
/** Maps field names to their merge strategy for an entity type. */
export type MergeSchema = Record<string, FieldStrategy>;
/**
 * Global registry. Registered schemas are used by SyncManager at sync time.
 * Unregistered entity types fall back to LWW.
 */
export declare class MergeRegistry {
    private static readonly registry;
    static register(entityType: string, schema: MergeSchema): void;
    static get(entityType: string): MergeSchema | undefined;
    static has(entityType: string): boolean;
    static getAll(): Record<string, MergeSchema>;
}
//# sourceMappingURL=merge-registry.d.ts.map