"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.MergeRegistry = void 0;
/**
 * Global registry. Registered schemas are used by SyncManager at sync time.
 * Unregistered entity types fall back to LWW.
 */
class MergeRegistry {
    static registry = new Map();
    static register(entityType, schema) {
        this.registry.set(entityType, schema);
    }
    static get(entityType) {
        return this.registry.get(entityType);
    }
    static has(entityType) {
        return this.registry.has(entityType);
    }
    static getAll() {
        return Object.fromEntries(this.registry.entries());
    }
}
exports.MergeRegistry = MergeRegistry;
//# sourceMappingURL=merge-registry.js.map