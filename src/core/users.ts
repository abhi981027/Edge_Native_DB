import { SyncEngine } from './engine';

/**
 * Users domain layer — thin wrapper over SyncEngine.
 *
 * Schema mirrors the requested spec:
 *   users(id TEXT, name TEXT, updated_at INTEGER, device_id TEXT)
 *
 * All writes go through the engine → oplog → entity store, so:
 *   - Atomic (SQLite transaction under the hood)
 *   - Crash-safe (WAL journal; oplog survives process kill)
 *   - Offline-safe (queued until network recovers)
 *   - Eventually consistent across nodes
 */

export type User = {
  id: string;
  name: string;
  updated_at: number; // epoch ms — set by the engine's HLC wallTime
  device_id: string;
};

const ENTITY_TYPE = 'users';

export class UserRepository {
  constructor(private readonly engine: SyncEngine) {}

  createUser(user: User): string {
    return this.engine.write(ENTITY_TYPE, user.id, {
      name: user.name,
      updated_at: user.updated_at,
      device_id: user.device_id,
    });
  }

  updateUser(id: string, patch: Partial<Omit<User, 'id'>>): string {
    const now = Date.now();
    return this.engine.write(ENTITY_TYPE, id, {
      ...patch,
      updated_at: patch.updated_at ?? now,
    });
  }

  getUser(id: string): User | undefined {
    const entity = this.engine.get(ENTITY_TYPE, id);
    if (!entity || entity.tombstone) return undefined;
    return entityToUser(entity.id, entity.data);
  }

  getAllUsers(): User[] {
    return this.engine
      .list(ENTITY_TYPE)
      .map(e => entityToUser(e.id, e.data));
  }

  deleteUser(id: string): string {
    return this.engine.remove(ENTITY_TYPE, id);
  }
}

function entityToUser(id: string, data: Record<string, unknown>): User {
  return {
    id,
    name: String(data['name'] ?? ''),
    updated_at: Number(data['updated_at'] ?? 0),
    device_id: String(data['device_id'] ?? ''),
  };
}
