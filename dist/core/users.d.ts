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
    updated_at: number;
    device_id: string;
};
export declare class UserRepository {
    private readonly engine;
    constructor(engine: SyncEngine);
    createUser(user: User): string;
    updateUser(id: string, patch: Partial<Omit<User, 'id'>>): string;
    getUser(id: string): User | undefined;
    getAllUsers(): User[];
    deleteUser(id: string): string;
}
//# sourceMappingURL=users.d.ts.map