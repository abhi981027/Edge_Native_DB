"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.UserRepository = void 0;
const ENTITY_TYPE = 'users';
class UserRepository {
    engine;
    constructor(engine) {
        this.engine = engine;
    }
    createUser(user) {
        return this.engine.write(ENTITY_TYPE, user.id, {
            name: user.name,
            updated_at: user.updated_at,
            device_id: user.device_id,
        });
    }
    updateUser(id, patch) {
        const now = Date.now();
        return this.engine.write(ENTITY_TYPE, id, {
            ...patch,
            updated_at: patch.updated_at ?? now,
        });
    }
    getUser(id) {
        const entity = this.engine.get(ENTITY_TYPE, id);
        if (!entity || entity.tombstone)
            return undefined;
        return entityToUser(entity.id, entity.data);
    }
    getAllUsers() {
        return this.engine
            .list(ENTITY_TYPE)
            .map(e => entityToUser(e.id, e.data));
    }
    deleteUser(id) {
        return this.engine.remove(ENTITY_TYPE, id);
    }
}
exports.UserRepository = UserRepository;
function entityToUser(id, data) {
    return {
        id,
        name: String(data['name'] ?? ''),
        updated_at: Number(data['updated_at'] ?? 0),
        device_id: String(data['device_id'] ?? ''),
    };
}
//# sourceMappingURL=users.js.map