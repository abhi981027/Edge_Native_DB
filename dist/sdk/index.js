"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSyncServer = exports.globalLogger = exports.Logger = exports.Collection = exports.Inspector = exports.SyncController = exports.EdgeDB = void 0;
// Public SDK surface — import from 'edge-native-db'
var edge_db_1 = require("./edge-db");
Object.defineProperty(exports, "EdgeDB", { enumerable: true, get: function () { return edge_db_1.EdgeDB; } });
Object.defineProperty(exports, "SyncController", { enumerable: true, get: function () { return edge_db_1.SyncController; } });
Object.defineProperty(exports, "Inspector", { enumerable: true, get: function () { return edge_db_1.Inspector; } });
var collection_1 = require("./collection");
Object.defineProperty(exports, "Collection", { enumerable: true, get: function () { return collection_1.Collection; } });
var logger_1 = require("./logger");
Object.defineProperty(exports, "Logger", { enumerable: true, get: function () { return logger_1.Logger; } });
Object.defineProperty(exports, "globalLogger", { enumerable: true, get: function () { return logger_1.globalLogger; } });
// Server-side sync server (separate import path in practice)
var sync_server_1 = require("../server/sync-server");
Object.defineProperty(exports, "createSyncServer", { enumerable: true, get: function () { return sync_server_1.createSyncServer; } });
//# sourceMappingURL=index.js.map