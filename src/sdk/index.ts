// Public SDK surface — import from 'edge-native-db'
export { EdgeDB, SyncController, Inspector } from './edge-db';
export type { EdgeDBConfig } from './edge-db';
export { Collection } from './collection';
export { Logger, globalLogger } from './logger';
export type { LogEntry, LogLevel, LogCategory } from './logger';

// Re-export core types that SDK consumers need
export type {
  Entity,
  Operation,
  HLC,
  Conflict,
  ConflictVersion,
  QueueItem,
  QueueItemStatus,
  SyncState,
  SyncStatus,
  SyncAttempt,
  SyncError,
} from '../core/types';

export type { MergeSchema, FieldStrategy } from '../core/merge-registry';

// Server-side sync server (separate import path in practice)
export { createSyncServer } from '../server/sync-server';
