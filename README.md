# Edge Native DB

Edge Native DB is an offline-first sync engine for edge and field devices.

It is designed for situations where devices can lose connectivity, keep writing locally, and later reconcile safely when the network comes back. The project combines Hybrid Logical Clocks (HLC), conflict tracking, CRDT-based merge strategies, and SQLite durability to provide predictable eventual consistency.

## Why this project exists

Traditional cloud-first data layers assume stable connectivity. Edge systems usually do not have that luxury.

This project focuses on:

- Reliable local writes while offline
- Safe synchronization when connectivity returns
- Conflict visibility and conflict resolution workflows
- Deterministic ordering and idempotent delivery
- Practical tooling for demos, debugging, and simulation

## Core capabilities

- Offline-first local writes with queued sync
- HLC ordering for distributed write causality
- Conflict detection with unresolved/resolved tracking
- CRDT field-level merge strategies with fallback to LWW
- Idempotent sync protocol and duplicate protection
- Retry and backoff behavior in sync lifecycle
- Multi-device simulation utilities
- In-browser dashboards and devtools endpoints

## Tech stack

- TypeScript
- Node.js (22+)
- Express
- SQLite (WAL mode)
- Jest + ts-jest

## Quick start

### 1) Install dependencies

```bash
npm install
```

### 2) Run in development mode

```bash
npm run dev
```

### 3) Open the local UIs

- Edge UI: http://localhost:3000
- Dashboard: http://localhost:3000/dashboard
- Multi-device demo: http://localhost:3000/multi
- Devtools: http://localhost:3000/devtools
- Product demo page: http://localhost:3000/product
- Sync server dashboard: http://localhost:3001

## Environment variables

You can customize runtime behavior with:

- NODE_ID: stable device ID used by HLC and server cursor tracking
- DB_PATH: path to local SQLite database file
- UI_PORT: local UI server port (default 3000)
- UPSTREAM_PORT: sync server port (default 3001)

Example:

```bash
NODE_ID=device-alpha DB_PATH=./edge-data.db UI_PORT=3000 UPSTREAM_PORT=3001 npm run dev
```

## Scripts

- npm run dev: run TypeScript entrypoint directly with ts-node
- npm run build: compile into dist using tsconfig.build.json
- npm start: run compiled build from dist
- npm test: run test suite
- npm run test:watch: run tests in watch mode
- npm run typecheck: check TypeScript types without emit

## SDK usage example

The SDK exposes a typed collection API and sync controls.

```ts
import { EdgeDB } from 'edge-native-db';

type Task = {
  id: string;
  title: string;
  done: boolean;
  updated_at: number;
};

const db = new EdgeDB({
  nodeId: 'field-unit-42',
  dbPath: './edge.db',
  upstreamUrl: 'http://localhost:3001',
  mergeSchemas: {
    tasks: {
      title: 'text-merge',
      done: 'max',
      updated_at: 'max',
    },
  },
});

db.connect();

const tasks = db.collection<Task>('tasks');
tasks.upsert({ id: 't1', title: 'Inspect sensor rack', done: false, updated_at: Date.now() });

const unsubscribe = tasks.subscribe((items) => {
  console.log('Task count:', items.length);
});

await db.sync.force();

unsubscribe();
db.disconnect();
```

## How syncing works (high level)

1. Local writes are appended as operations and reflected in local entity state.
2. Pending operations are pushed to the sync server.
3. The server stores incoming changes idempotently.
4. The server returns remote changes that this device has not seen yet.
5. The device applies incoming changes, detects conflicts, and resolves according to strategy.
6. Unresolved conflicts stay visible for manual or automatic merge workflows.

## Project layout

- src/core: clocks, engine, storage, conflict handling, sync queue/manager
- src/network: network adapters and retry behavior
- src/sdk: public SDK API, typed collections, logger
- src/server: sync server, mock server, local UI API server
- src/simulator: multi-device simulation
- tests: unit and integration-style test coverage
- public: HTML demo surfaces and dashboards

## Testing

Run all tests:

```bash
npm test
```

Watch mode:

```bash
npm run test:watch
```

Type checks:

```bash
npm run typecheck
```

## Notes for contributors

- Keep node IDs stable when testing real sync behavior.
- Prefer adding tests when touching conflict, queue, retry, or sync ordering logic.
- Use the devtools and multi-device pages to reproduce edge cases before changing core sync code.

## License

MIT
