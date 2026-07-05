/**
 * Minimal upstream sync server.
 *
 * Stores a sequence-ordered oplog. Edge nodes push their local ops here and
 * pull remote ops by cursor. The server is intentionally dumb — it stores
 * everything and lets edge nodes resolve conflicts locally.
 *
 * In production this would be replaced by a real backend. For development and
 * testing it lets you run the full sync flow on one machine.
 */
export declare function createMockServer(port: number, dbPath?: string): {
    close: () => void;
};
//# sourceMappingURL=mock-server.d.ts.map