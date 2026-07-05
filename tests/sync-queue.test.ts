import { SyncQueue } from '../src/core/sync-queue';
import { Operation } from '../src/core/types';

function makeOp(id: string, entityId = 'e1'): Operation {
  return {
    id,
    hlc: { wallTime: Date.now(), logical: 0, nodeId: 'n1' },
    entityType: 'users',
    entityId,
    type: 'update',
    payload: { name: 'Test' },
    nodeId: 'n1',
    synced: false,
  };
}

describe('SyncQueue', () => {
  describe('syncFromStorage', () => {
    it('adds new ops as queued', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a'), makeOp('b')]);
      const items = q.getItems();
      expect(items).toHaveLength(2);
      expect(items.every(i => i.status === 'queued')).toBe(true);
    });

    it('does not overwrite existing items (preserves failure history)', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markFailed(['a'], 'network error');
      q.syncFromStorage([makeOp('a')]);
      const item = q.getItem('a');
      expect(item?.status).toBe('failed');
      expect(item?.attempts).toBe(1);
    });

    it('removes items no longer in storage', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a'), makeOp('b')]);
      q.syncFromStorage([makeOp('a')]);
      expect(q.getItem('b')).toBeUndefined();
      expect(q.getItem('a')).toBeDefined();
    });

    it('keeps sending items when they are still present in storage', () => {
      const q = new SyncQueue();
      const op = makeOp('a');
      q.syncFromStorage([op]);
      q.markSending(['a']);
      // op is still pending in storage (not yet acknowledged)
      q.syncFromStorage([op]);
      expect(q.getItem('a')).toBeDefined();
      expect(q.getItem('a')?.status).toBe('sending');
    });

    it('removes stale sending items absent from storage (crash recovery)', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markSending(['a']);
      // Next cycle: op is gone from storage (was acknowledged before restart)
      q.syncFromStorage([]);
      expect(q.getItem('a')).toBeUndefined();
    });
  });

  describe('markSending', () => {
    it('transitions items to sending and records lastAttempt', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      const before = Date.now();
      q.markSending(['a']);
      const item = q.getItem('a')!;
      expect(item.status).toBe('sending');
      expect(item.lastAttempt).toBeGreaterThanOrEqual(before);
    });
  });

  describe('markSent', () => {
    it('transitions to sent, increments attempts, clears error', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markFailed(['a'], 'prior error');
      q.syncFromStorage([makeOp('a')]);
      q.markSending(['a']);
      q.markSent(['a']);
      const item = q.getItem('a')!;
      expect(item.status).toBe('sent');
      expect(item.attempts).toBe(2);
      expect(item.lastError).toBeNull();
    });

    it('auto-removes sent items after 2s', (done) => {
      jest.useFakeTimers();
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markSent(['a']);
      expect(q.getItem('a')).toBeDefined();
      jest.advanceTimersByTime(2100);
      expect(q.getItem('a')).toBeUndefined();
      jest.useRealTimers();
      done();
    });
  });

  describe('markFailed', () => {
    it('transitions to failed with error and increments attempts', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markSending(['a']);
      q.markFailed(['a'], 'connection refused');
      const item = q.getItem('a')!;
      expect(item.status).toBe('failed');
      expect(item.attempts).toBe(1);
      expect(item.lastError).toBe('connection refused');
    });

    it('caps error message at 120 characters', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a')]);
      q.markFailed(['a'], 'x'.repeat(200));
      expect(q.getItem('a')!.lastError).toHaveLength(120);
    });
  });

  describe('counts', () => {
    it('size() returns total item count', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a'), makeOp('b')]);
      expect(q.size()).toBe(2);
    });

    it('failedCount() counts only failed items', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a'), makeOp('b')]);
      q.markFailed(['a'], 'err');
      expect(q.failedCount()).toBe(1);
    });

    it('pendingCount() excludes sent items', () => {
      const q = new SyncQueue();
      q.syncFromStorage([makeOp('a'), makeOp('b')]);
      q.markSent(['a']);
      expect(q.pendingCount()).toBe(1);
    });
  });

  describe('getItems ordering', () => {
    it('returns items sorted oldest-first by queuedAt', () => {
      const q = new SyncQueue();
      const ops = [
        { ...makeOp('a'), hlc: { wallTime: 3000, logical: 0, nodeId: 'n1' } },
        { ...makeOp('b'), hlc: { wallTime: 1000, logical: 0, nodeId: 'n1' } },
        { ...makeOp('c'), hlc: { wallTime: 2000, logical: 0, nodeId: 'n1' } },
      ];
      q.syncFromStorage(ops);
      const ids = q.getItems().map(i => i.opId);
      expect(ids).toEqual(['b', 'c', 'a']);
    });
  });

  describe('onChange', () => {
    it('fires on syncFromStorage, markSending, markSent, markFailed', () => {
      const q = new SyncQueue();
      const calls: string[] = [];
      q.onChange(() => calls.push('change'));

      q.syncFromStorage([makeOp('a')]);
      q.markSending(['a']);
      q.markFailed(['a'], 'err');
      q.syncFromStorage([makeOp('a')]);
      q.markSent(['a']);

      expect(calls.length).toBeGreaterThanOrEqual(5);
    });

    it('unsubscribe stops notifications', () => {
      const q = new SyncQueue();
      let count = 0;
      const unsub = q.onChange(() => count++);
      q.syncFromStorage([makeOp('a')]);
      unsub();
      q.syncFromStorage([makeOp('a')]);
      expect(count).toBe(1);
    });
  });
});
