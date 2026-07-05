import { HybridLogicalClock } from '../src/core/hlc';

describe('HybridLogicalClock', () => {
  test('now() advances monotonically', () => {
    const hlc = new HybridLogicalClock('A');
    const t1 = hlc.now();
    const t2 = hlc.now();
    expect(hlc.compare(t2, t1)).toBeGreaterThan(0);
  });

  test('same-millisecond calls increment logical counter', () => {
    const hlc = new HybridLogicalClock('A');
    const real = Date.now;
    Date.now = () => 1_000_000; // freeze time

    try {
      const t1 = hlc.now();
      const t2 = hlc.now();
      expect(t2.wallTime).toBe(t1.wallTime);
      expect(t2.logical).toBe(t1.logical + 1);
    } finally {
      Date.now = real;
    }
  });

  test('receive() merges remote clock ahead of local', () => {
    const hlcA = new HybridLogicalClock('A');
    const hlcB = new HybridLogicalClock('B');

    Date.now = () => 1_000;
    hlcA.now(); // wallTime=1000

    Date.now = () => 5_000;
    const tB = hlcB.now(); // wallTime=5000

    Date.now = () => 2_000; // A's wall is behind B's
    const merged = hlcA.receive(tB);
    expect(merged.wallTime).toBe(5_000); // advanced to B's wall
  });

  test('compare(): wall time takes priority over logical', () => {
    const hlc = new HybridLogicalClock('A');
    const earlier = { wallTime: 100, logical: 999, nodeId: 'A' };
    const later   = { wallTime: 200, logical: 0,   nodeId: 'A' };
    expect(hlc.compare(later, earlier)).toBeGreaterThan(0);
    expect(hlc.compare(earlier, later)).toBeLessThan(0);
  });

  test('compare(): nodeId breaks ties', () => {
    const hlc = new HybridLogicalClock('A');
    const a = { wallTime: 1, logical: 1, nodeId: 'A' };
    const b = { wallTime: 1, logical: 1, nodeId: 'B' };
    expect(hlc.compare(a, b)).toBeLessThan(0); // 'A' < 'B'
    expect(hlc.compare(b, a)).toBeGreaterThan(0);
  });

  test('serialize / deserialize roundtrip', () => {
    const hlc = new HybridLogicalClock('node-abc-123');
    const t = hlc.now();
    const back = hlc.deserialize(hlc.serialize(t));
    expect(back).toEqual(t);
  });
});
