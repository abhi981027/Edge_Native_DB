import { NetworkMonitor } from '../src/core/network-monitor';

function makeMonitor(probe: () => Promise<boolean>) {
  return new NetworkMonitor(probe);
}

describe('NetworkMonitor', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(()  => { jest.useRealTimers(); });

  it('starts as offline', () => {
    const m = makeMonitor(() => Promise.resolve(true));
    expect(m.online).toBe(false);
  });

  it('fires onChange(true) when probe returns true on first poll', async () => {
    const m = makeMonitor(() => Promise.resolve(true));
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve(); // flush micro-tasks
    expect(events).toEqual([true]);
    expect(m.online).toBe(true);
    m.stop();
  });

  it('fires onChange(false) when probe returns false on first poll', async () => {
    const m = makeMonitor(() => Promise.resolve(false));
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();
    // offline → offline: no change event (both start as false)
    expect(events).toEqual([]);
    expect(m.online).toBe(false);
    m.stop();
  });

  it('detects reconnect on subsequent poll', async () => {
    let callCount = 0;
    const m = makeMonitor(async () => { callCount++; return callCount >= 2; });
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();   // poll 1: false → false (no event)
    expect(events).toEqual([]);

    jest.advanceTimersByTime(3000);
    await Promise.resolve();   // poll 2: false → true
    expect(events).toEqual([true]);
    expect(m.online).toBe(true);
    m.stop();
  });

  it('detects disconnection', async () => {
    let count = 0;
    const m = makeMonitor(async () => ++count === 1);
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();   // poll 1: online
    expect(events).toEqual([true]);

    jest.advanceTimersByTime(3000);
    await Promise.resolve();   // poll 2: offline
    expect(events).toEqual([true, false]);
    expect(m.online).toBe(false);
    m.stop();
  });

  it('does not emit duplicate events when state is unchanged', async () => {
    const m = makeMonitor(() => Promise.resolve(true));
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();

    jest.advanceTimersByTime(3000);
    await Promise.resolve();
    jest.advanceTimersByTime(3000);
    await Promise.resolve();

    // Should only emit once (true→true produces no further events)
    expect(events).toEqual([true]);
    m.stop();
  });

  it('treats probe rejection as offline', async () => {
    const m = makeMonitor(() => Promise.reject(new Error('timeout')));
    const events: boolean[] = [];
    m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();
    expect(m.online).toBe(false);
    expect(events).toEqual([]);  // false → false, no event
    m.stop();
  });

  it('stop() cancels the interval', async () => {
    let probeCount = 0;
    const m = makeMonitor(async () => { probeCount++; return true; });
    m.start();
    await Promise.resolve();
    m.stop();
    const countAfterStop = probeCount;

    jest.advanceTimersByTime(9000);
    await Promise.resolve();

    expect(probeCount).toBe(countAfterStop);
  });

  it('unsubscribe removes the listener', async () => {
    let count = 0;
    const m = makeMonitor(async () => ++count <= 1);
    const events: boolean[] = [];
    const unsub = m.onChange(v => events.push(v));
    m.start();
    await Promise.resolve();
    unsub();

    jest.advanceTimersByTime(3000);
    await Promise.resolve();

    expect(events).toHaveLength(1);
    m.stop();
  });
});
