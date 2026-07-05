import { CircuitBreaker, withRetry } from '../src/network/retry';

describe('CircuitBreaker', () => {
  test('starts closed', () => {
    const cb = new CircuitBreaker(3, 1000);
    expect(cb.isOpen()).toBe(false);
    expect(cb.getState()).toBe('closed');
  });

  test('opens after threshold failures', () => {
    const cb = new CircuitBreaker(3, 10_000);
    cb.onFailure(); cb.onFailure(); expect(cb.isOpen()).toBe(false);
    cb.onFailure(); expect(cb.isOpen()).toBe(true);
    expect(cb.getState()).toBe('open');
  });

  test('resets to closed after success', () => {
    const cb = new CircuitBreaker(2, 10_000);
    cb.onFailure(); cb.onFailure();
    expect(cb.isOpen()).toBe(true);
    cb.onSuccess();
    expect(cb.isOpen()).toBe(false);
    expect(cb.getState()).toBe('closed');
  });

  test('transitions open → half-open after recovery window', async () => {
    const cb = new CircuitBreaker(1, 50);
    cb.onFailure();
    expect(cb.isOpen()).toBe(true);
    await new Promise(r => setTimeout(r, 60));
    expect(cb.isOpen()).toBe(false); // half-open, allows probe
    expect(cb.getState()).toBe('half-open');
  });
});

describe('withRetry', () => {
  test('returns result on first success', async () => {
    const fn = jest.fn().mockResolvedValue('ok');
    const result = await withRetry(fn, { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 10, jitterFactor: 0 });
    expect(result).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('retries up to maxAttempts then throws', async () => {
    const err = new Error('fail');
    const fn  = jest.fn().mockRejectedValue(err);
    await expect(
      withRetry(fn, { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 10, jitterFactor: 0 })
    ).rejects.toThrow('fail');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  test('succeeds on second attempt', async () => {
    let calls = 0;
    const fn = jest.fn().mockImplementation(() => {
      calls++;
      if (calls < 2) return Promise.reject(new Error('transient'));
      return Promise.resolve('recovered');
    });
    const result = await withRetry(fn, { maxAttempts: 4, baseDelayMs: 1, maxDelayMs: 10, jitterFactor: 0 });
    expect(result).toBe('recovered');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  test('fails fast when circuit is open', async () => {
    const cb = new CircuitBreaker(1, 60_000);
    cb.onFailure(); // open the circuit

    const fn = jest.fn().mockResolvedValue('ok');
    await expect(
      withRetry(fn, { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 10, jitterFactor: 0 }, cb)
    ).rejects.toThrow('Circuit open');
    expect(fn).not.toHaveBeenCalled(); // no call made
  });
});
