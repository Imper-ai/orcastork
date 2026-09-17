/** Bounding a real await: the port of `asyncio.wait_for`. */

import { describe, expect, it } from 'vitest';
import { OrchestrationError } from '../../src/orcastork/exceptions.js';
import { OperationTimeoutError, withTimeout } from '../../src/orcastork/internal/timeouts.js';

/** A promise that never settles — what a wedged operator looks like to the bound around it. */
const hangs = (): Promise<never> => new Promise<never>(() => undefined);

describe('withTimeout', () => {
  it('returns the operation value when it finishes inside the bound', async () => {
    await expect(withTimeout(Promise.resolve('done'), 1_000)).resolves.toBe('done');
  });

  it('rejects with an OperationTimeoutError when the operation outlives the bound', async () => {
    await expect(withTimeout(hangs(), 5)).rejects.toBeInstanceOf(OperationTimeoutError);
  });

  it('raises a framework error, never a bare one, so a caller can catch it by type', async () => {
    const error = await withTimeout(hangs(), 5).catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(OrchestrationError);
    expect((error as Error).name).toBe('OperationTimeoutError');
    expect((error as Error).message).toContain('5 ms');
  });

  it('propagates the failure the operation itself raised rather than dressing it as a timeout', async () => {
    const failure = new Error('boom');

    await expect(withTimeout(Promise.reject(failure), 1_000)).rejects.toBe(failure);
  });

  it('treats a bound no timer can express as no bound at all', async () => {
    // A `setTimeout` beyond the 32-bit range fires immediately, which would turn "no deadline" into
    // "deadline now" — the one failure mode a bound must never have.
    await expect(withTimeout(Promise.resolve('kept'), Number.POSITIVE_INFINITY)).resolves.toBe('kept');
    await expect(withTimeout(Promise.resolve('kept'), 2 ** 40)).resolves.toBe('kept');
  });

  it('lets a rejection that arrives after the bound has fired go unhandled by no one', async () => {
    // The loser of the race still rejects; `race` keeps a handler subscribed to it, so the process
    // never sees an unhandled rejection from an operation that was abandoned.
    let reject: (reason: unknown) => void = () => undefined;
    const late = new Promise<never>((_resolve, rejectLate) => {
      reject = rejectLate;
    });

    await expect(withTimeout(late, 5)).rejects.toBeInstanceOf(OperationTimeoutError);
    reject(new Error('too late to matter'));
    await new Promise<void>((resolve) => setImmediate(resolve));
  });
});
