import { describe, expect, it } from 'vitest';
import { Deferred } from '../../src/orcastork/internal/deferred.js';

describe('Deferred', () => {
  it('stays pending until someone resolves it', async () => {
    const deferred = new Deferred<string>();
    const order: string[] = [];

    const waiter = deferred.promise.then((value) => {
      order.push(`woken:${value}`);
    });
    order.push('waiting');
    deferred.resolve('ready');
    await waiter;

    expect(order).toEqual(['waiting', 'woken:ready']);
  });

  it('reports whether it has settled', () => {
    const deferred = new Deferred<void>();
    expect(deferred.settled).toBe(false);

    deferred.resolve(undefined);

    expect(deferred.settled).toBe(true);
  });

  it('rejects with the reason it was given', async () => {
    const deferred = new Deferred<void>();
    const failure = new Error('no');

    deferred.reject(failure);

    await expect(deferred.promise).rejects.toBe(failure);
    expect(deferred.settled).toBe(true);
  });

  it('ignores a second settlement, as a promise does', async () => {
    const deferred = new Deferred<number>();

    deferred.resolve(1);
    deferred.resolve(2);

    await expect(deferred.promise).resolves.toBe(1);
  });
});
