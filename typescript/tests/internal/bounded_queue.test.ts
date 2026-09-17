import { describe, expect, it } from 'vitest';
import { BoundedQueue } from '../../src/orcastork/internal/bounded_queue.js';

describe('BoundedQueue', () => {
  it('delivers items in the order they were put', async () => {
    const queue = new BoundedQueue<number>(4);

    await queue.put(1);
    await queue.put(2);

    await expect(queue.get()).resolves.toBe(1);
    await expect(queue.get()).resolves.toBe(2);
  });

  it('makes a producer wait while the queue is full, which is the backpressure', async () => {
    const queue = new BoundedQueue<number>(1);
    const accepted: number[] = [];

    await queue.put(1);
    const blocked = queue.put(2).then(() => {
      accepted.push(2);
    });

    // Nothing has drained yet, so the second put is still parked on a full queue.
    await Promise.resolve();
    expect(accepted).toEqual([]);
    expect(queue.size).toBe(1);

    await expect(queue.get()).resolves.toBe(1);
    await blocked;

    expect(accepted).toEqual([2]);
    expect(queue.size).toBe(1);
  });

  it('makes a consumer wait while the queue is empty', async () => {
    const queue = new BoundedQueue<string>(2);
    const received: (string | undefined)[] = [];

    const waiting = queue.get().then((item) => {
      received.push(item);
    });
    await Promise.resolve();
    expect(received).toEqual([]);

    await queue.put('late');
    await waiting;

    expect(received).toEqual(['late']);
  });

  it('never blocks when it is unbounded', async () => {
    const queue = new BoundedQueue<number>();

    await queue.put(1);
    await queue.put(2);
    await queue.put(3);

    expect(queue.size).toBe(3);
  });

  it('hands out slots in arrival order, so a slow producer is not starved', async () => {
    const queue = new BoundedQueue<string>(1);
    const enqueued: string[] = [];

    await queue.put('first');
    const second = queue.put('second').then(() => enqueued.push('second'));
    const third = queue.put('third').then(() => enqueued.push('third'));

    await expect(queue.get()).resolves.toBe('first');
    await second;
    await expect(queue.get()).resolves.toBe('second');
    await third;

    expect(enqueued).toEqual(['second', 'third']);
  });

  it('drains what is queued before reporting the end of the stream', async () => {
    const queue = new BoundedQueue<number>(4);
    await queue.put(1);
    await queue.put(2);

    queue.close();

    await expect(queue.get()).resolves.toBe(1);
    await expect(queue.get()).resolves.toBe(2);
    await expect(queue.get()).resolves.toBeUndefined();
  });

  it('wakes a waiting consumer when it closes', async () => {
    const queue = new BoundedQueue<number>(1);
    const waiting = queue.get();

    queue.close();

    await expect(waiting).resolves.toBeUndefined();
  });

  it('wakes a waiting producer when it closes, telling it the item was dropped', async () => {
    const queue = new BoundedQueue<number>(1);
    await queue.put(1);
    const blocked = queue.put(2);

    queue.close();

    await expect(blocked).resolves.toBe(false);
  });

  it('refuses a put on a closed queue instead of buffering it forever', async () => {
    const queue = new BoundedQueue<number>(1);
    queue.close();

    await expect(queue.put(1)).resolves.toBe(false);
    expect(queue.size).toBe(0);
    expect(queue.closed).toBe(true);
  });

  it('closes idempotently', async () => {
    const queue = new BoundedQueue<number>(1);

    queue.close();
    queue.close();

    await expect(queue.get()).resolves.toBeUndefined();
  });

  it('iterates until it is closed and drained', async () => {
    const queue = new BoundedQueue<number>(2);
    const seen: number[] = [];

    const consumer = (async (): Promise<void> => {
      for await (const item of queue) {
        seen.push(item);
      }
    })();

    await queue.put(1);
    await queue.put(2);
    await queue.put(3);
    queue.close();
    await consumer;

    expect(seen).toEqual([1, 2, 3]);
  });
});
