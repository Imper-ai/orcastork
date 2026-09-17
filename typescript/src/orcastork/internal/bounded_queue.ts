/**
 * A bounded asynchronous FIFO — the port's `asyncio.Queue(maxsize)`.
 *
 * Bounded on purpose: the queue is the seam between a producer the engine does not control (an
 * operator's async generator) and a consumer it does (the sole-mutator loop). An unbounded queue
 * turns a fast operator into unbounded memory growth, while a bounded one turns it into
 * backpressure — `put` simply waits, exactly as it does in Python.
 *
 * Nothing here reads a clock: waiting is on another task's progress, never on time. A caller that
 * wants a *timed* wait races the returned promise against `clock.sleep(ms)`.
 *
 * @module
 */

import { Deferred } from './deferred.js';

/**
 * A first-in, first-out queue with a maximum depth.
 *
 * `T` must not include `undefined`: {@link BoundedQueue.get} resolves `undefined` to mean "closed
 * and drained", which is the one signal a consumer needs and is cheaper than an exception on a
 * path that ends every session.
 */
export class BoundedQueue<T> implements AsyncIterable<T> {
  private readonly items: T[] = [];
  private readonly getWaiters: Deferred<void>[] = [];
  private readonly putWaiters: Deferred<void>[] = [];
  private isClosed = false;

  /** `capacity` of zero or less is unbounded, mirroring `asyncio.Queue(maxsize=0)`. */
  public constructor(public readonly capacity: number = 0) {}

  /** How many items are buffered right now. */
  public get size(): number {
    return this.items.length;
  }

  /** Whether {@link BoundedQueue.close} has been called. */
  public get closed(): boolean {
    return this.isClosed;
  }

  /**
   * Append `item`, waiting while the queue is full.
   *
   * Resolves `true` once the item is queued, or `false` if the queue was closed before or while
   * waiting — a closed queue drops the item rather than throwing, because the producer losing the
   * race with a shutdown is normal, not exceptional.
   */
  public async put(item: T): Promise<boolean> {
    while (!this.isClosed && this.isFull()) {
      const waiter = new Deferred<void>();
      this.putWaiters.push(waiter);
      await waiter.promise;
    }
    if (this.isClosed) {
      return false;
    }
    this.items.push(item);
    wakeOne(this.getWaiters);
    return true;
  }

  /**
   * Take the oldest item, waiting while the queue is empty.
   *
   * Resolves `undefined` once the queue is closed **and** drained — the items already queued are
   * always delivered first, so closing is a graceful end-of-stream and not a discard.
   */
  public async get(): Promise<T | undefined> {
    while (this.items.length === 0 && !this.isClosed) {
      const waiter = new Deferred<void>();
      this.getWaiters.push(waiter);
      await waiter.promise;
    }
    return this.getNowait();
  }

  /**
   * Take the oldest item if one is buffered, else `undefined` — never waits.
   *
   * The port of `asyncio.Queue.get_nowait()`, without its `QueueEmpty`: the sole-mutator loop
   * drains everything queued behind the signal it woke on, and an empty queue is the ordinary end
   * of that drain rather than an exceptional condition.
   */
  public getNowait(): T | undefined {
    if (this.items.length === 0) {
      return undefined;
    }
    const item = this.items.shift();
    wakeOne(this.putWaiters);
    return item;
  }

  /**
   * Close the queue and wake every waiter.
   *
   * Waiting producers resolve `false`, and consumers resolve `undefined` once what is queued has
   * been drained. Closing twice is a no-op.
   */
  public close(): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    wakeAll(this.getWaiters);
    wakeAll(this.putWaiters);
  }

  /** Iterate until the queue is closed and drained. */
  public async *[Symbol.asyncIterator](): AsyncGenerator<T, void, void> {
    while (true) {
      const item = await this.get();
      if (item === undefined) {
        return;
      }
      yield item;
    }
  }

  private isFull(): boolean {
    return this.capacity > 0 && this.items.length >= this.capacity;
  }
}

/** Wake the longest-waiting task, so a queue hands out slots and items in arrival order. */
const wakeOne = (waiters: Deferred<void>[]): void => {
  waiters.shift()?.resolve(undefined);
};

const wakeAll = (waiters: Deferred<void>[]): void => {
  while (waiters.length > 0) {
    waiters.shift()?.resolve(undefined);
  }
};
