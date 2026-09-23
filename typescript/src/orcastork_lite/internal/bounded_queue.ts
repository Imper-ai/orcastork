/**
 * An asynchronous FIFO with an optional depth bound — the port of `asyncio.Queue(maxsize)`.
 *
 * The queue is the seam between a producer the engine does not control (an operator's async
 * generator) and the consumer it does (the sole-writer gathering loop). A bound turns a fast
 * operator into backpressure instead of unbounded memory growth: `put` simply waits, exactly as it
 * does in Python. A `capacity` of zero is unbounded, which is what the orchestrator uses, because
 * that is what `asyncio.Queue()` gives the Python loop.
 *
 * Nothing here reads a clock: waiting is on another task's progress, never on time. A caller that
 * wants a *timed* wait races {@link BoundedQueue.whenNotEmpty} against `clock.sleep(ms)` — which is
 * why that wait is a broadcast that takes nothing off the queue. A waiter cancelled by losing such
 * a race must not have swallowed an item, or the signal it lost would never be seen again.
 *
 * Its own copy, not a shared one: `orcastork_lite` never imports `orcastork`.
 *
 * @module
 */

import { Deferred } from './deferred.js';

/**
 * A first-in, first-out queue with an optional maximum depth.
 *
 * `T` must not include `undefined`: {@link BoundedQueue.get} and {@link BoundedQueue.getNowait}
 * both resolve `undefined` to mean "nothing there", which is the one signal a consumer needs and
 * is cheaper than an exception on a path that runs once per emission.
 */
export class BoundedQueue<T> {
  private readonly items: T[] = [];
  private readonly arrivals: Deferred<void>[] = [];
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
   * waiting — a closed queue drops the item rather than throwing, because a producer losing the
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
    wakeAll(this.arrivals);
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
      await this.whenNotEmpty();
    }
    return this.getNowait();
  }

  /** The oldest item if one is buffered, else `undefined` — never waits. */
  public getNowait(): T | undefined {
    if (this.items.length === 0) {
      return undefined;
    }
    const item = this.items.shift();
    wakeOne(this.putWaiters);
    return item;
  }

  /**
   * Resolve as soon as an item is buffered (or the queue closes), **taking nothing off the queue**.
   *
   * Every waiter is woken, not one: this is a condition a caller may abandon (by losing a race
   * with a window coming due), and a wake-one hand-off to a waiter nobody is listening to any more
   * would strand the item behind it.
   */
  public whenNotEmpty(): Promise<void> {
    if (this.items.length > 0 || this.isClosed) {
      return Promise.resolve();
    }
    const waiter = new Deferred<void>();
    this.arrivals.push(waiter);
    return waiter.promise;
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
    wakeAll(this.arrivals);
    wakeAll(this.putWaiters);
  }

  private isFull(): boolean {
    return this.capacity > 0 && this.items.length >= this.capacity;
  }
}

/** Wake the longest-waiting producer, so a bounded queue hands out slots in arrival order. */
const wakeOne = (waiters: Deferred<void>[]): void => {
  waiters.shift()?.resolve(undefined);
};

const wakeAll = (waiters: Deferred<void>[]): void => {
  while (waiters.length > 0) {
    waiters.shift()?.resolve(undefined);
  }
};
