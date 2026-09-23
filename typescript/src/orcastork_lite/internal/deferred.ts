/**
 * A promise whose settlement is handed to someone else.
 *
 * The loop waits on things another task resolves — an item arriving on the emission queue, a slot
 * freed by the consumer. Python writes those as `asyncio.Future`/`Event`; the direct equivalent is
 * a promise plus its resolvers, kept together so the waiter and the waker read apart at the call
 * site.
 *
 * Its own copy, not a shared one: `orcastork_lite` never imports `orcastork`.
 *
 * @module
 */

/** A pending promise with its `resolve`/`reject` exposed. */
export class Deferred<T> {
  /** The promise handed to whoever waits. */
  public readonly promise: Promise<T>;

  private settledAt = false;
  private resolveFn!: (value: T | PromiseLike<T>) => void;
  private rejectFn!: (reason?: unknown) => void;

  public constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolveFn = resolve;
      this.rejectFn = reject;
    });
  }

  /** Whether the promise has already been settled — settling twice is a silent no-op, as in JS. */
  public get settled(): boolean {
    return this.settledAt;
  }

  /** Settle the promise with `value`. */
  public resolve(value: T | PromiseLike<T>): void {
    this.settledAt = true;
    this.resolveFn(value);
  }

  /** Settle the promise with a failure. */
  public reject(reason?: unknown): void {
    this.settledAt = true;
    this.rejectFn(reason);
  }
}
