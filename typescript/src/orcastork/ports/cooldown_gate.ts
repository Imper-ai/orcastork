/**
 * The `CooldownGate` port — an atomic, durable per-key cooldown.
 *
 * Backs the manager's `SchedulingGate`: may a new session for this key (a namespace, a device)
 * start now? `tryAcquire` is check-and-arm in one atomic step — a `true` return immediately starts
 * the cooldown, so two racing callers can never both win — and the state is owned by the backing
 * store, so the cooldown holds across pods and restarts.
 *
 * @module
 */

/** The durable "not again yet" a scheduling gate is built on. */
export interface CooldownGate {
  /** Atomically start a cooldown for `key` iff none is active; `true` means proceed. */
  tryAcquire(key: string, cooldownMs: number): Promise<boolean>;
}
