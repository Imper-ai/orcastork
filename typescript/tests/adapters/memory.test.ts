/**
 * MEM — in-memory-adapter mechanics: determinism, infra-free, FakeClock-driven.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryDataPointStore,
  InMemoryInbox,
  InMemoryRateLimiter,
  InMemorySessionLock,
} from '../../src/orcastork/adapters/memory/index.js';
import { Epoch, newSessionId, SessionId } from '../../src/orcastork/ids.js';
import { withTimeout } from '../../src/orcastork/internal/index.js';
import { EffectClaim, effectPendingState } from '../../src/orcastork/ports/datapoint_store.js';
import { NullRateLimiter } from '../../src/orcastork/ports/index.js';
import { buildInMemoryRuntime } from '../../src/orcastork/runtime.js';
import { FakeClock } from '../doubles/clock.js';
import { workEmail } from '../doubles/datapoints.js';

/** One second of token accrual, in the milliseconds the port speaks. */
const ONE_SECOND_MS = 1000;

describe('the in-memory adapter family', () => {
  it('round-trips a DataPoint through a runtime with no infra', async () => {
    // A fully in-memory runtime exposes all its ports and round-trips a DataPoint with no infra.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const session = newSessionId();
    await runtime.store.write(session, [workEmail('a@e.example')], { epoch: Epoch(1) });
    expect((await runtime.store.snapshot(session)).all()).toHaveLength(1);
    for (const adapter of [
      runtime.store,
      runtime.inbox,
      runtime.lock,
      runtime.audit,
      runtime.durable,
      runtime.catalog,
      runtime.rateLimiter,
    ]) {
      expect(adapter).not.toBeNull();
    }
  });

  it('governs lock expiry by the fake clock alone', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const session = SessionId('mem-session');

    await runtime.lock.acquire(session);
    expect(await runtime.lock.isHeld(session)).toBe(true); // no wall-clock time passes on its own

    clock.advance(31 * ONE_SECOND_MS);
    expect(await runtime.lock.isHeld(session)).toBe(false); // expiry is driven purely by the FakeClock
  });

  it('hands out the burst token immediately, then paces', async () => {
    const clock = new FakeClock();
    const limiter = new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: 1 });

    await limiter.acquire('namespace-1:idp');
    expect(clock.monotonic()).toBe(0); // the burst token → immediate

    await limiter.acquire('namespace-1:idp');
    expect(clock.monotonic()).toBe(ONE_SECOND_MS); // waited (via clock.sleep) until the next token accrued
  });

  it('keeps one bucket per key', async () => {
    const clock = new FakeClock();
    const limiter = new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: 1 });

    await limiter.acquire('namespace-1:idp');
    await limiter.acquire('namespace-2:idp'); // a different key draws from its own untouched bucket

    expect(clock.monotonic()).toBe(0);
  });

  it('caps a refill at the burst', async () => {
    const clock = new FakeClock();
    const limiter = new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: 2 });
    await limiter.acquire('k');
    await limiter.acquire('k'); // the bucket is empty at t=0

    clock.advance(60 * ONE_SECOND_MS); // a long idle period refills at most `burst` tokens, never more

    await limiter.acquire('k');
    await limiter.acquire('k');
    expect(clock.monotonic()).toBe(60 * ONE_SECOND_MS); // both came from the capped refill — immediate
    await limiter.acquire('k');
    expect(clock.monotonic()).toBe(61 * ONE_SECOND_MS); // the third had to wait out a full period again
  });

  it('passes straight through the null rate limiter', async () => {
    const limiter = new NullRateLimiter();

    // A short wall-clock bound proves each acquire returns immediately — the null limiter never
    // waits and never accumulates state across calls.
    for (let index = 0; index < 3; index += 1) {
      await withTimeout(limiter.acquire('namespace-1:idp'), 100);
    }
  });

  it('never lets a stale release free a successor live lease', async () => {
    // A fenced predecessor (epoch 1) must not be able to drop a successor's (epoch 2) live lease:
    // release is a no-op unless the stored lease belongs to the calling epoch (no split-brain).
    const clock = new FakeClock();
    const lock = new InMemorySessionLock(clock, { ttlMs: 30 * ONE_SECOND_MS });
    const session = SessionId('mem-stale-release');

    const epoch1 = await lock.acquire(session);
    clock.advance(31 * ONE_SECOND_MS); // epoch 1's lease expires
    const epoch2 = await lock.acquire(session); // epoch 2 takes over, now the live holder
    expect(epoch2).toBeGreaterThan(epoch1);

    await lock.release(session, { epoch: epoch1 }); // the stale predecessor tries to release

    expect(await lock.isHeld(session)).toBe(true); // the successor's lease survived
    expect(await lock.currentEpoch(session)).toBe(epoch2);
  });

  it('rejects a non-positive rate and a sub-unit burst', () => {
    // The fail-fast guards protect the 'waits, never fails' runtime invariant: a zero/negative rate
    // would never refill (hang) and a sub-1 burst would divide by zero on the first contended
    // acquire.
    const clock = new FakeClock();
    expect(() => new InMemoryRateLimiter(clock, { ratePerSecond: 0, burst: 1 })).toThrow(/ratePerSecond/);
    expect(() => new InMemoryRateLimiter(clock, { ratePerSecond: -1, burst: 1 })).toThrow(/ratePerSecond/);
    expect(() => new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: 0 })).toThrow(/burst/);
    expect(() => new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: -1 })).toThrow(/burst/);
  });

  // The Redis rate limiter's constructor-parity contract (the two adapters reject the same
  // misconfiguration identically) lands with the Redis adapter.

  it('never lets concurrent waiters share one token', async () => {
    // burst=1, rate=1.0: two tasks both find the bucket empty and sleep. When the clock yields
    // exactly one token, only one waiter may take it; the other must re-check, find nothing, and
    // sleep a further full period. Total simulated wait is two periods — no over-admission.
    const clock = new FakeClock();
    const limiter = new InMemoryRateLimiter(clock, { ratePerSecond: 1, burst: 1 });

    await limiter.acquire('k'); // drain the single burst token at t=0
    expect(clock.monotonic()).toBe(0);

    const finishTimes: number[] = [];

    const contend = async (): Promise<void> => {
      await limiter.acquire('k');
      finishTimes.push(clock.monotonic());
    };

    // Both tasks start contended; each clock.sleep fast-forwards time and yields, so the two
    // interleave through the re-check loop rather than both grabbing the same token.
    await Promise.all([contend(), contend()]);

    // One token per full period, the second waited again.
    expect([...finishTimes].sort((left, right) => left - right)).toEqual([ONE_SECOND_MS, 2 * ONE_SECOND_MS]);
    expect(clock.monotonic()).toBe(2 * ONE_SECOND_MS); // two full periods — the token was not double-spent
  });

  it('never double-records a repeated quarantine', async () => {
    // A retried disposal (e.g. on resume) of an already-quarantined entry must be a safe no-op:
    // the message is already gone, so nothing is removed and no second record is appended.
    const inbox = new InMemoryInbox();
    const session = SessionId('mem-quarantine-idem');

    const entryId = await inbox.append(session, workEmail('a@work.example'));
    const delivered = await inbox.consume(session); // claim it so quarantine actually removes it
    expect(delivered.map((entry) => entry.entryId)).toEqual([entryId]);

    await inbox.quarantine(session, entryId, { reason: 'poison', epoch: Epoch(1) });
    expect(await inbox.quarantined(session)).toHaveLength(1);

    await inbox.quarantine(session, entryId, { reason: 'poison', epoch: Epoch(1) }); // retry

    expect(await inbox.quarantined(session)).toHaveLength(1); // no duplicate record
    expect(await inbox.pendingCount(session)).toBe(0);
  });

  it('makes a commit over a foreign pending mark a no-op', async () => {
    // A successor at epoch 2 that has NOT reclaimed a predecessor's 'pending:1' must never
    // fabricate 'committed' for that unowned claim — commit only transitions this epoch's own
    // pending mark.
    const store = new InMemoryDataPointStore();
    const session = SessionId('mem-commit-foreign');
    const key = 'send-email';

    expect(await store.claimEffect(session, key, { epoch: Epoch(1), reclaimStale: false })).toBe(EffectClaim.ACQUIRED);

    await store.commitEffect(session, key, { epoch: Epoch(2) }); // successor, no reclaim

    expect(await store.getEffectState(session, key)).toBe(effectPendingState(Epoch(1))); // left intact
    // The recovery policy is still in force: the predecessor's mid-effect claim is reported as stale.
    expect(await store.claimEffect(session, key, { epoch: Epoch(2), reclaimStale: false })).toBe(
      EffectClaim.PENDING_STALE_EPOCH,
    );
  });
});
