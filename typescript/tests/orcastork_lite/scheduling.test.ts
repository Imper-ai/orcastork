/** Readiness, coalescing windows, the circuit breaker, and the backoff schedule. */

import { describe, expect, it } from 'vitest';
import { ZodError } from 'zod';
import type {
  CapabilityClass,
  DataPointClass,
  OperatorPolicyInit,
  RetryPolicyInit,
} from '../../src/orcastork_lite/index.js';
import { OperatorId, OperatorPolicy, RetryPolicy } from '../../src/orcastork_lite/index.js';
import {
  backoffDelays,
  CircuitBreaker,
  DebounceController,
  isReady,
  seedFor,
} from '../../src/orcastork_lite/scheduling.js';
import { FakeClock } from '../doubles/clock.js';
import { Email, Ip, makeCapability, makeOperator, Risk, WorkEmail } from './fixtures.js';

const OP = OperatorId('op');
const SECOND = 1_000;

const noTypes = new Set<DataPointClass>();
const noCapabilities = new Set<CapabilityClass>();

describe('readiness', () => {
  it('needs every dependency and every capability, subtype-aware', () => {
    const cap = makeCapability('geo');
    const operator = makeOperator('op', { dependsOn: [Email, Ip], requires: [cap] });

    expect(isReady(operator, { presentTypes: new Set([WorkEmail]), availableCapabilityTypes: new Set([cap]) })).toBe(
      false,
    );
    expect(
      isReady(operator, { presentTypes: new Set([WorkEmail, Ip]), availableCapabilityTypes: noCapabilities }),
    ).toBe(false);
    expect(
      isReady(operator, { presentTypes: new Set([WorkEmail, Ip]), availableCapabilityTypes: new Set([cap]) }),
    ).toBe(true);
    expect(isReady(makeOperator('free'), { presentTypes: noTypes, availableCapabilityTypes: noCapabilities })).toBe(
      true,
    );
  });

  it('never gates readiness on a `uses` type', () => {
    const operator = makeOperator('op', { dependsOn: [Ip], uses: [Risk] });

    expect(isReady(operator, { presentTypes: new Set([Ip]), availableCapabilityTypes: noCapabilities })).toBe(true);
  });
});

describe('DebounceController', () => {
  it('becomes due when the clock advances, and a default window is due at once', () => {
    const clock = new FakeClock();
    const debounce = new DebounceController(clock);
    expect(debounce.isScheduled(OP)).toBe(false);
    expect(debounce.dueAt(OP)).toBeNull();

    debounce.schedule(OP, { windowMs: 2 * SECOND });
    expect(debounce.isScheduled(OP)).toBe(true);
    expect(debounce.isDue(OP)).toBe(false);

    clock.advance(2 * SECOND);
    expect(debounce.isDue(OP)).toBe(true);

    debounce.clear(OP);
    expect(debounce.isScheduled(OP)).toBe(false);

    // The default window is zero: due immediately.
    debounce.schedule(OP);
    expect(debounce.isDue(OP)).toBe(true);
  });
});

describe('CircuitBreaker', () => {
  it('trips at the cap and ignores uncapped operators', () => {
    const breaker = new CircuitBreaker(new Map([[OP, 2]]));

    breaker.recordRun(OP);
    expect(breaker.isTripped(OP)).toBe(false);
    breaker.recordRun(OP);
    expect(breaker.isTripped(OP)).toBe(true);

    const free = OperatorId('free');
    for (let run = 0; run < 10; run += 1) {
      breaker.recordRun(free);
    }
    expect(breaker.isTripped(free)).toBe(false);
  });
});

describe('backoff', () => {
  it('is exponential, deterministic for a seed, and bounded by the jitter', () => {
    const policy = RetryPolicy({ maxAttempts: 4, baseDelayMs: SECOND, jitter: 0 });
    expect(backoffDelays(policy, { seed: seedFor('s', 'op') })).toEqual([SECOND, 2 * SECOND, 4 * SECOND, 8 * SECOND]);

    const jittered = RetryPolicy({ maxAttempts: 3, baseDelayMs: SECOND, jitter: 0.5 });
    const first = backoffDelays(jittered, { seed: seedFor('s', 'op') });

    expect(first).toEqual(backoffDelays(jittered, { seed: seedFor('s', 'op') }));
    first.forEach((delay, attempt) => {
      expect(delay).toBeGreaterThanOrEqual(0.5 * SECOND * 2 ** attempt);
      expect(delay).toBeLessThanOrEqual(1.5 * SECOND * 2 ** attempt);
    });
  });
});

describe('the policies', () => {
  it.each<{ readonly label: string; readonly init: RetryPolicyInit }>([
    { label: 'no attempt at all', init: { maxAttempts: 0 } },
    { label: 'a negative base delay', init: { baseDelayMs: -SECOND } },
    { label: 'jitter beyond ±100%', init: { jitter: 1.5 } },
  ])('reject a retry policy with $label', ({ init }) => {
    expect(() => RetryPolicy(init)).toThrow(ZodError);
  });

  it('are frozen, and an operator policy has no default answer to new data', () => {
    const policy = OperatorPolicy({ rerunOnNewData: true });

    expect(() => {
      (policy as { rerunOnNewData: boolean }).rerunOnNewData = false;
    }).toThrow(TypeError);
    expect(() => OperatorPolicy({} as OperatorPolicyInit)).toThrow(ZodError);
  });
});
