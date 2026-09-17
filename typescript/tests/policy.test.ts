/**
 * POLICY — rerun eligibility and debounce/coalescing.
 *
 * POLICY-03 (no cancellation), POLICY-06 (stopped only by timeout/deadline) and POLICY-07 (rerun
 * processes only `ctx.delta`) are orchestrator behaviours and are exercised in the orchestrator
 * suite; this suite covers the rerun decision + the debounce primitive.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import type { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { InvalidOperatorError } from '../src/orcastork/exceptions.js';
import { OperatorId } from '../src/orcastork/ids.js';
import type { ConcreteOperatorClass, OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import { DebounceController, rerunEligible, windowDefersToFinalize } from '../src/orcastork/scheduling/index.js';
import { FakeClock } from './doubles/clock.js';
import { RiskDataPoint, WorkEmailDataPoint } from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

describe('rerun eligibility', () => {
  it('is eligible when the policy opts in and data arrives', () => {
    const policy = OperatorPolicy({ rerunOnNewData: true });

    expect(rerunEligible(policy, { hasRelevantNewData: true })).toBe(true);
    expect(rerunEligible(policy, { hasRelevantNewData: false })).toBe(false); // nothing new → no rerun
  });

  it('never reinvokes an operator whose policy opts out', () => {
    const policy = OperatorPolicy({ rerunOnNewData: false });

    expect(rerunEligible(policy, { hasRelevantNewData: true })).toBe(false);
  });
});

describe('debounce windows', () => {
  it('coalesces arrivals within the window into a single rerun', () => {
    const clock = new FakeClock();
    const controller = new DebounceController(clock, { defaultWindowMs: 10_000 });
    const chatty = OperatorId('chatty');

    controller.schedule(chatty); // arrival 1
    clock.advance(5_000);
    controller.schedule(chatty); // arrival 2 within the window → coalesces
    clock.advance(5_000);
    expect(controller.isDue(chatty)).toBe(false); // 10s since arrival 1 but only 5s since arrival 2
    clock.advance(5_000);
    expect(controller.isDue(chatty)).toBe(true); // one rerun becomes due for the whole burst
    controller.clear(chatty);
    expect(controller.isDue(chatty)).toBe(false); // no second rerun for the same burst
  });

  it('lets a per-operator override beat the global default', () => {
    const clock = new FakeClock();
    const controller = new DebounceController(clock, { defaultWindowMs: 10_000 });
    const chatty = OperatorId('chatty');
    const latencySensitive = OperatorId('latency_sensitive');

    controller.schedule(chatty); // global 10s
    controller.schedule(latencySensitive, { windowMs: 1_000 }); // per-op override
    clock.advance(1_000);

    expect(controller.isDue(latencySensitive)).toBe(true);
    expect(controller.isDue(chatty)).toBe(false);
  });

  it('reruns immediately on a zero window', () => {
    const clock = new FakeClock();
    const controller = new DebounceController(clock, { defaultWindowMs: 10_000 });
    const immediate = OperatorId('immediate');

    controller.schedule(immediate, { windowMs: 0 });

    expect(controller.isDue(immediate)).toBe(true); // no coalescing delay
  });

  it('keeps the window fixed when re-arming is gated', () => {
    // The orchestrator arms a window once and never re-arms while isScheduled is true, so a burst
    // collapses into a FIXED window measured from the FIRST arrival. This pins the behaviour the
    // orchestrator actually relies on (isScheduled gating) rather than the controller's own sliding.
    const clock = new FakeClock();
    const controller = new DebounceController(clock, { defaultWindowMs: 10_000 });
    const chatty = OperatorId('chatty');

    expect(controller.isScheduled(chatty)).toBe(false); // nothing armed yet
    expect(controller.dueAt(chatty)).toBeNull(); // and so no due time
    controller.schedule(chatty); // arrival 1 arms the window
    expect(controller.isScheduled(chatty)).toBe(true);
    expect(typeof controller.dueAt(chatty)).toBe('number');
    clock.advance(5_000);
    // A second arrival during the window: the orchestrator does NOT call schedule again because
    // isScheduled() is already true — so the window must NOT slide.
    expect(controller.isScheduled(chatty)).toBe(true);
    clock.advance(5_000);
    expect(controller.isDue(chatty)).toBe(true); // due 10s after arrival 1, NOT 10s after the later arrival
  });

  it('disarms on clear, so isScheduled and dueAt reset', () => {
    const clock = new FakeClock();
    const controller = new DebounceController(clock, { defaultWindowMs: 10_000 });
    const op = OperatorId('op');

    controller.schedule(op);
    expect(controller.isScheduled(op)).toBe(true);
    expect(controller.dueAt(op)).not.toBeNull();
    controller.clear(op);

    expect(controller.isScheduled(op)).toBe(false); // consumed: a fresh burst must re-arm from scratch
    expect(controller.dueAt(op)).toBeNull(); // no stale due time lingers
  });

  it('reports no due time for an unknown operator', () => {
    const controller = new DebounceController(new FakeClock(), { defaultWindowMs: 10_000 });

    expect(controller.dueAt(OperatorId('never-scheduled'))).toBeNull();
  });
});

describe('policy declaration', () => {
  it('rejects a concrete operator that declares no policy', () => {
    // A concrete operator (has operatorId, implements run) that forgets the `policy` static must be
    // rejected at declaration time — distinct from the abstract-base skip (no operatorId) and from
    // OperatorPolicy()'s own validation.
    let thrown: unknown;
    try {
      class NoPolicyOp extends Operator {
        public static readonly operatorId = OperatorId('no_policy');

        public async *run(_ctx: OperatorContext): AsyncIterable<DataPointEmission> {
          // A concrete async generator — never actually run; the registration guard rejects the
          // class at declaration time, before any instantiation.
          yield WorkEmailDataPoint.emit('x@e.example');
        }
      }
      // The class declares no `policy`, so it does not satisfy the decorator's own type — which is
      // exactly the mistake under test, and only a cast can express it.
      operator(NoPolicyOp as unknown as ConcreteOperatorClass);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(InvalidOperatorError);
    expect((thrown as Error).message).toMatch(/scheduling `policy`/);
  });
});

describe('the completion-tail window exemption', () => {
  it('defers only an interim window to an imminent finalize', () => {
    // An armed window normally holds the gathering loop open — that is what makes the coalescing
    // real. The single exemption is an interim refold whose output the finalize pass is about to
    // rewrite anyway; nothing else may be abandoned, whatever the completion state.
    const interim = makeAggregator('interim', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
    });
    const batch = makeAggregator('batch', { dependsOn: [RiskDataPoint], rerunOnNewData: true });
    const plain = makeOperator('plain', { dependsOn: [RiskDataPoint], rerunOnNewData: true });

    expect(windowDefersToFinalize(interim, { completionSatisfied: true })).toBe(true);
    // Unsatisfied: the session is heading for an inbox wait instead, where the interim write is the
    // only live view a reader gets — so the window still holds.
    expect(windowDefersToFinalize(interim, { completionSatisfied: false })).toBe(false);
    // An ordinary operator's rerun feeds the finalize data it would otherwise never see, and a
    // non-interim aggregator never reruns during gathering at all.
    expect(windowDefersToFinalize(plain, { completionSatisfied: true })).toBe(false);
    expect(windowDefersToFinalize(batch, { completionSatisfied: true })).toBe(false);
  });
});
