/**
 * COMP — declarative completion conditions: the AST itself + the orchestrator-level wiring.
 */

import { describe, expect, it } from 'vitest';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { DataPointSet, DataPointView } from '../src/orcastork/datapoints/index.js';
import { InvalidCompletionConditionError } from '../src/orcastork/exceptions.js';
import { NamespaceId, SessionId } from '../src/orcastork/ids.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import type { CompletionCondition, CompletionItem } from '../src/orcastork/scheduling/index.js';
import {
  AllOf,
  AnyOf,
  allOf,
  anyOf,
  describeCondition,
  normalizeCompletion,
  TypePresent,
} from '../src/orcastork/scheduling/index.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  RiskDataPoint,
  risk,
  workEmail,
} from './doubles/datapoints.js';

const SID = SessionId('comp-session');
const NAMESPACE = NamespaceId('comp-namespace');

/** The session deadline default, in the milliseconds the port counts in. */
const DEFAULT_DEADLINE_MS = 300_000;

/** Hand the event loop back once — the port of the Python helpers' `await asyncio.sleep(0)`. */
const yieldOnce = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

/** Yield `times` times, the port of the Python helper's `for _ in range(n): await asyncio.sleep(0)`. */
const yieldTimes = async (times: number): Promise<void> => {
  for (let index = 0; index < times; index += 1) {
    await yieldOnce();
  }
};

describe('the completion AST', () => {
  it('satisfies TypePresent only when an instance of the type is present', () => {
    const condition = new TypePresent(RiskDataPoint);
    expect(condition.isSatisfied(new DataPointView([risk()]))).toBe(true);
    expect(condition.isSatisfied(new DataPointView([ip()]))).toBe(false);
    expect(condition.isSatisfied(new DataPointView())).toBe(false);
  });

  it('makes TypePresent subtype-aware', () => {
    // Consistent with readiness: a present leaf satisfies a base-type condition.
    expect(new TypePresent(EmailDataPoint).isSatisfied(new DataPointView([workEmail()]))).toBe(true);
  });

  it('requires every child of allOf', () => {
    const condition = allOf(RiskDataPoint, ChatAnswerDataPoint);
    expect(condition.isSatisfied(new DataPointView([risk()]))).toBe(false);
    expect(condition.isSatisfied(new DataPointView([risk(), chatAnswer()]))).toBe(true);
  });

  it('requires at least one child of anyOf', () => {
    const condition = anyOf(RiskDataPoint, ChatAnswerDataPoint);
    expect(condition.isSatisfied(new DataPointView([ip()]))).toBe(false);
    expect(condition.isSatisfied(new DataPointView([chatAnswer()]))).toBe(true);
  });

  it('nests conditions', () => {
    // "A risk score AND (an answer OR an ip)" — the report-and-all-answers-scored shape.
    const condition = allOf(RiskDataPoint, anyOf(ChatAnswerDataPoint, IpDataPoint));
    expect(condition.isSatisfied(new DataPointView([risk()]))).toBe(false);
    expect(condition.isSatisfied(new DataPointView([risk(), ip()]))).toBe(true);
    expect(condition.isSatisfied(new DataPointView([risk(), chatAnswer()]))).toBe(true);
    expect(condition.isSatisfied(new DataPointView([chatAnswer(), ip()]))).toBe(false);
  });

  it('follows the conventional identities for empty combinators', () => {
    // allOf() is the empty conjunction (true); anyOf() is the empty disjunction (false).
    expect(allOf().isSatisfied(new DataPointView())).toBe(true);
    expect(anyOf().isSatisfied(new DataPointView())).toBe(false);
  });

  it('normalizes bare types in the constructors and passes conditions through', () => {
    const inner = anyOf(ChatAnswerDataPoint);
    const condition = allOf(RiskDataPoint, inner);
    expect(condition).toStrictEqual(new AllOf([new TypePresent(RiskDataPoint), inner]));
    expect(anyOf(RiskDataPoint, new TypePresent(IpDataPoint))).toStrictEqual(
      new AnyOf([new TypePresent(RiskDataPoint), new TypePresent(IpDataPoint)]),
    );
  });

  it('makes normalizeCompletion the single compatibility seam', () => {
    expect(normalizeCompletion(null)).toBeNull();
    expect(normalizeCompletion(RiskDataPoint)).toStrictEqual(new TypePresent(RiskDataPoint));
    const condition = anyOf(RiskDataPoint, ChatAnswerDataPoint);
    expect(normalizeCompletion(condition)).toBe(condition);
  });

  it('rejects non-DataPoint items at construction', () => {
    // A class, but not a DataPoint type.
    expect(() => allOf(String as unknown as CompletionItem)).toThrow(InvalidCompletionConditionError);
    // A value, not a type or condition.
    expect(() => anyOf('risk' as unknown as CompletionItem)).toThrow(InvalidCompletionConditionError);
  });

  it('falls back to the class name for a custom condition without describe', () => {
    // A user-supplied condition is only obligated to implement `isSatisfied`; `describe()` is
    // optional. Its fingerprint must still be deterministic across pods/deploys, so the fallback
    // uses the class name — never an object's default string form, which for a class is its whole
    // source text. (Python uses `module.qualname`; JavaScript exposes no module path at runtime.)
    class CustomCondition implements CompletionCondition {
      public isSatisfied(_view: DataPointView): boolean {
        return false;
      }
    }

    const text = describeCondition(new CustomCondition());
    expect(text).toBe('CustomCondition');
    expect(text).not.toContain('class'); // no class-object source text
    expect(text).not.toContain('=>');
    expect(describeCondition(new CustomCondition())).toBe(text); // process-stable: independent of instance identity
  });
});

// --- orchestrator-level wiring ----------------------------------------------------------

describe('a session driven by a completion condition', () => {
  it.each<{ readonly branch: string; readonly arrival: () => AnyDataPoint }>([
    { branch: 'the chat answer', arrival: () => chatAnswer('it was me') },
    { branch: 'the ip', arrival: () => ip('198.51.100.7') },
  ])('completes an anyOf as soon as $branch arrives', async ({ arrival }) => {
    // A "verdict OR user-abandoned" flow: whichever of the two types lands first completes it.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const arrived = arrival();

    const userActs = async (): Promise<void> => {
      await yieldTimes(5); // give the session time to reach the inbox wait
      await runtime.inbox.append(SID, arrived);
    };

    const orchestrator = new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: anyOf(ChatAnswerDataPoint, IpDataPoint),
    });
    const [result] = await Promise.all([orchestrator.run(), userActs()]);

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(new DataPointSet((await runtime.store.snapshot(SID)).all()).has(arrived)).toBe(true); // folded in
    expect(clock.monotonic()).toBeLessThan(DEFAULT_DEADLINE_MS); // one branch satisfied it — no wait to the deadline
  });

  it('keeps waiting on the inbox for an allOf until every branch is present', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);

    const userActs = async (): Promise<void> => {
      await yieldTimes(5); // give the session time to reach the inbox wait
      await runtime.inbox.append(SID, chatAnswer('first'));
      await yieldTimes(10); // let the session apply the entry and re-evaluate completion
      expect(await runtime.lock.isComplete(SID)).toBe(false); // one of two branches present → still waiting
      await runtime.inbox.append(SID, ip('198.51.100.7'));
    };

    const orchestrator = new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: allOf(ChatAnswerDataPoint, IpDataPoint),
    });
    const [result] = await Promise.all([orchestrator.run(), userActs()]);

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const types = new Set((await runtime.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));
    expect(types.has('chat_answer')).toBe(true); // both arrivals were folded in before completion
    expect(types.has('ip')).toBe(true);
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // both applied and acked
    expect(clock.monotonic()).toBeLessThan(DEFAULT_DEADLINE_MS); // completed by satisfaction, not by the deadline
  });
});
