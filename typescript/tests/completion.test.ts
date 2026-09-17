/**
 * COMP — declarative completion conditions: the AST itself.
 *
 * The orchestrator-level wiring (a session that waits on the inbox until the condition is
 * satisfied) is ported with the orchestrator; the Python originals are
 * `test_comp_10_any_of_completes_when_either_branch_arrives` and
 * `test_comp_11_all_of_keeps_waiting_on_the_inbox_until_every_branch_is_present`.
 */

import { describe, expect, it } from 'vitest';
import { DataPointView } from '../src/orcastork/datapoints/index.js';
import { InvalidCompletionConditionError } from '../src/orcastork/exceptions.js';
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
