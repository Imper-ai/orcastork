/**
 * Declarative session-completion conditions.
 *
 * A flow that expects mid-session input declares *when the session is complete* as a tiny AST of
 * frozen nodes — {@link TypePresent} (a DataPoint type is in the session, subtype-aware like
 * readiness) combined with {@link AllOf} / {@link AnyOf} — instead of a predicate function.
 * Deliberately **no callables**: an AST can be inspected, compared and later serialized (a parked
 * session resumed on another pod, CI tooling that lints flow definitions), where an opaque function
 * could only ever be executed.
 *
 * Empty combinators follow the conventional identities: `AllOf([])` is satisfied (the empty
 * conjunction is true) and `AnyOf([])` is not (the empty disjunction is false).
 *
 * The public `completesWhen` parameter stays backwards compatible — a bare DataPoint type is
 * shorthand for {@link TypePresent} and is normalized in exactly one place,
 * {@link normalizeCompletion}.
 *
 * @module
 */

import type { AnyDataPoint, DataPointClass, DataPointView } from '../datapoints/index.js';
import { BaseDataPoint, isSubclass } from '../datapoints/index.js';
import { InvalidCompletionConditionError } from '../exceptions.js';

/**
 * The condition contract: a pure, side-effect-free check over the session's view.
 *
 * `describe` is optional — a custom condition is only obligated to answer `isSatisfied`;
 * {@link describeCondition} falls back to the class identity when it declares none.
 */
export interface CompletionCondition {
  /** Whether the session's current DataPoints satisfy this condition. */
  isSatisfied(view: DataPointView): boolean;

  /** A deterministic textual form of this node, used for flow fingerprinting. */
  describe?(): string;
}

/** What `completesWhen` accepts: a bare DataPoint type (shorthand) or a built condition. */
export type CompletionItem = DataPointClass<AnyDataPoint> | CompletionCondition;

/**
 * Satisfied when at least one DataPoint of `dataPointType` is present.
 *
 * Subtype-aware, consistent with operator readiness: a present leaf satisfies a base-type
 * condition.
 */
export class TypePresent implements CompletionCondition {
  public readonly dataPointType: DataPointClass<AnyDataPoint>;

  public constructor(dataPointType: DataPointClass<AnyDataPoint>) {
    this.dataPointType = dataPointType;
    Object.freeze(this);
  }

  public isSatisfied(view: DataPointView): boolean {
    return view.ofType(this.dataPointType).length > 0;
  }

  public describe(): string {
    // The class NAME, never the class object: a class's default string form is its whole source
    // text, which is both noisy and unstable under a minifier.
    return `TypePresent(${this.dataPointType.name})`;
  }
}

/** Satisfied when every child is satisfied; the empty conjunction is satisfied. */
export class AllOf implements CompletionCondition {
  public readonly children: readonly CompletionCondition[];

  public constructor(children: Iterable<CompletionCondition>) {
    this.children = Object.freeze([...children]);
    Object.freeze(this);
  }

  public isSatisfied(view: DataPointView): boolean {
    return this.children.every((child) => child.isSatisfied(view));
  }

  public describe(): string {
    return `AllOf(${this.children.map((child) => describeCondition(child)).join(', ')})`;
  }
}

/** Satisfied when at least one child is satisfied; the empty disjunction is not. */
export class AnyOf implements CompletionCondition {
  public readonly children: readonly CompletionCondition[];

  public constructor(children: Iterable<CompletionCondition>) {
    this.children = Object.freeze([...children]);
    Object.freeze(this);
  }

  public isSatisfied(view: DataPointView): boolean {
    return this.children.some((child) => child.isSatisfied(view));
  }

  public describe(): string {
    return `AnyOf(${this.children.map((child) => describeCondition(child)).join(', ')})`;
  }
}

/**
 * A deterministic, process-stable textual form of a condition (flow fingerprinting).
 *
 * The AST nodes self-describe via `describe()`; a custom condition without one falls back to its
 * class identity. Both forms are built from names only — never an object's default string form,
 * which for a class is its entire source text — so the text is identical across processes and
 * deploys.
 *
 * Python names the class as `module.qualname`; JavaScript exposes no module path at runtime, so the
 * fallback is the constructor name alone. Two custom conditions that share a class name are
 * therefore indistinguishable in a fingerprint — give a custom condition a `describe()` when that
 * matters.
 */
export const describeCondition = (condition: CompletionCondition): string => {
  if (typeof condition.describe === 'function') {
    return String(condition.describe());
  }
  return condition.constructor.name;
};

/**
 * The DataPoint types a completion condition keys on (subtype-aware presence checks).
 *
 * Returns `null` for an opaque custom {@link CompletionCondition} the AST can't introspect —
 * callers that prune the operator graph must treat `null` as "completion needs unknown types" and
 * decline to prune, so a completion producer is never dropped. `null` input (no completion) → empty
 * set.
 */
export const referencedTypes = (
  condition: CompletionCondition | null,
): ReadonlySet<DataPointClass<AnyDataPoint>> | null => {
  if (condition === null) {
    return new Set();
  }
  if (condition instanceof TypePresent) {
    return new Set([condition.dataPointType]);
  }
  if (condition instanceof AllOf || condition instanceof AnyOf) {
    const union = new Set<DataPointClass<AnyDataPoint>>();
    for (const child of condition.children) {
      const childTypes = referencedTypes(child);
      if (childTypes === null) {
        return null; // a child was opaque → the whole condition's types are unknown
      }
      for (const dataPointType of childTypes) {
        union.add(dataPointType);
      }
    }
    return union;
  }
  return null; // an opaque custom condition — types cannot be determined
};

/** Combine conditions conjunctively; a bare DataPoint type is shorthand for {@link TypePresent}. */
export const allOf = (...items: readonly CompletionItem[]): AllOf => new AllOf(items.map(normalizeItem));

/** Combine conditions disjunctively; a bare DataPoint type is shorthand for {@link TypePresent}. */
export const anyOf = (...items: readonly CompletionItem[]): AnyOf => new AnyOf(items.map(normalizeItem));

/** Normalize the public `completesWhen` parameter — the one place bare types become AST. */
export const normalizeCompletion = (completesWhen: CompletionItem | null): CompletionCondition | null =>
  completesWhen === null ? null : normalizeItem(completesWhen);

/**
 * Turn one `completesWhen` item into a condition, rejecting anything that is neither.
 *
 * Validated eagerly so a malformed flow definition fails at construction, not deep inside the
 * gather loop when the condition is first evaluated.
 */
const normalizeItem = (item: CompletionItem): CompletionCondition => {
  if (typeof item === 'function') {
    if (isSubclass(item, BaseDataPoint)) {
      return new TypePresent(item);
    }
    throw new InvalidCompletionConditionError(`${item.name} is not a DataPoint type or CompletionCondition`);
  }
  if (typeof (item as CompletionCondition | undefined)?.isSatisfied === 'function') {
    return item;
  }
  throw new InvalidCompletionConditionError(`${String(item)} is not a DataPoint type or CompletionCondition`);
};
