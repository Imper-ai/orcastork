/**
 * First-class flow definitions.
 *
 * A {@link FlowDefinition} names a flow once — its operators, capabilities, completion condition,
 * retry/parking policy and orchestrator tuning — so the manager's `startSession` / `resume` /
 * `deliver` all drive the *same* definition instead of re-threading loose options whose docs could
 * only ask callers to keep them consistent across calls.
 *
 * Its {@link FlowDefinition.fingerprint} is a stable digest of the flow's **graph-shape identity**.
 * The orchestrator persists it per session and flags a resume whose flow no longer matches
 * (`AuditKind.FLOW_DRIFT_DETECTED`) — so a deploy that changes the operator set can never *silently*
 * change quiescence/graph semantics mid-session.
 *
 * @module
 */

import { createHash } from 'node:crypto';
import { z } from 'zod';
import type { RetryPolicy } from './aggregation/retry.js';
import type { CapabilityClass } from './capabilities/base.js';
import type { AnyDataPoint, DataPointClass } from './datapoints/index.js';
import { capabilityIdOf, dependsOnOf, requiresOf } from './graph/builder.js';
import type { OperatorClass } from './operators/base.js';
import type { CompletionItem } from './scheduling/index.js';
import { describeCondition, normalizeCompletion } from './scheduling/index.js';

/** What the orchestrator needs for drift detection: the flow's name + fingerprint. */
export interface FlowIdentity {
  readonly name: string;

  readonly fingerprint: string;
}

/** Build a frozen {@link FlowIdentity}. */
export const FlowIdentity = (init: FlowIdentity): FlowIdentity =>
  Object.freeze({ name: init.name, fingerprint: init.fingerprint });

/** What a {@link FlowDefinition} is built from; everything but `name` and `operators` is optional. */
export interface FlowDefinitionInit {
  readonly name: string;

  readonly operators: readonly OperatorClass[];

  readonly capabilities?: readonly CapabilityClass[];

  readonly completesWhen?: CompletionItem | null;

  readonly retryPolicy?: RetryPolicy | null;

  /** Milliseconds of continuous inbox-wait idleness before the session parks. */
  readonly parkAfterMs?: number | null;

  /** Orchestrator tuning the flow may pin; `null` falls back to the orchestrator's defaults. */
  readonly operationTimeoutMs?: number | null;

  readonly sessionDeadlineMs?: number | null;

  readonly maxInboxDeliveries?: number | null;

  readonly emissionQueueSize?: number | null;
}

// Only the trust boundary is paid for: a flow is authored once, by hand, and a malformed one must
// fail where it is written rather than deep inside the gather loop. The knobs keep Python's types
// exactly — no bound Python does not have, since the orchestrator itself tolerates e.g. a
// non-positive `emissionQueueSize` by falling back to its default.
const classArray = z.array(z.custom<unknown>((value) => typeof value === 'function', 'expected a class'));

const flowDefinitionSchema = z.object({
  name: z.string(),
  operators: classArray,
  capabilities: classArray.optional(),
  completesWhen: z.unknown().nullish(),
  retryPolicy: z.unknown().nullish(),
  parkAfterMs: z.number().nullish(),
  operationTimeoutMs: z.number().nullish(),
  sessionDeadlineMs: z.number().nullish(),
  maxInboxDeliveries: z.number().int().nullish(),
  emissionQueueSize: z.number().int().nullish(),
});

/**
 * A capability's name in a fingerprint.
 *
 * A concrete capability is named by its registry identity; an abstract intermediate (no id) falls
 * back to its class name. Both are stable across processes — never a default object form, which for
 * a class is its whole source text.
 *
 * @internal Exported so the fingerprint's stability can be pinned directly, as Python's test imports
 * `_capability_name`.
 */
export const capabilityName = (capability: CapabilityClass): string => capabilityIdOf(capability) ?? capability.name;

/** Code-unit ordering, so a digest never depends on the host's locale. */
const sortedNames = (names: Iterable<string>): readonly string[] =>
  [...names].sort((left, right) => (left === right ? 0 : left < right ? -1 : 1));

/** Deduplicated (Python holds these as frozensets of classes), named, sorted, comma-joined. */
const dataPointNames = (types: readonly DataPointClass<AnyDataPoint>[]): string =>
  sortedNames([...new Set(types)].map((dataPointType) => dataPointType.name)).join(',');

const capabilityNames = (types: readonly CapabilityClass[]): string =>
  sortedNames([...new Set(types)].map(capabilityName)).join(',');

/** Python's `repr` of a bool — the digest must read identically in both implementations. */
const pythonBool = (value: boolean): string => (value ? 'True' : 'False');

/** Python's `repr` of an `int | None`. */
const pythonOptionalInt = (value: number | null): string => (value === null ? 'None' : String(value));

/** A named flow: what to run, when it is complete, and how the orchestrator should be tuned. */
export class FlowDefinition {
  public readonly name: string;

  public readonly operators: readonly OperatorClass[];

  public readonly capabilities: readonly CapabilityClass[];

  public readonly completesWhen: CompletionItem | null;

  public readonly retryPolicy: RetryPolicy | null;

  public readonly parkAfterMs: number | null;

  public readonly operationTimeoutMs: number | null;

  public readonly sessionDeadlineMs: number | null;

  public readonly maxInboxDeliveries: number | null;

  public readonly emissionQueueSize: number | null;

  public constructor(init: FlowDefinitionInit) {
    flowDefinitionSchema.parse(init);
    this.name = init.name;
    this.operators = Object.freeze([...init.operators]);
    this.capabilities = Object.freeze([...(init.capabilities ?? [])]);
    this.completesWhen = init.completesWhen ?? null;
    this.retryPolicy = init.retryPolicy ?? null;
    this.parkAfterMs = init.parkAfterMs ?? null;
    this.operationTimeoutMs = init.operationTimeoutMs ?? null;
    this.sessionDeadlineMs = init.sessionDeadlineMs ?? null;
    this.maxInboxDeliveries = init.maxInboxDeliveries ?? null;
    this.emissionQueueSize = init.emissionQueueSize ?? null;
    Object.freeze(this);
  }

  public identity(): FlowIdentity {
    return FlowIdentity({ name: this.name, fingerprint: this.fingerprint() });
  }

  /**
   * A stable sha256 hexdigest of the flow's graph-shape identity.
   *
   * Covers exactly what changes the scheduler's graph reasoning: each operator's id, its declared
   * `dependsOn`/`produces`/`requires` and the policy knobs that affect scheduling semantics
   * (`rerunOnNewData`, `rerunOn`, `maxCycles`); each capability's id and declarations; and the
   * completion condition's canonical text. Every section is sorted, so declaration order never
   * matters, and every name is a registry id or a class name, so the digest is identical across
   * processes.
   *
   * Deliberately NOT covered: the flow name, retries/parking/tuning (runtime behavior, not graph
   * shape) and per-namespace operator gating — gating is runtime configuration the catalog applies
   * per grant, not flow code identity, so a namespace config change must never read as flow drift.
   *
   * The hashed text is byte-for-byte the Python implementation's, so a session started by a Python
   * worker resumes under a TypeScript one without reading as drift. The one input that cannot be
   * reproduced exactly is a class *name*: Python hashes `__qualname__`, which for a nested or
   * locally-defined class carries its enclosing scope (`test_x.<locals>.Provider`) where JavaScript
   * exposes only `Provider`. Top-level classes — everything a real flow declares — are identical in
   * both.
   */
  public fingerprint(): string {
    const lines: string[] = [];
    const operators = [...this.operators].sort((left, right) =>
      left.operatorId === right.operatorId ? 0 : left.operatorId < right.operatorId ? -1 : 1,
    );
    for (const operator of operators) {
      const policy = operator.policy;
      lines.push(
        `operator ${operator.operatorId}` +
          ` depends_on=[${dataPointNames(operator.dependsOn ?? [])}]` +
          ` produces=[${dataPointNames(operator.produces ?? [])}]` +
          ` requires=[${capabilityNames(operator.requires ?? [])}]` +
          ` rerun_on_new_data=${pythonBool(policy.rerunOnNewData)}` +
          ` rerun_on=${policy.rerunOn}` +
          ` max_cycles=${pythonOptionalInt(policy.maxCycles)}`,
      );
    }
    const capabilities = [...this.capabilities].sort((left, right) => {
      const leftName = capabilityName(left);
      const rightName = capabilityName(right);
      return leftName === rightName ? 0 : leftName < rightName ? -1 : 1;
    });
    for (const capability of capabilities) {
      lines.push(
        `capability ${capabilityName(capability)}` +
          ` depends_on=[${dataPointNames(dependsOnOf(capability))}]` +
          ` requires=[${capabilityNames(requiresOf(capability))}]`,
      );
    }
    const condition = normalizeCompletion(this.completesWhen ?? null);
    lines.push(`completes_when ${condition === null ? 'None' : describeCondition(condition)}`);
    return createHash('sha256').update(lines.join('\n'), 'utf8').digest('hex');
  }
}
