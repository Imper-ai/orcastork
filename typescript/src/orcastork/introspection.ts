/**
 * Read-only session introspection — stuck-session forensics from persisted state alone.
 *
 * {@link describeSession} answers "why is this session stuck / why this verdict" without driving the
 * engine: it reads only the persisted ports (store, lock, inbox, durable contribution markers) plus
 * the flow definition. It is **strictly read-only** — no epoch is minted and no port is mutated — so
 * it is safe to call at any time, including while a live orchestrator owns the session.
 *
 * Readiness and missing-dependency names are computed with the SAME pure functions the scheduler
 * runs ({@link isReady}, subtype-aware, and {@link computeAvailable} over the namespace's catalog
 * permissions), so the description can never disagree with what the engine would do. A capability
 * that is registered in the flow but namespace-forbidden is simply never available, so every
 * operator requiring it reports that capability as missing.
 *
 * @module
 */

import type { CapabilityClass, ConcreteCapabilityClass } from './capabilities/index.js';
import { computeAvailable } from './capabilities/index.js';
import type { AnyDataPoint, DataPointClass } from './datapoints/index.js';
import { isSubclass } from './datapoints/index.js';
import type { FlowDefinition } from './flow.js';
import type { CapabilityId, Epoch, NamespaceId, OperatorId, Revision, SessionId } from './ids.js';
import type { OperatorClass } from './operators/index.js';
import { Aggregator } from './operators/index.js';
import type { QuarantinedEntry } from './ports/index.js';
import type { OrchestratorRuntime } from './runtime.js';
import { isReady } from './scheduling/index.js';

/** One operator's persisted standing in a session, and what (if anything) is blocking it. */
export interface OperatorState {
  readonly operatorId: OperatorId;

  /** A persisted watermark exists (gathering operators advance one per completed run). */
  readonly hasRun: boolean;

  readonly watermark: Revision | null;

  /** The scheduler's readiness check over the current snapshot (gating aside). */
  readonly isReadyNow: boolean;

  /** `dependsOn` type names with no present (sub)type. */
  readonly missingDataPoints: readonly string[];

  /** `requires` type names with no available (sub)type. */
  readonly missingCapabilities: readonly string[];

  /** Excluded by the namespace's permitted operators — it can never launch for this namespace. */
  readonly isGated: boolean;

  /** Aggregators only (their durable "ran" flag); `null` otherwise. */
  readonly contributionMarked: boolean | null;
}

/** Everything the persisted ports say about one session, read against one flow. */
export interface SessionDescription {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  readonly flowName: string;

  readonly isComplete: boolean;

  /** A live (un-expired) ownership lease exists right now. */
  readonly isOwned: boolean;

  readonly currentEpoch: Epoch;

  readonly revision: Revision;

  /** The persisted wall-clock session deadline (set at the first gather). */
  readonly deadline: Date | null;

  readonly storedFlowFingerprint: string | null;

  readonly fingerprintMatches: boolean;

  readonly pendingInbox: number;

  readonly quarantined: readonly QuarantinedEntry[];

  /** Concrete DataPoint type name → count in the snapshot. */
  readonly presentTypes: ReadonlyMap<string, number>;

  readonly operators: readonly OperatorState[];
}

/** Code-unit ordering, the way Python's `sorted` compares strings — never the locale's order. */
const compareText = (left: string, right: string): number => (left === right ? 0 : left < right ? -1 : 1);

const missingDataPoints = (
  operator: OperatorClass,
  presentTypes: ReadonlySet<DataPointClass<AnyDataPoint>>,
): readonly string[] =>
  (operator.dependsOn ?? [])
    .filter((required) => ![...presentTypes].some((present) => isSubclass(present, required)))
    .map((required) => required.name)
    .sort(compareText);

const missingCapabilities = (
  operator: OperatorClass,
  availableTypes: ReadonlySet<CapabilityClass>,
): readonly string[] =>
  (operator.requires ?? [])
    .filter((required) => ![...availableTypes].some((available) => isSubclass(available, required)))
    .map((required) => required.name)
    .sort(compareText);

/** What {@link describeSession} describes (Python's keyword-only arguments). */
export interface DescribeSessionOptions {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  readonly flow: FlowDefinition;
}

/** Describe a session's persisted state against `flow` (strictly read-only). */
export const describeSession = async (
  runtime: OrchestratorRuntime,
  options: DescribeSessionOptions,
): Promise<SessionDescription> => {
  const { sessionId, namespaceId, flow } = options;
  const view = await runtime.store.snapshot(sessionId);
  const presentTypeSet = new Set<DataPointClass<AnyDataPoint>>(
    view.all().map((dataPoint) => dataPoint.constructor as DataPointClass<AnyDataPoint>),
  );
  const registered = new Map<CapabilityId, ConcreteCapabilityClass>(
    flow.capabilities.map((capability) => {
      const concrete = capability as ConcreteCapabilityClass;
      return [concrete.capabilityId, concrete] as const;
    }),
  );
  const permitted = await runtime.catalog.permittedCapabilities(namespaceId);
  const availableIds = computeAvailable({ registered, permitted, presentTypes: presentTypeSet });
  const availableTypes = new Set<CapabilityClass>(
    [...availableIds].map((capabilityId) => registered.get(capabilityId) as ConcreteCapabilityClass),
  );
  const permittedOperators = await runtime.catalog.permittedOperators(namespaceId);

  const operators: OperatorState[] = [];
  for (const operator of flow.operators) {
    const watermark = await runtime.store.getWatermark(sessionId, operator.operatorId);
    const contributionMarked = isSubclass(operator, Aggregator)
      ? await runtime.durable.isContributionMarked(sessionId, operator.operatorId)
      : null;
    operators.push(
      Object.freeze({
        operatorId: operator.operatorId,
        hasRun: watermark !== null,
        watermark,
        isReadyNow: isReady(operator, { presentTypes: presentTypeSet, availableCapabilityTypes: availableTypes }),
        missingDataPoints: Object.freeze(missingDataPoints(operator, presentTypeSet)),
        missingCapabilities: Object.freeze(missingCapabilities(operator, availableTypes)),
        isGated: permittedOperators !== null && !permittedOperators.has(operator.operatorId),
        contributionMarked,
      }),
    );
  }

  const presentCounts = new Map<string, number>();
  for (const dataPoint of view.all()) {
    const typeName = dataPoint.constructor.name;
    presentCounts.set(typeName, (presentCounts.get(typeName) ?? 0) + 1);
  }

  const storedFingerprint = await runtime.store.getFlowFingerprint(sessionId);
  return Object.freeze({
    sessionId,
    namespaceId,
    flowName: flow.name,
    isComplete: await runtime.lock.isComplete(sessionId),
    isOwned: await runtime.lock.isHeld(sessionId),
    currentEpoch: await runtime.lock.currentEpoch(sessionId),
    revision: await runtime.store.revision(sessionId),
    deadline: await runtime.store.getSessionDeadline(sessionId),
    storedFlowFingerprint: storedFingerprint,
    // A session with no persisted fingerprint has nothing to drift from — the same reading the
    // orchestrator's drift detection takes when it persists the first fingerprint silently.
    fingerprintMatches: storedFingerprint === null || storedFingerprint === flow.fingerprint(),
    pendingInbox: await runtime.inbox.pendingCount(sessionId),
    quarantined: await runtime.inbox.quarantined(sessionId),
    presentTypes: presentCounts,
    operators: Object.freeze(operators),
  });
};

const describeBlockers = (state: OperatorState): string => {
  const blockers: string[] = [];
  if (state.isGated) {
    blockers.push('gated for this namespace');
  }
  if (state.missingDataPoints.length > 0) {
    blockers.push(`missing data: ${state.missingDataPoints.join(', ')}`);
  }
  if (state.missingCapabilities.length > 0) {
    blockers.push(`missing capabilities: ${state.missingCapabilities.join(', ')}`);
  }
  // No blockers means the readiness check passes — the operator simply has not launched yet (e.g.
  // the description was taken mid-run, or the session is parked/unowned).
  return blockers.length > 0 ? blockers.join('; ') : 'ready, not yet run';
};

/** A compact, log-friendly rendering: a status line, then one line per not-yet-run operator. */
export const renderText = (description: SessionDescription): string => {
  const status = description.isComplete ? 'complete' : 'incomplete';
  const ownership = description.isOwned ? 'owned' : 'unowned';
  const fingerprint = description.fingerprintMatches ? 'match' : 'DRIFTED';
  const lines = [
    `session ${description.sessionId} (flow ${description.flowName}): ${status}, ${ownership}, ` +
      `epoch=${description.currentEpoch}, revision=${description.revision}, ` +
      `pending_inbox=${description.pendingInbox}, quarantined=${description.quarantined.length}, ` +
      `fingerprint=${fingerprint}`,
  ];
  if (description.deadline !== null) {
    lines.push(`deadline: ${description.deadline.toISOString()}`);
  }
  const present = [...description.presentTypes.entries()]
    .sort(([left], [right]) => compareText(left, right))
    .map(([name, count]) => `${name}=${count}`)
    .join(', ');
  lines.push(`present: ${present === '' ? '(none)' : present}`);
  // An aggregator never advances a watermark; its contribution marker is its "ran" flag.
  const pending = description.operators.filter((state) => !state.hasRun && state.contributionMarked !== true);
  if (pending.length === 0) {
    lines.push('all operators have run');
    return lines.join('\n');
  }
  lines.push(...pending.map((state) => `- ${state.operatorId}: ${describeBlockers(state)}`));
  return lines.join('\n');
};
