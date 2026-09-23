/**
 * Session replay — re-run a flow over a previously-archived session's raw DataPoints.
 *
 * The archive (the second durable write path) persists every non-ephemeral DataPoint of a session as
 * an {@link ArchivedDataPoint}. {@link replaySession} makes the promise behind it executable:
 * reconstruct the raw DataPoints from those documents and drive a (possibly different/newer) flow
 * over them on a **fresh in-memory runtime** — re-deriving aggregates and reprocessing raw signals
 * without re-running collection. Use it for regression-testing a graph change against a recorded
 * session, what-if analysis, and computing new aggregates that did not exist when the session
 * originally ran (the framework's analog of Temporal replay testing / Flink savepoint reprocessing).
 *
 * Like `runtime.ts`, this module is a wiring seam: replay always runs on a fresh, isolated in-memory
 * substrate, so it deliberately builds on the in-memory adapters.
 *
 * @module
 */

import { InMemoryCapabilityCatalog } from './adapters/memory/index.js';
import type { ArchivedDataPoint } from './archive/index.js';
import type { CapabilityClass, ConcreteCapabilityClass } from './capabilities/index.js';
import type { Clock } from './clock.js';
import type { AnyDataPoint } from './datapoints/index.js';
import { parseDataPoint } from './datapoints/index.js';
import { ReplayError } from './exceptions.js';
import type { FlowDefinition } from './flow.js';
import type { CapabilityId, NamespaceId, SessionId } from './ids.js';
import type { ConcreteOperatorClass } from './operators/index.js';
import type { OrchestratorResult } from './orchestrator/index.js';
import { Orchestrator } from './orchestrator/index.js';
import type { CapabilityCatalog } from './ports/index.js';
import type { OrchestratorRuntime } from './runtime.js';
import { buildInMemoryRuntime } from './runtime.js';

/**
 * What a replay produced.
 *
 * `runtime` is the fresh in-memory runtime the replay ran on, so callers can inspect the durable
 * store / audit / archive ports directly; `dataPoints` is the final store snapshot (the
 * reconstructed seed plus everything the replayed flow derived).
 */
export interface ReplayResult {
  readonly result: OrchestratorResult;

  readonly runtime: OrchestratorRuntime;

  readonly dataPoints: readonly AnyDataPoint[];
}

/** What {@link replaySession} may be told about the replay (Python's keyword-only arguments). */
export interface ReplaySessionOptions {
  readonly flow: FlowDefinition;

  /** Defaults to the first archived entry's session id. */
  readonly sessionId?: SessionId | null;

  /** Defaults to the first archived entry's namespace id. */
  readonly namespaceId?: NamespaceId | null;

  /** Supplied instead of the auto-permit-everything default, to replay under real entitlements. */
  readonly catalog?: CapabilityCatalog | null;

  readonly clock?: Clock | null;
}

/**
 * Rebuild one archived document into its concrete DataPoint leaf.
 *
 * Registry-driven: the discriminated union picks the concrete leaf, so the rebuilt DataPoint carries
 * its real class (subtype-aware queries, pii/ephemeral config) — not a generic shell.
 */
const reconstruct = (entry: ArchivedDataPoint): AnyDataPoint =>
  parseDataPoint({
    type: entry.type,
    value: entry.value,
    retrieved_by: entry.retrievedBy,
    first_retrieved: entry.firstRetrieved,
    last_retrieved: entry.lastRetrieved,
  });

/**
 * Reconstruct an archived session's raw DataPoints and re-run `flow` over them.
 *
 * Each entry is rebuilt into its concrete DataPoint leaf via the registry (provenance and
 * first/last-retrieved timestamps preserved) and used as the seed of a fresh {@link Orchestrator} on
 * a fresh in-memory runtime — collection is not re-run; the data arrives as it was recorded. The
 * flow may differ from the one that produced the archive (new operators, new aggregators, removed
 * operators).
 *
 * Values are passed through exactly as stored: the caller is responsible for unsealing PII values
 * before replay (the in-memory archive's `read` already returns plaintext; entries pulled from a
 * production backend must be unsealed with that backend's cipher).
 *
 * `sessionId` / `namespaceId` default to those of the first archived entry.
 *
 * @throws UnknownDataPointTypeError when an entry's `type` has no registered leaf in the current
 * code — a meaningful replay failure (the flow no longer ships that DataPoint), deliberately not
 * skipped.
 * @throws ReplayError when `archived` is empty and no explicit session/namespace ids were supplied,
 * so there is nothing to derive them from.
 * @throws ZodError when an entry's `type` is still a registered leaf but its archived `value` no
 * longer matches that leaf's schema (value-shape drift after a flow change) — the real validation
 * error is preserved rather than re-wrapped, so replay-as-regression-testing surfaces schema drift
 * with its precise cause.
 */
export const replaySession = async (
  archived: Iterable<ArchivedDataPoint>,
  options: ReplaySessionOptions,
): Promise<ReplayResult> => {
  const { flow } = options;
  const sessionId = options.sessionId ?? null;
  const namespaceId = options.namespaceId ?? null;
  const entries = [...archived];
  const first = entries[0];
  if (first === undefined && (sessionId === null || namespaceId === null)) {
    throw new ReplayError('cannot replay an empty archive without explicit ids — pass sessionId and namespaceId');
  }
  // Narrowed by the guard above: an empty archive got here only with both ids supplied.
  const resolvedSession = sessionId ?? (first as ArchivedDataPoint).sessionId;
  const resolvedNamespace = namespaceId ?? (first as ArchivedDataPoint).namespaceId;
  const seed = entries.map(reconstruct);

  let catalog = options.catalog ?? null;
  if (catalog === null) {
    // Replay reprocesses data that already arrived, so every flow capability is permitted rather
    // than re-checked against live namespace entitlements; credentials are empty because a replayed
    // flow should not need a live backend.
    const permitted = new Set<CapabilityId>(
      flow.capabilities.map((capability) => (capability as ConcreteCapabilityClass).capabilityId),
    );
    catalog = new InMemoryCapabilityCatalog({
      permitted: [[resolvedNamespace, permitted]],
      credentials: [...permitted].map((capabilityId) => ({
        namespaceId: resolvedNamespace,
        capabilityId,
        credentials: {},
      })),
    });
  }
  const runtime = buildInMemoryRuntime(options.clock ?? undefined, { catalog });

  const result = await new Orchestrator({
    sessionId: resolvedSession,
    namespaceId: resolvedNamespace,
    runtime,
    // A flow names the classes to RUN, so they are constructible; the declaration-only class type
    // the flow stores is the one the graph functions read declarations off.
    operators: flow.operators as readonly ConcreteOperatorClass[],
    capabilities: flow.capabilities as readonly (CapabilityClass & ConcreteCapabilityClass)[],
    seed,
    completesWhen: flow.completesWhen,
    retryPolicy: flow.retryPolicy,
    parkAfterMs: flow.parkAfterMs,
    operationTimeoutMs: flow.operationTimeoutMs,
    sessionDeadlineMs: flow.sessionDeadlineMs,
    maxInboxDeliveries: flow.maxInboxDeliveries,
    emissionQueueSize: flow.emissionQueueSize,
    flowIdentity: flow.identity(),
  }).run();
  const snapshot = await runtime.store.snapshot(resolvedSession);
  return Object.freeze({ result, runtime, dataPoints: Object.freeze(snapshot.all()) });
};
