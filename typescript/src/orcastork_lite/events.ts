/**
 * Session events — the live view of a running session, and the port they are published through.
 *
 * `SessionState` is private to the orchestrator, so without this nothing outside the process could
 * see a DataPoint before `run()` returned. The loop publishes one event per change — a DataPoint
 * merged, an operator run finished, a capability activated, the session completed — through the
 * injected {@link SessionEventSink}. The shipped sinks are a no-op (the default), an in-memory
 * list (tests) and a Redis stream (`adapters/redis.ts`). A sink that raises is logged and the
 * event is dropped: the live view is a convenience, never a reason to wedge the session.
 *
 * Each event is built through its factory, which validates the fields once — at the trust
 * boundary, where a wrong field is still cheap to find — and hands back a frozen object.
 *
 * @module
 */

import { z } from 'zod';
import type { CapabilityId, NamespaceId, OperatorId, SessionId } from './ids.js';

/**
 * The `kind` discriminator carried by every event.
 *
 * The values are the Python strings verbatim: they reach a consumer as the Redis stream entry's
 * `kind` field, which is what a reader filters on without parsing the payload.
 */
export const SESSION_EVENT_KIND = {
  DATA_POINT_MERGED: 'data_point_merged',
  OPERATOR_RUN_COMPLETED: 'operator_run_completed',
  CAPABILITY_ACTIVATED: 'capability_activated',
  SESSION_COMPLETED: 'session_completed',
} as const;

/** One of the four {@link SESSION_EVENT_KIND} values. */
export type SessionEventKind = (typeof SESSION_EVENT_KIND)[keyof typeof SESSION_EVENT_KIND];

/** How a DataPoint landed: a new identity, or a fresher sighting of one already held. */
export type MergeKind = 'added' | 'updated';

/** How one operator run ended. */
export type RunOutcome = 'succeeded' | 'failed' | 'retrying' | 'cancelled';

/** How a capability activation ended. */
export type ActivationOutcome = 'activated' | 'failed';

/** What every session event carries: whose session it is, and when the change happened. */
export interface SessionEventBase {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  /** The session clock's `now()` when the change happened. */
  readonly at: Date;
}

/**
 * A DataPoint landed in the session: a new identity (`added`) or a fresher sighting (`updated`).
 *
 * One event per DataPoint on purpose: a stream entry is the unit a consumer filters and acts on,
 * so "react the moment this DataPoint lands" needs no unpacking of a batch. `retrievedBy` is the
 * stored DataPoint's provenance — its first observer. An `updated` merge only advances
 * `lastRetrieved`; the operator that re-observed the value is not recorded on the identity.
 */
export interface DataPointMerged extends SessionEventBase {
  readonly kind: typeof SESSION_EVENT_KIND.DATA_POINT_MERGED;

  /** The DataPoint class name. */
  readonly dataPointType: string;

  readonly value: unknown;

  readonly retrievedBy: OperatorId;

  readonly merge: MergeKind;

  /** The session revision the merge produced. */
  readonly revision: number;
}

/** One run of an operator finished, in whatever way. */
export interface OperatorRunCompleted extends SessionEventBase {
  readonly kind: typeof SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED;

  readonly operatorId: OperatorId;

  readonly outcome: RunOutcome;

  readonly attempt: number;

  /** The failure text, or `null` when the run did not fail. */
  readonly error: string | null;
}

/** A capability became available, or its activation failed terminally for the session. */
export interface CapabilityActivated extends SessionEventBase {
  readonly kind: typeof SESSION_EVENT_KIND.CAPABILITY_ACTIVATED;

  readonly capabilityId: CapabilityId;

  readonly outcome: ActivationOutcome;
}

/** The session returned; the last event a sink sees for a session. */
export interface SessionCompleted extends SessionEventBase {
  readonly kind: typeof SESSION_EVENT_KIND.SESSION_COMPLETED;

  readonly deadlineHit: boolean;

  /** Finished attempts per operator, retries included. */
  readonly operatorRuns: ReadonlyMap<OperatorId, number>;

  /** Last error of each operator that failed with no retry left, or was cancelled. */
  readonly failures: ReadonlyMap<OperatorId, string>;
}

/** Everything the orchestrator publishes; discriminated by `kind`. */
export type SessionEvent = DataPointMerged | OperatorRunCompleted | CapabilityActivated | SessionCompleted;

/** The fields {@link DataPointMerged} is built from — the event minus its fixed `kind`. */
export type DataPointMergedInit = Omit<DataPointMerged, 'kind'>;

/** The fields {@link OperatorRunCompleted} is built from; `error` may be left out entirely. */
export type OperatorRunCompletedInit = Omit<OperatorRunCompleted, 'kind' | 'error'> & {
  readonly error?: string | null;
};

/** The fields {@link CapabilityActivated} is built from. */
export type CapabilityActivatedInit = Omit<CapabilityActivated, 'kind'>;

/** The fields {@link SessionCompleted} is built from. */
export type SessionCompletedInit = Omit<SessionCompleted, 'kind'>;

/** Branded ids are plain strings at runtime, so the schemas validate them as strings. */
const baseShape = {
  sessionId: z.string(),
  namespaceId: z.string(),
  at: z.date(),
};

const dataPointMergedSchema = z.object({
  ...baseShape,
  dataPointType: z.string(),
  value: z.unknown(),
  retrievedBy: z.string(),
  merge: z.enum(['added', 'updated']),
  revision: z.number().int(),
});

const operatorRunCompletedSchema = z.object({
  ...baseShape,
  operatorId: z.string(),
  outcome: z.enum(['succeeded', 'failed', 'retrying', 'cancelled']),
  attempt: z.number().int(),
  error: z.string().nullish(),
});

const capabilityActivatedSchema = z.object({
  ...baseShape,
  capabilityId: z.string(),
  outcome: z.enum(['activated', 'failed']),
});

const sessionCompletedSchema = z.object({
  ...baseShape,
  deadlineHit: z.boolean(),
  operatorRuns: z.map(z.string(), z.number().int()),
  failures: z.map(z.string(), z.string()),
});

/**
 * The event's own copy of `at`.
 *
 * `Date` is mutable and `Object.freeze` does not reach inside one, so an event that kept the
 * caller's instance could have its timestamp moved after the fact.
 */
const instant = (at: Date): Date => new Date(at.getTime());

/** Build a {@link DataPointMerged}. */
export const DataPointMerged = (init: DataPointMergedInit): DataPointMerged => {
  dataPointMergedSchema.parse(init);
  return Object.freeze({
    kind: SESSION_EVENT_KIND.DATA_POINT_MERGED,
    sessionId: init.sessionId,
    namespaceId: init.namespaceId,
    at: instant(init.at),
    dataPointType: init.dataPointType,
    value: init.value,
    retrievedBy: init.retrievedBy,
    merge: init.merge,
    revision: init.revision,
  });
};

/** Build an {@link OperatorRunCompleted}; an omitted `error` is stored as `null`. */
export const OperatorRunCompleted = (init: OperatorRunCompletedInit): OperatorRunCompleted => {
  operatorRunCompletedSchema.parse(init);
  return Object.freeze({
    kind: SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED,
    sessionId: init.sessionId,
    namespaceId: init.namespaceId,
    at: instant(init.at),
    operatorId: init.operatorId,
    outcome: init.outcome,
    attempt: init.attempt,
    error: init.error ?? null,
  });
};

/** Build a {@link CapabilityActivated}. */
export const CapabilityActivated = (init: CapabilityActivatedInit): CapabilityActivated => {
  capabilityActivatedSchema.parse(init);
  return Object.freeze({
    kind: SESSION_EVENT_KIND.CAPABILITY_ACTIVATED,
    sessionId: init.sessionId,
    namespaceId: init.namespaceId,
    at: instant(init.at),
    capabilityId: init.capabilityId,
    outcome: init.outcome,
  });
};

/** Build a {@link SessionCompleted}; the two maps are copied, so the event cannot drift. */
export const SessionCompleted = (init: SessionCompletedInit): SessionCompleted => {
  sessionCompletedSchema.parse(init);
  return Object.freeze({
    kind: SESSION_EVENT_KIND.SESSION_COMPLETED,
    sessionId: init.sessionId,
    namespaceId: init.namespaceId,
    at: instant(init.at),
    deadlineHit: init.deadlineHit,
    operatorRuns: new Map(init.operatorRuns),
    failures: new Map(init.failures),
  });
};

/** Where the orchestrator publishes session events. Must be fast or buffer internally. */
export interface SessionEventSink {
  publish(event: SessionEvent): Promise<void>;
}

/** The default: events go nowhere, so a runtime that wires nothing loses nothing. */
export class NullSessionEventSink implements SessionEventSink {
  public publish(_event: SessionEvent): Promise<void> {
    return Promise.resolve();
  }
}
