/**
 * What every port-conformance suite shares: the fixed ids, the instants, and the harness shape.
 *
 * Each suite holds the behavioural contract for one port, written entirely against the port
 * interface — never a concrete adapter. A binding test file supplies the adapter through a
 * {@link ConformanceBinding}, so the same contract runs against in-memory now and Redis/Mongo
 * later. Time-dependent contracts call the harness's `advanceTime`, so each backend advances its
 * own clock.
 *
 * @module
 */

import { NamespaceId, OperatorId, SessionId } from '../../../src/orcastork/ids.js';
import { T0 } from '../datapoints.js';

/** The session every contract runs against. */
export const SID = SessionId('cnf-session');

/** A second session, for the per-session isolation contracts. */
export const OTHER_SID = SessionId('cnf-other-session');

/** The namespace the archived / audited contracts attribute their records to. */
export const NAMESPACE = NamespaceId('cnf-namespace');

/** A destination table (an output model's `tableName`). */
export const TBL = 'cnf-durable';

/** One hour after {@link T0}. */
export const T1 = new Date(T0.getTime() + 60 * 60 * 1000);

/** Two hours after {@link T0}. */
export const T2 = new Date(T0.getTime() + 2 * 60 * 60 * 1000);

/** The operator every contract attributes its DataPoints to. */
export const OP = OperatorId('cnf-op');

/**
 * A foreign producer appends a wire payload.
 *
 * Each inbox binding supplies the backend-specific write (a raw stream `XADD` for Redis, the
 * serialized seam for in-memory) and returns the entry id.
 */
export type AppendRaw = (sessionId: SessionId, payload: string) => Promise<string>;

/** What every port harness carries besides its adapter. */
export interface ConformanceHarness {
  /** Move the backend's own clock forward by `ms` (a no-op where the contract never waits). */
  advanceTime(ms: number): Promise<void>;

  /** Release whatever the harness opened (a client, a server); omitted when there is nothing to close. */
  close?(): Promise<void>;
}

/** One named adapter factory the suite runs its whole contract against. */
export interface ConformanceBinding<HarnessT extends ConformanceHarness> {
  /** How the adapter appears in the test report (its class name). */
  readonly name: string;

  /** A fresh, empty adapter per test — no state may leak between contracts. */
  create(): Promise<HarnessT>;
}

/**
 * The lease width the `SessionLock` contract is written against unless a harness names its own.
 *
 * An adapter whose expiry is decided by a `FakeClock` advances half a minute for free, so it keeps
 * the production default and the contract reads the way the invariant is stated.
 */
export const DEFAULT_LEASE_MS = 30_000;

/** The cooldown width the `CooldownGate` contract is written against unless a harness names its own. */
export const DEFAULT_COOLDOWN_MS = 60_000;

/**
 * Comfortably past a window of `ms` — what a contract advances to make an expiry certain.
 *
 * Every wait in a time-dependent contract is expressed as a multiple of the harness's own window
 * rather than as an absolute duration, because a backend whose expiry runs on a real server's
 * clock cannot be fast-forwarded: its binding configures a window of a few hundred milliseconds
 * and waits it out for real, and the same contract still holds. The margin is generous on purpose
 * — a real wait competes with scheduling jitter and a round-trip or two.
 */
export const pastWindow = (ms: number): number => Math.ceil(ms * 1.5);

/**
 * A point comfortably inside a window of `ms`.
 *
 * Two of these exceed the window, so a contract that advances here, renews, and advances again
 * proves the renew did the work: without it the lease would already have lapsed.
 */
export const withinWindow = (ms: number): number => Math.floor(ms * 0.6);
