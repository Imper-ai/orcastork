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
