/**
 * `OrchestratorRuntime` — the injected bundle of port implementations + the clock.
 *
 * The orchestrator and manager depend only on this bundle, never on a concrete backend.
 * {@link buildInMemoryRuntime} wires the deterministic in-memory adapters (the test and spike
 * substrate); the Redis/Mongo builder lands with those adapters.
 *
 * This is the only module permitted to import from `adapters/`.
 *
 * @module
 */

import {
  InMemoryAuditSink,
  InMemoryCapabilityCatalog,
  InMemoryDataPointArchive,
  InMemoryDataPointStore,
  InMemoryDurableStore,
  InMemoryInbox,
  InMemorySessionLock,
} from './adapters/memory/index.js';
import type { NamespaceCipherProvider } from './archive/cipher.js';
import { NullCipherProvider } from './archive/cipher.js';
import type { Clock } from './clock.js';
import { SystemClock } from './clock.js';
import type {
  AuditSink,
  CapabilityCatalog,
  DataPointArchive,
  DataPointStore,
  DurableStore,
  Inbox,
  RateLimiter,
  SessionLock,
} from './ports/index.js';
import { NullRateLimiter } from './ports/index.js';
import { Telemetry } from './telemetry.js';

/** Everything the engine reaches the outside world through. */
export interface OrchestratorRuntime {
  /** The only time source: every window, deadline and timestamp in the session comes from here. */
  readonly clock: Clock;

  /** The session blackboard and its epoch-fenced meta state. */
  readonly store: DataPointStore;

  /** Durable, at-least-once ingestion of user-action DataPoints. */
  readonly inbox: Inbox;

  /** Liveness lease, fencing epoch and completion flag. */
  readonly lock: SessionLock;

  /** The durable, append-only event log. */
  readonly audit: AuditSink;

  /** The second durable write path: live, keyed-upsert raw DataPoints. */
  readonly archive: DataPointArchive;

  /** Curated durable outputs, routed by the output model's table. */
  readonly durable: DurableStore;

  /** Per-namespace permitted capabilities/operators, credentials and provider preference. */
  readonly catalog: CapabilityCatalog;

  /** Fleet-level pacing of capability actions (waits, never fails). */
  readonly rateLimiter: RateLimiter;

  /**
   * Traces, metrics and log records.
   *
   * Defaults to the process-global OTel providers, which no-op until a deployment installs an SDK
   * — so a deployment that wires nothing loses nothing.
   */
  readonly telemetry: Telemetry;

  /**
   * Resolves a per-namespace cipher for sealing PII (the audit value, the archive).
   *
   * Defaults to the passthrough provider (no encryption) so the in-memory substrate and tests are
   * unaffected; a deployment injects a real per-namespace provider.
   */
  readonly cipherProvider: NamespaceCipherProvider;
}

/** The parts of an {@link OrchestratorRuntime}; the two with a shipped default may be left out. */
export interface OrchestratorRuntimeInit {
  readonly clock: Clock;
  readonly store: DataPointStore;
  readonly inbox: Inbox;
  readonly lock: SessionLock;
  readonly audit: AuditSink;
  readonly archive: DataPointArchive;
  readonly durable: DurableStore;
  readonly catalog: CapabilityCatalog;
  readonly rateLimiter: RateLimiter;
  readonly telemetry?: Telemetry | undefined;
  readonly cipherProvider?: NamespaceCipherProvider | undefined;
}

/** Bundle port implementations into a runtime, filling in the shipped telemetry/cipher defaults. */
export const OrchestratorRuntime = (init: OrchestratorRuntimeInit): OrchestratorRuntime =>
  Object.freeze({
    clock: init.clock,
    store: init.store,
    inbox: init.inbox,
    lock: init.lock,
    audit: init.audit,
    archive: init.archive,
    durable: init.durable,
    catalog: init.catalog,
    rateLimiter: init.rateLimiter,
    telemetry: init.telemetry ?? new Telemetry(),
    cipherProvider: init.cipherProvider ?? new NullCipherProvider(),
  });

/** The parts of an in-memory runtime a caller may supply; everything else is wired in-memory. */
export interface BuildInMemoryRuntimeOptions {
  readonly catalog?: CapabilityCatalog | undefined;
  readonly rateLimiter?: RateLimiter | undefined;
  readonly telemetry?: Telemetry | undefined;
}

/**
 * Build a runtime backed entirely by the in-memory adapters.
 *
 * The clock comes first because it is the part a test almost always replaces (a `FakeClock`), and
 * the lock is built on it so lease expiry is deterministic.
 */
export const buildInMemoryRuntime = (
  clock?: Clock | undefined,
  options: BuildInMemoryRuntimeOptions = {},
): OrchestratorRuntime => {
  const resolvedClock = clock ?? new SystemClock();
  return OrchestratorRuntime({
    clock: resolvedClock,
    store: new InMemoryDataPointStore(),
    inbox: new InMemoryInbox(),
    lock: new InMemorySessionLock(resolvedClock),
    audit: new InMemoryAuditSink(),
    archive: new InMemoryDataPointArchive(),
    durable: new InMemoryDurableStore(),
    catalog: options.catalog ?? new InMemoryCapabilityCatalog(),
    rateLimiter: options.rateLimiter ?? new NullRateLimiter(),
    telemetry: options.telemetry ?? new Telemetry(),
  });
};
