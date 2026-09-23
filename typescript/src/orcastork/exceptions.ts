/**
 * Framework exception hierarchy.
 *
 * Every known error case has a specific type rooted at {@link OrchestrationError} — the core
 * never throws a bare `Error`.
 *
 * @module
 */

/** Base class for every framework error. */
export class OrchestrationError extends Error {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    // Taken from the concrete constructor rather than hard-coded per subclass, so a subclass can
    // never drift from its own name and `error.name` always identifies what was actually thrown.
    this.name = new.target.name;
  }
}

// --- registration / model definition -------------------------------------------------

/** A registry key (DataPoint type, operator id, capability id) was defined twice. */
export class DuplicateRegistrationError extends OrchestrationError {}

/** A concrete DataPoint leaf is missing its `type` discriminator or its `config`. */
export class InvalidDataPointError extends OrchestrationError {}

/** A serialized DataPoint carried a `type` discriminator with no registered leaf. */
export class UnknownDataPointTypeError extends OrchestrationError {}

/** An operator/aggregator definition is missing a required static field. */
export class InvalidOperatorError extends OrchestrationError {}

// --- canonicalization ------------------------------------------------------------------

/**
 * A value could not be reduced to the canonical string the framework keys identity on.
 *
 * The port's analogue of the Python `_make_hashable` contract: Python rejects a value that
 * cannot be hashed, and the canonical-string form rejects a value that has no stable, reproducible
 * encoding (a `bigint`, a function, a symbol, a circular reference, an invalid `Date`). Failing at
 * the point the value is built keeps the failure inside the emitting operator's fault boundary
 * instead of surfacing later as an opaque error deep in the session state.
 */
export class UnstableValueError extends OrchestrationError {}

// --- fencing / concurrency ------------------------------------------------------------

/** A write was attempted with an epoch older than the session's current epoch. */
export class StaleEpochError extends OrchestrationError {}

/** A session lock acquisition was attempted while a live lease is held by another holder. */
export class LockHeldError extends OrchestrationError {}

/** A durable upsert's expected version did not match the stored version (OCC conflict). */
export class OptimisticConcurrencyError extends OrchestrationError {}

/**
 * A completion-tail step outlived its timeout, so the run stopped rather than hold the epoch.
 *
 * The tail (flush -> inbox re-check -> mark complete) is the one stretch the gathering loop's
 * lease renewals no longer cover, so a step that blocks past the lock's TTL would silently lose
 * the lease to a supervisor takeover. Failing fast keeps the lease honest: the run ends, the epoch
 * is released, and the successor re-drives from durable state.
 */
export class CompletionTailTimeoutError extends OrchestrationError {}

// --- graph ----------------------------------------------------------------------------

/** The dependency graph contains a cycle whose operators lack a circuit-breaker cap. */
export class UnboundedCycleError extends OrchestrationError {}

// --- capabilities ---------------------------------------------------------------------

/** An action was requested on a capability that is not currently available. */
export class CapabilityUnavailableError extends OrchestrationError {}

/** A capability declared a public method the audit wrapper cannot cover (it must be async). */
export class InvalidCapabilityError extends OrchestrationError {}

// --- orchestrator state mirror ----------------------------------------------------------

/**
 * The sole-mutator session mirror was used outside its contract.
 *
 * Thrown when the mirror is read before rehydration, asked about a different session, or queried
 * for a pre-rehydration revision whose change baseline was never primed. All three are
 * orchestrator programming errors — failing loudly beats silently serving wrong state.
 */
export class StateMirrorError extends OrchestrationError {}

// --- aggregation ----------------------------------------------------------------------

/** An aggregator exhausted its retries and was dead-lettered. */
export class AggregatorDeadLetteredError extends OrchestrationError {}

// --- scheduling / manager -------------------------------------------------------------

/** A session start was refused because its scheduling gate is still cooling down. */
export class SchedulingGateBlockedError extends OrchestrationError {}

/** A `completesWhen` item is neither a DataPoint type nor a `CompletionCondition`. */
export class InvalidCompletionConditionError extends OrchestrationError {}

// --- archive --------------------------------------------------------------------------

/** A PII DataPoint was archived to durable storage with no real cipher wired (fail closed). */
export class UnprotectedPiiError extends OrchestrationError {}

/**
 * Sealed PII was read back but the per-namespace cipher could not be resolved to unseal it.
 *
 * Because the archive fails closed on write (PII is only ever persisted under a real cipher),
 * encountering the passthrough `NullCipher` against sealed PII rows on read unambiguously means
 * the namespace key is unavailable — surfaced clearly rather than crashing opaquely inside
 * `unseal`.
 */
export class PiiKeyUnavailableError extends OrchestrationError {}

// --- replay ---------------------------------------------------------------------------

/** A session replay could not be constructed (e.g. an empty archive with no explicit ids). */
export class ReplayError extends OrchestrationError {}
