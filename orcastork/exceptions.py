"""Framework exception hierarchy.

Every known error case has a specific type rooted at :class:`OrchestrationError` — the
core never raises bare ``Exception``/``RuntimeError``.
"""


class OrchestrationError(Exception):
    """Base class for every framework error."""


# --- registration / model definition -------------------------------------------------
class DuplicateRegistrationError(OrchestrationError):
    """A registry key (DataPoint type, operator_id, capability_id) was defined twice."""


class InvalidDataPointError(OrchestrationError):
    """A concrete DataPoint leaf is missing its ``type`` Literal or its ``config``."""


class UnknownDataPointTypeError(OrchestrationError):
    """A serialized DataPoint carried a ``type`` discriminator with no registered leaf."""


class InvalidOperatorError(OrchestrationError):
    """An operator/aggregator definition is missing a required class attribute."""


# --- fencing / concurrency ------------------------------------------------------------
class StaleEpochError(OrchestrationError):
    """A write was attempted with an epoch older than the session's current epoch."""


class LockHeldError(OrchestrationError):
    """A session lock acquisition was attempted while a live lease is held by another holder."""


class OptimisticConcurrencyError(OrchestrationError):
    """A durable upsert's expected version did not match the stored version (OCC conflict)."""


class CompletionTailTimeoutError(OrchestrationError):
    """A completion-tail step outlived its timeout, so the run stopped rather than hold the epoch.

    The tail (flush -> inbox re-check -> mark complete) is the one stretch the gathering loop's lease
    renewals no longer cover, so a step that blocks past the lock's TTL would silently lose the lease to
    a supervisor takeover. Failing fast keeps the lease honest: the run ends, the epoch is released, and
    the successor re-drives from durable state.
    """


# --- graph ----------------------------------------------------------------------------
class UnboundedCycleError(OrchestrationError):
    """The dependency graph contains a cycle whose operators lack a circuit-breaker cap."""


# --- capabilities ---------------------------------------------------------------------
class CapabilityUnavailableError(OrchestrationError):
    """An action was requested on a capability that is not currently available."""


class InvalidCapabilityError(OrchestrationError):
    """A capability declared a public method the audit wrapper cannot cover (it must be async)."""


# --- orchestrator state mirror ----------------------------------------------------------
class StateMirrorError(OrchestrationError):
    """The sole-mutator session mirror was used outside its contract.

    Raised when the mirror is read before rehydration, asked about a different session, or
    queried for a pre-rehydration revision whose change baseline was never primed. All three
    are orchestrator programming errors — failing loudly beats silently serving wrong state.
    """


# --- aggregation ----------------------------------------------------------------------
class AggregatorDeadLetteredError(OrchestrationError):
    """An aggregator exhausted its retries and was dead-lettered."""


# --- scheduling / manager -------------------------------------------------------------
class SchedulingGateBlockedError(OrchestrationError):
    """A session start was refused because its scheduling gate is still cooling down."""


class InvalidCompletionConditionError(OrchestrationError):
    """A ``completes_when`` item is neither a DataPoint type nor a ``CompletionCondition``."""


# --- archive --------------------------------------------------------------------------
class UnprotectedPiiError(OrchestrationError):
    """A PII DataPoint was archived to durable storage with no real cipher wired (fail closed)."""


class PiiKeyUnavailableError(OrchestrationError):
    """Sealed PII was read back but the per-namespace cipher could not be resolved to unseal it.

    Because the archive fails closed on write (PII is only ever persisted under a real cipher),
    encountering the passthrough ``NullCipher`` against sealed PII rows on read unambiguously means
    the namespace key is unavailable — surfaced clearly rather than crashing opaquely inside ``unseal``.
    """


# --- replay ---------------------------------------------------------------------------
class ReplayError(OrchestrationError):
    """A session replay could not be constructed (e.g. an empty archive with no explicit ids)."""
