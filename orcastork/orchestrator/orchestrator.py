"""The session-scoped Orchestrator.

It owns one session: acquire ownership (minting the fencing epoch), seed, then run the
**gathering** loop. The loop is *eager*, not phased: every ready operator is launched as a
concurrent task, and the orchestrator drains the available signals off a shared queue each
pass — merging each emission the instant it is produced and re-evaluating readiness, so a
downstream operator starts as soon as its input lands rather than waiting for a whole batch to
finish. Operators are never cancelled by new data; reruns are coalesced on a debounce window.
When nothing is running and no operator can become ready (graph-aware quiescence), it runs the
**aggregation** phase, records the session complete durably (so a supervisor never re-drives a
finished session), and returns. A flow that expects mid-session input declares
``completes_when`` — a DataPoint type or a declarative ``CompletionCondition`` AST
(``all_of``/``any_of``/``TypePresent``): until it is satisfied, the would-be-quiescent session
instead **waits on the inbox** (event-driven, lease kept alive, bounded by the session
deadline) and resumes the loop when input arrives. A wait that stays idle past ``park_after``
instead **parks** the session: the run returns PARKED without aggregating or marking complete,
releasing the pod, the subscription and the lease — the durable inbox plus the supervisor's
resume-on-deliver re-drive it when input finally lands. The session deadline is persisted in
wall-clock terms at the first gather and rehydrated by every later run, so a parked (or
crash-looping) session consumes one shrinking budget rather than a fresh full window per
process. The epoch is always released and the audit/archive always flushed, even on an
unexpected error (``try/finally``).

It is the **sole mutator** of the store: every write (seed, emissions, inbox entries,
watermarks) is epoch-guarded and applied on the single gathering loop — operators only stream
emissions onto the queue, so no two writers ever touch the store at once. Being the sole
mutator, it keeps a local :class:`SessionStateMirror` of the session's DataPoint state —
rehydrated once per run, read locally on every pass, written through to the store per merge
batch — so the hot loop never re-reads the store it alone writes. Aggregators write
the curated durable outputs, and the orchestrator additionally **live-archives** every
non-ephemeral DataPoint through the same epoch-guarded write-behind buffer (the second durable
write path) — so both persistence boundaries are framework-owned. Each DataPoint-added and
capability-activation is audited per-event off the hot path. An operator that raises or times
out is isolated — its already-emitted DataPoints persist, the failure is logged, and the
scheduler proceeds (no wedge). A policy that declares ``retry`` relaunches the failed operator
on a loop-scheduled backoff window (like a debounced rerun) — never an in-task sleep, so lease
renewal, the session deadline, and inbox draining stay live throughout the backoff. Inbox
entries get the same isolation: an undecodable (poison) entry is quarantined on sight, and a
valid entry whose apply keeps failing is redelivered up to a bounded cap and then quarantined —
one bad message never crash-loops the session.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from itertools import count
from typing import Any

from loguru import logger
from opentelemetry.trace import Span, StatusCode

from ..aggregation.helpers import AggregationHelpers
from ..aggregation.retry import RetryPolicy, backoff_delays, run_with_retry, seed_for
from ..archive import ArchivedDataPoint
from ..archive.cipher import NullCipher, ValueCipher
from ..audit import (
    AuditKind,
    AuditLogEntry,
    CapabilityAuditInfo,
    DataPointAuditInfo,
    FlowAuditInfo,
    InboxAuditInfo,
    OperatorAuditInfo,
    OperatorOutcome,
)
from ..capabilities.availability import CapabilityActivator
from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint, DataPointView, MergeKind
from ..exceptions import AggregatorDeadLetteredError, CompletionTailTimeoutError, StaleEpochError
from ..flow import FlowIdentity
from ..graph import CircuitBreaker, backward_reachable, build_graph, find_cycles, validate_acyclic_or_bounded
from ..ids import CapabilityId, Epoch, NamespaceId, OperatorId, Revision, SessionId
from ..operators import Aggregator, EffectGuard, Operator, OperatorContext, RerunOn
from ..operators.context import CapabilityView, InvocationDelta
from ..ports.change_set import DeliveredInboxEntry, InboxEntry, PoisonInboxEntry
from ..runtime import OrchestratorRuntime
from ..scheduling import (
    CompletionCondition,
    DebounceController,
    is_ready,
    normalize_completion,
    operator_delta,
    reachable_pending,
    referenced_types,
    rerun_eligible,
    window_defers_to_finalize,
)
from .mirror import MirrorWriteResult, SessionStateMirror

DEFAULT_OPERATION_TIMEOUT = 30.0
DEFAULT_SESSION_DEADLINE = 300.0
# How often the orchestrator renews its ownership lease while working. Must be shorter than the
# lock adapter's TTL — and the TTL should exceed the longest single operator run the loop can be
# blocked on without renewing: the global `operation_timeout`, raised by any larger per-operator
# `OperatorPolicy.timeout` override. A renew that finds a higher epoch raises and is turned into
# a clean SUPERSEDED stop.
DEFAULT_LEASE_RENEW_INTERVAL = 10.0
# How many deliveries a valid-but-unappliable inbox entry gets before it is quarantined instead
# of redelivered: enough for a transient store fault to clear, small enough that a persistently
# bad entry cannot grind against the store for the session's whole lifetime.
DEFAULT_MAX_INBOX_DELIVERIES = 5
# Upper bound on queued-but-not-yet-merged emissions: large enough that a healthy flow never
# touches it, small enough that a runaway streaming operator cannot grow the heap without limit
# (the founding Reactive Streams lesson — backpressure, not unbounded buffering).
DEFAULT_EMISSION_QUEUE_SIZE = 1024


class SessionStatus(Enum):
    COMPLETED = 'completed'
    SUPERSEDED = 'superseded'  # a higher epoch took over mid-run; this orchestrator stopped without finalizing
    PARKED = 'parked'  # idle past park_after while waiting on the inbox; not finalized — a deliver/resume re-drives it


@dataclass(frozen=True)
class DeadLetter:
    operator_id: OperatorId
    reason: str


@dataclass(frozen=True)
class OrchestratorResult:
    status: SessionStatus
    epoch: Epoch
    operator_runs: dict[OperatorId, int]
    dead_letters: tuple[DeadLetter, ...] = ()  # dead-lettered aggregator failures, for manual re-drive


@dataclass(frozen=True)
class _Emitted:
    # None when the merge is not an operator emission (the seed, an inbox entry) — the same
    # merge-item shape carries every DataPoint through the sole-mutator merge point.
    operator_id: OperatorId | None
    data_point: BaseDataPoint[Any]  # already finalized inside the fault-isolation boundary (_run_one)


@dataclass(frozen=True)
class _Completed:
    operator_id: OperatorId
    error: Exception | None  # set if the operator raised/timed out; its prior emissions are already persisted
    run_seconds: float = 0.0  # the operator's own run time (invoked -> completed), for the audit/profile


@dataclass
class _Running:
    task: asyncio.Task[None]
    observed_revision: Revision  # store revision the launch snapshot saw — becomes this run's watermark


@dataclass
class _Runnable:
    operator: type[Operator]
    delta: InvocationDelta  # computed once here and threaded into the run (no recompute)
    is_rerun: bool
    is_retry: bool = False  # a due relaunch of a failed run (consumes the armed retry, not the debounce)


@dataclass(frozen=True)
class _InboxWait:
    last_renew: float  # updated renew bookkeeping — the wait renews the lease on the loop's cadence
    parked: bool = False  # the idle window (park_after) elapsed before any input arrived


@dataclass
class _GatherPlan:
    runnable: list[_Runnable]  # operators to launch now (first-runs + due reruns/retries, never already-running)
    not_yet_run: list[type[Operator]]  # for the graph-aware quiescence check
    next_due_in: float | None  # seconds until the soonest armed-but-not-due rerun/retry, else None
    present_types: frozenset[type[BaseDataPoint[Any]]]  # DataPoint types in the snapshot (quiescence check)
    available_types: frozenset[type[Capability]]  # available capability types (quiescence check)


class Orchestrator:
    def __init__(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        runtime: OrchestratorRuntime,
        operators: Iterable[type[Operator]],
        capabilities: Iterable[type[Capability]] = (),
        seed: Sequence[BaseDataPoint[Any]] = (),
        completes_when: type[BaseDataPoint[Any]] | CompletionCondition | None = None,
        operation_timeout: float | None = None,
        session_deadline: float | None = None,
        park_after: float | None = None,
        lease_renew_interval: float = DEFAULT_LEASE_RENEW_INTERVAL,
        max_inbox_deliveries: int | None = None,
        emission_queue_size: int | None = None,
        retry_policy: RetryPolicy | None = None,
        flow_identity: FlowIdentity | None = None,
        fresh_deadline: bool = False,
    ) -> None:
        self._session_id = session_id
        self._namespace_id = namespace_id
        self._runtime = runtime
        # Resolved once at run start from runtime.cipher_provider; seals PII audit values under the
        # namespace key. None (or NullCipher) -> PII stays '<redacted>' rather than written in clear.
        self._audit_cipher: ValueCipher | None = None
        self._operators = list(operators)
        self._capabilities = list(capabilities)
        self._seed = list(seed)
        self._completes_when = normalize_completion(completes_when)
        # None means "use the orchestrator default", so a FlowDefinition can leave tuning unset
        # without this module's defaults leaking into the flow layer.
        self._operation_timeout = DEFAULT_OPERATION_TIMEOUT if operation_timeout is None else operation_timeout
        # type -> merges suppressed from the per-emission trail, reported as one row each before release
        self._coalesced_audits: Counter[str] = Counter()
        # type -> merges that re-observed an existing identity, reported as one row each
        self._reobserved_audits: Counter[str] = Counter()
        self._session_deadline = DEFAULT_SESSION_DEADLINE if session_deadline is None else session_deadline
        self._park_after = park_after
        self._lease_renew_interval = lease_renew_interval
        self._max_inbox_deliveries = (
            DEFAULT_MAX_INBOX_DELIVERIES if max_inbox_deliveries is None else max_inbox_deliveries
        )
        # A non-positive size means an unbounded asyncio.Queue (maxsize<=0), which silently defeats
        # backpressure — coerce it to the bounded default so the queue is never the immortal-buffer footgun.
        self._emission_queue_size = (
            DEFAULT_EMISSION_QUEUE_SIZE
            if emission_queue_size is None or emission_queue_size <= 0
            else emission_queue_size
        )
        self._retry_policy = retry_policy or RetryPolicy()
        self._flow_identity = flow_identity
        # When True, the persisted deadline is discarded and a fresh full budget is set at the first gather.
        # Used for re-opens: the original deadline is almost certainly blown, but gathering is a no-op
        # (the completion condition is already satisfied) and the aggregation phase needs a live budget.
        self._fresh_deadline = fresh_deadline
        self._dead_letters: list[DeadLetter] = []
        # Aggregators normally run only in the aggregation phase; an `interim_refresh` aggregator
        # ALSO joins the gather set so the operator scheduler (readiness + rerun-on-new-data) drives
        # live interim writes. It stays in `self._aggregators` too, for the authoritative finalize pass.
        self._gathering = [op for op in self._operators if not issubclass(op, Aggregator) or op.interim_refresh]
        self._aggregators = [op for op in self._operators if issubclass(op, Aggregator)]
        self._prune_to_consumed_closure()
        self._operators_by_id = {operator.operator_id: operator for operator in self._operators}
        self._undeclared_emissions: set[tuple[OperatorId, str]] = set()
        self._registered_caps = {capability.capability_id: capability for capability in self._capabilities}
        self._runs: dict[OperatorId, int] = {}
        self._failed_attempts: dict[OperatorId, int] = {}  # consecutive failures of the current retry sequence
        self._prev_caps: dict[OperatorId, frozenset[CapabilityId]] = {}
        self._watermarks: dict[OperatorId, Revision | None] = {}  # rehydrated once per gather (sole writer)
        self._activated_seen: set[CapabilityId] = set()
        # The sole-mutator local state (rehydrated in run): every keyed-merge resolves here and
        # every hot-loop read is served here, so the store is written once per batch and fully
        # read once per run instead of once per pass.
        self._mirror = SessionStateMirror(runtime.store, session_id)
        # Fail fast on an unbounded cycle rather than busy-looping to the session deadline.
        validate_acyclic_or_bounded(build_graph(self._gathering, self._capabilities))

    def _prune_to_consumed_closure(self) -> None:
        """Run only the operators whose output is (transitively) consumed by an aggregator.

        Opt-in: when an aggregator declares ``consumes`` (the DataPoint types it folds/persists), the
        gather set is restricted to the backward-reachable closure of those sinks — plus the
        aggregators' own gate inputs and the completion condition — so an operator producing data
        nothing considers never runs (and can't hold the session open). No declared ``consumes`` (or a
        completion condition whose types can't be introspected) leaves every operator in place, so
        existing flows are unaffected. Aggregators are sinks and are always kept.
        """
        declared = frozenset[type[BaseDataPoint[Any]]]().union(*(agg.consumes for agg in self._aggregators))
        if not declared:
            return
        completion_types = referenced_types(self._completes_when)
        if completion_types is None:
            return  # opaque completion condition — cannot prove a completion producer is safe to drop
        gate_inputs = frozenset[type[BaseDataPoint[Any]]]().union(*(agg.depends_on for agg in self._aggregators))
        sinks = declared | gate_inputs | completion_types
        runnable = [op for op in self._gathering if not issubclass(op, Aggregator)]
        kept = backward_reachable(runnable, self._capabilities, sinks)
        pruned = [op for op in runnable if op not in kept]
        if not pruned:
            return
        self._gathering = [op for op in self._gathering if issubclass(op, Aggregator) or op in kept]
        logger.info(
            'Pruned operators whose output no aggregator consumes (backward-reachable closure)',
            session_id=self._session_id,
            pruned=sorted(str(op.operator_id) for op in pruned),
            kept=sorted(str(op.operator_id) for op in runnable if op in kept),
        )

    async def run(self) -> OrchestratorResult:
        # The root span covers everything ownership-scoped — acquire, gather, aggregation,
        # the final flushes — so one trace shows the whole run. Per-session identifiers are
        # fine as span attributes (each span stands alone; only metric attributes create series).
        with self._runtime.telemetry.tracer.start_as_current_span(
            'session.run',
            attributes={'session_id': self._session_id, 'namespace_id': self._namespace_id},
        ) as span:
            return await self._run(span)

    async def _run(self, span: Span) -> OrchestratorResult:
        epoch = await self._runtime.lock.acquire(self._session_id)
        span.set_attribute('epoch', int(epoch))
        logger.info(
            'Session run started after acquiring the fencing epoch',
            session_id=self._session_id,
            namespace_id=self._namespace_id,
            epoch=epoch,
            flow_name=None if self._flow_identity is None else self._flow_identity.name,
        )
        self._audit_cipher = await self._resolve_audit_cipher()
        try:
            await self._exclude_unpermitted_operators()
            await self._detect_flow_drift(epoch)

            # Audit every capability invocation at the view's seam; the hook carries this run's epoch.
            async def audit_invocation(
                capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]
            ) -> None:
                await self._audit_capability_invocation(capability_id, action, parameters, epoch)

            # A terminal activation failure means a whole dependent subgraph silently disappeared for
            # the session — that must be visible in the audit trail, mirroring _audit_capability.
            async def audit_terminal_activation_failure(capability_id: CapabilityId, error: Exception) -> None:
                await self._runtime.audit.append(
                    AuditLogEntry(
                        session_id=self._session_id,
                        namespace_id=self._namespace_id,
                        epoch=epoch,
                        timestamp=self._runtime.clock.now(),
                        kind=AuditKind.CAPABILITY_ACTIVATION_FAILED,
                        capability=CapabilityAuditInfo(capability_id=capability_id, error=str(error)),
                    )
                )

            activator = CapabilityActivator(
                self._registered_caps,
                self._runtime.catalog,
                self._namespace_id,
                self._runtime.clock,
                on_invoke=audit_invocation,
                activation_retry=self._retry_policy,
                on_terminal_failure=audit_terminal_activation_failure,
                rate_limiter=self._runtime.rate_limiter,
                telemetry=self._runtime.telemetry,
            )
            # Rehydrate the sole-mutator mirror before the first merge: from here on every
            # DataPoint read and keyed-merge is local, written through to the store per batch.
            await self._mirror.rehydrate()
            if self._seed:
                logger.debug(
                    'Seeding the store with initial data points before the first gather',
                    session_id=self._session_id,
                    seed_types=sorted({data_point.type for data_point in self._seed}),
                )
                await self._merge(self._seed, epoch)
            breaker = self._build_circuit_breaker()
            result: OrchestratorResult | None = None
            flushed = False  # set once the loop's own flush ran, so the finally-flush stays a crash-path backstop
            # Drain-before-release: while it holds the epoch the orchestrator owns the inbox, so after
            # aggregating + flushing it re-checks the inbox and, if a deliver landed during the flush
            # window, loops back to gather + aggregate on the SAME epoch (fresh drain budget) instead of
            # releasing. It marks complete + releases only once the inbox is empty at that point. The
            # happy path is one iteration; a late deliver folds in-run rather than waiting for a re-spawn.
            while True:
                gather_started = self._runtime.clock.monotonic()
                with self._runtime.telemetry.tracer.start_as_current_span('session.gather'):
                    parked = await self._gather(epoch, activator, breaker)
                self._runtime.telemetry.session_gather_seconds.record(self._runtime.clock.monotonic() - gather_started)
                if parked:
                    # Parked: no aggregation and no completion mark, so the session stays resumable —
                    # a supervisor sees a not-complete, unlocked session and re-drives it on delivery.
                    # The audit entries are appended here so the trail shows the park.
                    #
                    # The coalesced counts are appended here too: they live only on this instance and the
                    # resume that follows starts them at zero, so a park — which releases cleanly, unlike a
                    # crash — must commit them or the trail permanently under-counts the merges it stood in
                    # for. Appending clears the counters, so a run that parks cannot re-report them.
                    await self._append_coalesced_audits(epoch)
                    await self._audit_session_parked(epoch)
                    logger.info(
                        'Session parked: idle waiting exceeded park_after; releasing the pod until input arrives',
                        session_id=self._session_id,
                        park_after=self._park_after,
                    )
                    self._count_session(SessionStatus.PARKED, span)
                    result = OrchestratorResult(
                        SessionStatus.PARKED, epoch, dict(self._runs), tuple(self._dead_letters)
                    )
                    return result
                aggregation_started = self._runtime.clock.monotonic()
                with self._runtime.telemetry.tracer.start_as_current_span('session.aggregate'):
                    await self._aggregate(epoch, activator)
                self._runtime.telemetry.session_aggregation_seconds.record(
                    self._runtime.clock.monotonic() - aggregation_started
                )
                # Flush the write-behind archive BEFORE the inbox check (moved out of the finally): a
                # deliver racing the check must be re-drivable off durable state, and every durable write
                # is complete before the drained-before-release decision is made.
                await self._append_coalesced_audits(epoch)
                await self._bounded_tail_step(self._flush_write_behind(), 'write-behind flush')
                flushed = True
                if (
                    await self._bounded_tail_step(
                        self._runtime.inbox.pending_count(self._session_id), 'inbox pending-count re-check'
                    )
                    > 0
                ):
                    # A late deliver landed during aggregate/flush. Re-drive on the SAME epoch: clear each
                    # aggregator's contribution so it re-folds the new data, then loop back to gather.
                    await self._prepare_redrive()
                    continue
                # Mark complete before releasing the epoch. The flag is co-located with the epoch counter
                # on the lock, so this is an atomic compare-and-set against the live epoch: a fenced
                # predecessor cannot finalize, and a successor (even on another pod) reads it to skip a
                # finished session.
                await self._bounded_tail_step(
                    self._runtime.lock.mark_complete(self._session_id, epoch=epoch), 'mark complete'
                )
                logger.info(
                    'Session completed; durable outputs written and session marked complete',
                    session_id=self._session_id,
                    epoch=epoch,
                    operator_runs=sum(self._runs.values()),
                    dead_letters=len(self._dead_letters),
                )
                self._count_session(SessionStatus.COMPLETED, span)
                result = OrchestratorResult(
                    SessionStatus.COMPLETED, epoch, dict(self._runs), tuple(self._dead_letters)
                )
                return result
        except StaleEpochError:
            # A higher epoch took over while we were working (a renew, store write, or audit/durable
            # write was fenced). The successor now owns the session; stop without finalizing. No
            # corruption is possible — every fenced write was already rejected — so this is a clean
            # exit, not a failure. The successor re-drives any unfinished work idempotently.
            logger.warning(
                'Orchestrator fenced by a higher epoch; stopping (the successor owns the session)',
                session_id=self._session_id,
                epoch=epoch,
            )
            self._count_session(SessionStatus.SUPERSEDED, span)
            result = OrchestratorResult(SessionStatus.SUPERSEDED, epoch, dict(self._runs), tuple(self._dead_letters))
            return result
        finally:
            # Always release the epoch, even if a flush raises — a held epoch would otherwise block
            # recovery until the lease TTL expired. The loop flushes the write-behind archive itself on
            # the completion path; this backstops the park/crash/error paths where that flush never ran. A
            # crash before it completes is safe: its buffer is durable and replayed on resume, and the
            # release below still runs.
            #
            # The check->release window is non-atomic and spans two separate ops (the completion-path
            # pending_count read, then this release): a cross-pod deliver appending a straggler in that gap
            # is invisible to a read taken while we still hold the epoch, so it cannot be caught in-run. The
            # manager's post-release pending_count probe (holding no epoch, reading after run() returns)
            # catches it and re-spawns to drain — that probe subsumes any pre-release flag. Residual: a pod
            # crash in the microscopic [release, manager-probe] window is a double-failure that needs a
            # host-level reopen-capable re-drive scanner (out of scope here); a post-probe deliver is already
            # covered by deliver-spawns-on-free (reopen on a free lock).
            try:
                if not flushed:
                    await self._bounded_tail_step(self._flush_write_behind(), 'write-behind flush (backstop)')
            finally:
                await self._runtime.lock.release(self._session_id, epoch=epoch)

    async def _append_coalesced_audits(self, epoch: Epoch) -> None:
        """One row per type per reason, carrying how many merges it stood in for.

        Two reasons collapse into counts rather than rows: a type that opted out of per-emission
        auditing ("merged"), and a merge that only re-observed an already-audited identity
        ("re-observed"). They are labelled distinctly so the trail says which happened.

        Written on every path that releases the epoch cleanly — completion and park alike — so the trail
        accounts for every DataPoint. Appending clears the counters and an empty tally appends nothing, so
        a second call on the same run is a no-op. In-memory until then: a run that dies before appending
        loses the counts, not the DataPoints, which are durable in the store either way — the trade this
        coalescing exists to make.
        """
        counted = [(t, n, 'merged') for t, n in sorted(self._coalesced_audits.items())]
        counted += [(t, n, 're-observed') for t, n in sorted(self._reobserved_audits.items())]
        if not counted:
            return
        entries = [
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                operator_id=None,
                kind=AuditKind.DATA_POINTS_COALESCED,
                data_point=DataPointAuditInfo(data_point_type=data_point_type, summary=f'{count} {what}'),
            )
            for data_point_type, count, what in counted
        ]
        self._coalesced_audits.clear()
        self._reobserved_audits.clear()
        await self._append_audit_entries(entries)

    async def _flush_write_behind(self) -> None:
        """Flush the write-behind archive. Idempotent — a repeat flush is a no-op.

        The audit is not flushed: its appends are durable where ``replay`` reads them, so the trail
        needs nothing from the tail.
        """
        archive_entries = await self._runtime.archive.flush(self._session_id)
        self._runtime.telemetry.archive_flush_entries.record(float(archive_entries))

    async def _prepare_redrive(self) -> None:
        # A late inbox entry arrived during aggregate/flush. Re-drive on the SAME epoch: clear each
        # aggregator's contribution marker so it re-runs and folds the new data, and grant a fresh drain
        # budget so a re-drive after a deadline-hit completion isn't instantly starved (which would spin).
        for aggregator in self._aggregators:
            await self._runtime.durable.clear_contribution(self._session_id, aggregator.operator_id)
        self._fresh_deadline = True  # honored by _remaining_session_budget on the next gather pass

    async def _exclude_unpermitted_operators(self) -> None:
        """Apply per-namespace operator gating for this run, mirroring the capability semantics.

        The catalog decides; a config change is visible to the next grant, never to a run in
        flight. Only the operator lists the loop works from are filtered, so an excluded
        operator never launches and never counts for readiness, quiescence or graph-stall
        warnings. The ctor-validated full graph is left alone: removing nodes cannot create a
        cycle, so the unbounded-cycle validation done at construction still holds. Aggregators
        are operators too — a namespace that gates one accepts the missing-output disposition a
        never-ready aggregator already has (its durable domain simply isn't written).
        """
        permitted = await self._runtime.catalog.permitted_operators(self._namespace_id)
        if permitted is None:
            return
        excluded = sorted(
            operator.operator_id for operator in self._operators if operator.operator_id not in permitted
        )
        if not excluded:
            return
        self._gathering = [operator for operator in self._gathering if operator.operator_id in permitted]
        self._aggregators = [operator for operator in self._aggregators if operator.operator_id in permitted]
        logger.info(
            'Operators excluded from this run: not permitted for the namespace',
            session_id=self._session_id,
            namespace_id=self._namespace_id,
            excluded_operator_ids=excluded,
        )

    async def _detect_flow_drift(self, epoch: Epoch) -> None:
        """Compare this spawn's flow fingerprint against the one persisted for the session.

        Drift is *detected*, never pinned: the run continues under the current flow, because
        the idempotent dataflow model (keyed-merge, watermarks, contribution markers) tolerates
        a changed operator set far better than a replay-based engine would — the WARNING and
        the audit entry make the change visible instead of blocking the session. The current
        fingerprint is persisted right away, so later resumes with the same changed flow stay
        quiet. A directly-constructed orchestrator with no flow identity skips detection.
        """
        if self._flow_identity is None:
            return
        stored = await self._runtime.store.get_flow_fingerprint(self._session_id)
        current = self._flow_identity.fingerprint
        if stored == current:
            return
        if stored is not None:
            logger.warning(
                'Flow definition drift detected on spawn; continuing under the current flow',
                session_id=self._session_id,
                flow_name=self._flow_identity.name,
                stored_fingerprint=stored,
                current_fingerprint=current,
            )
            await self._runtime.audit.append(
                AuditLogEntry(
                    session_id=self._session_id,
                    namespace_id=self._namespace_id,
                    epoch=epoch,
                    timestamp=self._runtime.clock.now(),
                    kind=AuditKind.FLOW_DRIFT_DETECTED,
                    flow=FlowAuditInfo(
                        flow_name=self._flow_identity.name, stored_fingerprint=stored, current_fingerprint=current
                    ),
                )
            )
        await self._runtime.store.set_flow_fingerprint(self._session_id, current, epoch=epoch)

    def _build_circuit_breaker(self) -> CircuitBreaker:
        caps: dict[OperatorId, int] = {}
        for cycle in find_cycles(build_graph(self._gathering, self._capabilities)):
            for node in cycle:
                if issubclass(node, Operator) and node.policy.max_cycles is not None:
                    caps[node.operator_id] = node.policy.max_cycles
        return CircuitBreaker(caps)

    def _build_emission_queue(self) -> asyncio.Queue[_Emitted | _Completed]:
        """The bounded signal queue between operator tasks and the gathering loop.

        Bounding it is the backpressure boundary: ``_run_one`` awaits its puts, so a full
        queue suspends the emitting operator until the loop drains, instead of letting a
        runaway streamer grow the heap without limit. The deliberate consequences:

        - A suspended emitter's per-operation timeout keeps ticking — that timeout is the
          bound on an operator whose loop genuinely cannot drain (a wedged consumer never
          turns into an unbounded buffer OR an immortal producer).
        - ``_Completed`` puts are awaited too, so a completion signal can be delayed by a
          full queue but never lost — the loop always learns every run's disposition.
        - On a deadline-cancel, a task blocked in ``put`` receives ``CancelledError``
          exactly like one blocked in its own awaits: emissions already merged are kept,
          the rest die with the task (the same semantics as before the bound).
        """
        return asyncio.Queue(maxsize=self._emission_queue_size)

    async def _gather(self, epoch: Epoch, activator: CapabilityActivator, breaker: CircuitBreaker) -> bool:
        """Run the gathering loop; ``True`` means the session parked instead of going quiescent."""
        debounce = DebounceController(self._runtime.clock)
        # Retries reuse the same due-time mechanics as debounced reruns: the loop (not the failed
        # task) owns the backoff window, so nothing sleeps in-line past the lock TTL.
        retries = DebounceController(self._runtime.clock)
        # The loop keeps its monotonic arithmetic, but the budget it counts down is the REMAINING
        # share of the persisted wall-clock deadline — an exhausted budget never enters the loop
        # and goes straight to aggregation, the same disposition as an in-loop deadline hit.
        deadline = self._runtime.clock.monotonic() + await self._remaining_session_budget(epoch)
        queue = self._build_emission_queue()
        running: dict[OperatorId, _Running] = {}
        last_renew = self._runtime.clock.monotonic()
        # Rehydrate each operator's watermark once (this is how a resume knows what already ran).
        # The orchestrator is the session's sole watermark writer while it holds the epoch, so the
        # cache stays authoritative and the hot loop never re-reads watermarks from the store.
        self._watermarks = {
            operator.operator_id: await self._runtime.store.get_watermark(self._session_id, operator.operator_id)
            for operator in self._gathering
        }
        # A rehydrated watermark can predate the mirror's snapshot (a resume); the added/updated
        # split for those revisions lives only in the store, so each distinct one is primed here
        # once and every later delta read stays local.
        for watermark in sorted({mark for mark in self._watermarks.values() if mark is not None}):
            await self._mirror.prime_change_baseline(watermark)
        try:
            while self._runtime.clock.monotonic() < deadline:
                last_renew = await self._renew_lease_if_due(epoch, last_renew)
                await self._drain_inbox(epoch)
                view = await self._mirror.snapshot(self._session_id)
                observed = await self._mirror.revision(self._session_id)  # the revision `view` reflects
                capabilities = await self._refresh_capabilities(activator, view, epoch)
                # Evaluated once per pass and shared with the plan on purpose: "may an interim
                # window be abandoned" and "may the loop break to aggregation" are the same
                # question asked twice, and a pass that answered them against different views
                # could drop a refold and then go on to wait rather than finalize.
                completion_satisfied = self._completes_when is None or self._completes_when.is_satisfied(view)
                plan = await self._plan(
                    view,
                    capabilities,
                    breaker,
                    debounce,
                    retries,
                    running,
                    completion_satisfied=completion_satisfied,
                )
                for runnable in plan.runnable:
                    logger.debug(
                        'Launching operator for the current gathering iteration',
                        session_id=self._session_id,
                        operator_id=runnable.operator.operator_id,
                        is_rerun=runnable.is_rerun,
                        is_retry=runnable.is_retry,
                    )
                    # Record prev_caps + the observed revision at launch: both must reflect what this run
                    # saw, and by completion the live store/capability set will have moved on.
                    self._prev_caps[runnable.operator.operator_id] = capabilities.available_ids()
                    task = asyncio.create_task(
                        self._run_one(runnable.operator, runnable.delta, epoch, view, capabilities, queue)
                    )
                    running[runnable.operator.operator_id] = _Running(task, observed)
                    if runnable.is_retry:
                        retries.clear(runnable.operator.operator_id)  # consume the armed retry
                    elif runnable.is_rerun:
                        debounce.clear(runnable.operator.operator_id)  # consume the coalesced rerun
                        self._runtime.telemetry.operator_reruns_total.add(
                            1, {'operator_id': runnable.operator.operator_id}
                        )
                if running:
                    # Block on the first available signal — an emission to merge or an operator's
                    # completion — then drain every signal already queued behind it, and re-plan once
                    # for the whole batch. Nothing is delayed (each drained signal was produced before
                    # the re-plan), so eagerness is intact; planning once on the superset launches
                    # everything any individual signal would have, without a snapshot per emission.
                    # No wall-clock timeout: operators always finish (a per-op timeout bounds them), and a
                    # debounce window is only fast-forwarded (via the injected clock) once nothing runs.
                    signals: list[_Emitted | _Completed] = [await queue.get()]
                    while True:
                        try:
                            signals.append(queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                    await self._consume_batch(signals, epoch, breaker, retries, running)
                    continue
                if plan.next_due_in is not None:
                    await self._runtime.clock.sleep(plan.next_due_in)  # fast-forward to the soonest rerun window
                    continue
                if completion_satisfied:
                    self._warn_if_stalled(plan)
                    logger.debug('Gathering quiescent; proceeding to aggregation', session_id=self._session_id)
                    break  # nothing runnable, nothing pending, and no further wait declared → aggregate
                # The flow declares a completion condition that is not satisfied yet, and nothing can
                # make progress from within the session — wait for mid-session input (a user action, a
                # webhook) to arrive on the inbox, bounded by the session deadline, then re-plan. The
                # idle window restarts here on every wait: an inbox arrival is activity, so only
                # *continuous* idleness can park the session.
                wait = await self._wait_for_inbox(epoch, deadline, last_renew, park_at=self._park_at())
                last_renew = wait.last_renew
                if wait.parked:
                    return True
            else:
                # Falling out of the loop condition (never via break/park) is exactly a deadline hit —
                # including a resume whose persisted budget was already spent and never entered the loop.
                self._runtime.telemetry.session_deadline_hits_total.add(1)
                logger.warning(
                    'Session deadline hit; gathering stopped and any in-flight operators will be cancelled',
                    session_id=self._session_id,
                    in_flight_operator_ids=sorted(running),
                )
        finally:
            await self._drain_remaining(queue, running, epoch)
        return False

    def _warn_if_stalled(self, plan: _GatherPlan) -> None:
        # Defensive invariant guard: with nothing running and nothing due, no pending operator should
        # still have a producible-input path — if one does, the dependency graph and the scheduler
        # disagree, and the operators named here silently never ran.
        stuck = reachable_pending(
            plan.not_yet_run, present_types=plan.present_types, available_capability_types=plan.available_types
        )
        if stuck:
            logger.warning(
                'Gathering stopped while pending operators could still become ready; check the dependency graph',
                session_id=self._session_id,
                stuck_operator_ids=sorted(operator.operator_id for operator in stuck),
            )

    async def _wait_for_inbox(
        self, epoch: Epoch, deadline: float, last_renew: float, *, park_at: float | None
    ) -> _InboxWait:
        """Block until the inbox signals a new entry; returns the renew bookkeeping + park verdict.

        Event-driven, not polling: one long-lived push subscription (``Inbox.wait_for_entry``)
        is raced against a renew-cadence timer on the injected clock, so the lease stays alive
        across an arbitrarily long wait and tests stay deterministic. The wait ends at the
        earliest of: an inbox wakeup (re-plan), the session deadline (the gather loop exits and
        aggregation runs, exactly like a deadline hit with operators in flight), or ``park_at``
        (the session parks — holding a task, a subscription and a renewing lease for an
        hours-long human-in-the-loop wait would waste the pod). A wakeup landing in the same
        pass as an elapsed park window wins: data beats parking. A fenced renew raises
        ``StaleEpochError`` out of here, which ``run`` turns into a clean SUPERSEDED stop.
        """
        logger.debug('Waiting on the inbox for mid-session input', session_id=self._session_id)
        waiter = asyncio.create_task(self._runtime.inbox.wait_for_entry(self._session_id))
        # The span makes the (potentially very long) wait visible in the trace; its outcome
        # attribute says what ended it. A fenced renew or a failed waiter raises through it.
        with self._runtime.telemetry.tracer.start_as_current_span('session.inbox_wait') as span:
            try:
                while not waiter.done():
                    now = self._runtime.clock.monotonic()
                    if now >= deadline:
                        span.set_attribute('outcome', 'deadline')
                        return _InboxWait(last_renew)
                    if park_at is not None and now >= park_at:
                        span.set_attribute('outcome', 'parked')
                        return _InboxWait(last_renew, parked=True)
                    next_renew_in = max(self._lease_renew_interval - (now - last_renew), 0.0)
                    bound = min(next_renew_in, deadline - now)
                    if park_at is not None:
                        bound = min(bound, park_at - now)
                    timer = asyncio.create_task(self._runtime.clock.sleep(bound))
                    try:
                        await asyncio.wait({waiter, timer}, return_when=asyncio.FIRST_COMPLETED)
                    finally:
                        timer.cancel()
                        await asyncio.gather(timer, return_exceptions=True)
                    last_renew = await self._renew_lease_if_due(epoch, last_renew)
                await waiter  # propagate adapter errors — a failed wait must not be mistaken for a wakeup
                span.set_attribute('outcome', 'wakeup')
                logger.debug('Inbox wakeup received; resuming the gathering loop', session_id=self._session_id)
                return _InboxWait(last_renew)
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

    def _park_at(self) -> float | None:
        # Parking is measured only from entering an inbox wait (continuous idleness). Armed
        # retry/debounce windows go through the next_due_in sleep path instead — they are
        # scheduled progress, never idleness, so they can never park the session.
        return None if self._park_after is None else self._runtime.clock.monotonic() + self._park_after

    async def _remaining_session_budget(self, epoch: Epoch) -> float:
        """Seconds left of the session's persisted wall-clock deadline (first gather persists it).

        Persisting the deadline (rather than restarting ``session_deadline`` per process) is what
        gives a repeatedly-parked or crash-looping session one overall budget: every resume
        continues a shrinking window, and a resume after the deadline passed gets a non-positive
        budget — straight to aggregation.

        When ``fresh_deadline`` is set (a re-open for late data), the persisted deadline is
        discarded and a new full budget is written. The original deadline is almost certainly
        elapsed (a budgeted run finishes in seconds, and late participant data arrives much later),
        so rehydrating it would give a non-positive budget, which would log a spurious deadline hit
        and skip the gather loop. Gathering IS already done (the completion condition is satisfied
        in the rehydrated store), so the gather loop exits immediately on quiescence, not on the
        deadline — the fresh budget is needed only to ensure the aggregation phase has a live lease
        window, not to enable any actual gathering.
        """
        stored = await self._runtime.store.get_session_deadline(self._session_id)
        if stored is None or self._fresh_deadline:
            deadline = self._runtime.clock.now() + timedelta(seconds=self._session_deadline)
            await self._runtime.store.set_session_deadline(self._session_id, deadline, epoch=epoch)
            return self._session_deadline
        return (stored - self._runtime.clock.now()).total_seconds()

    async def _consume_batch(
        self,
        signals: Sequence[_Emitted | _Completed],
        epoch: Epoch,
        breaker: CircuitBreaker,
        retries: DebounceController,
        running: dict[OperatorId, _Running],
    ) -> None:
        # ONE mirror/store apply for every emission in the drained batch (arrival order preserved,
        # so the merge resolution is the one per-signal merging would produce), then the completion
        # bookkeeping. Processing completions after the merge cannot change any outcome: a
        # completion never reads the live snapshot — its watermark is the launch-observed revision —
        # and an operator's own emissions always precede its completion on the queue anyway.
        await self._merge_emissions([signal for signal in signals if isinstance(signal, _Emitted)], epoch)
        for signal in signals:
            if isinstance(signal, _Completed):
                await self._record_completion(signal, epoch, breaker, retries, running)

    async def _record_completion(
        self,
        signal: _Completed,
        epoch: Epoch,
        breaker: CircuitBreaker,
        retries: DebounceController,
        running: dict[OperatorId, _Running],
    ) -> None:
        record = running.pop(signal.operator_id)
        self._runs[signal.operator_id] = self._runs.get(signal.operator_id, 0) + 1
        # Every attempt counts toward the breaker: it bounds cycle iterations, and a retried run on a
        # cycle still consumes that budget — otherwise a failing cycle operator could loop between the
        # retry scheduler and the cycle forever.
        breaker.record_run(signal.operator_id)
        attempt = self._failed_attempts.get(signal.operator_id, 0) + 1
        retrying = False
        if signal.error is None:
            self._failed_attempts.pop(signal.operator_id, None)
            logger.debug(
                'Operator run succeeded',
                session_id=self._session_id,
                operator_id=signal.operator_id,
                run_count=self._runs[signal.operator_id],
            )
        else:
            self._failed_attempts[signal.operator_id] = attempt
            retrying = self._maybe_schedule_retry(signal.operator_id, breaker, retries)
            if not retrying:
                # Terminal — a later data-driven rerun starts a fresh attempt sequence.
                self._failed_attempts.pop(signal.operator_id, None)
            self._log_run_failure(signal.operator_id, signal.error, attempt=attempt, retrying=retrying)
        outcome = OperatorOutcome.FAILED if signal.error is not None else OperatorOutcome.SUCCEEDED
        await self._audit_operator_run(
            signal.operator_id,
            epoch,
            outcome=outcome,
            run_count=self._runs[signal.operator_id],
            attempt=attempt,
            run_seconds=signal.run_seconds,
            error=signal.error,
        )
        if retrying:
            # A retried failure keeps its old watermark: the failed attempt never durably processed its
            # delta, so the relaunch must re-present the same changes. Its emissions are already merged —
            # the keyed-merge makes re-emitting them idempotent — and the armed retry (not the rerun
            # path, which stands aside while one is armed) owns the relaunch, so the stale watermark
            # cannot double-trigger.
            return
        # Advance to the revision this run *observed* (its launch snapshot), not the live one: DataPoints
        # merged by concurrent operators or the inbox while it ran were never seen by it, so they must
        # still count as new data and be able to trigger a rerun.
        await self._advance_watermark(signal.operator_id, record.observed_revision, epoch)

    def _maybe_schedule_retry(
        self, operator_id: OperatorId, breaker: CircuitBreaker, retries: DebounceController
    ) -> bool:
        """Arm a loop-scheduled relaunch of a failed run; False when the failure is terminal."""
        policy = self._operators_by_id[operator_id].policy.retry
        failed = self._failed_attempts[operator_id]
        # A tripped breaker is terminal even with attempts remaining — an armed retry could never launch.
        if policy is None or failed >= policy.max_attempts or breaker.is_tripped(operator_id):
            return False
        delay = backoff_delays(policy, seed=seed_for(self._session_id, operator_id))[failed - 1]
        retries.schedule(operator_id, window=timedelta(seconds=delay))
        self._runtime.telemetry.operator_retries_total.add(1, {'operator_id': operator_id})
        return True

    def _log_run_failure(self, operator_id: OperatorId, error: Exception, *, attempt: int, retrying: bool) -> None:
        if retrying:
            logger.opt(exception=error).warning(
                'Operator run failed; a retry is scheduled on backoff',
                operator_id=operator_id,
                attempt=attempt,
            )
        elif self._operators_by_id[operator_id].policy.retry is not None:
            logger.opt(exception=error).warning(
                'Operator run failed with no retry remaining; the scheduler is proceeding without it',
                operator_id=operator_id,
                attempt=attempt,
            )
        else:
            logger.opt(exception=error).error(
                'Operator run failed; its emissions were persisted and the scheduler is proceeding',
                operator_id=operator_id,
            )

    async def _drain_remaining(
        self, queue: asyncio.Queue[_Emitted | _Completed], running: dict[OperatorId, _Running], epoch: Epoch
    ) -> None:
        # Normal quiescent exit reaches here with both already empty (a no-op). The other exit is the
        # session deadline with operators still in flight: persist whatever was already emitted, then
        # cancel the stragglers — the deadline is the one signal allowed to stop a running operator.
        await self._drain_queue(queue, epoch)
        for record in running.values():
            record.task.cancel()
        if running:
            await asyncio.gather(*(record.task for record in running.values()), return_exceptions=True)
        # Drain once more: a task can enqueue a final emission right as it is cancelled, and that signal
        # (already past the epoch-guarded merge boundary) would otherwise be dropped.
        await self._drain_queue(queue, epoch)

    async def _drain_queue(self, queue: asyncio.Queue[_Emitted | _Completed], epoch: Epoch) -> None:
        emitted: list[_Emitted] = []
        while not queue.empty():
            signal = queue.get_nowait()
            if isinstance(signal, _Emitted):
                emitted.append(signal)
        await self._merge_emissions(emitted, epoch)

    async def _plan(
        self,
        view: DataPointView,
        capabilities: CapabilityView,
        breaker: CircuitBreaker,
        debounce: DebounceController,
        retries: DebounceController,
        running: dict[OperatorId, _Running],
        *,
        completion_satisfied: bool,
    ) -> _GatherPlan:
        present_types = frozenset(type(data_point) for data_point in view.all())
        available_types = capabilities.available_types()
        runnable: list[_Runnable] = []
        not_yet_run: list[type[Operator]] = []
        soonest_due: float | None = None
        for operator in self._gathering:
            if operator.operator_id in running:
                continue  # in flight — neither launch a second instance nor count it as pending-unstarted
            if breaker.is_tripped(operator.operator_id):
                continue
            # "Has run" spans this session's history (a persisted watermark, rehydrated into the
            # cache at gather start), so on resume a completed operator is not re-run unless its
            # policy makes it rerun-eligible.
            ran = operator.operator_id in self._runs or self._watermarks.get(operator.operator_id) is not None
            if not ran:
                not_yet_run.append(operator)
            if not is_ready(operator, present_types=present_types, available_capability_types=available_types):
                continue
            if retries.is_scheduled(operator.operator_id):
                # An armed retry owns this operator's next launch: it relaunches even with no new data
                # and even when the policy never reruns on data (its watermark is deliberately stale),
                # and quiescence must wait for it to come due rather than break to aggregation.
                if retries.is_due(operator.operator_id):
                    delta = await self._delta_for(operator, capabilities)
                    runnable.append(_Runnable(operator, delta, is_rerun=False, is_retry=True))
                else:
                    retry_due_at = retries.due_at(operator.operator_id)
                    if retry_due_at is not None:
                        soonest_due = retry_due_at if soonest_due is None else min(soonest_due, retry_due_at)
                continue
            if not ran:
                delta = await self._delta_for(operator, capabilities)
                runnable.append(_Runnable(operator, delta, is_rerun=False))  # first run is immediate
                continue
            if not operator.policy.rerun_on_new_data:
                continue  # ran already and never reruns
            delta = await self._delta_for(operator, capabilities)
            has_new = self._has_relevant_new_data(operator, delta)
            if not rerun_eligible(operator.policy, has_relevant_new_data=has_new):
                continue
            # Rerun-eligible: arm a coalescing window (arrivals within it collapse into one rerun)
            # and launch only once it is due.
            if not debounce.is_scheduled(operator.operator_id):
                debounce.schedule(operator.operator_id, window=operator.policy.debounce)
            if debounce.is_due(operator.operator_id):
                runnable.append(_Runnable(operator, delta, is_rerun=True))
            elif not window_defers_to_finalize(operator, completion_satisfied=completion_satisfied):
                # An armed window normally holds gathering open until it comes due — otherwise the
                # coalescing would be advisory, and a burst arriving with nothing else running would
                # be abandoned rather than collapsed. The one exemption is an interim refold the
                # imminent finalize pass would immediately overwrite: it stays armed and launchable
                # (a later pass with work still in flight runs it once it is due), it just no longer
                # keeps the loop waiting on a write nothing can read.
                due_at = debounce.due_at(operator.operator_id)
                if due_at is not None:
                    soonest_due = due_at if soonest_due is None else min(soonest_due, due_at)
        next_due_in = None if soonest_due is None else max(0.0, soonest_due - self._runtime.clock.monotonic())
        return _GatherPlan(runnable, not_yet_run, next_due_in, present_types, available_types)

    async def _delta_for(self, operator: type[Operator], capabilities: CapabilityView) -> InvocationDelta:
        return await operator_delta(
            self._mirror,
            self._session_id,
            watermark=self._watermarks.get(operator.operator_id),
            available_caps=capabilities.available_ids(),
            previous_caps=self._prev_caps.get(operator.operator_id, frozenset()),
        )

    @staticmethod
    def _has_relevant_new_data(operator: type[Operator], delta: InvocationDelta) -> bool:
        # A rerun is warranted by a newly-available capability, or by new data of a type the operator
        # depends on (subtype-aware). Under `RerunOn.ADDED_ONLY` a freshness-only re-observation of an
        # existing (type, value) identity (`delta.updated`) is not new data — chatty re-observation
        # must not keep re-triggering a pure value-computation operator. Unrelated types — including the
        # operator's own emissions of types it does NOT consume — don't match, so they never trigger a
        # rerun. An operator that consumes a type it also produces is a genuine cycle whose own emissions
        # DO re-trigger it: that is exactly why the graph requires such an operator to declare a
        # `max_cycles` circuit-breaker bounding the loop.
        if delta.newly_available_caps:
            return True
        new_data = delta.added if operator.policy.rerun_on is RerunOn.ADDED_ONLY else delta.added | delta.updated
        # Both readiness inputs (`depends_on`) and read-only inputs (`uses`) are rerun triggers: new data
        # of either re-fires the operator. Only `depends_on` gates readiness (see is_ready); `uses` does not.
        triggers = operator.depends_on | operator.uses
        return any(isinstance(data_point, trigger) for data_point in new_data for trigger in triggers)

    async def _run_one(
        self,
        operator: type[Operator],
        delta: InvocationDelta,
        epoch: Epoch,
        view: DataPointView,
        capabilities: CapabilityView,
        queue: asyncio.Queue[_Emitted | _Completed],
    ) -> None:
        aggregation = (
            AggregationHelpers(
                self._runtime.durable,
                session_id=self._session_id,
                operator_id=operator.operator_id,
                epoch=epoch,
                clock=self._runtime.clock,
                is_final=False,
            )
            if issubclass(operator, Aggregator)
            else None
        )
        context = OperatorContext(
            session_id=self._session_id,
            epoch=epoch,
            store=view,
            capabilities=capabilities,
            delta=delta,
            effects=self._effect_guard(operator.operator_id, epoch),
            aggregation=aggregation,
            is_final=False,
        )
        error: Exception | None = None
        started = self._runtime.clock.monotonic()

        async def drain(instance: Operator) -> None:
            # Stamp provenance + observation time here, inside the fault-isolation boundary: operators
            # emit value-only, so finalizing constructs the DataPoint and can raise on a malformed value
            # — that must be isolated to this operator, not propagate out and wedge the session. Each
            # finalized emission is streamed onto the queue as it is produced, so the loop can merge it
            # and start downstream operators while this one is still running. The observation time is
            # taken per emission, so a long-running operator's later yields carry their actual time.
            async for emission in instance.run(context):
                finalized = emission.finalize(retrieved_by=operator.operator_id, at=self._runtime.clock.now())
                await queue.put(_Emitted(operator.operator_id, finalized))

        with self._runtime.telemetry.tracer.start_as_current_span(
            f'operator.run {operator.operator_id}', attributes={'operator_id': operator.operator_id}
        ) as span:
            try:
                # Construct inside the boundary too: an operator __init__ that raises (e.g. config
                # validation) must be recorded as a FAILED run — not drop the task before it enqueues
                # _Completed, which would leave it wedged in `running` with no log/audit/breaker.
                instance = operator()
                await asyncio.wait_for(drain(instance), timeout=self._timeout_for(operator))
            except Exception as exc:  # operator fault-isolation boundary — never wedge the session
                error = exc
            outcome = OperatorOutcome.FAILED if error is not None else OperatorOutcome.SUCCEEDED
            span.set_attribute('outcome', outcome.value)
            if error is not None:
                # The failure is isolated (it never propagates out of this task), so the span
                # is marked failed explicitly — the context manager will see no exception.
                span.record_exception(error)
                span.set_status(StatusCode.ERROR, str(error))
        run_seconds = self._runtime.clock.monotonic() - started
        self._observe_run_seconds(operator.operator_id, run_seconds, outcome)
        await queue.put(_Completed(operator.operator_id, error, run_seconds))

    def _timeout_for(self, operator: type[Operator]) -> float:
        # A wrapped streaming collector legitimately runs far longer than a quick scoring operator,
        # so the policy may override the orchestrator-wide bound in either direction.
        return self._operation_timeout if operator.policy.timeout is None else operator.policy.timeout.total_seconds()

    def _effect_guard(self, operator_id: OperatorId, epoch: Epoch) -> EffectGuard:
        # Bound to the LIVE store (not the launch snapshot): an effect mark must be durable and
        # epoch-fenced the instant the operator claims it, even while the run is still streaming.
        return EffectGuard(self._runtime.store, session_id=self._session_id, operator_id=operator_id, epoch=epoch)

    async def _aggregate(self, epoch: Epoch, activator: CapabilityActivator) -> None:
        await self._runtime.lock.renew(self._session_id, epoch=epoch)  # hold ownership across the aggregation phase
        view = await self._mirror.snapshot(self._session_id)
        capabilities = await self._refresh_capabilities(activator, view, epoch)
        present_types = frozenset(type(data_point) for data_point in view.all())
        available_types = capabilities.available_types()
        launchable: list[type[Operator]] = []
        for aggregator in self._aggregators:
            if not is_ready(aggregator, present_types=present_types, available_capability_types=available_types):
                # A never-ready aggregator means a whole output domain was not written — that must be
                # visible in the logs and the audit trail, not just silently absent.
                await self._report_skipped_aggregator(aggregator, epoch, present_types, available_types)
                continue
            # Resume skips already-completed aggregators: the contribution marker makes re-drives idempotent.
            if await self._runtime.durable.is_contribution_marked(self._session_id, aggregator.operator_id):
                # Logged because it is the one path that completes a session having run NO finalize this
                # epoch: whatever the predecessor committed is the record's last word, so when a record
                # looks wrong on a resumed session this is the first thing to check.
                logger.debug(
                    'Skipping an aggregator that already contributed; its predecessor output stands',
                    session_id=self._session_id,
                    operator_id=aggregator.operator_id,
                    epoch=int(epoch),
                )
                continue
            launchable.append(aggregator)
        logger.debug(
            'Aggregation phase started; launching ready aggregators concurrently',
            session_id=self._session_id,
            aggregator_ids=sorted(aggregator.operator_id for aggregator in launchable),
        )
        # Aggregators are independent by design, so they run concurrently — one waiting out a retry
        # backoff must not delay its peers. The `self._dead_letters`/`self._runs` mutations need no
        # locking: everything shares the single event loop and each mutation is one non-awaiting step.
        results = await asyncio.gather(
            *(self._run_aggregator(aggregator, epoch, view, capabilities) for aggregator in launchable),
            return_exceptions=True,
        )
        # Failures are collected (never lost to sibling cancellation, and every sibling ran to its own
        # disposition); a fencing signal wins the re-raise so `run()` makes its clean SUPERSEDED stop.
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            fenced = next((failure for failure in failures if isinstance(failure, StaleEpochError)), None)
            raise fenced if fenced is not None else failures[0]

    async def _report_skipped_aggregator(
        self,
        aggregator: type[Operator],
        epoch: Epoch,
        present_types: frozenset[type[BaseDataPoint[Any]]],
        available_types: frozenset[type[Capability]],
    ) -> None:
        missing_data = sorted(
            required.__name__
            for required in aggregator.depends_on
            if not any(issubclass(present, required) for present in present_types)
        )
        missing_caps = sorted(
            required.__name__
            for required in aggregator.requires
            if not any(issubclass(available, required) for available in available_types)
        )
        logger.warning(
            'Aggregator skipped: its dependencies never materialized; no durable output was written',
            operator_id=aggregator.operator_id,
            missing_data_points=missing_data,
            missing_capabilities=missing_caps,
        )
        await self._audit_operator_run(
            aggregator.operator_id,
            epoch,
            outcome=OperatorOutcome.SKIPPED,
            run_count=0,
            error=f'dependencies never materialized: {", ".join(missing_data + missing_caps)}',
        )

    async def _run_aggregator(
        self, aggregator: type[Operator], epoch: Epoch, view: DataPointView, capabilities: CapabilityView
    ) -> None:
        """Run one aggregator with bounded, jittered-backoff retries; dead-letter when they are exhausted.

        A method (rather than a closure inside the loop) gives ``attempt``/``after_attempt`` a clean
        per-aggregator scope, so there is no loop-variable capture to default-bind. Backoff sleeps go
        through the injected clock, so retry timing is deterministic in tests and uses no wall clock.
        Each attempt is timeout-bounded, and the lease is renewed after every attempt.
        """
        delta = await operator_delta(
            self._mirror,
            self._session_id,
            watermark=None,  # aggregators never advance a watermark, so their delta is always first-invocation
            available_caps=capabilities.available_ids(),
            previous_caps=frozenset(),
        )
        helpers = AggregationHelpers(
            self._runtime.durable,
            session_id=self._session_id,
            operator_id=aggregator.operator_id,
            epoch=epoch,
            clock=self._runtime.clock,
            is_final=True,
        )
        context = OperatorContext(
            session_id=self._session_id,
            epoch=epoch,
            store=view,
            capabilities=capabilities,
            delta=delta,
            effects=self._effect_guard(aggregator.operator_id, epoch),
            aggregation=helpers,
            is_final=True,
        )

        async def drive() -> None:
            async for _emitted in aggregator().run(context):  # a fresh instance per attempt (re-reads OCC version)
                pass

        attempt_numbers = count(1)  # one span per attempt, carrying the ordinal run_with_retry drives
        last_run_seconds = 0.0  # this attempt's own run time, shared from attempt() to its after-hook audit

        async def attempt() -> None:
            # A hung attempt (e.g. a durable write that never returns) must dead-letter, not hang the
            # session forever: each attempt is bounded by the aggregator's policy timeout, falling back
            # to the orchestrator's operation timeout (the gathering bound does not otherwise apply here).
            nonlocal last_run_seconds
            started = self._runtime.clock.monotonic()
            with self._runtime.telemetry.tracer.start_as_current_span(
                f'aggregator.run {aggregator.operator_id}',
                attributes={'operator_id': aggregator.operator_id, 'attempt': next(attempt_numbers)},
            ) as span:
                try:
                    await asyncio.wait_for(drive(), timeout=self._timeout_for(aggregator))
                except Exception:
                    # The raise propagates through the span context manager, which records it
                    # and marks the span failed; only the outcome attribute is set here.
                    last_run_seconds = self._runtime.clock.monotonic() - started
                    span.set_attribute('outcome', OperatorOutcome.FAILED.value)
                    self._observe_run_seconds(aggregator.operator_id, last_run_seconds, OperatorOutcome.FAILED)
                    raise
                last_run_seconds = self._runtime.clock.monotonic() - started
                span.set_attribute('outcome', OperatorOutcome.SUCCEEDED.value)
                self._observe_run_seconds(aggregator.operator_id, last_run_seconds, OperatorOutcome.SUCCEEDED)

        async def after_attempt(attempt_number: int, error: Exception | None) -> None:
            outcome = OperatorOutcome.SUCCEEDED if error is None else OperatorOutcome.FAILED
            if error is not None and attempt_number < self._retry_policy.max_attempts:
                # run_with_retry will relaunch after this hook — the same retry semantics the gather
                # loop counts when it arms a backoff window.
                self._runtime.telemetry.operator_retries_total.add(1, {'operator_id': aggregator.operator_id})
            await self._audit_operator_run(
                aggregator.operator_id,
                epoch,
                outcome=outcome,
                attempt=attempt_number,
                run_seconds=last_run_seconds,
                error=error,
            )
            # Renew the lease after every attempt: a slow aggregator plus backoff could otherwise
            # outlive the lock TTL. This hook deliberately runs outside the operation's try/except
            # (see run_with_retry), so a fenced renew raises StaleEpochError out of the retry loop and
            # `run` turns it into the clean SUPERSEDED stop instead of mistaking the takeover for an
            # aggregator failure. Concurrent aggregators renewing together is a harmless repeated extend.
            await self._runtime.lock.renew(self._session_id, epoch=epoch)

        try:
            await run_with_retry(
                attempt,
                policy=self._retry_policy,
                seed=seed_for(self._session_id, aggregator.operator_id),
                sleep=self._runtime.clock.sleep,
                on_attempt=after_attempt,
            )
        except AggregatorDeadLetteredError as error:
            # Bounded retries exhausted → dead-letter + alert; the session still COMPLETES with this
            # failure recorded in `result.dead_letters`; other aggregators are unaffected. The
            # per-attempt failures were already audited; this records the terminal disposition.
            self._dead_letters.append(DeadLetter(aggregator.operator_id, str(error)))
            self._runtime.telemetry.aggregator_dead_letters_total.add(1, {'operator_id': aggregator.operator_id})
            await self._audit_operator_run(
                aggregator.operator_id,
                epoch,
                outcome=OperatorOutcome.DEAD_LETTERED,
                attempt=self._retry_policy.max_attempts,
                error=error,
            )
            logger.opt(exception=error).warning(
                'Aggregator dead-lettered after exhausting retries; its output domain is flagged for re-drive',
                operator_id=aggregator.operator_id,
            )
            return
        await self._runtime.durable.mark_contribution(self._session_id, aggregator.operator_id, epoch=epoch)
        self._runs[aggregator.operator_id] = self._runs.get(aggregator.operator_id, 0) + 1
        logger.debug(
            'Aggregator completed and its contribution marked idempotently',
            session_id=self._session_id,
            operator_id=aggregator.operator_id,
        )

    async def _drain_inbox(self, epoch: Epoch) -> None:
        # Reclaim first: re-present entries a predecessor claimed but never applied (crash recovery),
        # then consume never-delivered entries. Keyed-merge makes redelivery idempotent.
        entries: list[DeliveredInboxEntry] = [
            *await self._runtime.inbox.reclaim(self._session_id),
            *await self._runtime.inbox.consume(self._session_id),
        ]
        for entry in entries:
            if isinstance(entry, PoisonInboxEntry):
                # Redelivery can never fix malformed wire bytes — quarantine on first sight, or the
                # entry would be re-presented on every pass and every resume until the session TTL.
                await self._quarantine_inbox_entry(
                    entry.entry_id, reason=entry.error, delivery_count=entry.delivery_count, epoch=epoch
                )
        valid = [entry for entry in entries if isinstance(entry, InboxEntry)]
        if len(valid) <= 1:
            # A single entry needs no batch path — and keeps exactly one apply attempt per delivery.
            for entry in valid:
                await self._apply_inbox_entry(entry, epoch)
            return
        # Apply the whole drained batch in ONE mirror/store write, then ack per entry, each only
        # AFTER its DataPoint is durably applied. At-least-once is preserved: a crash between the
        # batched apply and any individual ack just redelivers those entries, and the keyed-merge
        # makes the re-apply idempotent — so batching the merge ahead of the acks loses nothing.
        try:
            await self._apply_inbox_data_points([entry.data_point for entry in valid], epoch)
        except StaleEpochError:
            raise  # fencing is correctness: a takeover must stop this orchestrator, never be absorbed
        except Exception:
            # The batch's STORE apply failed as a unit and nothing landed (the apply is atomic).
            # Fall back to per-entry applies so one unappliable entry burns only its own delivery
            # budget — its healthy batch-mates are applied and acked rather than redelivered
            # alongside it.
            for entry in valid:
                await self._apply_inbox_entry(entry, epoch)
            return
        for entry in valid:
            await self._runtime.inbox.ack(self._session_id, entry.entry_id, epoch=epoch)
            self._runtime.telemetry.inbox_entries_total.add(1, {'disposition': 'applied'})

    async def _apply_inbox_data_points(self, data_points: Sequence[BaseDataPoint[Any]], epoch: Epoch) -> None:
        """Durably apply delivered inbox DataPoints; only a store-apply failure propagates.

        The ack decision gates on the durable STORE apply alone. Once that landed, a failure in
        the post-apply bookkeeping (audit append, archive append, telemetry) must not leave the
        entries un-acked: redelivering an already-durably-applied entry burns its delivery
        budget and duplicates the audit trail without fixing anything, so bookkeeping failures
        are logged and absorbed. A fencing signal still propagates from everywhere.
        """
        emitted = [_Emitted(None, data_point) for data_point in data_points]
        result = await self._mirror.write([signal.data_point for signal in emitted], epoch=epoch)
        logger.debug(
            'Inbox data points applied',
            session_id=self._session_id,
            count=len(data_points),
            data_point_types=sorted({data_point.type for data_point in data_points}),
        )
        try:
            await self._merge_bookkeeping(emitted, result, epoch)
        except StaleEpochError:
            raise
        except Exception:
            logger.opt(exception=True).error(
                'Inbox post-apply bookkeeping failed; acking the durably applied entries anyway',
                session_id=self._session_id,
                data_point_types=sorted({data_point.type for data_point in data_points}),
            )

    async def _apply_inbox_entry(self, entry: InboxEntry, epoch: Epoch) -> None:
        """Apply one delivered entry under the per-entry isolation policy.

        One bad entry must never wedge the session or crash-loop a resume — only a fencing
        signal (``StaleEpochError``) may stop the orchestrator from here.
        """
        try:
            await self._apply_inbox_data_points([entry.data_point], epoch)  # durable apply first
        except StaleEpochError:
            raise  # fencing is correctness: a takeover must stop this orchestrator, never be absorbed
        except Exception as error:
            # The apply may succeed on a later delivery (e.g. a transient store fault), so
            # leave the entry un-acked for redelivery — but only up to the delivery cap,
            # past which it is quarantined rather than ground against the store forever.
            if entry.delivery_count >= self._max_inbox_deliveries:
                await self._quarantine_inbox_entry(
                    entry.entry_id, reason=str(error), delivery_count=entry.delivery_count, epoch=epoch
                )
                return
            logger.opt(exception=error).warning(
                'Inbox entry apply failed; leaving it un-acked for redelivery',
                session_id=self._session_id,
                entry_id=entry.entry_id,
                delivery_count=entry.delivery_count,
            )
            self._runtime.telemetry.inbox_entries_total.add(1, {'disposition': 'redelivered'})
            return
        await self._runtime.inbox.ack(self._session_id, entry.entry_id, epoch=epoch)  # ack after the apply
        self._runtime.telemetry.inbox_entries_total.add(1, {'disposition': 'applied'})

    async def _quarantine_inbox_entry(self, entry_id: str, *, reason: str, delivery_count: int, epoch: Epoch) -> None:
        await self._runtime.inbox.quarantine(self._session_id, entry_id, reason=reason, epoch=epoch)
        self._runtime.telemetry.inbox_entries_total.add(1, {'disposition': 'quarantined'})
        logger.warning(
            'Inbox entry quarantined; it will not be redelivered',
            session_id=self._session_id,
            entry_id=entry_id,
            reason=reason,
            delivery_count=delivery_count,
        )
        await self._runtime.audit.append(
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                kind=AuditKind.INBOX_ENTRY_QUARANTINED,
                inbox=InboxAuditInfo(entry_id=entry_id, reason=reason, delivery_count=delivery_count),
            )
        )

    async def _merge(
        self, data_points: Sequence[BaseDataPoint[Any]], epoch: Epoch, *, operator_id: OperatorId | None = None
    ) -> None:
        await self._merge_emissions([_Emitted(operator_id, data_point) for data_point in data_points], epoch)

    async def _merge_emissions(self, emitted: Sequence[_Emitted], epoch: Epoch) -> None:
        """Merge a batch through the mirror (one store apply), then do the per-DataPoint bookkeeping.

        Per-event granularity is preserved end to end: one audit entry and one archive entry
        per DataPoint, batched onto the sinks in arrival order. Non-ephemeral DataPoints ride
        the epoch-guarded write-behind archive buffer (the second durable write path); ephemeral
        ones are never archived — the same rule aggregation follows.
        """
        if not emitted:
            return
        result = await self._mirror.write([signal.data_point for signal in emitted], epoch=epoch)
        await self._merge_bookkeeping(emitted, result, epoch)

    async def _merge_bookkeeping(self, emitted: Sequence[_Emitted], result: MirrorWriteResult, epoch: Epoch) -> None:
        """The per-DataPoint bookkeeping that follows a durable apply (audit, archive, telemetry)."""
        audit_entries: list[AuditLogEntry] = []
        archive_entries: list[ArchivedDataPoint] = []
        merge_kinds: list[str] = []  # one per DataPoint, counted only once the audit row actually lands
        for signal, outcome in zip(emitted, result.outcomes, strict=True):
            if signal.operator_id is not None:
                self._check_emission_declared(signal.operator_id, signal.data_point)
            merge_kinds.append(outcome.kind.value)
            if not signal.data_point.audits_every_emission:
                # A high-volume type: counted here and reported as one coalesced row, so hundreds of
                # merges cost one audit write instead of hundreds.
                self._coalesced_audits[signal.data_point.type] += 1
            elif outcome.kind is MergeKind.UPDATED:
                # The identity already existed and only its last_retrieved moved. Operators that rerun
                # on new data re-emit their whole output every pass — a converter can run hundreds of
                # times in one session — so a row each records the same value over and over. The value
                # is already in the store, its latest sighting is on the DataPoint, and the count is
                # reported below; what a per-row trail would add is the timing of each individual
                # re-sighting, at ~99% of the trail's volume.
                self._reobserved_audits[signal.data_point.type] += 1
            else:
                audit_entries.append(self._data_point_audit_entry(signal.data_point, signal.operator_id, epoch))
            if not signal.data_point.is_ephemeral:
                archive_entries.append(
                    ArchivedDataPoint.from_data_point(
                        signal.data_point,
                        session_id=self._session_id,
                        namespace_id=self._namespace_id,
                        epoch=epoch,
                    )
                )
        await self._append_audit_entries(audit_entries)
        # One count per DataPoint presented at the sole-mutator merge point, emitted only AFTER the audit
        # append succeeded — the count rides the audit seam, so a failed append leaves neither the row nor
        # the count behind and the metric can never disagree with the replayed trail.
        for kind in merge_kinds:
            self._runtime.telemetry.data_points_merged_total.add(1, {'kind': kind})
        await self._archive_entries(archive_entries)

    async def _append_audit_entries(self, entries: Sequence[AuditLogEntry]) -> None:
        # The batch variant only when more than one entry is in hand; single-event paths keep append.
        if len(entries) == 1:
            await self._runtime.audit.append(entries[0])
        elif entries:
            await self._runtime.audit.append_many(entries)

    async def _archive_entries(self, entries: Sequence[ArchivedDataPoint]) -> None:
        if len(entries) == 1:
            await self._runtime.archive.archive(entries[0])
        elif entries:
            await self._runtime.archive.archive_many(entries)

    def _check_emission_declared(self, operator_id: OperatorId, data_point: BaseDataPoint[Any]) -> None:
        # The graph, cycle detection, and quiescence all reason from `produces` declarations, so an
        # undeclared emission silently invalidates the scheduler's reasoning. The DataPoint is still
        # merged — production data is never dropped — but the mismatch is an ERROR (once per
        # operator-and-type, not per emission) so the declaration gets fixed at the source.
        declared = self._operators_by_id[operator_id].produces
        if any(isinstance(data_point, declared_type) for declared_type in declared):
            return
        if (operator_id, data_point.type) in self._undeclared_emissions:
            return
        self._undeclared_emissions.add((operator_id, data_point.type))
        logger.error(
            'Operator emitted a DataPoint type missing from its produces declaration; merging it anyway',
            operator_id=operator_id,
            data_point_type=data_point.type,
            declared_produces=sorted(declared_type.__name__ for declared_type in declared),
        )

    async def _refresh_capabilities(
        self, activator: CapabilityActivator, view: DataPointView, epoch: Epoch
    ) -> CapabilityView:
        capabilities = await activator.refresh(view)
        for capability_id in activator.activated_ids() - self._activated_seen:
            await self._audit_capability(capability_id, epoch)
        self._activated_seen |= set(activator.activated_ids())
        return capabilities

    async def _bounded_tail_step[T](self, step: Coroutine[Any, Any, T], description: str) -> T:
        """Run one completion-tail step under the operation timeout.

        The loop renews its lease as it gathers, but nothing renews across the tail, and the lock's TTL is
        specified to exceed the longest single unrenewed await. These are the awaits that broke that
        promise: a slow durable write blocks here for as long as it likes and the lease quietly lapses
        under a live owner — after which the supervisor resumes the session on top of this one,
        mid-finalize.

        Bounding restores the promise instead of working around it (a background keepalive would only make
        the lease lie). Cancelling a half-done flush is safe: the write-behind archive's buffer is durable
        and replayed on resume, and the epoch is released either way.
        """
        started = self._runtime.clock.monotonic()
        try:
            result = await asyncio.wait_for(step, timeout=self._operation_timeout)
        except (TimeoutError, asyncio.TimeoutError) as error:
            logger.exception(
                'Completion-tail step timed out; stopping so the lease is not held past its TTL',
                session_id=self._session_id,
                step=description,
                timeout_seconds=self._operation_timeout,
            )
            raise CompletionTailTimeoutError(
                f'{description} did not finish within {self._operation_timeout}s'
            ) from error
        # Logged on SUCCESS too: without it the only signal a tail step produces is its timeout, so how
        # close a healthy session runs to the budget is invisible until it crosses it.
        logger.info(
            'Completion-tail step finished inside its budget; compare elapsed_seconds against '
            'budget_seconds to see how much headroom the session had',
            session_id=self._session_id,
            step=description,
            elapsed_seconds=round(self._runtime.clock.monotonic() - started, 3),
            budget_seconds=self._operation_timeout,
        )
        return result

    async def _renew_lease_if_due(self, epoch: Epoch, last_renew: float) -> float:
        # Keep ownership while we work: a session can run longer than the lock's TTL, so renew the
        # lease on a cadence rather than once at acquire. Throttled so a chatty loop (one pass per
        # emission) does not hammer the lock. ``renew`` raises ``StaleEpochError`` if a higher epoch
        # has taken over, which ``run`` turns into a clean SUPERSEDED stop. Returns the (possibly
        # updated) last-renew time.
        now = self._runtime.clock.monotonic()
        if now - last_renew < self._lease_renew_interval:
            return last_renew
        await self._runtime.lock.renew(self._session_id, epoch=epoch)
        return now

    async def _advance_watermark(self, operator_id: OperatorId, revision: Revision, epoch: Epoch) -> None:
        await self._runtime.store.set_watermark(self._session_id, operator_id, revision, epoch=epoch)
        self._watermarks[operator_id] = revision

    async def _resolve_audit_cipher(self) -> ValueCipher | None:
        """Resolve the per-namespace cipher used to seal PII audit values for this run.

        The provider is injected by the flow; the framework does not know its failure modes (a namespace
        without a key, a KMS hiccup), so any resolution failure falls back to ``None`` and PII stays
        redacted — fail closed, never block the run or leak plaintext.
        """
        try:
            return await self._runtime.cipher_provider.for_namespace(self._namespace_id)
        except Exception as exc:  # noqa: BLE001 — provider-defined failures; degrade to redaction, never crash
            logger.opt(exception=exc).warning(
                'Could not resolve per-namespace cipher; PII audit values stay redacted',
                session_id=self._session_id,
                namespace_id=self._namespace_id,
            )
            return None

    def _data_point_audit_entry(
        self, data_point: BaseDataPoint[Any], operator_id: OperatorId | None, epoch: Epoch
    ) -> AuditLogEntry:
        summary = data_point.audit_summary()
        if summary is None:
            if data_point.is_pii:
                # Seal the value under the namespace key when a real cipher resolved this run; otherwise keep
                # it redacted. A NullCipher (identity) counts as "no cipher" so a passthrough
                # deployment never writes plaintext PII into the audit trail.
                summary = (
                    self._audit_cipher.encrypt(str(data_point.value))
                    if self._audit_cipher is not None and not isinstance(self._audit_cipher, NullCipher)
                    else '<redacted>'
                )
            else:
                summary = str(data_point.value)
        info = DataPointAuditInfo(
            data_point_type=data_point.type,
            summary=summary,
        )
        return AuditLogEntry(
            session_id=self._session_id,
            namespace_id=self._namespace_id,
            epoch=epoch,
            timestamp=self._runtime.clock.now(),
            operator_id=operator_id,
            kind=AuditKind.DATA_POINT_ADDED,
            data_point=info,
        )

    async def _audit_session_parked(self, epoch: Epoch) -> None:
        await self._runtime.audit.append(
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                kind=AuditKind.SESSION_PARKED,
            )
        )

    async def _audit_capability(self, capability_id: CapabilityId, epoch: Epoch) -> None:
        logger.debug(
            'Capability activated for this session; recording the audit entry',
            session_id=self._session_id,
            capability_id=capability_id,
        )
        await self._runtime.audit.append(
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                kind=AuditKind.CAPABILITY_ACTIVATED,
                capability=CapabilityAuditInfo(capability_id=capability_id),
            )
        )

    def _count_session(self, status: SessionStatus, span: Span) -> None:
        # The disposition lands on the run span and the counter at the same seam, so traces
        # and metrics can never disagree on how the session ended.
        span.set_attribute('status', status.value)
        self._runtime.telemetry.sessions_total.add(1, {'status': status.value})

    def _observe_run_seconds(self, operator_id: OperatorId, run_seconds: float, outcome: OperatorOutcome) -> None:
        self._runtime.telemetry.operator_run_seconds.record(
            run_seconds, {'operator_id': operator_id, 'outcome': outcome.value}
        )

    async def _audit_operator_run(
        self,
        operator_id: OperatorId,
        epoch: Epoch,
        *,
        outcome: OperatorOutcome,
        run_count: int = 1,
        attempt: int = 1,
        run_seconds: float | None = None,
        error: Exception | str | None = None,
    ) -> None:
        # The counter rides the audit seam so metrics and the audit trail can never disagree on how
        # many run dispositions (including SKIPPED and the terminal DEAD_LETTERED record) the
        # session produced.
        self._runtime.telemetry.operator_runs_total.add(1, {'operator_id': operator_id, 'outcome': outcome.value})
        await self._runtime.audit.append(
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                operator_id=operator_id,
                kind=AuditKind.OPERATOR_INVOKED,
                operator=OperatorAuditInfo(
                    outcome=outcome,
                    run_count=run_count,
                    attempt=attempt,
                    run_seconds=run_seconds,
                    error=None if error is None else str(error),
                ),
            )
        )

    async def _audit_capability_invocation(
        self, capability_id: CapabilityId, action: str, parameters: Mapping[str, Any], epoch: Epoch
    ) -> None:
        # Redact every parameter value (keys retained): the engine can't tell which carry PII, so the
        # trail records which action ran on which capability, never the argument values. The log
        # follows the same discipline — keys only. It is emitted inside the capability.action span
        # (the auditor runs within it), so the bridged record correlates with the action's trace.
        logger.debug(
            'Capability action invoked',
            session_id=self._session_id,
            capability_id=capability_id,
            action=action,
            parameter_keys=sorted(parameters),
        )
        redacted = {key: '<redacted>' for key in parameters}
        await self._runtime.audit.append(
            AuditLogEntry(
                session_id=self._session_id,
                namespace_id=self._namespace_id,
                epoch=epoch,
                timestamp=self._runtime.clock.now(),
                kind=AuditKind.CAPABILITY_INVOKED,
                capability=CapabilityAuditInfo(capability_id=capability_id, action=action, parameters=redacted),
            )
        )
