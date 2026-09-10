"""The session-scoped ``Orchestrator`` — the gathering loop, and nothing else.

It owns one session: seed, then run every ready operator as a concurrent task, merging each
emission the instant it is produced and re-evaluating readiness, so a downstream operator
starts as soon as its input lands. Operators are never cancelled by new data; reruns are
coalesced on a debounce window and failed runs are relaunched on a backoff window, both
scheduled by the loop on the injected clock (never an in-task sleep). When nothing is running
and no window is armed, the session is quiescent: the gathered DataPoints are returned.

The loop is the **sole writer** of the session state: operators only stream emissions onto a
queue. An operator that raises or times out is isolated — its already-emitted DataPoints stay,
the failure is logged and reported in the result, and the scheduler proceeds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from loguru import logger

from .capabilities import Capability, CapabilityActivator, CapabilityView
from .datapoints import DataPoint, DataPointView
from .exceptions import DuplicateIdError
from .graph import backward_reachable, build_edges, cycle_caps, validate_acyclic_or_bounded
from .ids import CapabilityId, NamespaceId, OperatorId, SessionId
from .operators import InvocationDelta, Operator, OperatorContext, RerunOn
from .runtime import Runtime
from .scheduling import CircuitBreaker, DebounceController, backoff_delays, is_ready, seed_for
from .state import SessionState

DEFAULT_OPERATION_TIMEOUT = 30.0


@dataclass(frozen=True)
class SessionResult:
    operator_runs: dict[OperatorId, int]  # every attempt counts, retries included
    data_points: DataPointView  # the session's final DataPoint set
    failures: dict[OperatorId, str]  # last error of each operator that failed with no retry left


@dataclass(frozen=True)
class _Emitted:
    operator_id: OperatorId
    data_point: DataPoint[Any]


@dataclass(frozen=True)
class _Completed:
    operator_id: OperatorId
    error: Exception | None


@dataclass(frozen=True)
class _Running:
    task: asyncio.Task[None]
    observed_revision: int  # the revision the launch snapshot saw — becomes this run's watermark


@dataclass(frozen=True)
class _Runnable:
    operator: type[Operator]
    delta: InvocationDelta
    is_retry: bool


@dataclass(frozen=True)
class _Plan:
    runnable: list[_Runnable]
    next_due_in: float | None  # seconds until the soonest armed-but-not-due window, else None


class Orchestrator:
    def __init__(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        runtime: Runtime,
        operators: Iterable[type[Operator]],
        capabilities: Iterable[type[Capability]] = (),
        seed: Sequence[DataPoint[Any]] = (),
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,
    ) -> None:
        self._session_id = session_id
        self._namespace_id = namespace_id
        self._runtime = runtime
        self._operators = list(operators)
        self._capabilities = list(capabilities)
        self._seed = list(seed)
        self._operation_timeout = operation_timeout
        self._check_unique_ids()
        self._prune_to_consumed_closure()
        self._operators_by_id = {operator.operator_id: operator for operator in self._operators}
        # Fail fast on an unbounded cycle rather than looping forever.
        edges = build_edges(self._operators, self._capabilities)
        validate_acyclic_or_bounded(edges)
        self._breaker = CircuitBreaker(cycle_caps(edges))
        self._state = SessionState()
        self._runs: dict[OperatorId, int] = {}
        self._failures: dict[OperatorId, str] = {}
        self._failed_attempts: dict[OperatorId, int] = {}  # consecutive failures of the current retry sequence
        self._prev_caps: dict[OperatorId, frozenset[CapabilityId]] = {}
        self._undeclared_emissions: set[tuple[OperatorId, type[DataPoint[Any]]]] = set()

    def _check_unique_ids(self) -> None:
        operator_ids: list[str] = [operator.operator_id for operator in self._operators]
        capability_ids: list[str] = [capability.capability_id for capability in self._capabilities]
        for kind, ids in (('operator_id', operator_ids), ('capability_id', capability_ids)):
            duplicates = sorted({value for value in ids if ids.count(value) > 1})
            if duplicates:
                raise DuplicateIdError(f'duplicate {kind}: {duplicates}')

    def _prune_to_consumed_closure(self) -> None:
        """Run only the operators whose output is (transitively) needed by a ``consumes`` declaration.

        Sinks are every declared ``consumes`` type plus the declaring operators' own gate inputs; an
        operator outside the backward-reachable closure of those sinks produces data nothing considers,
        so it never runs. No ``consumes`` anywhere leaves every operator in place.
        """
        sink_operators = [operator for operator in self._operators if operator.consumes]
        if not sink_operators:
            return
        sinks = frozenset[type[DataPoint[Any]]]().union(
            *(operator.consumes | operator.depends_on for operator in sink_operators)
        )
        kept = backward_reachable(self._operators, self._capabilities, sinks)
        pruned = [op for op in self._operators if op not in kept and op not in sink_operators]
        if not pruned:
            return
        self._operators = [op for op in self._operators if op not in pruned]
        logger.info(
            'Pruned operators whose output nothing consumes',
            session_id=self._session_id,
            pruned=sorted(str(op.operator_id) for op in pruned),
        )

    async def run(self) -> SessionResult:
        await self._exclude_unpermitted_operators()
        activator = CapabilityActivator(
            {capability.capability_id: capability for capability in self._capabilities},
            self._runtime.catalog,
            self._namespace_id,
        )
        self._state.merge(self._seed)
        await self._gather(activator)
        logger.info(
            'Session completed',
            session_id=self._session_id,
            operator_runs=sum(self._runs.values()),
            failures=sorted(self._failures),
        )
        return SessionResult(dict(self._runs), self._state.view(), dict(self._failures))

    async def _exclude_unpermitted_operators(self) -> None:
        """Per-namespace operator gating: the catalog decides, ``None`` means unrestricted."""
        permitted = await self._runtime.catalog.permitted_operators(self._namespace_id)
        if permitted is None:
            return
        excluded = sorted(op.operator_id for op in self._operators if op.operator_id not in permitted)
        if not excluded:
            return
        self._operators = [op for op in self._operators if op.operator_id in permitted]
        logger.info(
            'Operators excluded: not permitted for the namespace',
            session_id=self._session_id,
            namespace_id=self._namespace_id,
            excluded_operator_ids=excluded,
        )

    async def _gather(self, activator: CapabilityActivator) -> None:
        clock = self._runtime.clock
        debounce = DebounceController(clock)
        # Retries reuse the same due-time mechanics as debounced reruns: the loop owns the backoff window.
        retries = DebounceController(clock)
        queue: asyncio.Queue[_Emitted | _Completed] = asyncio.Queue()
        running: dict[OperatorId, _Running] = {}
        try:
            while True:
                view = self._state.view()
                observed = self._state.revision
                capabilities = await activator.refresh(view)
                plan = self._plan(view, capabilities, debounce, retries, running)
                for runnable in plan.runnable:
                    operator_id = runnable.operator.operator_id
                    logger.debug(
                        'Launching operator',
                        session_id=self._session_id,
                        operator_id=operator_id,
                        is_retry=runnable.is_retry,
                    )
                    # Recorded at launch: both must reflect what this run saw, not the live state at completion.
                    self._prev_caps[operator_id] = capabilities.available_ids()
                    task = asyncio.create_task(
                        self._run_one(runnable.operator, runnable.delta, view, capabilities, queue)
                    )
                    running[operator_id] = _Running(task, observed)
                    (retries if runnable.is_retry else debounce).clear(operator_id)
                if running:
                    # Block on the first signal, then drain everything already queued behind it and
                    # re-plan once for the whole batch: nothing is delayed, and one snapshot serves all.
                    signals: list[_Emitted | _Completed] = [await queue.get()]
                    while not queue.empty():
                        signals.append(queue.get_nowait())
                    self._consume(signals, retries, running)
                    continue
                if plan.next_due_in is not None:
                    await clock.sleep(plan.next_due_in)  # fast-forward to the soonest armed window
                    continue
                break  # nothing running, nothing armed → quiescent
        finally:
            # Only an unexpected error reaches here with work in flight; never leave tasks dangling.
            for record in running.values():
                record.task.cancel()
            if running:
                await asyncio.gather(*(record.task for record in running.values()), return_exceptions=True)

    def _plan(
        self,
        view: DataPointView,
        capabilities: CapabilityView,
        debounce: DebounceController,
        retries: DebounceController,
        running: dict[OperatorId, _Running],
    ) -> _Plan:
        present_types = view.present_types()
        available_types = capabilities.available_types()
        runnable: list[_Runnable] = []
        soonest_due: float | None = None

        def track(due_at: float | None) -> None:
            nonlocal soonest_due
            if due_at is not None:
                soonest_due = due_at if soonest_due is None else min(soonest_due, due_at)

        for operator in self._operators:
            operator_id = operator.operator_id
            if operator_id in running or self._breaker.is_tripped(operator_id):
                continue
            if not is_ready(operator, present_types=present_types, available_capability_types=available_types):
                continue
            if retries.is_scheduled(operator_id):
                # An armed retry owns this operator's next launch: it relaunches even with no new data and
                # its watermark is deliberately stale, so the rerun path must stand aside until it fires.
                if retries.is_due(operator_id):
                    runnable.append(_Runnable(operator, self._delta_for(operator, capabilities), is_retry=True))
                else:
                    track(retries.due_at(operator_id))
                continue
            if not self._state.has_run(operator_id):
                runnable.append(_Runnable(operator, self._delta_for(operator, capabilities), is_retry=False))
                continue
            if not operator.policy.rerun_on_new_data:
                continue
            delta = self._delta_for(operator, capabilities)
            if not self._has_relevant_new_data(operator, delta):
                continue
            # Rerun-eligible: arm a coalescing window (arrivals within it collapse into one rerun) and
            # launch only once it is due. An armed window holds the loop open until then.
            if not debounce.is_scheduled(operator_id):
                debounce.schedule(operator_id, window=operator.policy.debounce)
            if debounce.is_due(operator_id):
                runnable.append(_Runnable(operator, delta, is_retry=False))
            else:
                track(debounce.due_at(operator_id))
        next_due_in = None if soonest_due is None else max(0.0, soonest_due - self._runtime.clock.monotonic())
        return _Plan(runnable, next_due_in)

    def _delta_for(self, operator: type[Operator], capabilities: CapabilityView) -> InvocationDelta:
        return self._state.delta_for(
            operator.operator_id,
            previous_caps=self._prev_caps.get(operator.operator_id, frozenset()),
            available_caps=capabilities.available_ids(),
        )

    @staticmethod
    def _has_relevant_new_data(operator: type[Operator], delta: InvocationDelta) -> bool:
        # A newly-available capability always warrants a rerun. Otherwise new data must match a type the
        # operator consumes (`depends_on` or `uses`, subtype-aware); under ADDED_ONLY a freshness-only
        # re-observation does not count. An operator consuming what it produces is a genuine cycle whose
        # own emissions re-trigger it — which is why the graph requires it to declare `max_cycles`.
        if delta.newly_available_caps:
            return True
        new_data = delta.added if operator.policy.rerun_on is RerunOn.ADDED_ONLY else delta.added | delta.updated
        triggers = operator.depends_on | operator.uses
        return any(isinstance(data_point, trigger) for data_point in new_data for trigger in triggers)

    async def _run_one(
        self,
        operator: type[Operator],
        delta: InvocationDelta,
        view: DataPointView,
        capabilities: CapabilityView,
        queue: asyncio.Queue[_Emitted | _Completed],
    ) -> None:
        context = OperatorContext(
            session_id=self._session_id,
            namespace_id=self._namespace_id,
            store=view,
            capabilities=capabilities,
            delta=delta,
        )
        clock = self._runtime.clock

        async def drain(instance: Operator) -> None:
            # Finalizing stamps provenance and can raise on a malformed value; it happens inside the
            # fault boundary so a bad emission is isolated to this operator. Each emission streams onto
            # the queue as produced, so the loop merges it while this operator is still running.
            async for emission in instance.run(context):
                await queue.put(
                    _Emitted(
                        operator.operator_id, emission.finalize(retrieved_by=operator.operator_id, at=clock.now())
                    )
                )

        error: Exception | None = None
        try:
            # Constructed inside the boundary too: an __init__ that raises is a FAILED run, not a lost task.
            await asyncio.wait_for(drain(operator()), timeout=self._timeout_for(operator))
        except Exception as exc:  # operator fault-isolation boundary — never wedge the session
            error = exc
        await queue.put(_Completed(operator.operator_id, error))

    def _timeout_for(self, operator: type[Operator]) -> float:
        return self._operation_timeout if operator.policy.timeout is None else operator.policy.timeout.total_seconds()

    def _consume(
        self,
        signals: Sequence[_Emitted | _Completed],
        retries: DebounceController,
        running: dict[OperatorId, _Running],
    ) -> None:
        # ONE merge for every emission in the batch (arrival order preserved), then the completion
        # bookkeeping — an operator's emissions always precede its completion on the queue anyway.
        emitted = [signal for signal in signals if isinstance(signal, _Emitted)]
        for emission in emitted:
            self._check_emission_declared(emission.operator_id, emission.data_point)
        self._state.merge(emission.data_point for emission in emitted)
        for signal in signals:
            if isinstance(signal, _Completed):
                self._record_completion(signal, retries, running)

    def _record_completion(
        self, signal: _Completed, retries: DebounceController, running: dict[OperatorId, _Running]
    ) -> None:
        operator_id = signal.operator_id
        record = running.pop(operator_id)
        self._runs[operator_id] = self._runs.get(operator_id, 0) + 1
        # Every attempt counts toward the breaker, so a failing cycle operator cannot loop between the
        # retry scheduler and the cycle forever.
        self._breaker.record_run(operator_id)
        if signal.error is None:
            self._failed_attempts.pop(operator_id, None)
            self._state.advance_watermark(operator_id, record.observed_revision)
            return
        attempts = self._failed_attempts.get(operator_id, 0) + 1
        self._failed_attempts[operator_id] = attempts
        if self._schedule_retry(operator_id, attempts, retries):
            logger.opt(exception=signal.error).warning(
                'Operator run failed; a retry is scheduled on backoff', operator_id=operator_id, attempt=attempts
            )
            # A retried failure keeps its old watermark: the relaunch must re-present the same delta.
            # Its emissions are already merged, and re-emitting them is idempotent (keyed-merge).
            return
        # Terminal — a later data-driven rerun starts a fresh attempt sequence.
        self._failed_attempts.pop(operator_id, None)
        self._failures[operator_id] = str(signal.error)
        logger.opt(exception=signal.error).error(
            'Operator run failed; its emissions were kept and the scheduler is proceeding',
            operator_id=operator_id,
            attempt=attempts,
        )
        # Advance to the revision this run *observed*, not the live one: DataPoints merged while it ran were
        # never seen by it, so they must still count as new data and be able to trigger a rerun.
        self._state.advance_watermark(operator_id, record.observed_revision)

    def _schedule_retry(self, operator_id: OperatorId, attempts: int, retries: DebounceController) -> bool:
        """Arm a loop-scheduled relaunch of a failed run; False when the failure is terminal."""
        policy = self._operators_by_id[operator_id].policy.retry
        # A tripped breaker is terminal even with attempts remaining — an armed retry could never launch.
        if policy is None or attempts >= policy.max_attempts or self._breaker.is_tripped(operator_id):
            return False
        delay = backoff_delays(policy, seed=seed_for(self._session_id, operator_id))[attempts - 1]
        retries.schedule(operator_id, window=timedelta(seconds=delay))
        return True

    def _check_emission_declared(self, operator_id: OperatorId, data_point: DataPoint[Any]) -> None:
        # Cycle detection and pruning reason from `produces`, so an undeclared emission silently invalidates
        # them. The DataPoint is still merged — data is never dropped — but the mismatch is logged as an
        # ERROR once per operator-and-type so the declaration gets fixed at the source.
        declared = self._operators_by_id[operator_id].produces
        if any(isinstance(data_point, declared_type) for declared_type in declared):
            return
        key = (operator_id, type(data_point))
        if key in self._undeclared_emissions:
            return
        self._undeclared_emissions.add(key)
        logger.error(
            'Operator emitted a DataPoint type missing from its produces declaration; merging it anyway',
            operator_id=operator_id,
            data_point_type=type(data_point).__name__,
            declared_produces=sorted(declared_type.__name__ for declared_type in declared),
        )
