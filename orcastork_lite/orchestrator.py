"""The session-scoped ``Orchestrator`` — the gathering loop, and nothing else.

It owns one session: seed, then run every ready operator as a concurrent task, merging each
emission the instant it is produced and re-evaluating readiness, so a downstream operator
starts as soon as its input lands. Operators are never cancelled by new data; reruns are
coalesced on a debounce window and failed runs are relaunched on a backoff window, both
scheduled by the loop on the injected clock (never an in-task sleep). When nothing is running
and no window is armed, the session is quiescent: the gathered DataPoints are returned.

The whole session is bounded by ``session_deadline`` on the same clock: the deadline is checked
between passes and caps every window the loop waits out, and when it passes the in-flight
operators are cancelled (their already-streamed emissions are kept), so a flow that keeps
re-triggering itself cannot hold a process forever. A single operator run is bounded by its
own timeout, so the overshoot past the deadline is at most one operation timeout.

The loop is the **sole writer** of the session state: operators only stream emissions onto a
queue. It publishes every change — a merge, a run outcome, an activation, the completion — to
the runtime's ``SessionEventSink`` so a consumer can act on a DataPoint the moment it lands.
An operator that raises or times out is isolated — its already-emitted DataPoints stay, the
failure is logged and reported in the result, and the scheduler proceeds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from datetime import timedelta
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel, ConfigDict

from .capabilities import Capability, CapabilityActivator, CapabilityView
from .datapoints import DataPoint, DataPointView
from .events import CapabilityActivated, DataPointMerged, OperatorRunCompleted, SessionCompleted, SessionEvent
from .exceptions import DuplicateIdError
from .graph import backward_reachable, build_edges, cycle_caps, validate_acyclic_or_bounded
from .ids import CapabilityId, NamespaceId, OperatorId, SessionId
from .operators import InvocationDelta, Operator, OperatorContext, RerunOn
from .runtime import Runtime
from .scheduling import CircuitBreaker, DebounceController, backoff_delays, is_ready, seed_for
from .state import MergeOutcome, SessionState

DEFAULT_OPERATION_TIMEOUT = 30.0
DEFAULT_SESSION_DEADLINE = 300.0
_CANCELLED_AT_DEADLINE = 'cancelled: the session deadline passed while the operator was running'


class SessionResult(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    operator_runs: dict[OperatorId, int]  # every finished attempt counts, retries included
    data_points: DataPointView  # the session's final DataPoint set
    failures: dict[OperatorId, str]  # last error of each operator that failed with no retry left, or was cancelled
    deadline_hit: bool  # the session deadline ended gathering before it went quiescent


class _Emitted(BaseModel):
    model_config = ConfigDict(frozen=True)

    operator_id: OperatorId
    data_point: DataPoint  # bare: see InvocationDelta for why the annotation is not parametrized


class _Completed(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    operator_id: OperatorId
    error: Exception | None


class _Running(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    task: asyncio.Task[None]
    observed_revision: int  # the revision the launch snapshot saw — becomes this run's watermark


class _Runnable(BaseModel):
    model_config = ConfigDict(frozen=True)

    operator: type[Operator]
    delta: InvocationDelta
    is_retry: bool


class _Plan(BaseModel):
    model_config = ConfigDict(frozen=True)

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
        session_deadline: float | None = DEFAULT_SESSION_DEADLINE,
    ) -> None:
        self._session_id = session_id
        self._namespace_id = namespace_id
        self._runtime = runtime
        self._operators = list(operators)
        self._capabilities = list(capabilities)
        self._seed = list(seed)
        self._operation_timeout = operation_timeout
        self._session_deadline = session_deadline  # seconds of session clock; None → unbounded
        self._check_unique_ids()
        self._prune_to_consumed_closure()
        self._operators_by_id = {operator.operator_id: operator for operator in self._operators}
        # Fail fast on an unbounded cycle rather than looping to the deadline.
        edges = build_edges(self._operators, self._capabilities)
        validate_acyclic_or_bounded(edges)
        self._breaker = CircuitBreaker(cycle_caps(edges))
        self._state = SessionState()
        self._runs: dict[OperatorId, int] = {}
        self._failures: dict[OperatorId, str] = {}
        self._failed_attempts: dict[OperatorId, int] = {}  # consecutive failures of the current retry sequence
        self._prev_caps: dict[OperatorId, frozenset[CapabilityId]] = {}
        self._undeclared_emissions: set[tuple[OperatorId, type[DataPoint[Any]]]] = set()
        self._published_caps: set[CapabilityId] = set()
        self._deadline_hit = False

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
            activation_timeout=self._operation_timeout,
        )
        await self._merge(self._seed)
        await self._gather(activator)
        logger.info(
            'Session completed',
            session_id=self._session_id,
            operator_runs=sum(self._runs.values()),
            failures=sorted(self._failures),
            deadline_hit=self._deadline_hit,
        )
        await self._publish(
            SessionCompleted(
                **self._event_base(),
                deadline_hit=self._deadline_hit,
                operator_runs=dict(self._runs),
                failures=dict(self._failures),
            )
        )
        return SessionResult(
            operator_runs=dict(self._runs),
            data_points=self._state.view(),
            failures=dict(self._failures),
            deadline_hit=self._deadline_hit,
        )

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
        deadline = None if self._session_deadline is None else clock.monotonic() + self._session_deadline
        queue: asyncio.Queue[_Emitted | _Completed] = asyncio.Queue()
        running: dict[OperatorId, _Running] = {}
        try:
            while deadline is None or clock.monotonic() < deadline:
                view = self._state.view()
                observed = self._state.revision
                capabilities = await self._refresh_capabilities(activator, view)
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
                    running[operator_id] = _Running(task=task, observed_revision=observed)
                    (retries if runnable.is_retry else debounce).clear(operator_id)
                if running:
                    # Block on the first signal, then drain everything already queued behind it and
                    # re-plan once for the whole batch: nothing is delayed, and one snapshot serves all.
                    signals: list[_Emitted | _Completed] = [await queue.get()]
                    while not queue.empty():
                        signals.append(queue.get_nowait())
                    await self._consume(signals, retries, running)
                    continue
                if plan.next_due_in is not None:
                    # Fast-forward to the soonest armed window, never past the deadline.
                    wait = (
                        plan.next_due_in if deadline is None else min(plan.next_due_in, deadline - clock.monotonic())
                    )
                    await clock.sleep(max(0.0, wait))
                    continue
                return  # nothing running, nothing armed → quiescent
            # Falling out of the loop condition is exactly a deadline hit.
            self._deadline_hit = True
            logger.warning(
                'Session deadline hit; gathering stopped and in-flight operators are cancelled',
                session_id=self._session_id,
                session_deadline=self._session_deadline,
                in_flight_operator_ids=sorted(running),
            )
        finally:
            await self._drain_remaining(queue, running)

    async def _drain_remaining(
        self, queue: asyncio.Queue[_Emitted | _Completed], running: dict[OperatorId, _Running]
    ) -> None:
        # A quiescent exit reaches here with both empty (a no-op). A deadline hit — or an unexpected
        # error — reaches here with operators in flight: keep whatever they already emitted, then cancel
        # them. The deadline is the one signal allowed to stop a running operator.
        await self._drain_queue(queue)
        for record in running.values():
            record.task.cancel()
        if running:
            await asyncio.gather(*(record.task for record in running.values()), return_exceptions=True)
        # A task can enqueue a final emission right as it is cancelled; drain once more so it is not dropped.
        await self._drain_queue(queue)
        for operator_id in sorted(running):
            self._failures[operator_id] = _CANCELLED_AT_DEADLINE
            await self._publish(
                OperatorRunCompleted(
                    **self._event_base(),
                    operator_id=operator_id,
                    outcome='cancelled',
                    attempt=self._failed_attempts.get(operator_id, 0) + 1,
                    error=_CANCELLED_AT_DEADLINE,
                )
            )
        running.clear()

    async def _drain_queue(self, queue: asyncio.Queue[_Emitted | _Completed]) -> None:
        emitted: list[_Emitted] = []
        while not queue.empty():
            signal = queue.get_nowait()
            if isinstance(signal, _Emitted):
                emitted.append(signal)
        await self._merge_emissions(emitted)

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
                    runnable.append(
                        _Runnable(operator=operator, delta=self._delta_for(operator, capabilities), is_retry=True)
                    )
                else:
                    track(retries.due_at(operator_id))
                continue
            if not self._state.has_run(operator_id):
                runnable.append(
                    _Runnable(operator=operator, delta=self._delta_for(operator, capabilities), is_retry=False)
                )
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
                runnable.append(_Runnable(operator=operator, delta=delta, is_retry=False))
            else:
                track(debounce.due_at(operator_id))
        next_due_in = None if soonest_due is None else max(0.0, soonest_due - self._runtime.clock.monotonic())
        return _Plan(runnable=runnable, next_due_in=next_due_in)

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
                finalized = emission.finalize(retrieved_by=operator.operator_id, at=clock.now())
                await queue.put(_Emitted(operator_id=operator.operator_id, data_point=finalized))

        error: Exception | None = None
        try:
            # Constructed inside the boundary too: an __init__ that raises is a FAILED run, not a lost task.
            await asyncio.wait_for(drain(operator()), timeout=self._timeout_for(operator))
        except Exception as exc:  # operator fault-isolation boundary — never wedge the session
            error = exc
        await queue.put(_Completed(operator_id=operator.operator_id, error=error))

    def _timeout_for(self, operator: type[Operator]) -> float:
        return self._operation_timeout if operator.policy.timeout is None else operator.policy.timeout.total_seconds()

    async def _refresh_capabilities(self, activator: CapabilityActivator, view: DataPointView) -> CapabilityView:
        capabilities = await activator.refresh(view)
        outcomes: list[tuple[CapabilityId, Literal['activated', 'failed']]] = [
            *((capability_id, 'activated') for capability_id in sorted(activator.activated_ids())),
            *((capability_id, 'failed') for capability_id in sorted(activator.failed_ids())),
        ]
        for capability_id, outcome in outcomes:
            if capability_id in self._published_caps:
                continue
            self._published_caps.add(capability_id)
            await self._publish(
                CapabilityActivated(**self._event_base(), capability_id=capability_id, outcome=outcome)
            )
        return capabilities

    async def _consume(
        self,
        signals: Sequence[_Emitted | _Completed],
        retries: DebounceController,
        running: dict[OperatorId, _Running],
    ) -> None:
        # ONE merge for every emission in the batch (arrival order preserved), then the completion
        # bookkeeping — an operator's emissions always precede its completion on the queue anyway.
        await self._merge_emissions([signal for signal in signals if isinstance(signal, _Emitted)])
        for signal in signals:
            if isinstance(signal, _Completed):
                await self._record_completion(signal, retries, running)

    async def _merge(self, data_points: Sequence[DataPoint[Any]]) -> None:
        await self._publish_merge(self._state.merge(data_points))

    async def _merge_emissions(self, emitted: Sequence[_Emitted]) -> None:
        if not emitted:
            return
        for emission in emitted:
            self._check_emission_declared(emission.operator_id, emission.data_point)
        await self._publish_merge(self._state.merge(emission.data_point for emission in emitted))

    async def _publish_merge(self, outcome: MergeOutcome) -> None:
        for merge, data_points in (('added', outcome.added), ('updated', outcome.updated)):
            for data_point in data_points:
                await self._publish(
                    DataPointMerged(
                        **self._event_base(),
                        data_point_type=type(data_point).__name__,
                        value=data_point.value,
                        retrieved_by=data_point.retrieved_by,
                        merge=merge,
                        revision=outcome.revision,
                    )
                )

    async def _record_completion(
        self, signal: _Completed, retries: DebounceController, running: dict[OperatorId, _Running]
    ) -> None:
        operator_id = signal.operator_id
        record = running.pop(operator_id)
        self._runs[operator_id] = self._runs.get(operator_id, 0) + 1
        # Every attempt counts toward the breaker, so a failing cycle operator cannot loop between the
        # retry scheduler and the cycle forever.
        self._breaker.record_run(operator_id)
        attempts = self._failed_attempts.get(operator_id, 0) + 1
        if signal.error is None:
            self._failed_attempts.pop(operator_id, None)
            self._state.advance_watermark(operator_id, record.observed_revision)
            await self._publish_run(operator_id, 'succeeded', attempts, None)
            return
        self._failed_attempts[operator_id] = attempts
        if self._schedule_retry(operator_id, attempts, retries):
            logger.opt(exception=signal.error).warning(
                'Operator run failed; a retry is scheduled on backoff', operator_id=operator_id, attempt=attempts
            )
            # A retried failure keeps its old watermark: the relaunch must re-present the same delta.
            # Its emissions are already merged, and re-emitting them is idempotent (keyed-merge).
            await self._publish_run(operator_id, 'retrying', attempts, signal.error)
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
        await self._publish_run(operator_id, 'failed', attempts, signal.error)

    async def _publish_run(
        self,
        operator_id: OperatorId,
        outcome: Literal['succeeded', 'failed', 'retrying', 'cancelled'],
        attempt: int,
        error: Exception | None,
    ) -> None:
        await self._publish(
            OperatorRunCompleted(
                **self._event_base(),
                operator_id=operator_id,
                outcome=outcome,
                attempt=attempt,
                error=None if error is None else str(error),
            )
        )

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

    def _event_base(self) -> dict[str, Any]:
        return {'session_id': self._session_id, 'namespace_id': self._namespace_id, 'at': self._runtime.clock.now()}

    async def _publish(self, event: SessionEvent) -> None:
        # The sink is a view of the session, not part of it: a failing publish is logged and the event
        # dropped, never allowed to stop the loop.
        try:
            await self._runtime.events.publish(event)
        except Exception:  # sink fault-isolation boundary
            logger.opt(exception=True).warning(
                'Session event publish failed; the event was dropped',
                session_id=self._session_id,
                event_kind=event.kind,
            )
