"""``SessionOrchestrationManager`` — the fleet-level supervisor.

Three jobs: supply the per-namespace capability catalog (already on the runtime), mint a
higher fencing epoch on **every** grant (delegated to the SessionLock the orchestrator
acquires — strictly increasing, so a predecessor is fenced), and resurrect orphaned
sessions. A session is **orphaned** when it is not yet COMPLETED (a flag on the
``SessionLock``, co-located with the fencing epoch and set by the orchestrator that finished
it) yet its ownership lock has expired; resume spawns a fresh orchestrator that rehydrates
from the persisted store and re-drives unfinished work (capabilities are re-activated fresh,
never resumed in place). Completion lives on the lock alongside the epoch, not in this
process, so a peer supervisor on another pod sees a finished session and never re-drives it.

Every call drives one :class:`~orcastork.flow.FlowDefinition` — the flow is
named once and its fingerprint travels with every spawn, so a resume whose flow no longer
matches the session's persisted one is detected and audited by the orchestrator instead of
silently changing scheduling semantics mid-session.

A ``SchedulingGate`` decides when a *new* session may start (e.g. the device-posture
cooldown) — a clock-driven gate, not an in-session timer; ``start_session`` enforces it and
refuses a start that is still cooling down.

``deliver`` is the ingress front door for mid-session input: it appends the DataPoint to the
durable inbox first, then guarantees some orchestrator will process it — a live holder is
woken by the inbox push; an orphaned session is resumed on this pod. Because the appending
ingress always runs on a live pod, append-triggered resume closes the pod-kill recovery loop
without a global orphan scanner; a host-level periodic re-drive (the host owns the session
index) remains the backstop for the double-failure case.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from ..datapoints import BaseDataPoint
from ..exceptions import LockHeldError, SchedulingGateBlockedError
from ..flow import FlowDefinition
from ..ids import NamespaceId, SessionId
from ..operators.aggregator import Aggregator
from ..orchestrator import Orchestrator, OrchestratorResult, SessionStatus
from ..ports.cooldown_gate import CooldownGate
from ..runtime import OrchestratorRuntime

# How long deliver waits before rechecking a session whose lock was held at delivery time. Must
# exceed the lock adapter's lease TTL: if the holder died right after the append, its lease has
# expired by the recheck and the resume re-drives the message.
DEFAULT_REDELIVERY_RECHECK_SECONDS = 35.0

# The manager's post-run backstop re-spawns to drain a straggler that landed in the completing
# orchestrator's non-atomic check->release gap. Each re-spawn re-checks and re-drives, so the loop
# terminates as soon as no deliver races the gap. This bound only fires under an adversarial burst of
# gap-landing delivers; it caps the manager's own work so a pathological caller cannot spin it forever.
MAX_BACKSTOP_RESPAWNS = 8

# ``SessionLock.current_epoch`` reports 0 for a session whose lock was never acquired, which is the one
# signal that distinguishes "never started" from "started, then its owner died" — the two states an
# expired (absent) lease otherwise looks identical from.
_NEVER_MINTED_EPOCH = 0


class SchedulingGate:
    """A per-key cooldown deciding when the next session for that key may start.

    A thin policy wrapper over the :class:`CooldownGate` port: the cooldown state lives in
    the backing store (cross-pod, restart-safe), and check-and-arm is one atomic step, so
    two racing starts can never both pass.
    """

    def __init__(self, gate: CooldownGate, *, cooldown_seconds: float) -> None:
        self._gate = gate
        self._cooldown = cooldown_seconds

    async def try_start(self, key: str) -> bool:
        """Atomically arm the cooldown for ``key`` iff none is active; ``True`` means proceed."""
        return await self._gate.try_acquire(key, self._cooldown)


@dataclass(slots=True)
class _TakeoverGuardEntry:
    """One session's takeover lock and how many callers currently hold or await it.

    The count is the eviction signal because the lock cannot be one: ``asyncio.Lock`` reports itself
    unlocked in the window between ``release()`` and the woken waiter re-acquiring, so a caller that is
    queued is invisible to ``locked()``.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class SessionOrchestrationManager:
    def __init__(
        self,
        runtime: OrchestratorRuntime,
        *,
        scheduling_gate: SchedulingGate | None = None,
        redelivery_recheck_seconds: float = DEFAULT_REDELIVERY_RECHECK_SECONDS,
    ) -> None:
        self._runtime = runtime
        self._gate = scheduling_gate
        self._redelivery_recheck_seconds = redelivery_recheck_seconds
        self._recheck_tasks: set[asyncio.Task[None]] = set()  # held so fire-and-forget rechecks aren't collected
        # Serializes RESUME attempts for one session WITHIN this process. Deliberately not applied to
        # reopen: a serialized reopen caller finds the session complete again once the winner finishes and
        # correctly re-opens it, so a lock there queues re-opens instead of collapsing them. Resume has no
        # such property — a serialized caller sees is_complete and stands down. The distributed lock already
        # decides who owns a session, but it is acquired inside the spawn — so without this every racer
        # first builds an orchestrator and activates the whole capability set, and only then discovers it
        # lost. That is the expensive half of a takeover paid N times over: N sets of Mongo/Redis/HTTP
        # clients and N rounds of secret loading, which is enough to OOM the pod on a burst. Holding this
        # first means the losers re-run the cheap ``is_held`` guard, see the winner, and stand down having
        # built nothing.
        self._takeover_locks: dict[SessionId, _TakeoverGuardEntry] = {}

    @asynccontextmanager
    async def _takeover_guard(self, session_id: SessionId) -> AsyncIterator[None]:
        """Serialize this process's takeover attempts for one session, cheapest-check-first.

        Entries are dropped once no caller holds or awaits them, so a long-lived process does not
        accumulate a lock per session it ever touched.
        """
        entry = self._takeover_locks.get(session_id)
        if entry is None:
            entry = _TakeoverGuardEntry()
            self._takeover_locks[session_id] = entry
        # Counted BEFORE the acquire, so a caller queued behind the current holder keeps the entry
        # mapped. Dropping it there would hand the next arrival an empty slot: it mints a second lock,
        # serializes against nobody, and enters the spawn alongside the queued caller — the concurrent
        # orchestrator-and-client build this guard exists to collapse.
        entry.users += 1
        try:
            async with entry.lock:
                yield
        finally:
            entry.users -= 1
            # Identity-checked so a departing caller can only ever evict the entry it counted itself
            # into, never a successor's live one.
            if entry.users == 0 and self._takeover_locks.get(session_id) is entry:
                del self._takeover_locks[session_id]

    async def start_session(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
        seed: Sequence[BaseDataPoint[Any]] = (),
        gate_key: str | None = None,
    ) -> OrchestratorResult:
        """Spawn a fresh orchestrator for a new session (mints epoch 1, injects the catalog).

        When a scheduling gate is configured the start is enforced here, keyed on ``gate_key``
        (defaulting to the namespace): a start still cooling down raises
        :class:`SchedulingGateBlockedError`. Enforcement lives in the manager so a caller
        cannot bypass the cooldown, and the gate's check-and-arm is atomic so concurrent
        starts for the same key admit exactly one.
        """
        with self._runtime.telemetry.tracer.start_as_current_span(
            'session.start',
            attributes={'session_id': session_id, 'namespace_id': namespace_id, 'flow_name': flow.name},
        ):
            logger.debug(
                'Session start requested; spawning a fresh orchestrator for the new session',
                session_id=session_id,
                namespace_id=namespace_id,
                flow_name=flow.name,
            )
            if self._gate is not None:
                key = gate_key if gate_key is not None else str(namespace_id)
                if not await self._gate.try_start(key):
                    logger.warning(
                        'Session start blocked by the scheduling gate cool-down',
                        session_id=session_id,
                        gate_key=key,
                    )
                    # Propagates through the span, so blocked starts are visible in traces.
                    raise SchedulingGateBlockedError(f'scheduling gate is cooling down for key {key!r}')
            return await self._spawn_and_drain(session_id, namespace_id, flow, seed)

    async def is_orphaned(self, session_id: SessionId) -> bool:
        """Orphaned iff the session was started, is not COMPLETED, and its ownership lock has expired."""
        if await self._runtime.lock.is_complete(session_id):
            return False
        if await self._runtime.lock.current_epoch(session_id) == _NEVER_MINTED_EPOCH:
            # Never started: no owner can have died and nothing is persisted to rehydrate.
            return False
        return not await self._runtime.lock.is_held(session_id)

    async def resume(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
    ) -> OrchestratorResult | None:
        """Resume an orphaned session with a fresh, higher-epoch orchestrator (rehydrate + re-drive).

        Returns ``None`` if the session is already COMPLETED (never resumed) or still owned by
        a live orchestrator. Resume does not re-seed — it rehydrates from the persisted store.
        ``flow`` should be the same definition the session started with; one that drifted (a
        deploy changed the operator set) is detected against the session's persisted flow
        fingerprint and audited, then driven anyway. A *parked* session holds no lock and is
        not complete, so it resumes exactly like a crashed one — with a shrinking deadline
        budget, since the wall-clock deadline persisted at the first gather is rehydrated.
        """
        async with self._takeover_guard(session_id):
            return await self._resume_locked(session_id=session_id, namespace_id=namespace_id, flow=flow)

    async def _resume_locked(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
    ) -> OrchestratorResult | None:
        """``resume`` with this process's takeover lock already held."""
        with self._runtime.telemetry.tracer.start_as_current_span(
            'session.resume',
            attributes={'session_id': session_id, 'namespace_id': namespace_id, 'flow_name': flow.name},
        ) as span:
            if await self._runtime.lock.is_complete(session_id):
                span.set_attribute('disposition', 'already_complete')
                logger.debug('Resume skipped: session already completed', session_id=session_id)
                return None
            if await self._runtime.lock.current_epoch(session_id) == _NEVER_MINTED_EPOCH:
                # Resume rehydrates from the persisted store, so a session that was never started has nothing
                # to rehydrate: spawning here would create it from whatever the inbox happens to hold, with no
                # seed. That is reachable from ``deliver`` — a caller that spawns the session with the seed
                # (``start_session``) races any delivery that arrives before its acquire lands, and the loser
                # is fenced out. Standing down keeps the seeded start the session's sole creator; the delivery
                # is already durably appended, so the start drains it.
                span.set_attribute('disposition', 'never_started')
                logger.debug('Resume skipped: session was never started', session_id=session_id)
                return None
            if await self._runtime.lock.is_held(session_id):
                span.set_attribute('disposition', 'still_owned')
                logger.debug('Resume skipped: session still owned by a live orchestrator', session_id=session_id)
                return None
            logger.info(
                'Resuming orphaned session with a fresh higher-epoch orchestrator',
                session_id=session_id,
                namespace_id=namespace_id,
                flow_name=flow.name,
            )
            try:
                result = await self._spawn_and_drain(session_id, namespace_id, flow, ())
            except LockHeldError:
                # Lost the acquire race to a concurrent resumer (the is_held check above is not a
                # claim). The winner owns the session and its higher epoch fences us; nothing was
                # mutated, so standing down is the correct outcome — same contract as "still owned".
                span.set_attribute('disposition', 'lost_acquire_race')
                logger.debug('Resume skipped: lost the acquire race to a concurrent resumer', session_id=session_id)
                return None
            span.set_attribute('disposition', 'resumed')
            return result

    async def reopen(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
    ) -> OrchestratorResult | None:
        """Re-open a completed session to fold in late result-affecting data.

        Clears the completion flag and each aggregator's contribution marker so the freshly
        spawned orchestrator re-runs the aggregation phase, then marks complete again. A fresh
        deadline is granted so the re-open session can run even when the original deadline has
        elapsed (a budgeted run completes in seconds; late participant data arrives
        much later). The re-open acquires a strictly higher epoch — exactly like resume — so the
        re-aggregation write is epoch-fenced against any stale predecessor.

        Returns ``None`` if the session is no longer complete (another reopen raced ahead and
        cleared the flag first), if the lock is held by a live orchestrator (a concurrent reopen
        already won the race), or if this reopen loses the acquire race (LockHeldError stand-down).
        """
        with self._runtime.telemetry.tracer.start_as_current_span(
            'session.reopen',
            attributes={'session_id': session_id, 'namespace_id': namespace_id, 'flow_name': flow.name},
        ) as span:
            if not await self._runtime.lock.is_complete(session_id):
                span.set_attribute('disposition', 'not_complete')
                logger.debug('Reopen skipped: session is not complete (raced ahead)', session_id=session_id)
                return None
            if await self._runtime.lock.is_held(session_id):
                span.set_attribute('disposition', 'still_owned')
                logger.debug('Reopen skipped: session still owned by a live orchestrator', session_id=session_id)
                return None
            # Clear the completion flag before spawning so the new orchestrator can acquire the lock.
            await self._runtime.lock.clear_complete(session_id)
            # Clear each aggregator's contribution marker so the re-spawn re-runs aggregation and
            # folds in the late data. The new orchestrator marks contributions again under a fresh,
            # higher epoch — the epoch fence on mark_contribution keeps the re-marks safe.
            for operator in flow.operators:
                if issubclass(operator, Aggregator):
                    await self._runtime.durable.clear_contribution(session_id, operator.operator_id)
            logger.info(
                'Re-opening completed session for late result-affecting data; re-aggregating with a fresh epoch',
                session_id=session_id,
                namespace_id=namespace_id,
                flow_name=flow.name,
            )
            try:
                result = await self._spawn_and_drain(session_id, namespace_id, flow, (), fresh_deadline=True)
            except LockHeldError:
                # Lost the acquire race to a concurrent reopener. The winner's higher epoch fences us;
                # nothing was mutated by the acquire itself, so standing down is correct — same contract
                # as resume's LockHeldError stand-down.
                span.set_attribute('disposition', 'lost_acquire_race')
                logger.debug('Reopen skipped: lost the acquire race to a concurrent reopener', session_id=session_id)
                return None
            span.set_attribute('disposition', 'reopened_for_late_data')
            return result

    async def deliver(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        data_point: BaseDataPoint[Any],
        flow: FlowDefinition,
        reopen_if_complete: bool = False,
    ) -> OrchestratorResult | None:
        """Deliver mid-session input and guarantee some orchestrator will process it.

        The durable inbox append always happens first — delivery never depends on what follows.
        Then delivery guarantees a consumer only when the lock is free: a COMPLETED session with a free lock
        is re-opened (aggregator re-runs, folds in the late data) when the caller passes
        ``reopen_if_complete=True``; otherwise the entry is appended and left to expire unprocessed. Re-open
        is the caller's explicit intent, NOT inferred from the DataPoint — attribute leaves like
        NAME/WORK_EMAIL are ``ephemeral`` (never individually Mongo-archived) yet the aggregator DOES fold
        them into the durable result, so ephemerality cannot be the discriminator. A session whose lock is
        HELD is left to its live owner (woken by the inbox push): a completing owner drains its own inbox
        before releasing (the orchestrator owns the inbox while it holds the epoch), so a deliver landing in
        that window is folded in-run — no deliver-side reopen-recheck is needed. The one held-lock recheck
        that remains guards the *dead-holder* case: an owner that died right after the append is re-driven by
        a resume once its lease frees. An orphaned session is resumed on this pod and the result returned. A
        *parked* session holds no lock and is not complete, so the orphan branch covers it: the delivery
        itself resumes the session with the new entry folded in. Callers on a request path typically wrap
        this in ``asyncio.create_task``.
        """
        with self._runtime.telemetry.tracer.start_as_current_span(
            'session.deliver',
            attributes={
                'session_id': session_id,
                'namespace_id': namespace_id,
                'flow_name': flow.name,
                'data_point_type': data_point.type,
            },
        ) as span:
            await self._runtime.inbox.append(session_id, data_point)
            logger.debug(
                'DataPoint delivered to the session inbox',
                session_id=session_id,
                data_point_type=data_point.type,
            )
            if await self._runtime.lock.is_complete(session_id):
                if reopen_if_complete:
                    # The caller declares this late data affects the durable result (e.g. the participant
                    # endpoint delivering NAME / WORK_EMAIL): re-open so the aggregator re-runs and folds it in.
                    # reopen is a free-lock spawn — it stands down if the lock is still held, because a
                    # completing holder drains its own inbox before releasing the epoch, so a deliver landing in
                    # that window is folded in-run without any deliver-side recheck.
                    span.set_attribute('disposition', 'reopened_for_late_data')
                    return await self.reopen(session_id=session_id, namespace_id=namespace_id, flow=flow)
                # No re-open requested: the entry is durably appended but a completed session will not be
                # re-driven for it, so it expires unprocessed.
                span.set_attribute('disposition', 'already_complete_no_reopen')
                logger.info(
                    'Delivery to a completed session without a re-open request; the entry will expire unprocessed',
                    session_id=session_id,
                    data_point_type=data_point.type,
                )
                return None
            if await self._runtime.lock.is_held(session_id):
                span.set_attribute('disposition', 'owner_notified')
                logger.debug(
                    'Delivery left to the live session owner; a redelivery recheck is scheduled',
                    session_id=session_id,
                    recheck_seconds=self._redelivery_recheck_seconds,
                )
                # Carry the caller's re-open intent into the deferred re-drive. By the time it fires the
                # holder may have COMPLETED rather than died, and `resume` refuses a completed session — so
                # redriving with `resume` alone silently drops an entry the caller was told was delivered.
                self._schedule_recheck(
                    session_id=session_id,
                    namespace_id=namespace_id,
                    flow=flow,
                    redrive=self._reopen_or_resume if reopen_if_complete else self.resume,
                )
                return None
            span.set_attribute('disposition', 'resumed')
            return await self.resume(session_id=session_id, namespace_id=namespace_id, flow=flow)

    async def _reopen_or_resume(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
    ) -> OrchestratorResult | None:
        """Re-drive a session whose late data affects the durable result, whichever state it reached.

        The deferred recheck is scheduled for a holder that may have died — but by the time it fires the
        holder may instead have finished, and `resume` deliberately stands down on a COMPLETED session. A
        caller that passed ``reopen_if_complete`` has declared its entry changes the durable result, so the
        completed case has to re-open rather than stand down: otherwise the entry is dropped after the
        caller was told it was delivered, and every stand-down on that path logs at DEBUG.
        """
        if await self._runtime.lock.is_complete(session_id):
            return await self.reopen(session_id=session_id, namespace_id=namespace_id, flow=flow)
        return await self.resume(session_id=session_id, namespace_id=namespace_id, flow=flow)

    def _schedule_recheck(
        self,
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
        redrive: Callable[..., Awaitable[OrchestratorResult | None]],
    ) -> None:
        # One deferred re-drive after the lease TTL, covering the dead-holder delivery race:
        #   • redrive=resume — the holder seen by deliver may have died (or parked) right after the append,
        #     before processing it; a resume in that window re-drives the message (a no-op if the session
        #     completed or another owner took over; a racing second resumer loses the acquire and stands down).
        # A *completing* holder needs no recheck here: it drains its own inbox before releasing the epoch, so
        # a deliver landing in the completion window is folded in-run; the manager's post-run backstop covers
        # the tiny non-atomic check->release gap.
        async def recheck() -> None:
            await self._runtime.clock.sleep(self._redelivery_recheck_seconds)
            try:
                await redrive(session_id=session_id, namespace_id=namespace_id, flow=flow)
            except Exception:
                # Fire-and-forget: nothing awaits this task, so a raised error would vanish.
                # Surface it; the host-level periodic re-drive remains the backstop.
                logger.exception(
                    "Deferred recheck failed; the host's periodic re-drive must recover this session",
                    session_id=session_id,
                    namespace_id=namespace_id,
                )

        task = asyncio.create_task(recheck())
        self._recheck_tasks.add(task)
        task.add_done_callback(self._recheck_tasks.discard)

    async def _spawn_and_drain(
        self,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
        seed: Sequence[BaseDataPoint[Any]],
        fresh_deadline: bool = False,
    ) -> OrchestratorResult:
        """Spawn an orchestrator run and backstop the drain handoff at its non-atomic check->release gap.

        The orchestrator drains its own inbox before releasing the epoch (it owns the inbox while it holds
        it), but the check->release window is non-atomic and spans two separate ops: the orchestrator reads
        the inbox while it still holds the epoch, so a cross-pod deliver appending a straggler after that read
        yet before the release is structurally invisible to it. This wrapper reads ``pending_count`` AFTER the
        run returns (i.e. after the orchestrator released), so it observes exactly those gap-landing
        stragglers. The manager holds no epoch, so only it can then re-drive: it re-spawns (fenced with a
        fresh, higher epoch — exactly like a reopen) to fold the straggler, and each re-spawn drains the
        inbox, so the loop terminates once ``pending_count`` hits 0. Bounded by :data:`MAX_BACKSTOP_RESPAWNS`
        so an adversarial burst of gap-landing delivers cannot spin the manager; the common case is zero
        backstop re-spawns.

        Residual: a pod crash in the microscopic [release, manager-probe] window is a double-failure that
        needs a host-level reopen-capable re-drive scanner (out of scope here); a deliver landing AFTER the
        probe is already covered by ``deliver`` spawning on a free lock (reopen on a free lock).

        Shared by every spawn site (start_session / resume / reopen) so the handoff is backstopped uniformly.
        """
        result = await self._spawn(session_id, namespace_id, flow, seed, fresh_deadline=fresh_deadline)
        for _ in range(MAX_BACKSTOP_RESPAWNS):
            if (
                result.status is not SessionStatus.COMPLETED
                or await self._runtime.inbox.pending_count(session_id) == 0
            ):
                return result
            logger.debug(
                'Straggler observed in the inbox after the completing run released; backstop re-spawning to drain',
                session_id=session_id,
                namespace_id=namespace_id,
                epoch=result.epoch,
            )
            try:
                result = await self._respawn_to_drain(session_id, namespace_id, flow)
            except LockHeldError:
                # A concurrent consumer (a racing deliver-driven reopen) grabbed the lock first; its
                # higher epoch fences us and it will drain the straggler. Standing down is correct — same
                # contract as resume/reopen's LockHeldError. Return the completed result unchanged.
                logger.debug(
                    'Backstop re-spawn lost the acquire race; the concurrent owner drains the straggler',
                    session_id=session_id,
                    namespace_id=namespace_id,
                )
                return result
        return result

    async def _respawn_to_drain(
        self,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
    ) -> OrchestratorResult:
        """Fenced free-lock re-spawn of a just-completed session to fold a gap-landing straggler.

        Mirrors :meth:`reopen`'s clear steps (clear the completion flag so the re-spawn can acquire, clear
        each aggregator's contribution so it re-runs and folds the new data) then spawns with a fresh
        deadline under a strictly higher epoch. Calls the raw ``_spawn`` — not ``_spawn_and_drain`` — because
        the enclosing loop already owns the drain-until-clean bound.
        """
        await self._runtime.lock.clear_complete(session_id)
        for operator in flow.operators:
            if issubclass(operator, Aggregator):
                await self._runtime.durable.clear_contribution(session_id, operator.operator_id)
        return await self._spawn(session_id, namespace_id, flow, (), fresh_deadline=True)

    async def _spawn(
        self,
        session_id: SessionId,
        namespace_id: NamespaceId,
        flow: FlowDefinition,
        seed: Sequence[BaseDataPoint[Any]],
        fresh_deadline: bool = False,
    ) -> OrchestratorResult:
        orchestrator = Orchestrator(
            session_id=session_id,
            namespace_id=namespace_id,
            runtime=self._runtime,
            operators=flow.operators,
            capabilities=flow.capabilities,
            seed=seed,
            completes_when=flow.completes_when,
            retry_policy=flow.retry_policy,
            park_after=flow.park_after,
            # Flow-level tuning rides through as-is: None means "use the orchestrator default".
            operation_timeout=flow.operation_timeout,
            session_deadline=flow.session_deadline,
            max_inbox_deliveries=flow.max_inbox_deliveries,
            emission_queue_size=flow.emission_queue_size,
            flow_identity=flow.identity(),
            fresh_deadline=fresh_deadline,
        )
        return await orchestrator.run()
