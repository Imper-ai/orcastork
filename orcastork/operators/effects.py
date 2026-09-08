"""``EffectGuard`` — the claim/commit/revert gate for non-idempotent operator side effects.

Everything else the engine repeats is safe to repeat: emissions keyed-merge, archive writes
keyed-upsert, aggregator outputs are OCC-guarded and contribution-marked. A *side effect*
(sending an OTP, opening an ITSM ticket) is not — and gathering operators are deliberately
re-driven: retried on failure, rerun on new data, and re-run on crash-resume.
``async with ctx.once(key) as acquired:`` is the explicit guard such effects need: entering
claims the ``(operator, key)`` durably, the caller performs the effect iff ``acquired`` is
``True``, a clean exit commits the claim, and a failing exit reverts it — so a retry of a
failed attempt re-runs the effect instead of skipping it, while a committed effect never
fires again across reruns, retries and resumes.

Sole-mutator nuance: effect marks are written from INSIDE operator tasks, concurrent with
the gathering loop's own store writes. This does not violate the sole-mutator invariant —
the marks live in a separate keyspace from the DataPoint merge path (no revision, change-set,
or watermark interplay), each transition is a single-key atomic step, and every write is
epoch-fenced like any other, so a fenced predecessor's mark is rejected, never half-applied.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from enum import Enum

from loguru import logger

from ..ids import Epoch, OperatorId, SessionId
from ..ports.datapoint_store import DataPointStore, EffectClaim, effect_pending_epoch


class EffectRecovery(Enum):
    """Policy for a predecessor's mid-effect crash — its ``pending`` mark survives under a stale epoch,
    so whether the effect actually happened is unknowable."""

    # Reclaim and re-run: the framework's at-least-once posture everywhere else (inbox redelivery,
    # operator re-drives), accepting a possible duplicate over a possibly-lost effect.
    RERUN = 'rerun'
    # Skip, leaving the stale mark in place: a later resume then sees the same unknown state and
    # applies its own policy, rather than a fabricated 'committed'.
    SKIP = 'skip'


class EffectGuard:
    """Bound to one ``(store, session_id, operator_id, epoch)``; handed to operators via the context."""

    __slots__ = ('_store', '_session_id', '_operator_id', '_epoch')

    def __init__(self, store: DataPointStore, *, session_id: SessionId, operator_id: OperatorId, epoch: Epoch) -> None:
        self._store = store
        self._session_id = session_id
        self._operator_id = operator_id
        self._epoch = epoch

    @asynccontextmanager
    async def once(self, effect_key: str, *, on_unknown: EffectRecovery = EffectRecovery.RERUN) -> AsyncIterator[bool]:
        """Guard a non-idempotent side effect: ``acquired`` is ``True`` iff this attempt owns running it.

        The stored key is namespaced as ``{operator_id}:{effect_key}``, so two operators using
        the same key can never collide. Entering claims the key (``on_unknown`` decides what a
        predecessor's mid-effect crash means); a clean exit commits; any exception or
        cancellation reverts the claim and propagates, so the loop-scheduled retry of this
        attempt re-enters with ``True`` and the effect actually runs.
        """
        namespaced = f'{self._operator_id}:{effect_key}'
        if not await self._claim(namespaced, on_unknown):
            yield False
            return
        try:
            yield True
        except BaseException:
            # The revert often runs during cancellation unwind (a deadline/timeout cancelling the
            # operator task) — shield it so that a further cancellation cannot interrupt the
            # revert mid-flight; the store call itself still runs to completion.
            try:
                await asyncio.shield(self._store.revert_effect(self._session_id, namespaced, epoch=self._epoch))
            except Exception:
                # Rare double failure: the effect failed AND the revert failed, so the mark stays
                # pending under this epoch and a same-epoch retry sees PENDING_SAME_EPOCH → False.
                # We prefer a possibly-skipped effect over a possibly-double-fired one within a
                # single epoch; a cross-epoch resume still gets the RERUN/SKIP recovery policy.
                logger.opt(exception=True).warning(
                    'Effect claim revert failed; a same-epoch retry will skip this effect',
                    session_id=self._session_id,
                    effect_key=namespaced,
                )
            raise
        # The commit can also run during cancellation unwind (the body finishes right as a
        # deadline/timeout cancels the operator task) — shield it like the revert, or the claim
        # is stranded as `pending:<epoch>` and a same-epoch retry SKIPS an effect that DID run;
        # under cancellation the commit still runs to completion while the CancelledError
        # propagates.
        try:
            await asyncio.shield(self._store.commit_effect(self._session_id, namespaced, epoch=self._epoch))
        except Exception:
            # Rare double failure, mirroring the revert path: the effect ran AND the commit
            # failed, so the mark stays pending under this epoch and a same-epoch retry sees
            # PENDING_SAME_EPOCH → False. We prefer a possibly-skipped effect over a
            # possibly-double-fired one within a single epoch; a cross-epoch resume still gets
            # the RERUN/SKIP recovery policy.
            logger.opt(exception=True).warning(
                'Effect commit failed; the claim stays pending and a same-epoch retry will skip this effect',
                session_id=self._session_id,
                effect_key=namespaced,
            )

    async def _claim(self, namespaced: str, on_unknown: EffectRecovery) -> bool:
        claim = await self._store.claim_effect(self._session_id, namespaced, epoch=self._epoch, reclaim_stale=False)
        match claim:
            case EffectClaim.ACQUIRED:
                return True
            case EffectClaim.ALREADY_COMMITTED | EffectClaim.PENDING_SAME_EPOCH:
                return False
            case EffectClaim.PENDING_STALE_EPOCH:
                return await self._recover(namespaced, on_unknown)

    async def _recover(self, namespaced: str, on_unknown: EffectRecovery) -> bool:
        match on_unknown:
            case EffectRecovery.SKIP:
                # The stale mark is deliberately left in place: a later resume must see the same
                # unknown state and apply its own policy, not a fabricated outcome.
                return False
            case EffectRecovery.RERUN:
                stale_state = await self._store.get_effect_state(self._session_id, namespaced)
                logger.warning(
                    'Stale pending effect mark found: the outcome of a predecessor attempt is unknown; re-running',
                    session_id=self._session_id,
                    effect_key=namespaced,
                    stale_epoch=None if stale_state is None else effect_pending_epoch(stale_state),
                    epoch=int(self._epoch),
                )
                reclaim = await self._store.claim_effect(
                    self._session_id, namespaced, epoch=self._epoch, reclaim_stale=True
                )
                # A concurrent commit may have raced the reclaim; anything but ACQUIRED means the
                # effect must not run here.
                return reclaim is EffectClaim.ACQUIRED
