"""The ``DataPointStore`` port — session-scoped, keyed-merge, monotonic-revision, fenced.

Every mutating method is guarded by the session's fencing ``epoch``: a write whose epoch
is below the highest epoch the store has accepted for the session is rejected atomically
with :class:`~orcastork.exceptions.StaleEpochError` (no partial write).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, Final, Protocol

from ..datapoints import BaseDataPoint, DataPointView
from ..ids import Epoch, OperatorId, Revision, SessionId
from .change_set import ChangeSet

# The two stored effect states (the ``fx:{session}`` keyspace contract, shared by every adapter):
# an in-flight claim is ``pending:<epoch>`` — naming its owner so commit/revert/reclaim can fence —
# and a finished effect is ``committed``.
EFFECT_COMMITTED: Final = 'committed'
EFFECT_PENDING_PREFIX: Final = 'pending:'


def effect_pending_state(epoch: Epoch) -> str:
    """The stored form of an in-flight claim owned by ``epoch``."""
    return f'{EFFECT_PENDING_PREFIX}{int(epoch)}'


def effect_pending_epoch(state: str) -> Epoch | None:
    """The owning epoch of a ``pending:<epoch>`` state string, or ``None`` for any other state."""
    if not state.startswith(EFFECT_PENDING_PREFIX):
        return None
    # Corrupt persisted state (a mangled epoch suffix) must not take down claim/recovery — an
    # owner that cannot be parsed is reported as unknown, exactly like a non-pending state.
    try:
        return Epoch(int(state.removeprefix(EFFECT_PENDING_PREFIX)))
    except (ValueError, TypeError):
        return None


class EffectClaim(Enum):
    """Outcome of :meth:`DataPointStore.claim_effect` — what the stored state was and who owns it now."""

    ACQUIRED = 'acquired'  # the caller owns the claim and MUST resolve it (commit or revert)
    ALREADY_COMMITTED = 'already_committed'  # the effect ran to completion in some earlier attempt
    PENDING_SAME_EPOCH = 'pending_same_epoch'  # a duplicate claim within this run — do not run the effect
    PENDING_STALE_EPOCH = 'pending_stale_epoch'  # a predecessor died mid-effect; its outcome is unknown


class DataPointReader(Protocol):
    """The read surface delta computation needs — satisfied by every store and by the
    orchestrator's sole-mutator session mirror, so readiness/delta logic can be served from
    local state without touching the backend."""

    async def snapshot(self, session_id: SessionId) -> DataPointView:
        """A read-only, subtype-aware view of the session's current DataPoints."""
        ...

    async def revision(self, session_id: SessionId) -> Revision:
        """The session's current monotonic revision (baseline ``0`` when empty)."""
        ...

    async def change_set_since(self, session_id: SessionId, since: Revision) -> ChangeSet:
        """DataPoints added/updated since revision ``since``."""
        ...


class DataPointStore(DataPointReader, Protocol):
    async def write(
        self, session_id: SessionId, data_points: Iterable[BaseDataPoint[Any]], *, epoch: Epoch
    ) -> Revision:
        """Keyed-merge ``data_points`` into the session and return the resulting revision.

        Equal ``(type, value)`` identities merge (bumping ``last_retrieved``) rather than
        duplicate; the revision advances iff at least one DataPoint was added or freshened.
        """
        ...

    async def apply_resolved(
        self,
        session_id: SessionId,
        *,
        added: Sequence[BaseDataPoint[Any]],
        updated: Sequence[BaseDataPoint[Any]],
        epoch: Epoch,
    ) -> Revision:
        """Apply a keyed-merge a sole-mutator caller has ALREADY resolved; return the new revision.

        ``added`` are identities new to the session carrying their final field values;
        ``updated`` are existing identities carrying their merged timestamps. The adapter
        applies them blindly — no re-read, no merge of its own — atomically and epoch-guarded,
        stamping per-identity added/updated revisions exactly as :meth:`write` would (one new
        revision for the whole batch, allocated only when the batch is non-empty). A stale
        epoch is rejected with no partial write, even for an empty batch — fencing semantics
        are byte-identical to :meth:`write` on every path.
        """
        ...

    async def get_watermark(self, session_id: SessionId, operator_id: OperatorId) -> Revision | None:
        """The revision at an operator's last run, or ``None`` if it has never run."""
        ...

    async def set_watermark(
        self, session_id: SessionId, operator_id: OperatorId, revision: Revision, *, epoch: Epoch
    ) -> None:
        """Persist an operator's watermark (epoch-guarded session state)."""
        ...

    async def claim_effect(
        self, session_id: SessionId, effect_key: str, *, epoch: Epoch, reclaim_stale: bool
    ) -> EffectClaim:
        """Atomically claim a side-effect key for this epoch (epoch-guarded, atomic per key).

        Absent → stored as ``pending:<epoch>``, :attr:`EffectClaim.ACQUIRED` — the caller owns
        running the effect and MUST resolve the claim via :meth:`commit_effect` or
        :meth:`revert_effect`. ``committed`` → :attr:`EffectClaim.ALREADY_COMMITTED`. This epoch's
        own pending → :attr:`EffectClaim.PENDING_SAME_EPOCH`. Another epoch's pending (a
        predecessor died mid-effect, outcome unknown) → overwritten to ``pending:<epoch>`` and
        :attr:`EffectClaim.ACQUIRED` when ``reclaim_stale`` is true, else
        :attr:`EffectClaim.PENDING_STALE_EPOCH` with the stale mark left in place.

        Marks live alongside the session's other state and share its lifetime — they are read
        and written outside the DataPoint merge path (no revision/change-set interplay).
        """
        ...

    async def commit_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        """Transition this epoch's ``pending:<epoch>`` mark to ``committed`` (epoch-guarded).

        Idempotent over ``committed``. Any other state (absent, or another epoch's pending) is
        left untouched — a commit must never fabricate ``committed`` for an effect this epoch
        does not own.
        """
        ...

    async def revert_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        """Delete ONLY a ``pending:<epoch>`` mark owned by this epoch (epoch-guarded).

        Never deletes ``committed`` (the effect DID run) and never deletes another epoch's
        pending (that claim is resolved by its owner, or by a successor's recovery policy).
        """
        ...

    async def get_effect_state(self, session_id: SessionId, effect_key: str) -> str | None:
        """The stored effect state (``pending:<epoch>`` or ``committed``), or ``None`` if never claimed."""
        ...

    async def get_session_deadline(self, session_id: SessionId) -> datetime | None:
        """The session's persisted wall-clock completion deadline, or ``None`` if never set."""
        ...

    async def set_session_deadline(self, session_id: SessionId, deadline: datetime, *, epoch: Epoch) -> None:
        """Persist the session's wall-clock deadline (epoch-guarded session state).

        Written once at the session's first gather and rehydrated by every later run, so the
        overall budget keeps shrinking across parks, crashes and resumes — a process restart
        must never grant a fresh full deadline window.
        """
        ...

    async def get_flow_fingerprint(self, session_id: SessionId) -> str | None:
        """The flow fingerprint persisted for the session, or ``None`` if never set."""
        ...

    async def set_flow_fingerprint(self, session_id: SessionId, fingerprint: str, *, epoch: Epoch) -> None:
        """Persist the session's flow fingerprint (epoch-guarded session state).

        Written at spawn and rewritten whenever drift is detected, so every later resume
        compares against the *latest* flow that drove the session — repeated resumes with the
        same changed flow stay quiet.
        """
        ...
