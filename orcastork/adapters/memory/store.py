"""In-memory ``DataPointStore`` — deterministic, infra-free, fenced keyed-merge store.

Per session it keeps each identity's DataPoint plus the revisions at which it was first
added and last freshened, so ``change_set_since`` can cleanly split added vs updated. The
session revision advances once per write that actually changed something. Writes are
guarded by the highest epoch seen for the session (lower epoch → ``StaleEpochError``).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ...datapoints import BaseDataPoint, DataPointView, identity_key
from ...exceptions import StaleEpochError
from ...ids import Epoch, OperatorId, Revision, SessionId
from ...ports.change_set import ChangeSet
from ...ports.datapoint_store import EFFECT_COMMITTED, EffectClaim, effect_pending_state


@dataclass
class _Entry:
    data_point: BaseDataPoint[Any]
    added_rev: int
    updated_rev: int


@dataclass
class _SessionState:
    entries: dict[tuple[str, Any], _Entry] = field(default_factory=dict)
    revision: int = 0
    max_epoch: int = 0
    watermarks: dict[OperatorId, int] = field(default_factory=dict)
    effects: dict[str, str] = field(default_factory=dict)  # effect_key → 'pending:<epoch>' | 'committed'
    deadline: datetime | None = None  # the persisted wall-clock session deadline (set once, rehydrated on resume)
    flow_fingerprint: str | None = None  # the latest flow that drove the session (drift detection on resume)


class InMemoryDataPointStore:
    def __init__(self) -> None:
        self._sessions: dict[SessionId, _SessionState] = {}

    def _state(self, session_id: SessionId) -> _SessionState:
        return self._sessions.setdefault(session_id, _SessionState())

    @staticmethod
    def _guard_epoch(state: _SessionState, epoch: Epoch) -> None:
        # Ownership is the lock's job; recording the highest ACCEPTED epoch on EVERY guarded
        # write is what completes stale-writer rejection — a session whose first mutation is an
        # effect claim or a session-meta write must still fence a later lower-epoch writer.
        if epoch < state.max_epoch:
            raise StaleEpochError(f'epoch {epoch} is stale (current {state.max_epoch})')
        state.max_epoch = epoch

    async def write(
        self, session_id: SessionId, data_points: Iterable[BaseDataPoint[Any]], *, epoch: Epoch
    ) -> Revision:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        next_revision = state.revision + 1
        changed = False
        for data_point in data_points:
            key = identity_key(data_point)
            existing = state.entries.get(key)
            if existing is None:
                state.entries[key] = _Entry(data_point, added_rev=next_revision, updated_rev=next_revision)
                changed = True
            elif data_point.last_retrieved > existing.data_point.last_retrieved:
                existing.data_point = existing.data_point.reobserved(data_point.last_retrieved)
                existing.updated_rev = next_revision
                changed = True
        if changed:
            state.revision = next_revision
        return Revision(state.revision)

    async def apply_resolved(
        self,
        session_id: SessionId,
        *,
        added: Sequence[BaseDataPoint[Any]],
        updated: Sequence[BaseDataPoint[Any]],
        epoch: Epoch,
    ) -> Revision:
        # The caller (a sole mutator) already keyed-merged, so this applies blindly: new
        # identities land with both stamps at the batch revision, merged identities overwrite
        # their entry and advance only the updated stamp — exactly the stamping `write` does.
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        if not added and not updated:
            return Revision(state.revision)
        next_revision = state.revision + 1
        for data_point in added:
            state.entries[identity_key(data_point)] = _Entry(
                data_point, added_rev=next_revision, updated_rev=next_revision
            )
        for data_point in updated:
            key = identity_key(data_point)
            existing = state.entries.get(key)
            # An update of an unknown identity still lands (blind application, mirroring the
            # Redis HSET) — it simply carries no added stamp, like a field set by mode 'u'.
            added_rev = 0 if existing is None else existing.added_rev
            state.entries[key] = _Entry(data_point, added_rev=added_rev, updated_rev=next_revision)
        state.revision = next_revision
        return Revision(next_revision)

    async def snapshot(self, session_id: SessionId) -> DataPointView:
        return DataPointView(entry.data_point for entry in self._state(session_id).entries.values())

    async def revision(self, session_id: SessionId) -> Revision:
        return Revision(self._state(session_id).revision)

    async def change_set_since(self, session_id: SessionId, since: Revision) -> ChangeSet:
        state = self._state(session_id)
        added = tuple(e.data_point for e in state.entries.values() if e.added_rev > since)
        updated = tuple(e.data_point for e in state.entries.values() if e.added_rev <= since < e.updated_rev)
        return ChangeSet(added=added, updated=updated)

    async def get_watermark(self, session_id: SessionId, operator_id: OperatorId) -> Revision | None:
        watermark = self._state(session_id).watermarks.get(operator_id)
        return None if watermark is None else Revision(watermark)

    async def set_watermark(
        self, session_id: SessionId, operator_id: OperatorId, revision: Revision, *, epoch: Epoch
    ) -> None:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        state.watermarks[operator_id] = revision

    async def claim_effect(
        self, session_id: SessionId, effect_key: str, *, epoch: Epoch, reclaim_stale: bool
    ) -> EffectClaim:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        stored = state.effects.get(effect_key)
        if stored is None:
            state.effects[effect_key] = effect_pending_state(epoch)
            return EffectClaim.ACQUIRED
        if stored == EFFECT_COMMITTED:
            return EffectClaim.ALREADY_COMMITTED
        if stored == effect_pending_state(epoch):
            return EffectClaim.PENDING_SAME_EPOCH
        if reclaim_stale:
            state.effects[effect_key] = effect_pending_state(epoch)
            return EffectClaim.ACQUIRED
        return EffectClaim.PENDING_STALE_EPOCH

    async def commit_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        # Only this epoch's own pending mark transitions; 'committed' stays as-is (idempotent), and
        # any other state is left untouched — never fabricate 'committed' for an unowned claim.
        if state.effects.get(effect_key) in (effect_pending_state(epoch), EFFECT_COMMITTED):
            state.effects[effect_key] = EFFECT_COMMITTED

    async def revert_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        # Deletes ONLY this epoch's own pending mark: never 'committed' (the effect DID run) and
        # never another epoch's pending (resolved by its owner, or by a successor's recovery policy).
        if state.effects.get(effect_key) == effect_pending_state(epoch):
            del state.effects[effect_key]

    async def get_effect_state(self, session_id: SessionId, effect_key: str) -> str | None:
        return self._state(session_id).effects.get(effect_key)

    async def get_session_deadline(self, session_id: SessionId) -> datetime | None:
        return self._state(session_id).deadline

    async def set_session_deadline(self, session_id: SessionId, deadline: datetime, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        state.deadline = deadline

    async def get_flow_fingerprint(self, session_id: SessionId) -> str | None:
        return self._state(session_id).flow_fingerprint

    async def set_flow_fingerprint(self, session_id: SessionId, fingerprint: str, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        self._guard_epoch(state, epoch)
        state.flow_fingerprint = fingerprint
