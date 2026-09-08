"""``SessionStateMirror`` — the orchestrator's local copy of one session's DataPoint state.

While the orchestrator holds the fencing epoch it is the session's **sole mutator**, so the
store's contents are fully determined by what this process has already written — the
local-state-plus-durable-changelog insight from Kafka Streams / Flink. The mirror rehydrates
once (one snapshot + one revision read), then serves every snapshot / revision / change-set
read locally and resolves every keyed-merge locally, forwarding each resolved batch to the
durable store via :meth:`DataPointStore.apply_resolved`. The store stays the system of record
**and the revision allocator** — the mirror never invents a revision; it applies its local
copy at the revision the store returned.

Fencing is untouched: every forwarded write is epoch-guarded by the store, and a
:class:`~orcastork.exceptions.StaleEpochError` propagates *before* the local
copy is touched — a fenced orchestrator dies holding a mirror that never diverged from what
the store accepted. The local reads are trustworthy for the same reason the optimization is
sound: any other writer must hold a higher epoch, and this process's next forwarded write
would then raise instead of silently diverging. Sanctioned mid-session input (the inbox) is
merged *through* the mirror on the gathering loop, so it is never invisible to local reads.

Change-set answers for revisions that predate the rehydration (a resumed operator's persisted
watermark) cannot be derived from the snapshot alone — those per-identity revision stamps
live only in the store — so the orchestrator primes each such baseline once at gather start
(:meth:`prime_change_baseline`); every later query overlays the local stamps on the primed
split. Add-stamps are immutable in the store, so a primed baseline never goes stale: only
*later updates* can change an answer, and those are exactly what the local stamps record.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ..datapoints import BaseDataPoint, DataPointView, MergeKind, MergeResult, identity_key
from ..exceptions import StateMirrorError
from ..ids import Epoch, Revision, SessionId
from ..ports.change_set import ChangeSet
from ..ports.datapoint_store import DataPointStore

# The mirror keys entries exactly like the keyed-merge: (type, normalized value).
_IdentityKey = tuple[str, Any]


@dataclass
class _MirrorEntry:
    data_point: BaseDataPoint[Any]
    # Revision stamps mirroring the store's added/updated bookkeeping. ``0`` means "at or
    # before rehydration": those stamps live only in the store, so pre-rehydration queries
    # consult a primed baseline instead of these fields.
    added_rev: int
    updated_rev: int


@dataclass(frozen=True)
class _Resolution:
    """A batch keyed-merge resolved against the local state, before it is durably applied."""

    outcomes: tuple[MergeResult, ...]  # one per presented DataPoint, in input order
    added: dict[_IdentityKey, BaseDataPoint[Any]]  # identities new to the session → final values
    updated: dict[_IdentityKey, BaseDataPoint[Any]]  # existing identities → merged values (freshened only)


@dataclass(frozen=True)
class MirrorWriteResult:
    revision: Revision  # allocated by the store, never locally
    outcomes: tuple[MergeResult, ...]  # one per presented DataPoint, in input order


class SessionStateMirror:
    def __init__(self, store: DataPointStore, session_id: SessionId) -> None:
        self._store = store
        self._session_id = session_id
        self._entries: dict[_IdentityKey, _MirrorEntry] = {}
        self._baselines: dict[int, dict[_IdentityKey, MergeKind]] = {}
        self._revision = 0
        self._rehydrated_at = 0
        self._hydrated = False

    async def rehydrate(self) -> None:
        """Load the authoritative local copy — the mirror's only full read of the store."""
        view = await self._store.snapshot(self._session_id)
        revision = await self._store.revision(self._session_id)
        self._entries = {
            identity_key(data_point): _MirrorEntry(data_point, added_rev=0, updated_rev=0) for data_point in view.all()
        }
        self._revision = int(revision)
        self._rehydrated_at = int(revision)
        self._baselines = {}
        self._hydrated = True

    async def prime_change_baseline(self, since: Revision) -> None:
        """Capture the store's change split for one pre-rehydration revision (one read, reused forever).

        A revision at or past the rehydration point needs no baseline — the local stamps answer
        it exactly — so priming one is a no-op.
        """
        self._require_hydrated()
        if int(since) >= self._rehydrated_at or int(since) in self._baselines:
            return
        change = await self._store.change_set_since(self._session_id, since)
        baseline = {identity_key(data_point): MergeKind.ADDED for data_point in change.added}
        baseline |= {identity_key(data_point): MergeKind.UPDATED for data_point in change.updated}
        self._baselines[int(since)] = baseline

    async def snapshot(self, session_id: SessionId) -> DataPointView:
        self._guard_read(session_id)
        return DataPointView(entry.data_point for entry in self._entries.values())

    async def revision(self, session_id: SessionId) -> Revision:
        self._guard_read(session_id)
        return Revision(self._revision)

    async def change_set_since(self, session_id: SessionId, since: Revision) -> ChangeSet:
        self._guard_read(session_id)
        if int(since) >= self._rehydrated_at:
            return self._local_change_set(int(since))
        return self._baselined_change_set(int(since))

    async def write(self, data_points: Sequence[BaseDataPoint[Any]], *, epoch: Epoch) -> MirrorWriteResult:
        """Keyed-merge locally, forward the resolved batch, apply at the store-returned revision.

        Even an all-no-op batch is forwarded, keeping fencing byte-identical to
        :meth:`DataPointStore.write`: a stale writer is rejected (and this orchestrator stops)
        whether or not its batch would have changed anything. A rejected forward leaves the
        local copy untouched, so the mirror can never run ahead of what the store accepted.
        """
        self._require_hydrated()
        resolution = self._resolve(data_points)
        revision = await self._store.apply_resolved(
            self._session_id,
            added=tuple(resolution.added.values()),
            updated=tuple(resolution.updated.values()),
            epoch=epoch,
        )
        self._apply(resolution, int(revision))
        return MirrorWriteResult(revision=revision, outcomes=resolution.outcomes)

    def _resolve(self, data_points: Sequence[BaseDataPoint[Any]]) -> _Resolution:
        # Reproduces the store's merge semantics over the local state, including duplicates
        # within one batch: an identity added then re-observed in the same batch stays one
        # `added` row carrying its final timestamps; `last_retrieved` advances only when newer
        # (`reobserved` keeps the max) and `first_retrieved` is immutable.
        outcomes: list[MergeResult] = []
        added: dict[_IdentityKey, BaseDataPoint[Any]] = {}
        updated: dict[_IdentityKey, BaseDataPoint[Any]] = {}
        for data_point in data_points:
            key = identity_key(data_point)
            if key in added:
                added[key] = added[key].reobserved(data_point.last_retrieved)
                outcomes.append(MergeResult(MergeKind.UPDATED, added[key]))
                continue
            if key in updated:
                updated[key] = updated[key].reobserved(data_point.last_retrieved)
                outcomes.append(MergeResult(MergeKind.UPDATED, updated[key]))
                continue
            existing = self._entries.get(key)
            if existing is None:
                added[key] = data_point
                outcomes.append(MergeResult(MergeKind.ADDED, data_point))
                continue
            merged = existing.data_point.reobserved(data_point.last_retrieved)
            outcomes.append(MergeResult(MergeKind.UPDATED, merged))
            if data_point.last_retrieved > existing.data_point.last_retrieved:
                updated[key] = merged
        return _Resolution(outcomes=tuple(outcomes), added=added, updated=updated)

    def _apply(self, resolution: _Resolution, revision: int) -> None:
        for key, data_point in resolution.added.items():
            self._entries[key] = _MirrorEntry(data_point, added_rev=revision, updated_rev=revision)
        for key, data_point in resolution.updated.items():
            entry = self._entries[key]
            entry.data_point = data_point
            entry.updated_rev = revision
        self._revision = revision

    def _local_change_set(self, since: int) -> ChangeSet:
        added = tuple(entry.data_point for entry in self._entries.values() if entry.added_rev > since)
        updated = tuple(
            entry.data_point for entry in self._entries.values() if entry.added_rev <= since < entry.updated_rev
        )
        return ChangeSet(added=added, updated=updated)

    def _baselined_change_set(self, since: int) -> ChangeSet:
        baseline = self._baselines.get(since)
        if baseline is None:
            raise StateMirrorError(
                f'no primed change baseline for pre-rehydration revision {since}'
                f' (mirror rehydrated at revision {self._rehydrated_at})'
            )
        added: list[BaseDataPoint[Any]] = []
        updated: list[BaseDataPoint[Any]] = []
        for key, entry in self._entries.items():
            # A local add postdates rehydration, so it postdates `since` too; a baseline ADDED
            # stays added forever (add-stamps are immutable). Anything else changed iff the
            # baseline saw an update or the local stamps recorded one after rehydration.
            if entry.added_rev > since or baseline.get(key) is MergeKind.ADDED:
                added.append(entry.data_point)
            elif baseline.get(key) is MergeKind.UPDATED or entry.updated_rev > since:
                updated.append(entry.data_point)
        return ChangeSet(added=tuple(added), updated=tuple(updated))

    def _require_hydrated(self) -> None:
        if not self._hydrated:
            raise StateMirrorError('the session mirror was used before rehydrate()')

    def _guard_read(self, session_id: SessionId) -> None:
        self._require_hydrated()
        if session_id != self._session_id:
            raise StateMirrorError(f'mirror for session {self._session_id!r} asked about session {session_id!r}')
