"""In-memory ``DataPointArchive`` — durable buffer + batched keyed-upsert, epoch-fenced.

``archive`` appends to a durable buffer (rejecting a stale-epoch writer's entry). ``flush``
folds the buffer into a committed map keyed by ``(type, value_hash)``: first observation
inserts, re-observation bumps ``last_retrieved`` and keeps ``first_retrieved`` — never a
duplicate, so redelivery/replay is idempotent. PII values are sealed via the injected
cipher (encrypted at rest in the buffer and the committed map) and unsealed on read, which
folds the buffer over the committed map so a session in flight is readable. An
optional clock-driven retention TTL expires committed entries lazily on read.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta

from ...archive import ArchivedDataPoint, NullCipher, ValueCipher
from ...archive.sealing import seal, unseal, value_hash
from ...clock import Clock
from ...exceptions import StaleEpochError
from ...ids import SessionId


@dataclass
class _ArchiveState:
    buffer: list[ArchivedDataPoint] = field(default_factory=list)
    committed: dict[tuple[str, str], ArchivedDataPoint] = field(default_factory=dict)
    max_epoch: int = 0


def _fold_in(folded: dict[tuple[str, str], ArchivedDataPoint], sealed: ArchivedDataPoint) -> None:
    """Apply one sealed observation to a keyed-upsert map: first inserts, re-observation advances.

    The single implementation of the fold, so ``flush`` (which folds into the committed map) and
    ``read`` (which folds the buffer on top of a copy of it) cannot disagree about what a flush
    would have produced.
    """
    key = (sealed.type, sealed.value_hash)
    existing = folded.get(key)
    if existing is None:
        folded[key] = sealed
        return
    # Keyed-upsert: keep the original first_retrieved, advance last_retrieved and epoch. Advancing
    # the epoch is what makes the stored row say which epoch last SAW this datapoint, rather than
    # which epoch first recorded it — the Mongo adapter's `$max` decides the same way, and a row whose
    # epoch is frozen at its first sighting reads as older than the session that actually produced it.
    folded[key] = existing.model_copy(
        update={
            'last_retrieved': max(existing.last_retrieved, sealed.last_retrieved),
            'epoch': max(existing.epoch, sealed.epoch),
        }
    )


class InMemoryDataPointArchive:
    def __init__(
        self,
        *,
        cipher: ValueCipher | None = None,
        clock: Clock | None = None,
        retention: timedelta | None = None,
    ) -> None:
        self._cipher = cipher or NullCipher()
        self._clock = clock
        self._retention = retention
        self._sessions: dict[SessionId, _ArchiveState] = {}

    def _state(self, session_id: SessionId) -> _ArchiveState:
        return self._sessions.setdefault(session_id, _ArchiveState())

    async def archive(self, entry: ArchivedDataPoint) -> None:
        state = self._state(entry.session_id)
        if entry.epoch < state.max_epoch:
            raise StaleEpochError(f'epoch {entry.epoch} is stale (current {state.max_epoch})')
        state.max_epoch = entry.epoch
        # Derive the key from the plaintext value, then seal the value — order matters.
        keyed = seal(entry, self._cipher).model_copy(update={'value_hash': value_hash(entry, self._cipher)})
        state.buffer.append(keyed)

    async def archive_many(self, entries: Sequence[ArchivedDataPoint]) -> None:
        # In memory a batch costs the same as N archives, and a (contract-homogeneous) stale
        # batch is rejected by its first entry before anything lands — atomic either way.
        for entry in entries:
            await self.archive(entry)

    async def flush(self, session_id: SessionId) -> int:
        state = self._state(session_id)
        flushed = len(state.buffer)
        for sealed in state.buffer:
            _fold_in(state.committed, sealed)
        state.buffer.clear()
        return flushed

    async def read(self, session_id: SessionId) -> tuple[ArchivedDataPoint, ...]:
        """Committed entries folded together with any still-buffered ones.

        Folded rather than concatenated: the buffer keeps per-observation granularity, so appending
        it raw would surface duplicates the keyed-upsert collapses. Folding on a copy leaves the
        buffer intact — a read is not a flush — while giving the same answer a flush-then-read would.
        """
        state = self._state(session_id)
        folded = dict(state.committed)
        for sealed in state.buffer:
            _fold_in(folded, sealed)
        return tuple(unseal(entry, self._cipher) for entry in folded.values() if self._live(entry))

    async def buffered_count(self, session_id: SessionId) -> int:
        return len(self._state(session_id).buffer)

    def _live(self, entry: ArchivedDataPoint) -> bool:
        if self._retention is None or self._clock is None:
            return True
        return self._clock.now() - entry.last_retrieved <= self._retention
