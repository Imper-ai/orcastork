"""In-memory ``Inbox`` — ordered, at-least-once, with explicit claim/ack/reclaim.

``consume`` claims not-yet-delivered entries (in append order); ``reclaim`` re-presents
claimed-but-unacked entries (crash recovery), bumping their delivery count so poison
messages surface; ``ack`` (epoch-guarded) removes a claimed entry, and is a safe no-op on
an unclaimed one (Redis ``XACK`` semantics). Append sets a per-session
arrival event that ``wait_for_entry`` blocks on — a push wakeup, no polling — but
delivery never depends on it.

Messages are held in their serialized wire form (what a stateless front door would put on a
real transport), so delivery exercises the same tolerant-reader path as the Redis adapter:
a payload whose bytes do not decode as JSON is surfaced as a ``PoisonInboxEntry`` instead of
raising, and ``quarantine`` removes it from delivery while keeping it inspectable. Semantic
deserialization failures (an unknown DataPoint type, a validation error) propagate instead.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import orjson

from ...datapoints import BaseDataPoint, parse_data_point
from ...exceptions import StaleEpochError
from ...ids import Epoch, SessionId
from ...ports.change_set import DeliveredInboxEntry, InboxEntry, PoisonInboxEntry, QuarantinedEntry


@dataclass
class _Message:
    entry_id: str
    payload: str  # the serialized wire form; parsed back on every delivery (tolerant reader)
    delivery_count: int = 0
    claimed: bool = False


@dataclass
class _InboxState:
    messages: list[_Message] = field(default_factory=list)
    quarantined: list[QuarantinedEntry] = field(default_factory=list)
    max_epoch: int = 0
    arrival: asyncio.Event = field(default_factory=asyncio.Event)


class InMemoryInbox:
    def __init__(self) -> None:
        self._sessions: dict[SessionId, _InboxState] = {}

    def _state(self, session_id: SessionId) -> _InboxState:
        return self._sessions.setdefault(session_id, _InboxState())

    def _guard_epoch(self, session_id: SessionId, epoch: Epoch) -> _InboxState:
        state = self._state(session_id)
        if epoch < state.max_epoch:
            raise StaleEpochError(f'epoch {epoch} is stale (current {state.max_epoch})')
        state.max_epoch = epoch
        return state

    async def append(self, session_id: SessionId, data_point: BaseDataPoint[Any]) -> str:
        # The wire form is str (what a real transport would carry) — decode the orjson bytes here.
        return await self.append_serialized(session_id, orjson.dumps(data_point.model_dump(mode='json')).decode())

    async def append_serialized(self, session_id: SessionId, payload: str) -> str:
        """Append a raw wire payload — the seam a foreign producer writes through.

        This is what a direct ``XADD`` is for the Redis adapter: the producer may run newer
        code (or be plain wrong), so the payload is not guaranteed to parse on this side.
        """
        state = self._state(session_id)
        entry_id = str(uuid4())
        state.messages.append(_Message(entry_id=entry_id, payload=payload))
        state.arrival.set()
        return entry_id

    @staticmethod
    def _to_entry(message: _Message) -> DeliveredInboxEntry:
        # Poison is reserved for a bad wire payload: only bytes that fail to decode as JSON.
        # Semantic deserialization failures from parse_data_point (an unknown type, a validation
        # error, a registry bug) propagate and fail fast — a quiet quarantine would hide a parser
        # regression, and redelivery on resume lets a newer deployment parse what an older one
        # could not.
        try:
            decoded = orjson.loads(message.payload)
        except orjson.JSONDecodeError as error:
            return PoisonInboxEntry(
                entry_id=message.entry_id,
                error=f'{type(error).__name__}: {error}',
                delivery_count=message.delivery_count,
                raw_payload=message.payload,
            )
        return InboxEntry(message.entry_id, parse_data_point(decoded), message.delivery_count)

    async def consume(
        self, session_id: SessionId, *, max_entries: int | None = None
    ) -> tuple[DeliveredInboxEntry, ...]:
        if max_entries is not None and max_entries <= 0:
            return ()
        claimed: list[DeliveredInboxEntry] = []
        for message in self._state(session_id).messages:
            if max_entries is not None and len(claimed) >= max_entries:
                break  # check the cap before claiming, so max_entries=0 claims nothing
            if message.claimed:
                continue
            message.claimed = True
            message.delivery_count += 1
            claimed.append(self._to_entry(message))
        return tuple(claimed)

    async def reclaim(self, session_id: SessionId) -> tuple[DeliveredInboxEntry, ...]:
        reclaimed: list[DeliveredInboxEntry] = []
        for message in self._state(session_id).messages:
            if not message.claimed:
                continue
            message.delivery_count += 1
            reclaimed.append(self._to_entry(message))
        return tuple(reclaimed)

    async def ack(self, session_id: SessionId, entry_id: str, *, epoch: Epoch) -> None:
        state = self._guard_epoch(session_id, epoch)
        for index, message in enumerate(state.messages):
            if message.entry_id == entry_id:
                # Redis XACK only removes claimed (pending) entries; mirroring that, an ack that
                # races ahead of delivery is a safe no-op and the entry stays deliverable.
                if message.claimed:
                    del state.messages[index]  # ack removes the entry — acked messages must not accumulate
                return
        # Unknown / already-acked entry is a safe no-op.

    async def quarantine(self, session_id: SessionId, entry_id: str, *, reason: str, epoch: Epoch) -> None:
        state = self._guard_epoch(session_id, epoch)
        for index, message in enumerate(state.messages):
            if message.entry_id == entry_id:
                # Disposal mirrors the Redis script: the record is pushed iff the XACK-equivalent
                # actually removed a claimed entry — a never-delivered entry is neither removed
                # nor recorded, so a premature quarantine cannot destroy or double-record it.
                if not message.claimed:
                    return
                del state.messages[index]  # out of delivery for good — quarantine is terminal, unlike un-acked
                state.quarantined.append(
                    QuarantinedEntry(
                        entry_id=entry_id,
                        reason=reason,
                        delivery_count=message.delivery_count,
                        raw_payload=message.payload,
                    )
                )
                return
        # Unknown / already-disposed entry is a safe no-op (mirrors ack) — never double-records.

    async def quarantined(self, session_id: SessionId) -> tuple[QuarantinedEntry, ...]:
        return tuple(self._state(session_id).quarantined)

    async def pending_count(self, session_id: SessionId) -> int:
        return len(self._state(session_id).messages)

    async def wait_for_entry(self, session_id: SessionId) -> None:
        state = self._state(session_id)
        if state.messages:
            return  # entries already pending — never wait on data that is already here
        # The pending check and the clear happen with no await between them (single event loop),
        # so an append can never slip into that gap unobserved: it either landed above or it will
        # set the event we are about to wait on.
        state.arrival.clear()
        await state.arrival.wait()
