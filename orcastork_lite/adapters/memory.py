"""In-memory ``SessionEventSink`` — every published event, in order, for tests and local runs."""

from __future__ import annotations

from ..events import SessionEvent
from ..ids import SessionId


class InMemorySessionEventSink:
    def __init__(self) -> None:
        self.events: list[SessionEvent] = []

    async def publish(self, event: SessionEvent) -> None:
        self.events.append(event)

    def for_session(self, session_id: SessionId) -> list[SessionEvent]:
        return [event for event in self.events if event.session_id == session_id]
