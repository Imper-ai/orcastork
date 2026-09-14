"""``Runtime`` — the injected bundle the orchestrator depends on: the clock, the catalog, the event sink."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .capabilities import CapabilityCatalog, InMemoryCapabilityCatalog
from .clock import Clock, SystemClock
from .events import NullSessionEventSink, SessionEventSink


class Runtime(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    clock: Clock = Field(default_factory=SystemClock)
    catalog: CapabilityCatalog = Field(default_factory=InMemoryCapabilityCatalog)
    events: SessionEventSink = Field(default_factory=NullSessionEventSink)


def build_runtime(
    clock: Clock | None = None,
    *,
    catalog: CapabilityCatalog | None = None,
    events: SessionEventSink | None = None,
) -> Runtime:
    """Build a runtime; unspecified parts fall back to the system clock, an empty catalog and no event sink."""
    return Runtime(
        clock=clock or SystemClock(),
        catalog=catalog or InMemoryCapabilityCatalog(),
        events=events or NullSessionEventSink(),
    )
