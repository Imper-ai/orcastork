"""``Runtime`` — the injected bundle the orchestrator depends on: the clock and the catalog."""

from __future__ import annotations

from dataclasses import dataclass, field

from .capabilities import CapabilityCatalog, InMemoryCapabilityCatalog
from .clock import Clock, SystemClock


@dataclass(frozen=True)
class Runtime:
    clock: Clock = field(default_factory=SystemClock)
    catalog: CapabilityCatalog = field(default_factory=InMemoryCapabilityCatalog)


def build_runtime(clock: Clock | None = None, *, catalog: CapabilityCatalog | None = None) -> Runtime:
    """Build a runtime; unspecified parts fall back to the system clock and an empty in-memory catalog."""
    return Runtime(clock=clock or SystemClock(), catalog=catalog or InMemoryCapabilityCatalog())
