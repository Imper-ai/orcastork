"""Runtime circuit-breaker — bounds a permitted cycle per session.

The orchestrator records each run of a cyclic operator; once an operator reaches its
``max_cycles`` cap it is *tripped* and the orchestrator stops re-running it, halting the
loop. The counter is per-session per-operator — a fresh breaker per session resets it.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..ids import OperatorId


class CircuitBreaker:
    def __init__(self, caps: Mapping[OperatorId, int]) -> None:
        self._caps = dict(caps)
        self._counts: dict[OperatorId, int] = {}

    def record_run(self, operator_id: OperatorId) -> None:
        self._counts[operator_id] = self._counts.get(operator_id, 0) + 1

    def is_tripped(self, operator_id: OperatorId) -> bool:
        cap = self._caps.get(operator_id)
        return cap is not None and self._counts.get(operator_id, 0) >= cap

    def runs(self, operator_id: OperatorId) -> int:
        return self._counts.get(operator_id, 0)
