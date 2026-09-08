"""The per-invocation ``OperatorContext`` and its change ``InvocationDelta``.

Every ``run(ctx)`` receives the current state (``store``), the currently-available
capabilities (``capabilities``), and a per-operator ``delta`` of what changed since *this*
operator last ran — so a rerun does incremental work instead of re-scanning.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any

from ..aggregation.helpers import AggregationHelpers
from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint, DataPointView
from ..exceptions import CapabilityUnavailableError
from ..ids import CapabilityId, Epoch, SessionId
from .effects import EffectGuard, EffectRecovery


@dataclass(frozen=True)
class InvocationDelta:
    added: frozenset[BaseDataPoint[Any]]  # new (type, value) identities
    updated: frozenset[BaseDataPoint[Any]]  # existing identities re-observed (last_retrieved bumped)
    newly_available_caps: frozenset[CapabilityId]  # capabilities that came online since the last run
    is_first_invocation: bool  # first run → `added` is the full current set


class CapabilityView:
    """Read view over the currently-available capabilities (resolve → preferred provider)."""

    def __init__(
        self,
        available: Mapping[CapabilityId, Capability] | None = None,
        preference: tuple[CapabilityId, ...] = (),
    ) -> None:
        self._available: dict[CapabilityId, Capability] = dict(available) if available is not None else {}
        self._preference = preference

    def available_ids(self) -> frozenset[CapabilityId]:
        return frozenset(self._available)

    def available_types(self) -> frozenset[type[Capability]]:
        return frozenset(type(capability) for capability in self._available.values())

    def is_available(self, capability_id: CapabilityId) -> bool:
        return capability_id in self._available

    def resolve[C: Capability](self, capability_type: type[C]) -> C | None:
        """The single preferred available provider of ``capability_type``, or None.

        Providers listed in the namespace's preference order win, in listed order; unlisted providers rank
        after every listed one and fall back to the stable alphabetical tie-break by id.

        Call the returned capability's action methods directly — they keep their real typed signatures,
        and each call audits itself (the framework wires the auditor when the capability activates).
        """
        matches = sorted(
            (cap for cap in self._available.values() if isinstance(cap, capability_type)),
            key=self._preference_rank,
        )
        return matches[0] if matches else None

    def _preference_rank(self, capability: Capability) -> tuple[int, CapabilityId]:
        if capability.capability_id in self._preference:
            return (self._preference.index(capability.capability_id), capability.capability_id)
        return (len(self._preference), capability.capability_id)

    def require[C: Capability](self, capability_type: type[C]) -> C:
        """Like :meth:`resolve`, but raise ``CapabilityUnavailableError`` when no provider is available.

        An operator that declared the capability in ``requires`` is only scheduled once it is
        available, so it can ``require`` the provider and use it without a ``None`` check.
        """
        capability = self.resolve(capability_type)
        if capability is None:
            raise CapabilityUnavailableError(f'no available provider for {capability_type.__name__}')
        return capability


@dataclass(frozen=True)
class OperatorContext:
    session_id: SessionId
    epoch: Epoch
    store: DataPointView
    capabilities: CapabilityView
    delta: InvocationDelta
    effects: EffectGuard  # claim/commit/revert gate for non-idempotent side effects (see operators/effects.py)
    aggregation: AggregationHelpers | None = None  # set only for aggregators (durable-write API)
    is_final: bool = False  # True only during the authoritative finalize pass; False for interim runs

    def latest[T: BaseDataPoint[Any]](self, data_point_type: type[T]) -> T | None:
        """The newest DataPoint of ``data_point_type`` by ``last_retrieved`` (ergonomic single-read)."""
        return self.store.latest(data_point_type)

    def once(
        self, effect_key: str, *, on_unknown: EffectRecovery = EffectRecovery.RERUN
    ) -> AbstractAsyncContextManager[bool]:
        """Guard a non-idempotent side effect — ``async with ctx.once('send-otp') as acquired:``.

        ``acquired`` is ``True`` iff this attempt owns running the effect; a clean exit commits
        the claim durably, a failing exit reverts it so a retry re-runs the effect.
        """
        return self.effects.once(effect_key, on_unknown=on_unknown)
