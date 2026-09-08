"""Capability availability (a fixpoint) and lazy activation.

A capability is **available** iff (1) it is namespace-permitted (catalog), (2) all its
``depends_on`` DataPoints are present (subtype-aware), and (3) all its ``requires``
capabilities are available. Condition (3) makes availability a fixpoint (layered
capabilities need their base first), computed base-before-layer until stable. Because
``depends_on`` is presence-based and DataPoints are only ever added within a session,
availability is **monotonic** (the only way to lose a capability is a namespace-config change).

A newly-available capability is **activated lazily** — constructed from catalog-supplied
credentials the first time it becomes available — so never-needed capabilities are never
constructed. A failed activation is re-attempted on a later ``refresh`` once a jittered
cool-off (the aggregation retry schedule, seeded on the capability id) has elapsed; after
``max_attempts`` total failures it is terminal for the session and never retried again.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from loguru import logger

from ..aggregation.retry import RetryPolicy, backoff_delays, seed_for
from ..clock import Clock
from ..datapoints import BaseDataPoint, DataPointView
from ..ids import CapabilityId, NamespaceId
from ..operators.context import CapabilityView
from ..ports.capability_catalog import CapabilityCatalog
from ..ports.rate_limiter import RateLimiter
from ..telemetry import Telemetry
from .base import Capability, CapabilityContext, InvocationAuditor

# Notified exactly once when a capability's activation retries are exhausted for the session.
OnTerminalFailure = Callable[[CapabilityId, Exception], Awaitable[None]]


@dataclass(frozen=True)
class _ActivationFailure:
    attempts: int  # total failed activation attempts so far
    next_attempt_at: float | None  # monotonic time the next attempt becomes eligible; None → terminal


def _deps_present(
    depends_on: Iterable[type[BaseDataPoint[Any]]], present_types: frozenset[type[BaseDataPoint[Any]]]
) -> bool:
    return all(any(issubclass(present, required) for present in present_types) for required in depends_on)


def _requires_available(
    requires: Iterable[type[Capability]],
    available: set[CapabilityId],
    registered: Mapping[CapabilityId, type[Capability]],
) -> bool:
    return all(
        any(issubclass(registered[available_id], required) for available_id in available) for required in requires
    )


def compute_available(
    *,
    registered: Mapping[CapabilityId, type[Capability]],
    permitted: frozenset[CapabilityId],
    present_types: frozenset[type[BaseDataPoint[Any]]],
) -> frozenset[CapabilityId]:
    """The set of currently-available capabilities (least fixpoint, base-before-layer)."""
    available: set[CapabilityId] = set()
    changed = True
    while changed:
        changed = False
        for capability_id, capability in registered.items():
            if capability_id in available or capability_id not in permitted:
                continue
            if not _deps_present(capability.depends_on, present_types):
                continue
            if not _requires_available(capability.requires, available, registered):
                continue
            available.add(capability_id)
            changed = True
    return frozenset(available)


class CapabilityActivator:
    """Tracks lazy activation across a session and produces the current ``CapabilityView``.

    Activated instances are cached, so availability is monotonic and a capability is built
    at most once. ``refresh`` recomputes availability from the present DataPoints + the
    current namespace catalog and activates any newly-available capability from its credentials.
    A failed activation cools off (jittered backoff against the injected clock — eligibility
    is checked, never awaited) and is re-attempted by a later ``refresh``; after
    ``activation_retry.max_attempts`` total failures it is terminal for the session.
    """

    def __init__(
        self,
        registered: Mapping[CapabilityId, type[Capability]],
        catalog: CapabilityCatalog,
        namespace_id: NamespaceId,
        clock: Clock,
        *,
        on_invoke: InvocationAuditor | None = None,
        activation_retry: RetryPolicy | None = None,
        on_terminal_failure: OnTerminalFailure | None = None,
        rate_limiter: RateLimiter | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        self._registered = dict(registered)
        self._catalog = catalog
        self._namespace_id = namespace_id
        self._clock = clock
        self._on_invoke = on_invoke  # threaded into every CapabilityView so invocations are audited
        self._activation_retry = activation_retry or RetryPolicy()
        self._on_terminal_failure = on_terminal_failure
        self._rate_limiter = rate_limiter  # bound per activation so every action paces fleet-wide
        self._telemetry = telemetry or Telemetry()
        self._activated: dict[CapabilityId, Capability] = {}
        self._failures: dict[CapabilityId, _ActivationFailure] = {}

    def activated_ids(self) -> frozenset[CapabilityId]:
        return frozenset(self._activated)

    def _may_attempt(self, capability_id: CapabilityId, now: float) -> bool:
        failure = self._failures.get(capability_id)
        if failure is None:
            return True
        return failure.next_attempt_at is not None and now >= failure.next_attempt_at

    async def _record_failure(self, capability_id: CapabilityId, error: Exception) -> None:
        attempts = self._failures[capability_id].attempts + 1 if capability_id in self._failures else 1
        if attempts >= self._activation_retry.max_attempts:
            self._failures[capability_id] = _ActivationFailure(attempts=attempts, next_attempt_at=None)
            self._telemetry.capability_activations_total.add(
                1, {'capability_id': capability_id, 'outcome': 'terminal'}
            )
            logger.opt(exception=True).warning(
                'Capability activation failed terminally; the capability is unavailable for this session',
                capability_id=capability_id,
                attempts=attempts,
            )
            if self._on_terminal_failure is not None:
                await self._on_terminal_failure(capability_id, error)
            return
        # The cool-off schedule is the shared jittered backoff, seeded on the capability id so it is
        # deterministic (and reproducible across processes) without coordinating any extra state.
        cool_off = backoff_delays(self._activation_retry, seed=seed_for(capability_id))[attempts - 1]
        self._failures[capability_id] = _ActivationFailure(
            attempts=attempts, next_attempt_at=self._clock.monotonic() + cool_off
        )
        self._telemetry.capability_activations_total.add(1, {'capability_id': capability_id, 'outcome': 'failed'})
        logger.opt(exception=True).warning(
            'Capability activation failed; it will be re-attempted after a cool-off',
            capability_id=capability_id,
            attempts=attempts,
            cool_off_seconds=cool_off,
        )

    async def refresh(self, store_view: DataPointView) -> CapabilityView:
        present_types = frozenset(type(data_point) for data_point in store_view.all())
        permitted = await self._catalog.permitted_capabilities(self._namespace_id)
        preference = await self._catalog.preferred_order(self._namespace_id)
        available_ids = compute_available(
            registered=self._registered, permitted=permitted, present_types=present_types
        )
        # Activate newly-available capabilities base-before-layer: a capability is activated
        # only once all the capabilities it requires are already activated (the fixpoint
        # guarantees this terminates). Previously-failed capabilities re-enter once their
        # cool-off has elapsed; terminally-failed ones never do.
        now = self._clock.monotonic()
        pending = [
            capability_id
            for capability_id in available_ids
            if capability_id not in self._activated and self._may_attempt(capability_id, now)
        ]
        while pending:
            ready = [
                capability_id
                for capability_id in pending
                if _requires_available(
                    self._registered[capability_id].requires, set(self._activated), self._registered
                )
            ]
            if not ready:
                break
            for capability_id in ready:
                pending.remove(capability_id)
                capability = self._registered[capability_id]()
                capability.bind_auditor(self._on_invoke)  # every action call on this instance now audits itself
                # The bucket is per (namespace, capability): one namespace's fleet of sessions shares the budget
                # for a provider, while other namespaces and other providers are unaffected.
                capability.bind_rate_limit(self._rate_limiter, f'{self._namespace_id}:{capability_id}')
                capability.bind_telemetry(self._telemetry)  # action spans ride the audited seam
                # Defensive copy so an adapter can't mutate catalog-owned credentials.
                credentials = dict(await self._catalog.credentials(self._namespace_id, capability_id) or {})
                try:
                    # A raised activation error propagates through the span context manager
                    # (which records it); the except below owns the cool-off bookkeeping.
                    with self._telemetry.tracer.start_as_current_span(
                        f'capability.activate {capability_id}', attributes={'capability_id': capability_id}
                    ):
                        await capability.activate(CapabilityContext(credentials=credentials, store=store_view))
                except Exception as error:
                    # A failed activation is isolated like an operator failure: the capability stays
                    # unavailable and the session continues; a later refresh re-attempts it after the
                    # cool-off, until the retry budget is exhausted.
                    await self._record_failure(capability_id, error)
                    continue
                self._failures.pop(capability_id, None)
                self._activated[capability_id] = capability
                self._telemetry.capability_activations_total.add(
                    1, {'capability_id': capability_id, 'outcome': 'succeeded'}
                )
        # Only currently-available activated capabilities are exposed for new resolution
        # (revocation blocks new acquisitions; in-flight users are not cancelled).
        available_instances = {
            capability_id: self._activated[capability_id]
            for capability_id in available_ids
            if capability_id in self._activated
        }
        return CapabilityView(available_instances, preference=preference)
