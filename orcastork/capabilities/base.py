"""``Capability`` — an injected provider of typed actions (vs. inert DataPoints).

Like operators, a capability declares the DataPoints it needs (``depends_on``) and the
capabilities it builds on (``requires``); the orchestrator decides availability and activates
it lazily. An operator gets the activated instance from ``ctx.capabilities`` and calls
its **action methods directly**, so call sites keep full static typing (real parameters, real
return type — no stringly-typed dispatch).

Auditing is intrinsic, not a call-site concern: at subclass registration every **public async
method** (other than ``activate``) is wrapped so each call is recorded through the capability's
injected auditor *before* the body runs. An author cannot forget to audit an action and an
operator cannot bypass it; internal helpers are simply named with a leading underscore. A public
method the wrapper cannot cover (sync, or an async generator) is rejected at class definition
with ``InvalidCapabilityError``, so no action ever escapes the audited seam. The same seam also
paces each action through the injected fleet ``RateLimiter`` (when one is bound) before the
invocation is recorded, and opens a telemetry span around the whole call — so every external
action is traced without the author instrumenting anything. Concrete capabilities self-register
by ``capability_id``; a duplicate id raises.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import wraps
from typing import Any, ClassVar

from ..datapoints import BaseDataPoint, DataPointView
from ..exceptions import DuplicateRegistrationError, InvalidCapabilityError
from ..ids import CapabilityId
from ..ports.rate_limiter import RateLimiter
from ..telemetry import Telemetry

# Records one capability invocation (capability id, action name, bound arguments) at the audited seam.
InvocationAuditor = Callable[[CapabilityId, str, Mapping[str, Any]], Awaitable[None]]

_AUDITED_MARKER = '__capability_audited__'  # set on a wrapper so an inherited action is never double-wrapped


@dataclass(frozen=True)
class CapabilityContext:
    """What a capability sees when it activates: its credentials + the current DataPoints."""

    credentials: Mapping[str, Any]
    store: DataPointView


def _audited(method: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Wrap an action so the invocation is recorded (capability id, action, bound args) before it runs.

    The arguments are bound by name (``self`` excluded) so the audit trail names each parameter; the
    auditor — not this wrapper — decides what to redact. The wrapper is installed by
    :meth:`Capability.__init_subclass__` via ``setattr``, which the type checker does not see, so the
    method keeps its real typed signature at every call site.
    """
    signature = inspect.signature(method)
    action_name = method.__name__

    @wraps(method)
    async def wrapper(self: Capability, *args: Any, **kwargs: Any) -> Any:
        bound = signature.bind(self, *args, **kwargs)
        bound.apply_defaults()
        parameters = {name: value for name, value in bound.arguments.items() if name != 'self'}
        # The span covers the caller-observed call — pacing, the audit record and the action
        # body — so time queued behind the fleet limiter is visible in the trace too. An
        # action that raises propagates through the span, which records it and marks it failed.
        with self._telemetry.tracer.start_as_current_span(
            f'capability.action {self.capability_id}.{action_name}',
            attributes={'capability_id': self.capability_id, 'action': action_name},
        ):
            # Pace before recording: the audit trail must hold only actions that actually proceeded, not
            # ones still queued behind the fleet's rate limit.
            await self._acquire_rate_limit()
            await self._record_invocation(action_name, parameters)
            return await method(self, *args, **kwargs)

    setattr(wrapper, _AUDITED_MARKER, True)
    return wrapper


def _is_auditable_action(name: str, attribute: object) -> bool:
    # A capability's public async methods are its action API; activate is lifecycle, not an action, and
    # an already-wrapped (inherited) method carries the marker. Underscore-prefixed methods are internal.
    return (
        not name.startswith('_')
        and name != 'activate'
        and inspect.iscoroutinefunction(attribute)
        and not getattr(attribute, '__isabstractmethod__', False)
        and not getattr(attribute, _AUDITED_MARKER, False)
    )


def _is_unaudited_public_method(name: str, attribute: object) -> bool:
    # Only coroutine functions get the audit wrapper, so any other plain public function (a sync method,
    # an async generator) would be an action that silently bypasses the audited seam — rejected at
    # definition rather than discovered in production. Names the Capability base itself defines (framework
    # lifecycle such as ``bind_auditor``) stay overridable, and properties/classmethods/staticmethods are
    # not plain functions, so they never trip this check.
    return (
        not name.startswith('_')
        and name not in vars(Capability)
        and inspect.isfunction(attribute)
        and not inspect.iscoroutinefunction(attribute)
    )


class Capability(ABC):
    capability_id: ClassVar[CapabilityId]
    depends_on: ClassVar[frozenset[type[BaseDataPoint[Any]]]] = frozenset()
    requires: ClassVar[frozenset[type['Capability']]] = frozenset()

    _registry: ClassVar[dict[CapabilityId, type['Capability']]] = {}
    _auditor: InvocationAuditor | None = None  # injected on activation; None → invocations are not recorded
    _rate_limiter: RateLimiter | None = None  # injected on activation; None → actions are not paced
    _rate_limit_key: str = ''
    _telemetry: Telemetry = Telemetry()  # injected on activation; the default rides the global providers

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Wrap each action method declared on THIS class so every call audits itself. Inherited actions
        # are already wrapped on their defining class; an override (in this class's own dict) is re-wrapped.
        for name, attribute in list(vars(cls).items()):
            if _is_auditable_action(name, attribute):
                setattr(cls, name, _audited(attribute))
            elif _is_unaudited_public_method(name, attribute):
                raise InvalidCapabilityError(
                    f'{cls.__name__}.{name} is a public non-async method: it would bypass the audited action '
                    f'seam. Make it async, or prefix it with an underscore if it is an internal helper.'
                )
        # Skip abstract bases/intermediates (no id) and not-yet-concrete classes.
        if not hasattr(cls, 'capability_id') or getattr(cls, '__abstractmethods__', None):
            return
        existing = Capability._registry.get(cls.capability_id)
        if existing is not None and existing is not cls:
            raise DuplicateRegistrationError(
                f'capability_id {cls.capability_id!r} is already registered to {existing.__name__}'
            )
        Capability._registry[cls.capability_id] = cls

    def bind_auditor(self, auditor: InvocationAuditor | None) -> None:
        """Inject the invocation auditor — the orchestrator wires this when the capability activates."""
        self._auditor = auditor

    def bind_rate_limit(self, limiter: RateLimiter | None, key: str) -> None:
        """Inject the fleet rate limiter — the orchestrator wires this when the capability activates."""
        self._rate_limiter = limiter
        self._rate_limit_key = key

    def bind_telemetry(self, telemetry: Telemetry) -> None:
        """Inject the telemetry backend — the orchestrator wires this when the capability activates."""
        self._telemetry = telemetry

    async def _record_invocation(self, action_name: str, parameters: Mapping[str, Any]) -> None:
        if self._auditor is not None:
            await self._auditor(self.capability_id, action_name, parameters)

    async def _acquire_rate_limit(self) -> None:
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire(self._rate_limit_key)

    @abstractmethod
    async def activate(self, ctx: CapabilityContext) -> None:
        """Build the underlying client from catalog-supplied credentials (lazy)."""
        ...
