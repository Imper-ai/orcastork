"""Fixtures and doubles for the orcastork_lite suite.

There are no registries to isolate: the factories build plain subclasses and hand them straight
to the orchestrator, so every test owns exactly the classes it creates.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from datetime import datetime, timedelta
from typing import Any

import pytest

from orcastork_lite import (
    Capability,
    CapabilityCatalog,
    CapabilityContext,
    DataPoint,
    DataPointEmission,
    NamespaceId,
    Operator,
    OperatorContext,
    OperatorId,
    OperatorPolicy,
    Orchestrator,
    RerunOn,
    RetryPolicy,
    SessionId,
    SessionResult,
    build_runtime,
)
from orcastork_lite.ids import CapabilityId

from ..doubles.clock import FakeClock

NAMESPACE = NamespaceId('ns')
SESSION = SessionId('s')
SEED = OperatorId('seed')


# --- a small DataPoint zoo -----------------------------------------------------------------------
class Ip(DataPoint[str]): ...


class Risk(DataPoint[float]): ...


class Flag(DataPoint[bool]): ...


class Email(DataPoint[str]):
    """An intermediate: operators may depend on it, leaves below it satisfy the dependency."""


class WorkEmail(Email): ...


class PersonalEmail(Email): ...


def dp[T](leaf: type[DataPoint[T]], value: T, at: datetime, *, by: OperatorId = SEED) -> DataPoint[T]:
    return leaf(value=value, retrieved_by=by, first_retrieved=at, last_retrieved=at)


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


# --- operator / capability factories ------------------------------------------------------------
def make_operator(
    operator_id: str,
    *,
    depends_on: Iterable[type[DataPoint[Any]]] = (),
    uses: Iterable[type[DataPoint[Any]]] = (),
    produces: Iterable[type[DataPoint[Any]]] = (),
    requires: Iterable[type[Capability]] = (),
    consumes: Iterable[type[DataPoint[Any]]] = (),
    rerun_on_new_data: bool = False,
    rerun_on: RerunOn = RerunOn.ADDED_OR_UPDATED,
    debounce: timedelta | None = None,
    max_cycles: int | None = None,
    timeout: timedelta | None = None,
    retry: RetryPolicy | None = None,
    emits: Iterable[DataPointEmission] = (),
    emit_factory: Callable[[OperatorContext], Iterable[DataPointEmission]] | None = None,
    sleep_after: float | None = None,
    raise_error: Exception | None = None,
    fail_first: int = 0,
    seen: list[OperatorContext] | None = None,
) -> type[Operator]:
    """A stub operator: emits ``emits`` (or ``emit_factory(ctx)``), optionally sleeps, optionally raises.

    ``fail_first`` raises on that many runs before succeeding (retry tests); ``seen`` collects every
    context the stub was invoked with, so a test can inspect deltas.
    """
    _policy = OperatorPolicy(
        rerun_on_new_data=rerun_on_new_data,
        rerun_on=rerun_on,
        debounce=debounce,
        max_cycles=max_cycles,
        timeout=timeout,
        retry=retry,
    )
    _emits, _factory = tuple(emits), emit_factory
    runs = {'count': 0}

    class _Stub(Operator):
        policy = _policy

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            runs['count'] += 1
            if seen is not None:
                seen.append(ctx)
            for emission in _emits if _factory is None else _factory(ctx):
                yield emission
            if sleep_after is not None:
                await asyncio.sleep(sleep_after)
            if raise_error is not None or runs['count'] <= fail_first:
                raise raise_error or RuntimeError(f'{operator_id} failed on run {runs["count"]}')

    _Stub.operator_id = OperatorId(operator_id)
    _Stub.depends_on = frozenset(depends_on)
    _Stub.uses = frozenset(uses)
    _Stub.produces = frozenset(produces)
    _Stub.requires = frozenset(requires)
    _Stub.consumes = frozenset(consumes)
    _Stub.__name__ = _Stub.__qualname__ = ''.join(part.title() for part in operator_id.split('_'))
    return _Stub


class StubCapability(Capability):
    """The typed shape every stub capability shares: remembers its credentials, exposes one action."""

    creds: dict[str, Any] = {}

    async def token(self) -> str:
        return str(self.creds.get('token', ''))


def make_capability(
    capability_id: str,
    *,
    depends_on: Iterable[type[DataPoint[Any]]] = (),
    requires: Iterable[type[Capability]] = (),
    base: type[StubCapability] = StubCapability,
    on_activate: Callable[[CapabilityContext], Awaitable[None]] | None = None,
    record_order: list[str] | None = None,
) -> type[StubCapability]:
    """A stub capability (optionally below ``base``, for provider families) that records activation order."""

    class _Stub(base):  # type: ignore[valid-type,misc]
        async def activate(self, ctx: CapabilityContext) -> None:
            if record_order is not None:
                record_order.append(capability_id)
            if on_activate is not None:
                await on_activate(ctx)
            self.creds = dict(ctx.credentials)

    _Stub.capability_id = CapabilityId(capability_id)
    _Stub.depends_on = frozenset(depends_on)
    _Stub.requires = frozenset(requires)
    _Stub.__name__ = _Stub.__qualname__ = ''.join(part.title() for part in capability_id.split('_'))
    return _Stub


async def run_session(
    fake_clock: FakeClock,
    operators: Sequence[type[Operator]],
    *,
    capabilities: Sequence[type[Capability]] = (),
    seed: Sequence[DataPoint[Any]] = (),
    catalog: CapabilityCatalog | None = None,
    namespace_id: NamespaceId = NAMESPACE,
    operation_timeout: float = 30.0,
) -> SessionResult:
    return await Orchestrator(
        session_id=SESSION,
        namespace_id=namespace_id,
        runtime=build_runtime(fake_clock, catalog=catalog),
        operators=operators,
        capabilities=capabilities,
        seed=seed,
        operation_timeout=operation_timeout,
    ).run()
