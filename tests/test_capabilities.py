"""CAP — capability availability fixpoint, lazy activation, layering, revocation, invocation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from orcastork.adapters.memory import InMemoryCapabilityCatalog, InMemoryRateLimiter
from orcastork.aggregation.retry import RetryPolicy
from orcastork.audit import AuditKind
from orcastork.capabilities import Capability, CapabilityActivator, CapabilityContext, compute_available
from orcastork.capabilities.base import InvocationAuditor
from orcastork.datapoints import DataPointEmission, DataPointView
from orcastork.exceptions import (
    CapabilityUnavailableError,
    DuplicateRegistrationError,
    InvalidCapabilityError,
)
from orcastork.ids import CapabilityId, NamespaceId, OperatorId, SessionId
from orcastork.operators import CapabilityView, Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import ChatAnswerDataPoint, EmailDataPoint, WorkEmailDataPoint, personal_email, work_email
from .doubles.logs import capture_logs
from .doubles.otel import TelemetryProbe

NAMESPACE = NamespaceId('namespace-1')
IDP_ID = CapabilityId('idp')


def _registered(*capabilities: type[Capability]) -> dict[CapabilityId, type[Capability]]:
    return {capability.capability_id: capability for capability in capabilities}


class _IdpFamily(Capability):
    """Abstract capability family (no id → not registered) for resolution-cardinality tests."""


class _Entra(_IdpFamily):
    capability_id = CapabilityId('entra')

    async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
        return None


class _Google(_IdpFamily):
    capability_id = CapabilityId('google')

    async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
        return None


def test_cap_01_available_iff_permitted_deps_present_and_requires_available() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    registered, permitted = _registered(cap), frozenset({IDP_ID})
    assert compute_available(registered=registered, permitted=permitted, present_types=frozenset()) == frozenset()
    assert compute_available(
        registered=registered, permitted=permitted, present_types=frozenset({WorkEmailDataPoint})
    ) == frozenset({IDP_ID})
    assert (
        compute_available(registered=registered, permitted=frozenset(), present_types=frozenset({WorkEmailDataPoint}))
        == frozenset()
    )


async def test_cap_02_lazy_activation_only_when_available() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())

    await activator.refresh(DataPointView([]))  # deps missing → not constructed
    assert cap.activations == []  # type: ignore[attr-defined]
    await activator.refresh(DataPointView([work_email()]))
    assert len(cap.activations) == 1  # type: ignore[attr-defined]


async def test_cap_03_layering_activates_after_base_and_deps() -> None:
    browser = make_capability('browser')
    auth = make_capability('auth_browser', depends_on={WorkEmailDataPoint}, requires={browser})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('browser'), CapabilityId('auth_browser')}})
    activator = CapabilityActivator(_registered(browser, auth), catalog, NAMESPACE, FakeClock())

    without_email = await activator.refresh(DataPointView([]))
    assert without_email.is_available(CapabilityId('browser'))
    assert not without_email.is_available(CapabilityId('auth_browser'))

    with_email = await activator.refresh(DataPointView([work_email()]))
    assert with_email.is_available(CapabilityId('auth_browser'))


def test_cap_04_monotonic_availability() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    registered, permitted = _registered(cap), frozenset({IDP_ID})
    empty = compute_available(registered=registered, permitted=permitted, present_types=frozenset())
    with_email = compute_available(
        registered=registered, permitted=permitted, present_types=frozenset({WorkEmailDataPoint})
    )
    assert empty <= with_email


def test_cap_05_resolution_cardinality() -> None:
    # requires → the single preferred available provider
    view = CapabilityView({CapabilityId('entra'): _Entra(), CapabilityId('google'): _Google()})
    provider = view.resolve(_IdpFamily)  # type: ignore[type-abstract]  # resolving an abstract family is the point
    assert provider is not None and provider.capability_id == CapabilityId('entra')
    # depends_on → ALL matching DataPoints
    assert len(DataPointView([work_email(), personal_email()]).of_type(EmailDataPoint)) == 2


async def test_cap_06_revocation_blocks_new_but_keeps_inflight() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())
    assert (await activator.refresh(DataPointView([work_email()]))).is_available(IDP_ID)

    catalog.set_permitted(NAMESPACE, set())  # mid-session revocation
    revoked = await activator.refresh(DataPointView([work_email()]))
    assert not revoked.is_available(IDP_ID)  # new acquisitions blocked
    assert IDP_ID in activator.activated_ids()  # in-flight instance not destroyed


async def test_cap_07_value_change_never_flips_availability() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())
    first = await activator.refresh(DataPointView([work_email('a@e.example')]))
    second = await activator.refresh(DataPointView([work_email('b@e.example')]))
    assert first.available_ids() == second.available_ids()


async def test_cap_08_capability_comes_online_when_dep_arrives() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())
    assert not (await activator.refresh(DataPointView([]))).is_available(IDP_ID)
    assert (await activator.refresh(DataPointView([work_email()]))).is_available(IDP_ID)


async def test_cap_09_activation_uses_catalog_credentials() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {IDP_ID}}, credentials={(NAMESPACE, IDP_ID): {'token': 'secret'}}
    )
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())
    await activator.refresh(DataPointView([work_email()]))
    assert cap.activations == [{'token': 'secret'}]  # type: ignore[attr-defined]


async def test_cap_10_unmet_prereqs_never_activate() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())
    for _ in range(3):
        await activator.refresh(DataPointView([personal_email()]))  # wrong leaf, never satisfies
    assert cap.activations == []  # type: ignore[attr-defined]


async def test_cap_11_layered_chain_terminates_in_dependency_order() -> None:
    order: list[CapabilityId] = []
    a = make_capability('a', record_order=order)
    b = make_capability('b', requires={a}, record_order=order)
    c = make_capability('c', requires={b}, record_order=order)
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {CapabilityId('a'), CapabilityId('b'), CapabilityId('c')}}
    )
    activator = CapabilityActivator(_registered(a, b, c), catalog, NAMESPACE, FakeClock())

    view = await activator.refresh(DataPointView([]))
    assert view.available_ids() == frozenset({CapabilityId('a'), CapabilityId('b'), CapabilityId('c')})
    assert order.index(CapabilityId('a')) < order.index(CapabilityId('b')) < order.index(CapabilityId('c'))


def test_cap_12_preferred_provider_selection_is_deterministic() -> None:
    # Insertion order varies; the preferred provider (stable tie-break by id) does not.
    view = CapabilityView({CapabilityId('google'): _Google(), CapabilityId('entra'): _Entra()})
    first = view.resolve(_IdpFamily)  # type: ignore[type-abstract]  # resolving an abstract family is the point
    second = view.resolve(_IdpFamily)  # type: ignore[type-abstract]
    assert first is not None and second is not None
    assert first.capability_id == second.capability_id == CapabilityId('entra')


async def test_cap_13_action_call_is_typed_and_audits_at_the_seam() -> None:
    performed: list[tuple[str, dict[str, Any]]] = []
    audited: list[tuple[CapabilityId, str, dict[str, Any]]] = []

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def send_challenge(self, user_id: str) -> str:  # a real, typed action method
            performed.append(('send_challenge', {'user_id': user_id}))
            return 'challenge-sent'

    async def record(capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]) -> None:
        audited.append((capability_id, action, dict(parameters)))

    idp = _Idp()
    idp.bind_auditor(record)  # the orchestrator wires this when the capability activates
    result = await idp.send_challenge(user_id='u-1')  # direct, statically-typed call — no string dispatch

    assert result == 'challenge-sent'  # the action's result is returned to the operator
    assert performed == [('send_challenge', {'user_id': 'u-1'})]
    # The seam sees raw parameters by name; redacting is the orchestrator's job at the audit boundary.
    assert audited == [(CapabilityId('idp'), 'send_challenge', {'user_id': 'u-1'})]


async def test_cap_14_require_unavailable_capability_raises() -> None:
    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

    with pytest.raises(CapabilityUnavailableError):
        CapabilityView({}).require(_Idp)  # no provider available


async def test_cap_15_only_public_actions_audit_not_private_helpers() -> None:
    audited: list[str] = []

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def lookup(self, key: str) -> str:  # public → an audited action
            return await self._fetch(key)

        async def _fetch(self, key: str) -> str:  # underscored helper → not an audited action
            return f'value-{key}'

    async def record(capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]) -> None:  # noqa: ARG001
        audited.append(action)

    idp = _Idp()
    idp.bind_auditor(record)
    assert await idp.lookup(key='k') == 'value-k'
    assert audited == ['lookup']  # only the public action audited; the private helper it called did not


async def test_cap_16_activation_failure_is_isolated_and_logged_with_traceback() -> None:
    cap = make_capability('idp', depends_on={WorkEmailDataPoint}, activate_error=ValueError('no creds'))
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, FakeClock())

    with capture_logs() as records:
        view = await activator.refresh(DataPointView([work_email()]))

    assert not view.is_available(IDP_ID)  # the failure is isolated: unavailable, nothing raised
    failure = next(record for record in records if record['extra'].get('capability_id') == IDP_ID)
    assert failure['exception'] is not None  # the log carries the traceback, not just the capability id
    assert 'no creds' in str(failure['exception'].value)
    retry = await activator.refresh(DataPointView([work_email()]))
    assert not retry.is_available(IDP_ID)  # still inside the cool-off → not re-attempted yet
    assert cap.attempts == 1  # type: ignore[attr-defined]


def test_cap_17_namespace_preference_overrides_alphabetical_resolution() -> None:
    # 'entra' sorts first alphabetically, but the namespace prefers google — the preference wins.
    view = CapabilityView(
        {CapabilityId('entra'): _Entra(), CapabilityId('google'): _Google()},
        preference=(CapabilityId('google'),),
    )
    provider = view.resolve(_IdpFamily)  # type: ignore[type-abstract]
    assert provider is not None and provider.capability_id == CapabilityId('google')


def test_cap_18_unlisted_providers_rank_after_listed_ones() -> None:
    # A listed provider beats every unlisted one, even when an absent id leads the preference list
    # and the unlisted provider would win the alphabetical tie-break.
    view = CapabilityView(
        {CapabilityId('entra'): _Entra(), CapabilityId('google'): _Google()},
        preference=(CapabilityId('okta'), CapabilityId('google')),
    )
    provider = view.resolve(_IdpFamily)  # type: ignore[type-abstract]
    assert provider is not None and provider.capability_id == CapabilityId('google')


def test_cap_19_empty_preference_keeps_alphabetical_tie_break() -> None:
    view = CapabilityView({CapabilityId('google'): _Google(), CapabilityId('entra'): _Entra()}, preference=())
    provider = view.resolve(_IdpFamily)  # type: ignore[type-abstract]
    assert provider is not None and provider.capability_id == CapabilityId('entra')


async def test_cap_20_activator_threads_namespace_preference_from_catalog_into_the_view() -> None:
    class _Family(Capability):
        """Abstract family (no id → not registered) the namespace's providers share."""

    class _GoogleIdp(_Family):
        capability_id = CapabilityId('idp.google')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

    class _EntraIdp(_Family):
        capability_id = CapabilityId('idp.entra')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {CapabilityId('idp.google'), CapabilityId('idp.entra')}},
        preferred={NAMESPACE: (CapabilityId('idp.google'),)},
    )
    activator = CapabilityActivator(_registered(_GoogleIdp, _EntraIdp), catalog, NAMESPACE, FakeClock())

    provider = (await activator.refresh(DataPointView([]))).resolve(_Family)  # type: ignore[type-abstract]

    assert provider is not None
    assert provider.capability_id == CapabilityId('idp.google')  # preferred, although 'idp.entra' sorts first


def test_cap_21_public_sync_method_is_rejected_at_definition() -> None:
    with pytest.raises(InvalidCapabilityError, match='fetch_token'):

        class _Leaky(Capability):
            capability_id = CapabilityId('leaky')

            async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
                return None

            def fetch_token(self) -> str:  # public sync → would silently bypass the audit wrapper
                return 'token'


def test_cap_22_underscored_sync_helper_is_allowed() -> None:
    class _WithHelper(Capability):
        capability_id = CapabilityId('with_helper')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        def _normalize(self, raw: str) -> str:  # internal helper → not an action, never audited
            return raw.strip()

    assert _WithHelper()._normalize(' x ') == 'x'


def test_cap_23_overriding_a_base_defined_method_is_allowed() -> None:
    class _Override(Capability):
        capability_id = CapabilityId('override')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        def bind_auditor(self, auditor: InvocationAuditor | None) -> None:  # framework lifecycle, sync on the base
            super().bind_auditor(auditor)

    assert Capability._registry[CapabilityId('override')] is _Override


def test_cap_24_descriptors_and_class_attributes_do_not_trip_the_sync_check() -> None:
    class _Descriptors(Capability):
        capability_id = CapabilityId('descriptors')
        region: str = 'eu'  # plain class attribute

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        @property
        def endpoint(self) -> str:
            return 'https://idp.example'

        @classmethod
        def family_name(cls) -> str:
            return cls.__name__

        @staticmethod
        def version() -> int:
            return 1

    assert Capability._registry[CapabilityId('descriptors')] is _Descriptors


async def test_cap_25_failed_activation_retries_after_cool_off_and_succeeds() -> None:
    clock = FakeClock()
    cap = make_capability('idp', depends_on={WorkEmailDataPoint}, activate_errors=[ValueError('transient')])
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(
        _registered(cap),
        catalog,
        NAMESPACE,
        clock,
        activation_retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )

    first = await activator.refresh(DataPointView([work_email()]))
    assert not first.is_available(IDP_ID)  # the transient failure leaves it unavailable for now

    still_cooling = await activator.refresh(DataPointView([work_email()]))
    assert not still_cooling.is_available(IDP_ID)
    assert cap.attempts == 1  # type: ignore[attr-defined]  # inside the cool-off → no re-attempt

    clock.advance(1.0)  # past the first cool-off (base_delay * 2**0, jitter disabled)
    recovered = await activator.refresh(DataPointView([work_email()]))
    assert recovered.is_available(IDP_ID)  # the re-attempt succeeded — the subgraph is back
    assert cap.attempts == 2  # type: ignore[attr-defined]


async def test_cap_26_exhausted_activation_retries_are_terminal_and_reported_once() -> None:
    clock = FakeClock()
    terminal: list[tuple[CapabilityId, str]] = []

    async def on_terminal(capability_id: CapabilityId, error: Exception) -> None:
        terminal.append((capability_id, str(error)))

    cap = make_capability('idp', depends_on={WorkEmailDataPoint}, activate_error=ValueError('no creds'))
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(
        _registered(cap),
        catalog,
        NAMESPACE,
        clock,
        activation_retry=RetryPolicy(max_attempts=2, base_delay=1.0, jitter=0.0),
        on_terminal_failure=on_terminal,
    )

    await activator.refresh(DataPointView([work_email()]))  # attempt 1 fails → cooling off
    assert terminal == []
    clock.advance(1.0)
    await activator.refresh(DataPointView([work_email()]))  # attempt 2 fails → retries exhausted
    assert terminal == [(IDP_ID, 'no creds')]

    clock.advance(3600.0)  # however long the session runs, a terminal failure is never re-attempted
    final = await activator.refresh(DataPointView([work_email()]))
    assert not final.is_available(IDP_ID)
    assert cap.attempts == 2  # type: ignore[attr-defined]
    assert terminal == [(IDP_ID, 'no creds')]  # the terminal callback fired exactly once


async def test_cap_27_first_try_success_is_unaffected_by_the_retry_machinery() -> None:
    clock = FakeClock()
    cap = make_capability('idp', depends_on={WorkEmailDataPoint})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(_registered(cap), catalog, NAMESPACE, clock)

    view = await activator.refresh(DataPointView([work_email()]))
    assert view.is_available(IDP_ID)
    clock.advance(3600.0)
    again = await activator.refresh(DataPointView([work_email()]))
    assert again.is_available(IDP_ID)
    assert cap.attempts == 1  # type: ignore[attr-defined]  # activated once, never re-attempted


async def test_cap_28_orchestrator_audits_terminal_activation_failure() -> None:
    session = SessionId('cap-session')
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    runtime = build_in_memory_runtime(FakeClock(), catalog=catalog)
    cap = make_capability('idp', activate_error=ValueError('no creds'))

    result = await Orchestrator(
        session_id=session,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        capabilities=[cap],
        retry_policy=RetryPolicy(max_attempts=1),  # the first failure exhausts the budget
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the lost capability degrades, never wedges
    failures = [
        entry for entry in await runtime.audit.replay(session) if entry.kind is AuditKind.CAPABILITY_ACTIVATION_FAILED
    ]
    assert len(failures) == 1  # terminal disposition recorded exactly once
    info = failures[0].capability
    assert info is not None and info.capability_id == IDP_ID
    assert 'no creds' in (info.error or '')
    assert failures[0].epoch == result.epoch  # epoch-stamped like every other audit entry


async def test_cap_29_action_acquires_the_rate_limit_before_auditing_and_the_body() -> None:
    order: list[str] = []

    class _RecordingLimiter:
        async def acquire(self, key: str) -> None:
            order.append(f'limit:{key}')

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def ping(self) -> str:
            order.append('body')
            return 'pong'

    async def record(capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]) -> None:  # noqa: ARG001
        order.append('audit')

    idp = _Idp()
    idp.bind_auditor(record)
    idp.bind_rate_limit(_RecordingLimiter(), 'namespace-1:idp')

    assert await idp.ping() == 'pong'
    # The action is paced first, recorded second, executed last — the audit trail holds only
    # invocations that actually got past the fleet's rate limit.
    assert order == ['limit:namespace-1:idp', 'audit', 'body']


async def test_cap_30_activator_binds_the_namespace_scoped_rate_limit_key() -> None:
    acquired: list[str] = []

    class _RecordingLimiter:
        async def acquire(self, key: str) -> None:
            acquired.append(key)

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def ping(self) -> str:
            return 'pong'

    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(
        _registered(_Idp), catalog, NAMESPACE, FakeClock(), rate_limiter=_RecordingLimiter()
    )

    view = await activator.refresh(DataPointView([]))
    await view.require(_Idp).ping()

    assert acquired == [f'{NAMESPACE}:{IDP_ID}']  # the fleet bucket is keyed per (namespace, capability)


def test_cap_31_requires_blocked_layer_stays_unavailable_independently_of_deps() -> None:
    # Condition (3) of the fixpoint gates a layer on its requires being AVAILABLE — not merely on its
    # own deps being present. A is registered but NOT permitted, so it can never enter `available`;
    # B (permitted, deps satisfied) must therefore stay perpetually out of the result. This isolates
    # the requires-blocked branch from the deps-missing branch.
    base = make_capability('base')
    layer = make_capability('layer', depends_on={WorkEmailDataPoint}, requires={base})
    registered = _registered(base, layer)

    # A registered-but-unpermitted base keeps the layer out even with the layer's own deps present.
    blocked = compute_available(
        registered=registered,
        permitted=frozenset({CapabilityId('layer')}),
        present_types=frozenset({WorkEmailDataPoint}),
    )
    assert blocked == frozenset()  # the requires gate alone excludes the layer

    # Permitting the base (its deps are empty, so present) lets the fixpoint admit both, base first.
    both = compute_available(
        registered=registered,
        permitted=frozenset({CapabilityId('base'), CapabilityId('layer')}),
        present_types=frozenset({WorkEmailDataPoint}),
    )
    assert both == frozenset({CapabilityId('base'), CapabilityId('layer')})


async def test_cap_32_action_that_raises_still_paces_and_audits_before_the_body() -> None:
    # The audited seam is `span -> acquire_rate_limit -> record_invocation -> body`. A failing body
    # must still have consumed a token and written the audit record (pacing and recording happen
    # BEFORE the body), and the exception must propagate out through the span (marked failed).
    order: list[str] = []

    class _RecordingLimiter:
        async def acquire(self, key: str) -> None:
            order.append(f'limit:{key}')

    class _BoomError(Exception):
        pass

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def act(self) -> str:
            order.append('body')
            raise _BoomError('downstream failed')

    async def record(capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]) -> None:  # noqa: ARG001
        order.append('audit')

    probe = TelemetryProbe()
    idp = _Idp()
    idp.bind_auditor(record)
    idp.bind_rate_limit(_RecordingLimiter(), 'namespace-1:idp')
    idp.bind_telemetry(probe.telemetry)

    with pytest.raises(_BoomError):
        await idp.act()

    # A failed external action still costs a token and a recorded invocation: the trail holds every
    # action that actually proceeded past the limiter, even one whose body then raised.
    assert order == ['limit:namespace-1:idp', 'audit', 'body']
    (span,) = probe.spans('capability.action idp.act')
    assert span.status.status_code is StatusCode.ERROR  # the raise propagated through the span


async def test_cap_33_layer_activates_only_after_its_failed_base_recovers_on_a_later_refresh() -> None:
    # Base A fails activation once (then succeeds); layer B requires A. On the first refresh A fails
    # and B (requires not yet activated) is never built — the activation fixpoint terminates via the
    # `if not ready: break`, no exception. After A's cool-off elapses, the next refresh activates A
    # and THEN B in the same pass.
    clock = FakeClock()
    order: list[CapabilityId] = []
    base = make_capability('base_layer', record_order=order, activate_errors=[ValueError('transient')])
    layer = make_capability('top_layer', requires={base}, record_order=order)
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('base_layer'), CapabilityId('top_layer')}})
    activator = CapabilityActivator(
        _registered(base, layer),
        catalog,
        NAMESPACE,
        clock,
        activation_retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )

    first = await activator.refresh(DataPointView([]))
    assert not first.is_available(CapabilityId('base_layer'))  # A's first attempt failed
    assert not first.is_available(CapabilityId('top_layer'))  # B never built without A activated
    assert CapabilityId('top_layer') not in activator.activated_ids()  # not stuck-activated
    assert base.attempts == 1  # type: ignore[attr-defined]
    assert layer.attempts == 0  # type: ignore[attr-defined]  # the layer was never even constructed

    clock.advance(1.0)  # past A's first cool-off (base_delay * 2**0, jitter disabled)
    recovered = await activator.refresh(DataPointView([]))
    assert recovered.is_available(CapabilityId('base_layer'))
    assert recovered.is_available(CapabilityId('top_layer'))  # B finally activates in the same refresh
    assert order == [CapabilityId('base_layer'), CapabilityId('top_layer')]  # base before layer


async def test_cap_34_layer_stays_unavailable_forever_when_its_base_is_terminal() -> None:
    # A's retry budget is exhausted on the first attempt (max_attempts=1 → terminal immediately);
    # B requires A. B must remain permanently unavailable across many refreshes without wedging,
    # and the loop must terminate every pass (no busy-loop on the unready layer).
    clock = FakeClock()
    base = make_capability('dead_base', activate_error=ValueError('no creds'))
    layer = make_capability('dead_top', requires={base})
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('dead_base'), CapabilityId('dead_top')}})
    activator = CapabilityActivator(
        _registered(base, layer), catalog, NAMESPACE, clock, activation_retry=RetryPolicy(max_attempts=1)
    )

    for _ in range(5):
        clock.advance(3600.0)  # however long the session runs, a terminal base never recovers
        view = await activator.refresh(DataPointView([]))
        assert not view.is_available(CapabilityId('dead_base'))
        assert not view.is_available(CapabilityId('dead_top'))  # the layer is permanently blocked
    assert base.attempts == 1  # type: ignore[attr-defined]  # terminal after one attempt, never re-tried
    assert layer.attempts == 0  # type: ignore[attr-defined]  # the layer was never constructed


async def test_cap_35_terminal_activation_without_a_callback_does_not_crash() -> None:
    # With on_terminal_failure left None (the orchestrator wired no notifier), exhausting the retry
    # budget must record the terminal disposition and return — never try to await the None callback,
    # and stay terminal forever.
    clock = FakeClock()
    cap = make_capability('idp', depends_on={WorkEmailDataPoint}, activate_error=ValueError('no creds'))
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    activator = CapabilityActivator(
        _registered(cap),
        catalog,
        NAMESPACE,
        clock,
        activation_retry=RetryPolicy(max_attempts=2, base_delay=1.0, jitter=0.0),
        # on_terminal_failure deliberately omitted → defaults to None
    )

    await activator.refresh(DataPointView([work_email()]))  # attempt 1 fails → cooling off
    assert cap.attempts == 1  # type: ignore[attr-defined]
    clock.advance(1.0)
    final = await activator.refresh(DataPointView([work_email()]))  # attempt 2 fails → terminal, no callback
    assert not final.is_available(IDP_ID)
    assert cap.attempts == 2  # type: ignore[attr-defined]

    clock.advance(3600.0)  # a terminal failure is never re-attempted, even with no notifier wired
    again = await activator.refresh(DataPointView([work_email()]))
    assert not again.is_available(IDP_ID)
    assert cap.attempts == 2  # type: ignore[attr-defined]  # no re-attempt; the None callback never crashed the run


async def test_cap_36_audit_binds_positional_args_by_name_and_fills_defaults() -> None:
    # The seam binds `signature.bind(self, *args, **kwargs)` + `apply_defaults()`, so an action called
    # positionally and with a defaulted parameter omitted is recorded with every parameter named and
    # the default filled in — `query('k')` → {'key': 'k', 'limit': 5}.
    audited: list[dict[str, Any]] = []

    class _Idp(Capability):
        capability_id = CapabilityId('idp')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def query(self, key: str, limit: int = 5) -> str:
            return f'{key}:{limit}'

    async def record(capability_id: CapabilityId, action: str, parameters: Mapping[str, Any]) -> None:  # noqa: ARG001
        audited.append(dict(parameters))

    idp = _Idp()
    idp.bind_auditor(record)

    assert await idp.query('k') == 'k:5'  # positional arg, default limit omitted
    # The positional 'k' is bound to its parameter name and the omitted default is filled in.
    assert audited == [{'key': 'k', 'limit': 5}]


def test_cap_37_duplicate_capability_id_raises_but_re_registering_the_same_class_does_not() -> None:
    class Foo(Capability):
        capability_id = CapabilityId('dup')

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

    # A second, DISTINCT concrete class colliding on the same id is rejected at class-definition time.
    with pytest.raises(DuplicateRegistrationError, match='dup'):

        class Bar(Capability):
            capability_id = CapabilityId('dup')

            async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
                return None

    # Re-registering the SAME class object (idempotent re-import) is allowed (the `is not cls` guard).
    Capability._registry[CapabilityId('dup')] = Foo
    Foo.__init_subclass__()
    assert Capability._registry[CapabilityId('dup')] is Foo


async def test_cap_38_rate_limit_wait_cancelled_by_per_op_timeout_isolates_and_skips_audit() -> None:
    # S1: a capability action paces at the audited seam BEFORE recording. When the fleet limiter wait
    # genuinely outlasts the per-operator timeout, asyncio.wait_for cancels the operator mid-acquire:
    # the failure is isolated (the session still COMPLETES + aggregates), and because pacing precedes
    # recording, the cancelled-before-proceeding action leaves NO audit entry — the trail holds only
    # actions that got past the limiter.
    blocked = asyncio.Event()  # never set → the limiter wait blocks in real time until cancelled

    class _BlockingLimiter:
        async def acquire(self, key: str) -> None:  # noqa: ARG002
            await blocked.wait()

    class _Idp(Capability):
        capability_id = CapabilityId('idp')
        depends_on = frozenset({EmailDataPoint})

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def call_out(self) -> str:
            return 'ok'

    class _Caller(Operator):
        operator_id = OperatorId('caller')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        requires = frozenset({_Idp})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            idp = ctx.capabilities.require(_Idp)
            result = await idp.call_out()  # blocks in the limiter wait until the per-op timeout fires
            yield ChatAnswerDataPoint.emit(result)

    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    runtime = build_in_memory_runtime(FakeClock(), catalog=catalog, rate_limiter=_BlockingLimiter())

    result = await Orchestrator(
        session_id=SessionId('cap-rl-session'),
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_Caller],
        capabilities=[_Idp],
        seed=[work_email()],
        operation_timeout=0.02,  # real-time bound; the limiter wait never returns, so wait_for cancels it
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the cancelled action is isolated; the session finishes
    entries = await runtime.audit.replay(SessionId('cap-rl-session'))
    invoked = [entry for entry in entries if entry.kind is AuditKind.CAPABILITY_INVOKED]
    assert invoked == []  # paced-then-audited: cancelled before proceeding → never recorded


async def test_cap_39_rate_limit_wait_inside_a_run_paces_then_proceeds_and_audits() -> None:
    # The companion to cap_38: when the fleet limiter wait DOES complete (it only paces, never fails),
    # the deterministic FakeClock-driven InMemoryRateLimiter sleep is fast-forwarded, the action
    # proceeds, and exactly one audit entry is recorded. A second action on the same bucket waits out
    # the refill — but still proceeds and audits, because the limiter waits, it never fails.
    clock = FakeClock()

    class _Idp(Capability):
        capability_id = CapabilityId('idp')
        depends_on = frozenset({EmailDataPoint})

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def call_out(self, marker: str) -> str:
            return marker

    class _Caller(Operator):
        operator_id = OperatorId('caller')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        requires = frozenset({_Idp})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            idp = ctx.capabilities.require(_Idp)
            yield ChatAnswerDataPoint.emit(await idp.call_out(marker='first'))  # consumes the burst token
            yield ChatAnswerDataPoint.emit(await idp.call_out(marker='second'))  # waits out the refill, then proceeds

    session = SessionId('cap-rl-pace-session')
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP_ID}})
    limiter = InMemoryRateLimiter(clock, rate_per_second=0.1, burst=1)  # second acquire sleeps ~10s on the clock
    runtime = build_in_memory_runtime(clock, catalog=catalog, rate_limiter=limiter)
    started_at = clock.monotonic()

    result = await Orchestrator(
        session_id=session,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_Caller],
        capabilities=[_Idp],
        seed=[work_email()],
        session_deadline=300.0,  # ample budget; the limiter sleep is fast-forwarded on the FakeClock
    ).run()

    assert result.status is SessionStatus.COMPLETED
    answers = {dp.value for dp in (await runtime.store.snapshot(session)).of_type(ChatAnswerDataPoint)}
    assert answers == {'first', 'second'}  # both paced actions proceeded — the limiter waited, never failed
    # The second acquire genuinely WAITED (it did not drop or skip): the burst was one token, so refilling
    # one at 0.1/s fast-forwards the injected clock ~10s — observable proof the pacing happened.
    assert clock.monotonic() - started_at >= 9.0
    invoked = [entry for entry in await runtime.audit.replay(session) if entry.kind is AuditKind.CAPABILITY_INVOKED]
    assert len(invoked) == 2  # exactly the two actions that got past the limiter are in the trail
