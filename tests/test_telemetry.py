"""TEL — built-in OpenTelemetry instrumentation: metrics, spans and bridged logs off the hot paths.

The framework instruments straight against the OTel API; without an SDK every emission is a
no-op and behavior is untouched. These tests inject per-test providers (the
``TelemetryProbe`` double) so the orchestrator's counters, span tree and bridged log records
can be asserted end-to-end through real OTel SDK exporters.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from dataclasses import replace
from datetime import timedelta
from itertools import count
from typing import Any

import pytest
from loguru import logger
from opentelemetry._logs import SeverityNumber
from opentelemetry.trace import StatusCode

from orcastork.adapters.memory import InMemoryCapabilityCatalog, InMemoryInbox
from orcastork.adapters.memory.store import InMemoryDataPointStore
from orcastork.aggregation import RetryPolicy
from orcastork.capabilities import Capability, CapabilityContext
from orcastork.capabilities.availability import CapabilityActivator
from orcastork.datapoints import DataPointEmission, DataPointView
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.logging_bridge import attach_otel_log_bridge, detach_otel_log_bridge
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime
from orcastork.telemetry import Telemetry

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import (
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RiskDataPoint,
    chat_answer,
    ip,
    risk,
    work_email,
)
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator
from .doubles.otel import TelemetryProbe

SID = SessionId('tel-session')
NAMESPACE = NamespaceId('tel-namespace')

# The metric attributes the orchestrator is allowed to emit — the cardinality rule in
# executable form: per-session identifiers must never become metric attribute keys.
LOW_CARDINALITY_LABEL_KEYS = frozenset({'operator_id', 'capability_id', 'outcome', 'status', 'disposition', 'kind'})


@pytest.fixture
def probe() -> TelemetryProbe:
    return TelemetryProbe()


async def _noop(ctx: OperatorContext) -> None:  # noqa: ARG001
    return None


async def test_tel_default_telemetry_is_inert_without_an_sdk_and_leaves_behavior_identical(
    fake_clock: FakeClock,
) -> None:
    # No SDK is installed on the process globals, so the default Telemetry() no-ops every
    # emission — a deployment that wires nothing loses nothing.
    runtime = build_in_memory_runtime(fake_clock)
    assert isinstance(runtime.telemetry, Telemetry)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('op'): 1}
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'work_email', 'risk'}


async def test_tel_happy_path_records_runs_durations_and_session_completion(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=_noop)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator, reporter], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    run_seconds = probe.histogram('operator_run_seconds', {'operator_id': 'op', 'outcome': 'succeeded'})
    assert run_seconds is not None and run_seconds.count == 1 and run_seconds.sum >= 0.0
    assert probe.counter('operator_runs_total', {'operator_id': 'op', 'outcome': 'succeeded'}) == 1
    assert probe.counter('operator_runs_total', {'operator_id': 'rep', 'outcome': 'succeeded'}) == 1
    assert probe.counter('sessions_total', {'status': 'completed'}) == 1
    assert probe.counter('data_points_merged_total', {'kind': 'added'}) == 2  # the seed + the risk emission
    assert probe.counter('data_points_merged_total', {'kind': 'updated'}) == 0  # both were new identities
    gather_seconds = probe.histogram('session_gather_seconds')
    assert gather_seconds is not None and gather_seconds.count == 1
    aggregation_seconds = probe.histogram('session_aggregation_seconds')
    assert aggregation_seconds is not None and aggregation_seconds.count == 1
    archive_flush = probe.histogram('archive_flush_entries')
    assert archive_flush is not None and archive_flush.count == 1
    assert probe.metric_attribute_keys() <= LOW_CARDINALITY_LABEL_KEYS  # session/namespace ids never leak into metrics


async def test_tel_scheduled_retries_count_one_per_armed_retry(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    flaky = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        raise_error=ValueError('boom'),
        retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[flaky], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('flaky')] == 3
    # Failures 1 and 2 armed a backoff relaunch; failure 3 exhausted the policy (terminal, no retry).
    assert probe.counter('operator_retries_total', {'operator_id': 'flaky'}) == 2
    assert probe.counter('operator_runs_total', {'operator_id': 'flaky', 'outcome': 'failed'}) == 3


async def test_tel_rerun_launches_are_counted_per_consumed_debounce(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    counter = count()
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=1),
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # noqa: ARG005
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    # Every run after the first was a launched rerun, whatever the breaker allowed.
    expected_reruns = result.operator_runs[OperatorId('selfloop')] - 1
    assert probe.counter('operator_reruns_total', {'operator_id': 'selfloop'}) == expected_reruns
    assert expected_reruns >= 1  # the cycle genuinely reran


async def test_tel_quarantined_poison_inbox_entry_counts_disposition(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    inbox = runtime.inbox
    assert isinstance(inbox, InMemoryInbox)
    await inbox.append_serialized(SID, 'not-json{')
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('inbox_entries_total', {'disposition': 'quarantined'}) == 1
    assert probe.counter('inbox_entries_total', {'disposition': 'applied'}) == 0


async def test_tel_applied_inbox_entry_counts_disposition(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    await runtime.inbox.append(SID, chat_answer('hello'))
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('inbox_entries_total', {'disposition': 'applied'}) == 1


class _RejectingStore(InMemoryDataPointStore):
    """In-memory store that rejects applies containing a marked value (a persistently bad apply)."""

    async def apply_resolved(
        self, session_id: SessionId, *, added: Iterable[Any], updated: Iterable[Any], epoch: Epoch
    ) -> Any:
        if any(dp.value == 'merge-bomb' for dp in (*added, *updated)):
            raise ValueError('store rejected the write')
        return await super().apply_resolved(session_id, added=tuple(added), updated=tuple(updated), epoch=epoch)


async def test_tel_redelivered_then_quarantined_inbox_entry_counts_both_dispositions(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = replace(build_in_memory_runtime(fake_clock, telemetry=probe.telemetry), store=_RejectingStore())
    await runtime.inbox.append(SID, chat_answer('merge-bomb'))
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # keeps the loop draining instead of exiting on the first pass
        session_deadline=30.0,
        max_inbox_deliveries=3,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('inbox_entries_total', {'disposition': 'redelivered'}) == 2  # deliveries under the cap
    assert probe.counter('inbox_entries_total', {'disposition': 'quarantined'}) == 1  # the capped delivery


async def test_tel_parked_session_counts_status_parked(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives — the wait stays idle
        session_deadline=300.0,
        park_after=30.0,
    ).run()

    assert result.status is SessionStatus.PARKED
    assert probe.counter('sessions_total', {'status': 'parked'}) == 1
    assert probe.counter('sessions_total', {'status': 'completed'}) == 0
    assert probe.counter('session_deadline_hits_total') == 0  # parking is not a deadline hit
    (run_span,) = probe.spans('session.run')
    # The run span carries the same disposition as the counter (set at the same seam).
    assert dict(run_span.attributes or {})['status'] == 'parked'
    (wait_span,) = probe.spans('session.inbox_wait')
    assert dict(wait_span.attributes or {})['outcome'] == 'parked'  # the wait span says what ended it


async def test_tel_session_deadline_hit_is_counted_logged_and_spanned(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[],
            seed=[work_email()],
            completes_when=ChatAnswerDataPoint,  # never arrives; only the deadline ends the wait
            session_deadline=30.0,
        ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('session_deadline_hits_total') == 1
    (wait_span,) = probe.spans('session.inbox_wait')
    assert dict(wait_span.attributes or {})['outcome'] == 'deadline'
    (warning,) = [record for record in records if record['message'].startswith('Session deadline hit')]
    assert warning['level'].name == 'WARNING' and warning['extra']['session_id'] == SID


async def test_tel_dead_lettered_aggregator_counts_attempts_retries_and_the_dead_letter(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)

    async def explode(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('durable write failed')

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=explode)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0, jitter=0.0),
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('aggregator_dead_letters_total', {'operator_id': 'rep'}) == 1
    assert probe.counter('operator_runs_total', {'operator_id': 'rep', 'outcome': 'failed'}) == 2
    assert probe.counter('operator_runs_total', {'operator_id': 'rep', 'outcome': 'dead_lettered'}) == 1
    assert probe.counter('operator_retries_total', {'operator_id': 'rep'}) == 1
    failed_seconds = probe.histogram('operator_run_seconds', {'operator_id': 'rep', 'outcome': 'failed'})
    assert failed_seconds is not None and failed_seconds.count == 2


async def test_tel_capability_activation_succeeded_and_terminal_outcomes(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('goodcap'), CapabilityId('badcap')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog, telemetry=probe.telemetry)
    goodcap = make_capability('goodcap')
    badcap = make_capability('badcap', activate_error=ValueError('no credentials'))
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[operator],
        capabilities=[goodcap, badcap],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=1),  # the first activation failure is terminal
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert probe.counter('capability_activations_total', {'capability_id': 'goodcap', 'outcome': 'succeeded'}) == 1
    assert probe.counter('capability_activations_total', {'capability_id': 'badcap', 'outcome': 'terminal'}) == 1
    assert probe.counter('capability_activations_total', {'capability_id': 'badcap', 'outcome': 'failed'}) == 0


async def test_tel_capability_activation_cool_off_failure_counts_failed(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    badcap = make_capability('badcap', activate_errors=[ValueError('first attempt fails')])
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('badcap')}})
    activator = CapabilityActivator(
        {CapabilityId('badcap'): badcap},
        catalog,
        NAMESPACE,
        fake_clock,
        activation_retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
        telemetry=probe.telemetry,
    )

    await activator.refresh(DataPointView(()))
    assert probe.counter('capability_activations_total', {'capability_id': 'badcap', 'outcome': 'failed'}) == 1

    fake_clock.advance(1.0)  # the cool-off elapses; the next refresh re-attempts and succeeds
    await activator.refresh(DataPointView(()))
    assert probe.counter('capability_activations_total', {'capability_id': 'badcap', 'outcome': 'succeeded'}) == 1
    assert probe.counter('capability_activations_total', {'capability_id': 'badcap', 'outcome': 'terminal'}) == 0


def test_tel_spans_nest_and_a_propagating_exception_marks_the_span_failed(probe: TelemetryProbe) -> None:
    tracer = probe.telemetry.tracer
    with tracer.start_as_current_span('outer', attributes={'session_id': 'sid'}):  # noqa: SIM117
        with tracer.start_as_current_span('inner'):
            pass
    with pytest.raises(ValueError, match='boom'):  # noqa: SIM117 — the nesting IS the contract under test
        with tracer.start_as_current_span('failing'):
            raise ValueError('boom')

    (outer,) = probe.spans('outer')
    (inner,) = probe.spans('inner')
    (failing,) = probe.spans('failing')
    assert probe.parent_of(inner) is outer and outer.parent is None
    assert dict(outer.attributes or {}) == {'session_id': 'sid'}
    assert failing.status.status_code is StatusCode.ERROR
    (event,) = failing.events
    assert event.name == 'exception' and dict(event.attributes or {})['exception.type'] == 'ValueError'


async def test_tel_session_run_emits_a_nested_span_tree(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=_noop)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator, reporter], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    (run_span,) = probe.spans('session.run')
    assert run_span.parent is None
    # Per-session identifiers ride span attributes (allowed there — no series is created).
    assert dict(run_span.attributes or {}) == {
        'session_id': SID,
        'namespace_id': NAMESPACE,
        'epoch': 1,
        'status': 'completed',
    }
    (gather_span,) = probe.spans('session.gather')
    (aggregate_span,) = probe.spans('session.aggregate')
    assert probe.parent_of(gather_span) is run_span and probe.parent_of(aggregate_span) is run_span
    (operator_span,) = probe.spans('operator.run op')
    assert probe.parent_of(operator_span) is gather_span  # task-launched while the gather span was active
    assert dict(operator_span.attributes or {}) == {'operator_id': 'op', 'outcome': 'succeeded'}
    (aggregator_span,) = probe.spans('aggregator.run rep')
    assert probe.parent_of(aggregator_span) is aggregate_span
    assert dict(aggregator_span.attributes or {}) == {'operator_id': 'rep', 'attempt': 1, 'outcome': 'succeeded'}
    # one trace, no failed spans
    assert {span.context.trace_id for span in probe.spans() if span.context is not None} == {
        run_span.context.trace_id if run_span.context is not None else None
    }
    assert all(span.status.status_code is StatusCode.UNSET for span in probe.spans())


async def test_tel_failed_operator_span_records_the_isolated_error(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    failer = make_operator('failer', depends_on={EmailDataPoint}, raise_error=ValueError('boom'))
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[failer], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the failure is isolated; the session still completes
    (operator_span,) = probe.spans('operator.run failer')
    assert dict(operator_span.attributes or {})['outcome'] == 'failed'
    assert operator_span.status.status_code is StatusCode.ERROR
    assert operator_span.status.description == 'boom'
    (event,) = operator_span.events
    assert event.name == 'exception' and dict(event.attributes or {})['exception.type'] == 'ValueError'
    # Isolation holds in the trace too: the failure marks the operator span, never the run span.
    (run_span,) = probe.spans('session.run')
    assert dict(run_span.attributes or {})['status'] == 'completed'
    assert run_span.status.status_code is StatusCode.UNSET


async def test_tel_aggregator_attempt_spans_carry_ordinals_and_failed_outcomes(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)

    async def explode(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('durable write failed')

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=explode)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0, jitter=0.0),
    ).run()

    assert result.status is SessionStatus.COMPLETED
    first, second = probe.spans('aggregator.run rep')
    assert dict(first.attributes or {})['attempt'] == 1 and dict(second.attributes or {})['attempt'] == 2
    for span in (first, second):
        assert dict(span.attributes or {})['outcome'] == 'failed'
        assert span.status.status_code is StatusCode.ERROR  # the raise propagated through the span
        (event,) = span.events
        assert event.name == 'exception' and dict(event.attributes or {})['exception.type'] == 'ValueError'
    (aggregate_span,) = probe.spans('session.aggregate')
    assert probe.parent_of(first) is aggregate_span and probe.parent_of(second) is aggregate_span


async def test_tel_capability_activation_spans_record_success_and_failure(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('goodcap'), CapabilityId('badcap')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog, telemetry=probe.telemetry)
    goodcap = make_capability('goodcap')
    badcap = make_capability('badcap', activate_error=ValueError('no credentials'))
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[operator],
        capabilities=[goodcap, badcap],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=1),  # the first activation failure is terminal
    ).run()

    assert result.status is SessionStatus.COMPLETED
    (good_span,) = probe.spans('capability.activate goodcap')
    assert good_span.status.status_code is StatusCode.UNSET
    assert dict(good_span.attributes or {}) == {'capability_id': 'goodcap'}
    (bad_span,) = probe.spans('capability.activate badcap')
    assert bad_span.status.status_code is StatusCode.ERROR
    (event,) = bad_span.events
    assert event.name == 'exception' and dict(event.attributes or {})['exception.type'] == 'ValueError'


async def test_tel_capability_action_span_rides_the_audited_seam(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('idp')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog, telemetry=probe.telemetry)

    class _Idp(Capability):
        capability_id = CapabilityId('idp')
        depends_on = frozenset({EmailDataPoint})

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def send_challenge(self, user_id: str) -> str:  # noqa: ARG002
            return 'challenge-sent'

    class _Caller(Operator):
        operator_id = OperatorId('caller')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        requires = frozenset({_Idp})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            idp = ctx.capabilities.require(_Idp)
            yield ChatAnswerDataPoint.emit(await idp.send_challenge(user_id='u-1'))

    with capture_logs(level='DEBUG') as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[_Caller],
            capabilities=[_Idp],
            seed=[work_email()],
        ).run()

    assert result.status is SessionStatus.COMPLETED
    (action_span,) = probe.spans('capability.action idp.send_challenge')
    assert dict(action_span.attributes or {}) == {'capability_id': 'idp', 'action': 'send_challenge'}
    (operator_span,) = probe.spans('operator.run caller')
    assert probe.parent_of(action_span) is operator_span  # the action nests inside the calling operator's run span
    assert 'u-1' not in str(action_span.attributes)  # argument values never land on the span (PII discipline)
    # The audit seams log the activation and the invocation — keys only, mirroring the audit redaction.
    (activated,) = [
        record
        for record in records
        if record['message'] == 'Capability activated for this session; recording the audit entry'
    ]
    assert activated['extra']['capability_id'] == 'idp'
    (invoked,) = [record for record in records if record['message'] == 'Capability action invoked']
    assert invoked['extra']['capability_id'] == 'idp'
    assert invoked['extra']['action'] == 'send_challenge'
    assert invoked['extra']['parameter_keys'] == ['user_id']
    assert 'u-1' not in str(invoked['extra'])  # argument values never reach the log either


def test_tel_log_bridge_forwards_loguru_records_with_attributes_and_exception_info(probe: TelemetryProbe) -> None:
    bridge_id = attach_otel_log_bridge(level='DEBUG', logger_provider=probe.logger_provider)
    try:
        logger.debug('step detail', step=1)
        logger.info('flow started', operator_ids=['a', 'b'])
        logger.warning('degraded')
        try:
            raise ValueError('boom')
        except ValueError:
            logger.opt(exception=True).error('failed hard', operator_id='op')
    finally:
        detach_otel_log_bridge(bridge_id)
    logger.error('after detach — never forwarded')

    debug, info, warning, error = probe.logs()
    assert debug.severity_number is SeverityNumber.DEBUG and dict(debug.attributes or {})['step'] == 1
    assert info.severity_number is SeverityNumber.INFO and info.body == 'flow started'
    info_attributes = dict(info.attributes or {})
    assert info_attributes['operator_ids'] == "['a', 'b']"  # non-scalar kwargs are stringified
    assert str(info_attributes['logger.name']).endswith('test_telemetry')
    assert warning.severity_number is SeverityNumber.WARN and warning.severity_text == 'WARNING'
    error_attributes = dict(error.attributes or {})
    assert error.severity_number is SeverityNumber.ERROR and error_attributes['operator_id'] == 'op'
    assert error_attributes['exception.type'] == 'ValueError'
    assert error_attributes['exception.message'] == 'boom'
    assert 'ValueError: boom' in str(error_attributes['exception.stacktrace'])


def test_tel_log_bridge_emits_empty_string_for_a_none_logger_name(probe: TelemetryProbe) -> None:
    # A record whose loguru name is None (records originating outside a named module) must still
    # produce a scalar 'logger.name' attribute — OTel attribute values are scalars, so a None
    # here would be a non-conformant attribute that a strict exporter could drop or reject. None is
    # exactly what loguru sets for a module-less origin; its Record stub types name as str, hence
    # the ignore for the deliberately-out-of-band value this test pins.
    bridge_id = attach_otel_log_bridge(logger_provider=probe.logger_provider)
    try:
        logger.patch(lambda record: record.update(name=None)).info('nameless origin')  # type: ignore[call-arg]
    finally:
        detach_otel_log_bridge(bridge_id)

    (record,) = probe.logs()
    attributes = dict(record.attributes or {})
    assert attributes['logger.name'] == ''  # the 'or '' ' fallback, a scalar — never None
    assert attributes['logger.name'] is not None


def test_tel_log_bridge_maps_success_and_critical_levels_preserving_their_names(probe: TelemetryProbe) -> None:
    # SUCCESS and CRITICAL are loguru-specific level names. The bridge maps each to its conventional
    # OTel SeverityNumber (SUCCESS collapses onto INFO; CRITICAL becomes FATAL) while keeping the
    # original loguru level name as severity_text, so a backend can both filter by severity number
    # and recover the distinct originating level. A custom/unknown level falls back to INFO.
    # An unknown level (not in the severity map) must hit the INFO fallback. Loguru levels are
    # process-global, so a prior run in this process may have already registered it.
    with contextlib.suppress(ValueError):
        logger.level('NOTICE', no=25)

    bridge_id = attach_otel_log_bridge(level='TRACE', logger_provider=probe.logger_provider)
    try:
        logger.success('done')
        logger.critical('halt')  # noqa: LOG009 — a terse fixture message; this test pins level mapping, not content
        logger.log('NOTICE', 'heads up')
    finally:
        detach_otel_log_bridge(bridge_id)

    success, critical, notice = probe.logs()
    assert success.severity_number is SeverityNumber.INFO  # SUCCESS collapses onto INFO
    assert success.severity_text == 'SUCCESS'  # but the distinct loguru level name is preserved
    assert critical.severity_number is SeverityNumber.FATAL
    assert critical.severity_text == 'CRITICAL'
    assert notice.severity_number is SeverityNumber.INFO  # unknown level → INFO fallback
    assert notice.severity_text == 'NOTICE'  # the original name still rides through


def test_tel_log_bridge_correlates_records_with_the_active_span(probe: TelemetryProbe) -> None:
    bridge_id = attach_otel_log_bridge(logger_provider=probe.logger_provider)
    try:
        with probe.telemetry.tracer.start_as_current_span('work') as span:
            logger.info('inside the span')
            span_context = span.get_span_context()
    finally:
        detach_otel_log_bridge(bridge_id)
    (record,) = probe.logs()
    assert record.trace_id == span_context.trace_id and record.span_id == span_context.span_id


async def test_tel_log_bridge_module_filter_forwards_framework_records_only(
    probe: TelemetryProbe, fake_clock: FakeClock
) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    failer = make_operator('failer', depends_on={EmailDataPoint}, raise_error=ValueError('boom'))
    bridge_id = attach_otel_log_bridge(
        level='WARNING', module_filter='orcastork', logger_provider=probe.logger_provider
    )
    try:
        logger.warning('test-module record — filtered out')
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[failer], seed=[work_email()]
        ).run()
    finally:
        detach_otel_log_bridge(bridge_id)

    assert result.status is SessionStatus.COMPLETED
    assert 'test-module record — filtered out' not in [record.body for record in probe.logs()]
    failure = next(record for record in probe.logs() if dict(record.attributes or {}).get('operator_id') == 'failer')
    assert failure.severity_number is SeverityNumber.ERROR
    assert dict(failure.attributes or {})['exception.type'] == 'ValueError'
    # The failure is logged on the gathering loop, so the record correlates with the gather span.
    (gather_span,) = probe.spans('session.gather')
    assert gather_span.context is not None and failure.span_id == gather_span.context.span_id


async def test_tel_lifecycle_logs_cover_start_progress_and_completion(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=_noop)
    with capture_logs(level='DEBUG') as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[operator, reporter],
            seed=[work_email()],
        ).run()

    assert result.status is SessionStatus.COMPLETED
    messages = [record['message'] for record in records]
    for expected in (
        'Session run started after acquiring the fencing epoch',
        'Seeding the store with initial data points before the first gather',
        'Launching operator for the current gathering iteration',
        'Operator run succeeded',
        'Gathering quiescent; proceeding to aggregation',
        'Aggregation phase started; launching ready aggregators concurrently',
        'Aggregator completed and its contribution marked idempotently',
        'Session completed; durable outputs written and session marked complete',
    ):
        assert expected in messages
    (started,) = [
        record for record in records if record['message'] == 'Session run started after acquiring the fencing epoch'
    ]
    assert started['level'].name == 'INFO'
    assert started['extra'] == {'session_id': SID, 'namespace_id': NAMESPACE, 'epoch': 1, 'flow_name': None}
    (completed,) = [
        record
        for record in records
        if record['message'] == 'Session completed; durable outputs written and session marked complete'
    ]
    assert completed['level'].name == 'INFO'
    assert completed['extra']['operator_runs'] == 2 and completed['extra']['dead_letters'] == 0
    (launched,) = [
        record for record in records if record['message'] == 'Launching operator for the current gathering iteration'
    ]
    assert launched['extra']['operator_id'] == 'op'
    assert launched['extra']['is_rerun'] is False and launched['extra']['is_retry'] is False


async def test_tel_inbox_wait_span_records_a_wakeup(probe: TelemetryProbe, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)

    async def user_acts() -> None:
        for _ in range(5):
            await asyncio.sleep(0)  # give the session time to reach the inbox wait
        await runtime.inbox.append(SID, chat_answer('it was me'))

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
    )
    result, _ = await asyncio.gather(orchestrator.run(), user_acts())

    assert result.status is SessionStatus.COMPLETED
    (wait_span,) = probe.spans('session.inbox_wait')
    assert dict(wait_span.attributes or {})['outcome'] == 'wakeup'  # the arrival ended the wait, not the deadline
    (gather_span,) = probe.spans('session.gather')
    assert probe.parent_of(wait_span) is gather_span
