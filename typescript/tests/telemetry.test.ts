/**
 * TEL — built-in OpenTelemetry instrumentation: the handles, the spans and the bridged logs.
 *
 * The framework instruments straight against the OTel API; without an SDK every emission is a
 * no-op and behaviour is untouched. These tests inject per-test providers (the `TelemetryProbe`
 * double) so the orchestrator's counters, span tree and bridged log records can be asserted
 * end-to-end through real OTel SDK exporters.
 */

import type { Attributes } from '@opentelemetry/api';
import { SpanStatusCode } from '@opentelemetry/api';
import { SeverityNumber } from '@opentelemetry/api-logs';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  InMemoryCapabilityCatalog,
  InMemoryDataPointStore,
  InMemoryInbox,
} from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import type { CapabilityContext } from '../src/orcastork/capabilities/index.js';
import { Capability, CapabilityActivator, capability } from '../src/orcastork/capabilities/index.js';
import type { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { DataPointView } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { Revision } from '../src/orcastork/ids.js';
import { CapabilityId, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import type { LogFields, Logger } from '../src/orcastork/logging.js';
import { getLogger, setLogger } from '../src/orcastork/logging.js';
import type { OtelLogBridgeOptions } from '../src/orcastork/logging_bridge.js';
import { attachOtelLogBridge, detachOtelLogBridge, severityNumberFor } from '../src/orcastork/logging_bridge.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions } from '../src/orcastork/orchestrator/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import type { ApplyResolvedOptions } from '../src/orcastork/ports/index.js';
import type { OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { buildInMemoryRuntime, OrchestratorRuntime as makeRuntime } from '../src/orcastork/runtime.js';
import { Telemetry, withSpan } from '../src/orcastork/telemetry.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  RiskDataPoint,
  risk,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';
import { TelemetryProbe } from './doubles/otel.js';

const SID = SessionId('tel-session');
const NAMESPACE = NamespaceId('tel-namespace');

/**
 * The metric attributes the orchestrator is allowed to emit — the cardinality rule in executable
 * form: per-session identifiers must never become metric attribute keys.
 */
const LOW_CARDINALITY_LABEL_KEYS: ReadonlySet<string> = new Set([
  'operator_id',
  'capability_id',
  'outcome',
  'status',
  'disposition',
  'kind',
]);

/** An orchestrator over the shared session/namespace, so a test names only what it varies. */
const orchestrate = (options: Omit<OrchestratorOptions, 'sessionId' | 'namespaceId'>): Orchestrator =>
  new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, ...options });

/** An aggregator body that does nothing — the port of the Python module's `_noop`. */
const noop = async (): Promise<void> => undefined;

/** In-memory store that rejects applies containing a marked value (a persistently bad apply). */
class RejectingStore extends InMemoryDataPointStore {
  public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    if ([...options.added, ...options.updated].some((dataPoint) => dataPoint.value === 'merge-bomb')) {
      throw new Error('store rejected the write');
    }
    return await super.applyResolved(sessionId, options);
  }
}

/** Every instrument the framework creates up front, with the unit it declares. */
const INSTRUMENT_UNITS: Readonly<Record<string, string>> = {
  sessions_total: '',
  session_deadline_hits_total: '',
  operator_runs_total: '',
  operator_retries_total: '',
  operator_reruns_total: '',
  data_points_merged_total: '',
  inbox_entries_total: '',
  capability_activations_total: '',
  aggregator_dead_letters_total: '',
  operator_run_seconds: 's',
  session_gather_seconds: 's',
  session_aggregation_seconds: 's',
  archive_flush_entries: '',
};

/** A logger that keeps what it was told, standing in for whatever a deployment has installed. */
class RecordingLogger implements Logger {
  public readonly records: { readonly level: string; readonly message: string }[] = [];

  public debug(message: string, _fields?: LogFields): void {
    this.records.push({ level: 'DEBUG', message });
  }

  public info(message: string, _fields?: LogFields): void {
    this.records.push({ level: 'INFO', message });
  }

  public warning(message: string, _fields?: LogFields): void {
    this.records.push({ level: 'WARNING', message });
  }

  public error(message: string, _fields?: LogFields): void {
    this.records.push({ level: 'ERROR', message });
  }
}

/**
 * Run `block` with a bridge attached over a recording logger, then detach and put back whatever
 * logger the process had — a bridge is process-global, so a test that leaked one would forward
 * every later test's records.
 */
const withBridge = async (
  options: OtelLogBridgeOptions,
  block: (detach: () => void) => Promise<void> | void,
): Promise<RecordingLogger> => {
  const previous = getLogger();
  const recording = new RecordingLogger();
  setLogger(recording);
  const bridgeId = attachOtelLogBridge(options);
  try {
    // The block may detach early to log something that must NOT be forwarded; detaching again in
    // the `finally` is a no-op, and the recording logger stays installed either way.
    await block(() => {
      detachOtelLogBridge(bridgeId);
    });
  } finally {
    detachOtelLogBridge(bridgeId);
    setLogger(previous);
  }
  return recording;
};

let probe: TelemetryProbe;

beforeEach(() => {
  probe = new TelemetryProbe();
});

/** The one span of that name, so an assertion reads as the Python `(span,) = probe.spans(...)`. */
const onlySpan = (name: string): ReadableSpan => {
  const spans = probe.spans(name);
  expect(spans).toHaveLength(1);
  const [span] = spans;
  if (span === undefined) {
    throw new Error(`no span named ${name}`);
  }
  return span;
};

/** The exception event a failed span records — the port of Python's `(event,) = span.events`. */
const onlyExceptionEvent = (span: ReadableSpan): Attributes => {
  expect(span.events).toHaveLength(1);
  const [event] = span.events;
  expect(event?.name).toBe('exception');
  return event?.attributes ?? {};
};

/** The DataPoint types the session holds, as the Python assertions read them. */
const storedTypes = async (runtime: OrchestratorRuntime): Promise<ReadonlySet<string>> =>
  new Set((await runtime.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));

describe('Telemetry', () => {
  it('is inert without an SDK, so a deployment that wires nothing loses nothing', async () => {
    const telemetry = new Telemetry();

    telemetry.sessionsTotal.add(1, { status: 'completed' });
    telemetry.operatorRunSeconds.record(0.01, { operator_id: 'op', outcome: 'succeeded' });
    const recording = await withSpan(telemetry.tracer, 'session.run', (span) => span.isRecording());

    expect(recording).toBe(false); // the global providers are no-ops until an SDK installs real ones
  });

  it('creates every instrument the framework emits, with its name and unit', async () => {
    const { telemetry } = probe;
    telemetry.sessionsTotal.add(1);
    telemetry.sessionDeadlineHitsTotal.add(1);
    telemetry.operatorRunsTotal.add(1);
    telemetry.operatorRetriesTotal.add(1);
    telemetry.operatorRerunsTotal.add(1);
    telemetry.dataPointsMergedTotal.add(1);
    telemetry.inboxEntriesTotal.add(1);
    telemetry.capabilityActivationsTotal.add(1);
    telemetry.aggregatorDeadLettersTotal.add(1);
    telemetry.operatorRunSeconds.record(0.5);
    telemetry.sessionGatherSeconds.record(0.5);
    telemetry.sessionAggregationSeconds.record(0.5);
    telemetry.archiveFlushEntries.record(3);

    expect(Object.fromEntries(await probe.metricUnits())).toEqual(INSTRUMENT_UNITS);
  });

  it('counts and measures under the attributes it was given', async () => {
    probe.telemetry.operatorRunsTotal.add(1, { operator_id: 'op', outcome: 'succeeded' });
    probe.telemetry.operatorRunsTotal.add(1, { operator_id: 'op', outcome: 'failed' });
    probe.telemetry.operatorRunSeconds.record(0.25, { operator_id: 'op', outcome: 'succeeded' });

    expect(await probe.counter('operator_runs_total', { operator_id: 'op', outcome: 'succeeded' })).toBe(1);
    expect(await probe.counter('operator_runs_total', { operator_id: 'op', outcome: 'parked' })).toBe(0);
    expect(await probe.counterTotal('operator_runs_total')).toBe(2);
    const seconds = await probe.histogram('operator_run_seconds', { operator_id: 'op', outcome: 'succeeded' });
    expect(seconds?.count).toBe(1);
    expect(seconds?.sum).toBe(0.25);
    expect(await probe.metricAttributeKeys()).toEqual(new Set(['operator_id', 'outcome']));
  });
});

describe('spans', () => {
  it('nest, and a propagating exception marks the span failed', async () => {
    const { tracer } = probe.telemetry;

    await withSpan(
      tracer,
      'outer',
      async () => {
        await withSpan(tracer, 'inner', () => undefined);
      },
      { attributes: { session_id: 'sid' } },
    );
    await expect(
      withSpan(tracer, 'failing', () => {
        throw new StaleEpochError('boom');
      }),
    ).rejects.toBeInstanceOf(StaleEpochError);

    const outer = onlySpan('outer');
    const failing = onlySpan('failing');
    expect(probe.parentOf(onlySpan('inner'))).toBe(outer);
    expect(outer.parentSpanContext).toBeUndefined();
    expect(outer.attributes).toEqual({ session_id: 'sid' });
    expect(failing.status.code).toBe(SpanStatusCode.ERROR);
    const [event] = failing.events;
    expect(event?.name).toBe('exception');
    expect(event?.attributes?.['exception.type']).toBe('StaleEpochError');
  });
});

describe('the log bridge', () => {
  it('forwards records with their attributes and exception info, and stops at detach', async () => {
    const failure = new StaleEpochError('boom');

    const wrapped = await withBridge({ level: 'DEBUG', loggerProvider: probe.loggerProvider }, (detach) => {
      getLogger().debug('step detail', { step: 1 });
      getLogger().info('flow started', { operator_ids: ['a', 'b'], logger_name: 'orcastork.orchestrator' });
      getLogger().warning('degraded');
      getLogger().error('failed hard', { operator_id: 'op', error: failure });
      detach();
      getLogger().error('after detach — never forwarded');
    });

    const [debug, info, warning, error] = probe.logs();
    expect(debug?.severityNumber).toBe(SeverityNumber.DEBUG);
    expect(debug?.attributes.step).toBe(1);
    expect(info?.severityNumber).toBe(SeverityNumber.INFO);
    expect(info?.body).toBe('flow started');
    expect(info?.attributes.operator_ids).toBe('["a","b"]'); // non-scalar fields are stringified
    expect(info?.attributes['logger.name']).toBe('orcastork.orchestrator');
    expect(warning?.severityNumber).toBe(SeverityNumber.WARN);
    expect(warning?.severityText).toBe('WARNING');
    expect(error?.severityNumber).toBe(SeverityNumber.ERROR);
    expect(error?.attributes.operator_id).toBe('op');
    expect(error?.attributes['exception.type']).toBe('StaleEpochError');
    expect(error?.attributes['exception.message']).toBe('boom');
    expect(String(error?.attributes['exception.stacktrace'])).toContain('StaleEpochError: boom');
    expect(probe.logs().map((record) => record.body)).not.toContain('after detach — never forwarded');
    // The bridge is a passthrough, never a replacement: the wrapped logger saw every record.
    expect(wrapped.records.map((record) => record.message)).toEqual([
      'step detail',
      'flow started',
      'degraded',
      'failed hard',
      'after detach — never forwarded',
    ]);
  });

  it('emits an empty string for a record that names no origin', async () => {
    // OTel attribute values are scalars, so a missing origin must still produce a `logger.name`
    // attribute — a null one is what a strict exporter drops or rejects.
    await withBridge({ loggerProvider: probe.loggerProvider }, () => {
      getLogger().info('nameless origin');
    });

    const [record] = probe.logs();
    expect(record?.attributes['logger.name']).toBe('');
  });

  it('maps every loguru level name, collapsing SUCCESS onto INFO and CRITICAL onto FATAL', () => {
    // SUCCESS and CRITICAL are loguru-specific names a deployment's own logger may still speak.
    // Each maps to its conventional severity number while the original name rides through as
    // severity_text, so a backend can filter by number and recover the originating level.
    expect(severityNumberFor('TRACE')).toBe(SeverityNumber.TRACE);
    expect(severityNumberFor('DEBUG')).toBe(SeverityNumber.DEBUG);
    expect(severityNumberFor('INFO')).toBe(SeverityNumber.INFO);
    expect(severityNumberFor('SUCCESS')).toBe(SeverityNumber.INFO);
    expect(severityNumberFor('WARNING')).toBe(SeverityNumber.WARN);
    expect(severityNumberFor('ERROR')).toBe(SeverityNumber.ERROR);
    expect(severityNumberFor('CRITICAL')).toBe(SeverityNumber.FATAL);
    expect(severityNumberFor('NOTICE')).toBe(SeverityNumber.INFO); // an unknown level falls back
  });

  it('keeps each record severity text as the level that was called', async () => {
    await withBridge({ level: 'DEBUG', loggerProvider: probe.loggerProvider }, () => {
      getLogger().debug('d');
      getLogger().info('i');
      getLogger().warning('w');
      getLogger().error('e');
    });

    expect(probe.logs().map((record) => record.severityText)).toEqual(['DEBUG', 'INFO', 'WARNING', 'ERROR']);
  });

  it('forwards nothing below the level it was attached at', async () => {
    await withBridge({ loggerProvider: probe.loggerProvider }, () => {
      getLogger().debug('below the default INFO floor');
      getLogger().info('at the floor');
    });

    expect(probe.logs().map((record) => record.body)).toEqual(['at the floor']);
  });

  it('forwards only the module tree its filter names', async () => {
    await withBridge({ moduleFilter: 'orcastork', loggerProvider: probe.loggerProvider }, () => {
      getLogger().info('framework record', { logger_name: 'orcastork.orchestrator' });
      getLogger().info('the module itself', { logger_name: 'orcastork' });
      getLogger().info('host record — filtered out', { logger_name: 'app.web' });
      getLogger().info('unattributed — filtered out');
    });

    expect(probe.logs().map((record) => record.body)).toEqual(['framework record', 'the module itself']);
  });

  it('correlates a record with the span that was active when it was logged', async () => {
    let traceId = '';
    let spanId = '';

    await withBridge({ loggerProvider: probe.loggerProvider }, async () => {
      await withSpan(probe.telemetry.tracer, 'work', (span) => {
        getLogger().info('inside the span');
        traceId = span.spanContext().traceId;
        spanId = span.spanContext().spanId;
      });
    });

    const [record] = probe.logs();
    expect(record?.spanContext?.traceId).toBe(traceId);
    expect(record?.spanContext?.spanId).toBe(spanId);
  });

  it('attaches once per process: a second attach is the same bridge, not a second forwarder', async () => {
    const previous = getLogger();
    setLogger(new RecordingLogger());
    const first = attachOtelLogBridge({ loggerProvider: probe.loggerProvider });
    const second = attachOtelLogBridge({ loggerProvider: probe.loggerProvider });
    try {
      getLogger().info('forwarded exactly once');
    } finally {
      detachOtelLogBridge(first);
      setLogger(previous);
    }

    expect(second).toBe(first);
    expect(probe.logs()).toHaveLength(1);
  });

  it('restores the logger it wrapped on detach, and ignores an id that is not its own', () => {
    const previous = getLogger();
    const wrapped = new RecordingLogger();
    setLogger(wrapped);
    const bridgeId = attachOtelLogBridge({ loggerProvider: probe.loggerProvider });
    try {
      expect(getLogger()).not.toBe(wrapped); // the bridge is what the process logs through now
      detachOtelLogBridge(bridgeId + 999); // a stale id from an earlier attach must change nothing
      expect(getLogger()).not.toBe(wrapped);

      detachOtelLogBridge(bridgeId);

      expect(getLogger()).toBe(wrapped);
    } finally {
      detachOtelLogBridge(bridgeId);
      setLogger(previous);
    }
  });
});

describe('session metrics', () => {
  it('is inert without an SDK and leaves the session’s behaviour identical', async () => {
    // No SDK is installed on the process globals, so the default Telemetry() no-ops every emission
    // — a deployment that wires nothing loses nothing.
    const runtime = buildInMemoryRuntime(new FakeClock());
    expect(runtime.telemetry).toBeInstanceOf(Telemetry);
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect([...result.operatorRuns]).toEqual([[OperatorId('op'), 1]]);
    expect(await storedTypes(runtime)).toEqual(new Set(['work_email', 'risk']));
  });

  it('records runs, durations and the session completion on the happy path', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: noop });

    const result = await orchestrate({ runtime, operators: [op, reporter], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const runSeconds = await probe.histogram('operator_run_seconds', { operator_id: 'op', outcome: 'succeeded' });
    expect(runSeconds?.count).toBe(1);
    expect(runSeconds?.sum ?? -1).toBeGreaterThanOrEqual(0);
    expect(await probe.counter('operator_runs_total', { operator_id: 'op', outcome: 'succeeded' })).toBe(1);
    expect(await probe.counter('operator_runs_total', { operator_id: 'rep', outcome: 'succeeded' })).toBe(1);
    expect(await probe.counter('sessions_total', { status: 'completed' })).toBe(1);
    expect(await probe.counter('data_points_merged_total', { kind: 'added' })).toBe(2); // the seed + the risk
    expect(await probe.counter('data_points_merged_total', { kind: 'updated' })).toBe(0); // both were new
    expect((await probe.histogram('session_gather_seconds'))?.count).toBe(1);
    expect((await probe.histogram('session_aggregation_seconds'))?.count).toBe(1);
    expect((await probe.histogram('archive_flush_entries'))?.count).toBe(1);
    // Session/namespace ids never leak into metrics.
    for (const key of await probe.metricAttributeKeys()) {
      expect(LOW_CARDINALITY_LABEL_KEYS.has(key)).toBe(true);
    }
  });

  it('counts one scheduled retry per armed relaunch', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const flaky = makeOperator('flaky', {
      dependsOn: [EmailDataPoint],
      raiseError: new Error('boom'),
      retry: RetryPolicy({ maxAttempts: 3, baseDelayMs: 1_000, jitter: 0 }),
    });

    const result = await orchestrate({ runtime, operators: [flaky], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('flaky'))).toBe(3);
    // Failures 1 and 2 armed a backoff relaunch; failure 3 exhausted the policy (terminal, no retry).
    expect(await probe.counter('operator_retries_total', { operator_id: 'flaky' })).toBe(2);
    expect(await probe.counter('operator_runs_total', { operator_id: 'flaky', outcome: 'failed' })).toBe(3);
  });

  it('counts a rerun launch per consumed debounce window', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    let emitted = 0;
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: 1_000,
      emitFactory: () => {
        const value = `ip-${emitted}`;
        emitted += 1;
        return [ip(value)];
      },
    });

    const result = await orchestrate({ runtime, operators: [selfCycle], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // Every run after the first was a launched rerun, whatever the breaker allowed.
    const expectedReruns = (result.operatorRuns.get(OperatorId('selfloop')) ?? 0) - 1;
    expect(await probe.counter('operator_reruns_total', { operator_id: 'selfloop' })).toBe(expectedReruns);
    expect(expectedReruns).toBeGreaterThanOrEqual(1); // the cycle genuinely reran
  });

  it('counts a quarantined poison inbox entry under its disposition', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const { inbox } = runtime;
    expect(inbox).toBeInstanceOf(InMemoryInbox);
    await (inbox as InMemoryInbox).appendSerialized(SID, 'not-json{');
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(await probe.counter('inbox_entries_total', { disposition: 'quarantined' })).toBe(1);
    expect(await probe.counter('inbox_entries_total', { disposition: 'applied' })).toBe(0);
  });

  it('counts an applied inbox entry under its disposition', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    await runtime.inbox.append(SID, chatAnswer('hello'));

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(await probe.counter('inbox_entries_total', { disposition: 'applied' })).toBe(1);
  });

  it('counts both dispositions of an entry redelivered until it is quarantined', async () => {
    const runtime = makeRuntime({
      ...buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry }),
      store: new RejectingStore(),
    });
    await runtime.inbox.append(SID, chatAnswer('merge-bomb'));

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // keeps the loop draining instead of exiting on the first pass
      sessionDeadlineMs: 30_000,
      maxInboxDeliveries: 3,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(await probe.counter('inbox_entries_total', { disposition: 'redelivered' })).toBe(2); // under the cap
    expect(await probe.counter('inbox_entries_total', { disposition: 'quarantined' })).toBe(1); // the capped one
  });

  it('counts a parked session under the parked status', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // never arrives — the wait stays idle
      sessionDeadlineMs: 300_000,
      parkAfterMs: 30_000,
    }).run();

    expect(result.status).toBe(SessionStatus.PARKED);
    expect(await probe.counter('sessions_total', { status: 'parked' })).toBe(1);
    expect(await probe.counter('sessions_total', { status: 'completed' })).toBe(0);
    expect(await probe.counterTotal('session_deadline_hits_total')).toBe(0); // parking is not a deadline hit
    // The run span carries the same disposition as the counter (set at the same seam).
    expect(onlySpan('session.run').attributes.status).toBe('parked');
    expect(onlySpan('session.inbox_wait').attributes.outcome).toBe('parked'); // the wait span says what ended it
  });

  it('counts, logs and spans a session deadline hit', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });

    const { records, result } = await captureLogs(
      async () =>
        await orchestrate({
          runtime,
          operators: [],
          seed: [workEmail()],
          completesWhen: ChatAnswerDataPoint, // never arrives; only the deadline ends the wait
          sessionDeadlineMs: 30_000,
        }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(await probe.counterTotal('session_deadline_hits_total')).toBe(1);
    expect(onlySpan('session.inbox_wait').attributes.outcome).toBe('deadline');
    const warnings = records.filter((record) => record.message.startsWith('Session deadline hit'));
    expect(warnings).toHaveLength(1);
    expect(warnings[0]?.level).toBe('WARNING');
    expect(warnings[0]?.fields.session_id).toBe(SID);
  });

  it('counts a dead-lettered aggregator’s attempts, retries and dead letter', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async () => {
        throw new Error('durable write failed');
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0, jitter: 0 }),
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(await probe.counter('aggregator_dead_letters_total', { operator_id: 'rep' })).toBe(1);
    expect(await probe.counter('operator_runs_total', { operator_id: 'rep', outcome: 'failed' })).toBe(2);
    expect(await probe.counter('operator_runs_total', { operator_id: 'rep', outcome: 'dead_lettered' })).toBe(1);
    expect(await probe.counter('operator_retries_total', { operator_id: 'rep' })).toBe(1);
    expect((await probe.histogram('operator_run_seconds', { operator_id: 'rep', outcome: 'failed' }))?.count).toBe(2);
  });

  it('counts a succeeded and a terminal capability activation', async () => {
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('goodcap'), CapabilityId('badcap')]]],
    });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog, telemetry: probe.telemetry });
    const goodcap = makeCapability('goodcap');
    const badcap = makeCapability('badcap', { activateError: new Error('no credentials') });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({
      runtime,
      operators: [op],
      capabilities: [goodcap, badcap],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 1 }), // the first activation failure is terminal
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(
      await probe.counter('capability_activations_total', { capability_id: 'goodcap', outcome: 'succeeded' }),
    ).toBe(1);
    expect(await probe.counter('capability_activations_total', { capability_id: 'badcap', outcome: 'terminal' })).toBe(
      1,
    );
    expect(await probe.counter('capability_activations_total', { capability_id: 'badcap', outcome: 'failed' })).toBe(
      0,
    );
  });

  it('counts a cooling-off capability activation failure as failed, not terminal', async () => {
    const clock = new FakeClock();
    const badcap = makeCapability('badcap', { activateErrors: [new Error('first attempt fails')] });
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('badcap')]]] });
    const activator = new CapabilityActivator([[CapabilityId('badcap'), badcap]], catalog, NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 3, baseDelayMs: 1_000, jitter: 0 }),
      telemetry: probe.telemetry,
    });

    await activator.refresh(new DataPointView([]));
    expect(await probe.counter('capability_activations_total', { capability_id: 'badcap', outcome: 'failed' })).toBe(
      1,
    );

    clock.advance(1_000); // the cool-off elapses; the next refresh re-attempts and succeeds
    await activator.refresh(new DataPointView([]));
    expect(
      await probe.counter('capability_activations_total', { capability_id: 'badcap', outcome: 'succeeded' }),
    ).toBe(1);
    expect(await probe.counter('capability_activations_total', { capability_id: 'badcap', outcome: 'terminal' })).toBe(
      0,
    );
  });
});

describe('session spans', () => {
  it('emits a nested span tree for a whole session run', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: noop });

    const result = await orchestrate({ runtime, operators: [op, reporter], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const runSpan = onlySpan('session.run');
    expect(runSpan.parentSpanContext).toBeUndefined();
    // Per-session identifiers ride span attributes (allowed there — no series is created).
    expect(runSpan.attributes).toEqual({
      session_id: SID,
      namespace_id: NAMESPACE,
      epoch: 1,
      status: 'completed',
    });
    const gatherSpan = onlySpan('session.gather');
    const aggregateSpan = onlySpan('session.aggregate');
    expect(probe.parentOf(gatherSpan)).toBe(runSpan);
    expect(probe.parentOf(aggregateSpan)).toBe(runSpan);
    const operatorSpan = onlySpan('operator.run op');
    expect(probe.parentOf(operatorSpan)).toBe(gatherSpan); // launched while the gather span was active
    expect(operatorSpan.attributes).toEqual({ operator_id: 'op', outcome: 'succeeded' });
    const aggregatorSpan = onlySpan('aggregator.run rep');
    expect(probe.parentOf(aggregatorSpan)).toBe(aggregateSpan);
    expect(aggregatorSpan.attributes).toEqual({ operator_id: 'rep', attempt: 1, outcome: 'succeeded' });
    // one trace, no failed spans
    expect(new Set(probe.spans().map((span) => span.spanContext().traceId))).toEqual(
      new Set([runSpan.spanContext().traceId]),
    );
    expect(probe.spans().every((span) => span.status.code === SpanStatusCode.UNSET)).toBe(true);
  });

  it('records the isolated error on a failed operator’s span only', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const failer = makeOperator('failer', { dependsOn: [EmailDataPoint], raiseError: new StaleEpochError('boom') });

    const result = await orchestrate({ runtime, operators: [failer], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the failure is isolated; the session completes
    const operatorSpan = onlySpan('operator.run failer');
    expect(operatorSpan.attributes.outcome).toBe('failed');
    expect(operatorSpan.status.code).toBe(SpanStatusCode.ERROR);
    expect(operatorSpan.status.message).toBe('boom');
    expect(onlyExceptionEvent(operatorSpan)['exception.type']).toBe('StaleEpochError');
    // Isolation holds in the trace too: the failure marks the operator span, never the run span.
    const runSpan = onlySpan('session.run');
    expect(runSpan.attributes.status).toBe('completed');
    expect(runSpan.status.code).toBe(SpanStatusCode.UNSET);
  });

  it('carries the attempt ordinal and a failed outcome on each aggregator attempt span', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async () => {
        throw new StaleEpochError('durable write failed');
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0, jitter: 0 }),
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const attempts = probe.spans('aggregator.run rep');
    expect(attempts).toHaveLength(2);
    const [first, second] = attempts;
    expect(first?.attributes.attempt).toBe(1);
    expect(second?.attributes.attempt).toBe(2);
    for (const span of attempts) {
      expect(span.attributes.outcome).toBe('failed');
      expect(span.status.code).toBe(SpanStatusCode.ERROR); // the raise propagated through the span
      expect(onlyExceptionEvent(span)['exception.type']).toBe('StaleEpochError');
    }
    const aggregateSpan = onlySpan('session.aggregate');
    expect(probe.parentOf(first as ReadableSpan)).toBe(aggregateSpan);
    expect(probe.parentOf(second as ReadableSpan)).toBe(aggregateSpan);
  });

  it('records success and failure on the capability activation spans', async () => {
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('goodcap'), CapabilityId('badcap')]]],
    });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog, telemetry: probe.telemetry });
    const goodcap = makeCapability('goodcap');
    const badcap = makeCapability('badcap', { activateError: new StaleEpochError('no credentials') });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({
      runtime,
      operators: [op],
      capabilities: [goodcap, badcap],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 1 }), // the first activation failure is terminal
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const goodSpan = onlySpan('capability.activate goodcap');
    expect(goodSpan.status.code).toBe(SpanStatusCode.UNSET);
    expect(goodSpan.attributes).toEqual({ capability_id: 'goodcap' });
    const badSpan = onlySpan('capability.activate badcap');
    expect(badSpan.status.code).toBe(SpanStatusCode.ERROR);
    expect(onlyExceptionEvent(badSpan)['exception.type']).toBe('StaleEpochError');
  });

  it('opens the capability action span on the audited seam, inside the calling operator’s run', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('idp')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog, telemetry: probe.telemetry });

    class Idp extends Capability {
      public static readonly capabilityId = CapabilityId('idp');
      public static readonly dependsOn = [EmailDataPoint];

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async sendChallenge(_args: { readonly userId: string }): Promise<string> {
        return 'challenge-sent';
      }
    }
    capability(Idp);

    class Caller extends Operator {
      public static readonly operatorId = OperatorId('caller');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly requires = [Idp];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        const idp = ctx.capabilities.require(Idp);
        yield ChatAnswerDataPoint.emit(await idp.sendChallenge({ userId: 'u-1' }));
      }
    }
    operator(Caller);

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [Caller], capabilities: [Idp], seed: [workEmail()] }).run(),
      { level: 'DEBUG' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const actionSpan = onlySpan('capability.action idp.sendChallenge');
    expect(actionSpan.attributes).toEqual({ capability_id: 'idp', action: 'sendChallenge' });
    // The action nests inside the calling operator's run span.
    expect(probe.parentOf(actionSpan)).toBe(onlySpan('operator.run caller'));
    expect(JSON.stringify(actionSpan.attributes)).not.toContain('u-1'); // argument values never land on the span
    // The audit seams log the activation and the invocation — keys only, mirroring the audit redaction.
    const activated = records.filter(
      (record) => record.message === 'Capability activated for this session; recording the audit entry',
    );
    expect(activated).toHaveLength(1);
    expect(activated[0]?.fields.capability_id).toBe(CapabilityId('idp'));
    const invoked = records.filter((record) => record.message === 'Capability action invoked');
    expect(invoked).toHaveLength(1);
    expect(invoked[0]?.fields.capability_id).toBe(CapabilityId('idp'));
    expect(invoked[0]?.fields.action).toBe('sendChallenge');
    expect(invoked[0]?.fields.parameter_keys).toEqual(['userId']);
    expect(JSON.stringify(invoked[0]?.fields)).not.toContain('u-1'); // argument values never reach the log either
  });

  it('records a wakeup on the inbox wait span', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });

    const userActs = async (): Promise<void> => {
      for (let step = 0; step < 5; step += 1) {
        await new Promise<void>((resolve) => {
          setImmediate(resolve);
        }); // give the session time to reach the inbox wait
      }
      await runtime.inbox.append(SID, chatAnswer('it was me'));
    };
    const orchestrator = orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
    });

    const [result] = await Promise.all([orchestrator.run(), userActs()]);

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The arrival ended the wait, not the deadline.
    expect(onlySpan('session.inbox_wait').attributes.outcome).toBe('wakeup');
    expect(probe.parentOf(onlySpan('session.inbox_wait'))).toBe(onlySpan('session.gather'));
  });
});

describe('session logs', () => {
  it('forwards framework records only when a module filter is set', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry });
    const failer = makeOperator('failer', { dependsOn: [EmailDataPoint], raiseError: new StaleEpochError('boom') });

    let status: SessionStatus | null = null;
    await withBridge(
      { level: 'WARNING', moduleFilter: 'orcastork', loggerProvider: probe.loggerProvider },
      async () => {
        getLogger().warning('test-module record — filtered out');
        status = (await orchestrate({ runtime, operators: [failer], seed: [workEmail()] }).run()).status;
      },
    );

    expect(status).toBe(SessionStatus.COMPLETED);
    expect(probe.logs().map((record) => record.body)).not.toContain('test-module record — filtered out');
    const failure = probe.logs().find((record) => record.attributes.operator_id === 'failer');
    expect(failure?.severityNumber).toBe(SeverityNumber.ERROR);
    expect(failure?.attributes['exception.type']).toBe('StaleEpochError');
    // The failure is logged on the gathering loop, so the record correlates with the gather span.
    expect(failure?.spanContext?.spanId).toBe(onlySpan('session.gather').spanContext().spanId);
  });

  it('covers start, progress and completion in the lifecycle logs', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: noop });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [op, reporter], seed: [workEmail()] }).run(),
      { level: 'DEBUG' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const messages = records.map((record) => record.message);
    for (const expected of [
      'Session run started after acquiring the fencing epoch',
      'Seeding the store with initial data points before the first gather',
      'Launching operator for the current gathering iteration',
      'Operator run succeeded',
      'Gathering quiescent; proceeding to aggregation',
      'Aggregation phase started; launching ready aggregators concurrently',
      'Aggregator completed and its contribution marked idempotently',
      'Session completed; durable outputs written and session marked complete',
    ]) {
      expect(messages).toContain(expected);
    }
    const started = records.filter(
      (record) => record.message === 'Session run started after acquiring the fencing epoch',
    );
    expect(started).toHaveLength(1);
    expect(started[0]?.level).toBe('INFO');
    expect(started[0]?.fields).toEqual({
      logger_name: 'orcastork.orchestrator.orchestrator',
      session_id: SID,
      namespace_id: NAMESPACE,
      epoch: 1,
      flow_name: null,
    });
    const completed = records.filter(
      (record) => record.message === 'Session completed; durable outputs written and session marked complete',
    );
    expect(completed).toHaveLength(1);
    expect(completed[0]?.level).toBe('INFO');
    expect(completed[0]?.fields.operator_runs).toBe(2);
    expect(completed[0]?.fields.dead_letters).toBe(0);
    const launched = records.filter(
      (record) => record.message === 'Launching operator for the current gathering iteration',
    );
    expect(launched).toHaveLength(1);
    expect(launched[0]?.fields.operator_id).toBe(OperatorId('op'));
    expect(launched[0]?.fields.is_rerun).toBe(false);
    expect(launched[0]?.fields.is_retry).toBe(false);
  });
});
