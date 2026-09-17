/**
 * TEL — built-in OpenTelemetry instrumentation: the handles, the spans and the bridged logs.
 *
 * The framework instruments straight against the OTel API; without an SDK every emission is a
 * no-op and behaviour is untouched. These tests inject per-test providers (the `TelemetryProbe`
 * double) so what the framework emits can be asserted through real OTel SDK exporters. The cases
 * that drive a whole session belong with the orchestrator; what is here is the seam itself.
 */

import { SpanStatusCode } from '@opentelemetry/api';
import { SeverityNumber } from '@opentelemetry/api-logs';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { beforeEach, describe, expect, it } from 'vitest';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { LogFields, Logger } from '../src/orcastork/logging.js';
import { getLogger, setLogger } from '../src/orcastork/logging.js';
import type { OtelLogBridgeOptions } from '../src/orcastork/logging_bridge.js';
import { attachOtelLogBridge, detachOtelLogBridge, severityNumberFor } from '../src/orcastork/logging_bridge.js';
import { Telemetry, withSpan } from '../src/orcastork/telemetry.js';
import { TelemetryProbe } from './doubles/otel.js';

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
