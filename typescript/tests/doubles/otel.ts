/**
 * OpenTelemetry test probe — per-test providers wired to synchronous in-memory exporters.
 *
 * A fresh probe per test keeps telemetry assertions isolated without touching OTel's
 * process-global providers (which can only be installed once per process). The `Simple*`
 * processors export synchronously — exactly what deterministic assertions need here, and exactly
 * what a production deployment must NOT wire (it uses the batching processors).
 *
 * The one global the probe does install is a context manager, because OTel's default is a no-op
 * that never propagates the active span: without it every span would be a root span and no log
 * record would carry a trace id. A deployment gets the same thing from its SDK setup.
 *
 * @module
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import type { Attributes, Context, ContextManager } from '@opentelemetry/api';
import { context, ROOT_CONTEXT } from '@opentelemetry/api';
import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import { InMemoryLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from '@opentelemetry/sdk-logs';
import type { CollectionResult, DataPoint, ExponentialHistogram, Histogram } from '@opentelemetry/sdk-metrics';
import { MeterProvider, MetricReader } from '@opentelemetry/sdk-metrics';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { Telemetry } from '../../src/orcastork/telemetry.js';

/**
 * A context manager over `AsyncLocalStorage` — what makes span nesting and log correlation real.
 *
 * The OTel JS API ships only a no-op context manager; the one production uses lives in an SDK
 * package this port does not depend on, so the test substrate carries its own four-method
 * implementation rather than adding a dependency for one behaviour.
 */
class AsyncLocalStorageContextManager implements ContextManager {
  private readonly storage = new AsyncLocalStorage<Context>();

  public active(): Context {
    return this.storage.getStore() ?? ROOT_CONTEXT;
  }

  public with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
    activeContext: Context,
    fn: F,
    thisArg?: ThisParameterType<F>,
    ...args: A
  ): ReturnType<F> {
    return this.storage.run(activeContext, () => fn.call(thisArg as ThisParameterType<F>, ...args));
  }

  public bind<T>(activeContext: Context, target: T): T {
    if (typeof target !== 'function') {
      return target;
    }
    const manager = this;
    // biome-ignore lint/suspicious/noExplicitAny: binding an arbitrary callable is what `bind` is.
    const bound = function (this: unknown, ...args: any[]): unknown {
      return manager.with(activeContext, () => (target as (...inner: unknown[]) => unknown).apply(this, args));
    };
    return bound as T;
  }

  public enable(): this {
    return this;
  }

  public disable(): this {
    return this;
  }
}

let contextManagerInstalled = false;

/** Install the context manager once per process; a second call is a no-op, as OTel's registry is. */
const installContextManager = (): void => {
  if (!contextManagerInstalled) {
    context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    contextManagerInstalled = true;
  }
};

/**
 * A reader that collects on demand — the counterpart of Python's `InMemoryMetricReader`.
 *
 * Nothing is exported anywhere: a collection pass is driven by the assertion that needs the
 * numbers, which is what keeps a metric test free of timing.
 */
class CollectingMetricReader extends MetricReader {
  protected override async onForceFlush(): Promise<void> {
    return;
  }

  protected override async onShutdown(): Promise<void> {
    return;
  }
}

/** One collected data point: a counter's number, or a histogram's aggregated value. */
export type MetricPoint = DataPoint<number> | DataPoint<Histogram> | DataPoint<ExponentialHistogram>;

/** Two attribute sets are the same series iff they carry exactly the same keys and values. */
const sameAttributes = (left: Attributes, right: Attributes): boolean => {
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  return leftKeys.length === rightKeys.length && leftKeys.every((key) => left[key] === right[key]);
};

/** `HrTime` is `[seconds, nanos]`; a span started earlier sorts first. */
const beforeInStartOrder = (left: ReadableSpan, right: ReadableSpan): number =>
  left.startTime[0] - right.startTime[0] || left.startTime[1] - right.startTime[1];

/** Per-test tracer/meter/logger providers plus the assertions a telemetry test is written in. */
export class TelemetryProbe {
  public readonly spanExporter = new InMemorySpanExporter();
  public readonly metricReader = new CollectingMetricReader();
  public readonly logExporter = new InMemoryLogRecordExporter();
  public readonly loggerProvider: LoggerProvider;
  public readonly telemetry: Telemetry;

  public constructor() {
    installContextManager();
    const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(this.spanExporter)] });
    this.loggerProvider = new LoggerProvider({
      processors: [new SimpleLogRecordProcessor({ exporter: this.logExporter })],
    });
    this.telemetry = new Telemetry({
      tracerProvider,
      meterProvider: new MeterProvider({ readers: [this.metricReader] }),
      loggerProvider: this.loggerProvider,
    });
  }

  /** Every data point of metric `name` from a fresh collection pass. */
  public async metricPoints(name: string): Promise<readonly MetricPoint[]> {
    const collected: CollectionResult = await this.metricReader.collect();
    const points: MetricPoint[] = [];
    for (const resourceMetrics of [collected.resourceMetrics]) {
      for (const scopeMetrics of resourceMetrics.scopeMetrics) {
        for (const metric of scopeMetrics.metrics) {
          if (metric.descriptor.name === name) {
            points.push(...metric.dataPoints);
          }
        }
      }
    }
    return points;
  }

  /** The aggregated count for one exact series (`0` if never incremented). */
  public async counter(name: string, attributes: Attributes = {}): Promise<number> {
    for (const point of await this.metricPoints(name)) {
      if (sameAttributes(point.attributes, attributes)) {
        return point.value as number;
      }
    }
    return 0;
  }

  /** The count summed across every attribute combination of `name`. */
  public async counterTotal(name: string): Promise<number> {
    const points = await this.metricPoints(name);
    return points.reduce((total, point) => total + (point.value as number), 0);
  }

  /** The histogram value (`.count`/`.sum`/...) for one exact series, or `undefined`. */
  public async histogram(name: string, attributes: Attributes = {}): Promise<Histogram | undefined> {
    for (const point of await this.metricPoints(name)) {
      if (sameAttributes(point.attributes, attributes)) {
        return point.value as Histogram;
      }
    }
    return undefined;
  }

  /** Every metric name that has been recorded, with the unit it was declared with. */
  public async metricUnits(): Promise<ReadonlyMap<string, string>> {
    const collected = await this.metricReader.collect();
    const units = new Map<string, string>();
    for (const scopeMetrics of collected.resourceMetrics.scopeMetrics) {
      for (const metric of scopeMetrics.metrics) {
        units.set(metric.descriptor.name, metric.descriptor.unit);
      }
    }
    return units;
  }

  /** Every metric attribute key ever emitted, across all metrics (cardinality-rule assertions). */
  public async metricAttributeKeys(): Promise<ReadonlySet<string>> {
    const collected = await this.metricReader.collect();
    const keys = new Set<string>();
    for (const scopeMetrics of collected.resourceMetrics.scopeMetrics) {
      for (const metric of scopeMetrics.metrics) {
        for (const point of metric.dataPoints) {
          for (const key of Object.keys(point.attributes)) {
            keys.add(key);
          }
        }
      }
    }
    return keys;
  }

  /** Finished spans in START order (the exporter holds them in end order), optionally by name. */
  public spans(name?: string): readonly ReadableSpan[] {
    const finished = [...this.spanExporter.getFinishedSpans()].sort(beforeInStartOrder);
    return name === undefined ? finished : finished.filter((span) => span.name === name);
  }

  /** The finished span that is `span`'s parent, or `undefined` for a root span. */
  public parentOf(span: ReadableSpan): ReadableSpan | undefined {
    const parentSpanId = span.parentSpanContext?.spanId;
    if (parentSpanId === undefined) {
      return undefined;
    }
    return this.spanExporter.getFinishedSpans().find((candidate) => candidate.spanContext().spanId === parentSpanId);
  }

  /** Every emitted OTel log record, in emission order. */
  public logs(): readonly ReadableLogRecord[] {
    return this.logExporter.getFinishedLogRecords();
  }
}
