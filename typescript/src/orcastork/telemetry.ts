/**
 * The framework's OpenTelemetry instrumentation handles — OTel **is** the telemetry standard here.
 *
 * OpenTelemetry is already the multi-backend abstraction (providers/exporters decide where the
 * data goes), so the framework instruments straight against the OTel API instead of wrapping it
 * in a port. Without an SDK wired, every API call is a no-op — a deployment that configures
 * nothing loses nothing, exactly as before. A deployment that wants the data configures providers
 * through the OTel SDK packages (globally, or injected here explicitly) and **must use batching
 * exporters** (`BatchSpanProcessor`, `PeriodicExportingMetricReader`, `BatchLogRecordProcessor`):
 * the orchestrator emits inline on its single gathering loop, so nothing may await, lock or
 * perform I/O on the calling path — the batching processors buffer locally and export off-thread,
 * which is what keeps a telemetry outage an observability problem and never a correctness one.
 *
 * {@link Telemetry} bundles the tracer, the OTel logger and every metric instrument the framework
 * emits, created once up front (re-creating instruments per emission would make the SDK warn about
 * duplicates). It rides the runtime like every other injected dependency, so tests pin providers
 * with in-memory exporters while production simply defaults to the process globals.
 *
 * **Metric label cardinality rule:** metric attributes must stay LOW-cardinality — values drawn
 * from small, closed sets such as `operator_id`, `capability_id`, `outcome`, `kind` or
 * `disposition`. Per-session identifiers (`session_id`, `namespace_id`) are NOT metric attributes:
 * one time series per session would explode the backend's series count. Span and log attributes
 * are the opposite case — each record stands alone, so per-session identifiers belong there; they
 * are what "find this session's trace" is built from.
 *
 * @module
 */

import type {
  Attributes,
  Counter,
  Histogram,
  Meter,
  MeterProvider,
  Span,
  Tracer,
  TracerProvider,
} from '@opentelemetry/api';
import { metrics, SpanStatusCode, trace } from '@opentelemetry/api';
import type { LoggerProvider, Logger as OtelLogger } from '@opentelemetry/api-logs';
import { logs } from '@opentelemetry/api-logs';

/** The instrumentation scope every span, metric and log record of the framework is created under. */
const INSTRUMENTATION_NAME = 'orcastork';

/**
 * Where {@link Telemetry} takes its tracer, meter and logger from.
 *
 * Every field is optional and resolves to the process global (the OTel default), which is inert
 * until a deployment installs an SDK. A test pins per-test providers; an already-resolved
 * `tracer`/`meter`/`logger` may be handed in directly for a double that is not a full provider.
 */
export interface TelemetryOptions {
  readonly tracerProvider?: TracerProvider;
  readonly meterProvider?: MeterProvider;
  readonly loggerProvider?: LoggerProvider;
  readonly tracer?: Tracer;
  readonly meter?: Meter;
  readonly logger?: OtelLogger;
}

/**
 * Tracer + logger + the framework's metric instruments, resolved once from the providers.
 *
 * Omitted providers resolve to the process globals (the OTel default), which are inert no-ops
 * until a deployment installs an SDK — so a bare `new Telemetry()` is always safe.
 */
export class Telemetry {
  /** Every span the framework opens is opened on this tracer. */
  public readonly tracer: Tracer;

  /** The OTel logs API handle the log bridge ships framework records through. */
  public readonly logger: OtelLogger;

  public readonly sessionsTotal: Counter;
  public readonly sessionDeadlineHitsTotal: Counter;
  public readonly operatorRunsTotal: Counter;
  public readonly operatorRetriesTotal: Counter;
  public readonly operatorRerunsTotal: Counter;
  public readonly dataPointsMergedTotal: Counter;
  public readonly inboxEntriesTotal: Counter;
  public readonly capabilityActivationsTotal: Counter;
  public readonly aggregatorDeadLettersTotal: Counter;

  /** Seconds, not milliseconds: the unit is part of the metric's contract with the backend. */
  public readonly operatorRunSeconds: Histogram;
  public readonly sessionGatherSeconds: Histogram;
  public readonly sessionAggregationSeconds: Histogram;
  public readonly archiveFlushEntries: Histogram;

  public constructor(options: TelemetryOptions = {}) {
    this.tracer =
      options.tracer ??
      options.tracerProvider?.getTracer(INSTRUMENTATION_NAME) ??
      trace.getTracer(INSTRUMENTATION_NAME);
    this.logger =
      options.logger ??
      options.loggerProvider?.getLogger(INSTRUMENTATION_NAME) ??
      logs.getLogger(INSTRUMENTATION_NAME);
    const meter =
      options.meter ?? options.meterProvider?.getMeter(INSTRUMENTATION_NAME) ?? metrics.getMeter(INSTRUMENTATION_NAME);
    this.sessionsTotal = meter.createCounter('sessions_total');
    this.sessionDeadlineHitsTotal = meter.createCounter('session_deadline_hits_total');
    this.operatorRunsTotal = meter.createCounter('operator_runs_total');
    this.operatorRetriesTotal = meter.createCounter('operator_retries_total');
    this.operatorRerunsTotal = meter.createCounter('operator_reruns_total');
    this.dataPointsMergedTotal = meter.createCounter('data_points_merged_total');
    this.inboxEntriesTotal = meter.createCounter('inbox_entries_total');
    this.capabilityActivationsTotal = meter.createCounter('capability_activations_total');
    this.aggregatorDeadLettersTotal = meter.createCounter('aggregator_dead_letters_total');
    this.operatorRunSeconds = meter.createHistogram('operator_run_seconds', { unit: 's' });
    this.sessionGatherSeconds = meter.createHistogram('session_gather_seconds', { unit: 's' });
    this.sessionAggregationSeconds = meter.createHistogram('session_aggregation_seconds', { unit: 's' });
    this.archiveFlushEntries = meter.createHistogram('archive_flush_entries');
  }
}

/** Attributes to stamp on a span at creation; per-session identifiers belong here, not on metrics. */
export interface WithSpanOptions {
  readonly attributes?: Attributes;
}

/**
 * Run `operation` inside a span that is the active one for its duration.
 *
 * The port of Python's `with tracer.start_as_current_span(...)`: the span ends however the block
 * leaves, and an exception that **propagates** out of it is recorded on the span and marks it
 * failed. A failure the framework isolates (an operator's, say) never propagates, so those call
 * sites mark their span themselves — exactly as the Python ones do.
 *
 * Nesting follows the OTel context, so a span opened inside `operation` parents under this one
 * without anything being threaded through by hand. That requires the deployment to have installed
 * a context manager (any OTel SDK setup does); without one every span is a root span, which
 * degrades the trace and nothing else.
 */
export const withSpan = async <T>(
  tracer: Tracer,
  name: string,
  operation: (span: Span) => Promise<T> | T,
  options: WithSpanOptions = {},
): Promise<T> =>
  await tracer.startActiveSpan(name, { attributes: options.attributes ?? {} }, async (span: Span): Promise<T> => {
    try {
      return await operation(span);
    } catch (error) {
      span.recordException(error instanceof Error ? error : String(error));
      span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
      throw error;
    } finally {
      span.end();
    }
  });
