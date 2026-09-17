/**
 * Structured-log → OpenTelemetry bridge — ships the framework's existing logs as OTel log records.
 *
 * The framework logs through `logging.ts` (a fixed message plus structured fields, locally
 * formatted). A deployment that wires an OTel logs pipeline usually wants those same records as
 * backend log entries, correlated with the spans the framework emits — without a second logging
 * call next to every `logger.*` call site. The bridge is that single seam: it wraps the process
 * logger so every record still reaches the logger that was installed before it, and emits the same
 * record through the OTel logs API, with the structured fields as attributes, an `Error` field
 * mapped onto the conventional `exception.*` attribute keys, and the active span's trace/span ids
 * stamped on by the API itself.
 *
 * Process-global by design (the framework's logger is process-global): attach **once** at
 * deployment wiring time, next to provider setup — never per runtime or per session. Where Python
 * adds a second loguru sink per call, this bridge is idempotent: attaching while one is attached
 * hands back the same id rather than forwarding every record twice. The forwarding runs inline on
 * the logging call, so it only does sync, non-blocking work; export happens off-thread in the
 * deployment's batching processor.
 *
 * @module
 */

import type { LogAttributes, LoggerProvider, Logger as OtelLogger } from '@opentelemetry/api-logs';
import { logs, SeverityNumber } from '@opentelemetry/api-logs';
import { OrchestrationError } from './exceptions.js';
import type { LogFields, Logger, LogLevel } from './logging.js';
import { getLogger, setLogger } from './logging.js';

const INSTRUMENTATION_NAME = 'orcastork';

/**
 * The field naming a record's origin module, lifted onto the `logger.name` attribute.
 *
 * Python gets the origin from loguru's record (`__name__` of the emitting module); nothing carries
 * it here, so a record names its own origin with this field when it has one. A record without it
 * still gets a `logger.name` attribute — the empty string, never `null`: OTel attribute values are
 * scalars, and a missing one is what a strict exporter drops or rejects.
 */
export const LOGGER_NAME_FIELD = 'logger_name';

/**
 * The fields an `Error` may ride on, in the order they are consulted.
 *
 * Whichever holds a real `Error` becomes the record's exception (`exception.type`,
 * `exception.message`, `exception.stacktrace`) instead of a plain attribute. A call site that
 * stringifies its error before logging it gets a plain attribute and no `exception.*` keys — pass
 * the `Error` itself to keep the traceback.
 */
export const ERROR_FIELDS = ['error', 'exception'] as const;

/**
 * Loguru level name → OTel severity number.
 *
 * The loguru-only names are kept even though this package's `Logger` emits four of them: a
 * deployment may install a logger of its own that speaks `SUCCESS`, `TRACE` or `CRITICAL`, and the
 * mapping is what a backend filters on. The original name always rides through as `severityText`,
 * so a `SUCCESS` record stays distinguishable from an `INFO` one.
 */
const SEVERITY_NUMBERS: Readonly<Record<string, SeverityNumber>> = {
  TRACE: SeverityNumber.TRACE,
  DEBUG: SeverityNumber.DEBUG,
  INFO: SeverityNumber.INFO,
  SUCCESS: SeverityNumber.INFO,
  WARNING: SeverityNumber.WARN,
  ERROR: SeverityNumber.ERROR,
  CRITICAL: SeverityNumber.FATAL,
};

/** The severity number a level name maps to; an unknown level falls back to `INFO`. */
export const severityNumberFor = (level: string): SeverityNumber => SEVERITY_NUMBERS[level] ?? SeverityNumber.INFO;

/**
 * OTel attribute values are scalars (or sequences thereof); anything richer that rides a log field
 * (a list of operator ids, a mapping) is stringified rather than dropped.
 */
const attributeValue = (value: unknown): string | number | boolean => {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    // A cycle, a `bigint`, a value with a throwing `toJSON`: the attribute is still worth keeping
    // in whatever form the value can express, and a bridge that throws would turn one incident
    // into two.
    return String(value);
  }
};

/** How the bridge forwards: which records, from where, and to which logs pipeline. */
export interface OtelLogBridgeOptions {
  /** The minimum level forwarded. Records below it reach the wrapped logger only. */
  readonly level?: LogLevel;

  /**
   * A module-path prefix (e.g. `'orcastork'`) restricting forwarding to that module tree — for a
   * host application that routes its own records through this logger and only wants the
   * framework's. Matched against the record's {@link LOGGER_NAME_FIELD}; a record that names no
   * origin is forwarded only when no filter is set.
   */
  readonly moduleFilter?: string;

  /** Defaults to the process-global logs provider (inert until an SDK is installed). */
  readonly loggerProvider?: LoggerProvider;
}

/** Whether `name` sits in the module tree `filter` names. */
const withinModule = (name: string, filter: string): boolean => name === filter || name.startsWith(`${filter}.`);

/**
 * The logger the bridge installs: everything reaches the wrapped logger, and qualifying records
 * are additionally emitted through the OTel logs API.
 */
class OtelLogBridge implements Logger {
  private readonly wrapped: Logger;
  private readonly otelLogger: OtelLogger;
  private readonly minimumSeverity: SeverityNumber;
  private readonly moduleFilter: string | undefined;

  public constructor(wrapped: Logger, otelLogger: OtelLogger, options: OtelLogBridgeOptions) {
    this.wrapped = wrapped;
    this.otelLogger = otelLogger;
    this.minimumSeverity = severityNumberFor(options.level ?? 'INFO');
    this.moduleFilter = options.moduleFilter;
  }

  public debug(message: string, fields?: LogFields): void {
    this.record('DEBUG', message, fields);
  }

  public info(message: string, fields?: LogFields): void {
    this.record('INFO', message, fields);
  }

  public warning(message: string, fields?: LogFields): void {
    this.record('WARNING', message, fields);
  }

  public error(message: string, fields?: LogFields): void {
    this.record('ERROR', message, fields);
  }

  /** The wrapped logger first: a bridge must never cost the deployment its local record. */
  private record(level: LogLevel, message: string, fields: LogFields | undefined): void {
    switch (level) {
      case 'DEBUG':
        this.wrapped.debug(message, fields);
        break;
      case 'INFO':
        this.wrapped.info(message, fields);
        break;
      case 'WARNING':
        this.wrapped.warning(message, fields);
        break;
      case 'ERROR':
        this.wrapped.error(message, fields);
        break;
      default: {
        // Exhaustive: every `LogLevel` is handled above, and the `never` is what proves it at
        // compile time. A framework error rather than a bare `Error`, which core never throws.
        const unreachable: never = level;
        throw new OrchestrationError(`unhandled log level: ${String(unreachable)}`);
      }
    }
    this.forward(level, message, fields);
  }

  private forward(level: LogLevel, message: string, fields: LogFields | undefined): void {
    const severityNumber = severityNumberFor(level);
    if (severityNumber < this.minimumSeverity) {
      return;
    }
    const origin = fields?.[LOGGER_NAME_FIELD];
    const loggerName = typeof origin === 'string' ? origin : '';
    if (this.moduleFilter !== undefined && !withinModule(loggerName, this.moduleFilter)) {
      return;
    }
    const errorField = ERROR_FIELDS.find((key) => fields?.[key] instanceof Error);
    const attributes: LogAttributes = { 'logger.name': loggerName };
    for (const [key, value] of Object.entries(fields ?? {})) {
      if (key !== LOGGER_NAME_FIELD && key !== errorField) {
        attributes[key] = attributeValue(value);
      }
    }
    if (errorField !== undefined) {
      const error = fields?.[errorField] as Error;
      attributes['exception.type'] = error.name;
      attributes['exception.message'] = error.message;
      attributes['exception.stacktrace'] = error.stack ?? `${error.name}: ${error.message}`;
    }
    try {
      this.otelLogger.emit({ severityNumber, severityText: level, body: message, attributes });
    } catch {
      // The logs pipeline is an observability concern: a sink that throws must not take the call
      // site down with it, exactly as loguru swallows a failing sink in the Python package.
    }
  }
}

/** The bridge currently installed on the process logger, if any. */
interface Attachment {
  readonly id: number;
  readonly bridge: Logger;
  readonly wrapped: Logger;
}

let attachment: Attachment | undefined;
let nextBridgeId = 1;

/**
 * Attach the bridge; returns its id (pass it to {@link detachOtelLogBridge}).
 *
 * Attaching while a bridge is already attached is a no-op that returns the existing id: wiring it
 * twice would forward every record twice. Detach first to re-attach with different options.
 */
export const attachOtelLogBridge = (options: OtelLogBridgeOptions = {}): number => {
  if (attachment !== undefined) {
    return attachment.id;
  }
  const otelLogger = options.loggerProvider?.getLogger(INSTRUMENTATION_NAME) ?? logs.getLogger(INSTRUMENTATION_NAME);
  const wrapped = getLogger();
  const bridge = new OtelLogBridge(wrapped, otelLogger, options);
  attachment = { id: nextBridgeId, bridge, wrapped };
  nextBridgeId += 1;
  setLogger(bridge);
  return attachment.id;
};

/**
 * Remove a previously attached bridge, restoring the logger it wrapped.
 *
 * An id that is not the attached bridge's is a no-op, and so is detaching after something else has
 * taken over the process logger: whoever installed that logger owns it now, and clobbering it
 * would silence a deployment on the way out of a test.
 */
export const detachOtelLogBridge = (bridgeId: number): void => {
  if (attachment === undefined || attachment.id !== bridgeId) {
    return;
  }
  if (getLogger() === attachment.bridge) {
    setLogger(attachment.wrapped);
  }
  attachment = undefined;
};

/** Whether a bridge is currently attached to the process logger. */
export const isOtelLogBridgeAttached = (): boolean => attachment !== undefined;
