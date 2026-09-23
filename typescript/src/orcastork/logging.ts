/**
 * Structured logging seam.
 *
 * The framework logs a **fixed message plus structured fields** — never an interpolated string —
 * so a record is greppable by message and filterable by field, exactly as the Python package's
 * `logger.info('Session started', session_id=sid)` calls are. Field names stay the Python
 * snake_case names wherever a record is something an operator team already greps for.
 *
 * The default sink writes one JSON object per line to stderr, which is what a container runtime
 * collects. A deployment replaces it with {@link setLogger} — the bridge that ships records to
 * `@opentelemetry/api-logs` is wired there too, and nowhere near a call site.
 *
 * @module
 */

import process from 'node:process';

/** Structured fields attached to a log record. */
export type LogFields = Readonly<Record<string, unknown>>;

/** The framework's logging surface — the only thing core code is allowed to log through. */
export interface Logger {
  /** Detail useful when reconstructing a run; off in most deployments. */
  debug(message: string, fields?: LogFields): void;

  /** A lifecycle fact worth keeping: a session started, an operator ran, a lease changed hands. */
  info(message: string, fields?: LogFields): void;

  /** Something recoverable happened that a human should eventually look at. */
  warning(message: string, fields?: LogFields): void;

  /** A failure the framework handled (a fault-isolated operator, a dead-lettered aggregator). */
  error(message: string, fields?: LogFields): void;
}

/** Severity label carried on a {@link ConsoleJsonLogger} record; the Python (loguru) level names. */
export type LogLevel = 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';

/**
 * The default logger: one JSON object per line on stderr.
 *
 * stderr rather than stdout because a library must never pollute the embedding application's
 * data output, and one line per record because that is what a log collector can parse without
 * being told anything about the framework. A value that cannot be serialized (a cycle, a
 * `bigint`) is replaced rather than thrown over: a logger that throws would turn an incident
 * into two.
 */
export class ConsoleJsonLogger implements Logger {
  public debug(message: string, fields?: LogFields): void {
    this.emit('DEBUG', message, fields);
  }

  public info(message: string, fields?: LogFields): void {
    this.emit('INFO', message, fields);
  }

  public warning(message: string, fields?: LogFields): void {
    this.emit('WARNING', message, fields);
  }

  public error(message: string, fields?: LogFields): void {
    this.emit('ERROR', message, fields);
  }

  private emit(level: LogLevel, message: string, fields?: LogFields): void {
    // The wall clock, deliberately: a log record is an observation of the host, not engine state,
    // and a test that fast-forwards a `FakeClock` must not make its own logs claim to be older.
    process.stderr.write(`${serialize(new Date().toISOString(), level, message, fields)}\n`);
  }
}

/** A `bigint` has no JSON form; a log field is not worth an exception, so it is stringified. */
const jsonSafe = (_key: string, value: unknown): unknown => (typeof value === 'bigint' ? value.toString() : value);

/** Best-effort JSON for one record — never throws, whatever a caller put in a field. */
const serialize = (timestamp: string, level: LogLevel, message: string, fields?: LogFields): string => {
  try {
    return JSON.stringify({ timestamp, level, message, ...fields }, jsonSafe);
  } catch {
    // A field held something JSON cannot express at all (a cycle): the record still carries the
    // part every consumer greps for rather than being lost with the field that broke it.
    return JSON.stringify({ timestamp, level, message });
  }
};

let current: Logger = new ConsoleJsonLogger();

/** The logger every call site reads; the process default until {@link setLogger} replaces it. */
export const getLogger = (): Logger => current;

/**
 * Install the process-wide logger.
 *
 * Process-global by design, like loguru's logger in the Python package: wire it once at
 * deployment start-up, never per runtime or per session, or a record is emitted once per wiring.
 */
export const setLogger = (logger: Logger): void => {
  current = logger;
};
