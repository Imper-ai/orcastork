/**
 * Log capture — collect the records emitted inside a block to assert on levels, fields and errors.
 *
 * The framework's logger is process-global, so the capture installs itself as that logger for the
 * duration of the block and restores whatever was there afterwards — the counterpart of the Python
 * double's loguru sink. Records still reach the logger that was installed before it, so a capture
 * never silences a deployment (or another double) that was already listening.
 *
 * @module
 */

import type { LogFields, Logger, LogLevel } from '../../src/orcastork/logging.js';
import { getLogger, setLogger } from '../../src/orcastork/logging.js';

/** One captured record: what was logged, at which level, with which fields. */
export interface CapturedLog {
  readonly level: LogLevel;
  readonly message: string;
  readonly fields: LogFields;
}

/** Severity order of the four levels the framework's `Logger` emits. */
const LEVEL_ORDER: Readonly<Record<LogLevel, number>> = { DEBUG: 0, INFO: 1, WARNING: 2, ERROR: 3 };

/** Which records a capture keeps. */
export interface CaptureLogsOptions {
  /** The minimum level collected; below it a record is passed through and not kept. */
  readonly level?: LogLevel;
}

/** What a captured block produced: its records, and whatever the block itself returned. */
export interface LogCapture<T> {
  readonly records: readonly CapturedLog[];
  readonly result: T;
}

class CapturingLogger implements Logger {
  private readonly wrapped: Logger;
  private readonly minimum: number;
  public readonly records: CapturedLog[] = [];

  public constructor(wrapped: Logger, level: LogLevel) {
    this.wrapped = wrapped;
    this.minimum = LEVEL_ORDER[level];
  }

  public debug(message: string, fields?: LogFields): void {
    this.wrapped.debug(message, fields);
    this.keep('DEBUG', message, fields);
  }

  public info(message: string, fields?: LogFields): void {
    this.wrapped.info(message, fields);
    this.keep('INFO', message, fields);
  }

  public warning(message: string, fields?: LogFields): void {
    this.wrapped.warning(message, fields);
    this.keep('WARNING', message, fields);
  }

  public error(message: string, fields?: LogFields): void {
    this.wrapped.error(message, fields);
    this.keep('ERROR', message, fields);
  }

  private keep(level: LogLevel, message: string, fields: LogFields | undefined): void {
    if (LEVEL_ORDER[level] >= this.minimum) {
      this.records.push({ level, message, fields: fields ?? {} });
    }
  }
}

/**
 * Run `block` with the framework's logger captured, returning its records and the block's result.
 *
 * The logger is restored even when the block throws, so one failing test cannot leave the process
 * logging into a dead collector.
 */
export const captureLogs = async <T>(
  block: () => Promise<T> | T,
  options: CaptureLogsOptions = {},
): Promise<LogCapture<T>> => {
  const previous = getLogger();
  const capturing = new CapturingLogger(previous, options.level ?? 'WARNING');
  setLogger(capturing);
  try {
    const result = await block();
    return { records: capturing.records, result };
  } finally {
    setLogger(previous);
  }
};
