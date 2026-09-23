import process from 'node:process';
import { describe, expect, it, vi } from 'vitest';
import { ConsoleJsonLogger, getLogger, type LogFields, type Logger, setLogger } from '../src/orcastork/logging.js';

/** A logger that keeps what it was told, so a test can assert on records instead of on stderr. */
class RecordingLogger implements Logger {
  public readonly records: { level: string; message: string; fields: LogFields | undefined }[] = [];

  public debug(message: string, fields?: LogFields): void {
    this.records.push({ level: 'DEBUG', message, fields });
  }

  public info(message: string, fields?: LogFields): void {
    this.records.push({ level: 'INFO', message, fields });
  }

  public warning(message: string, fields?: LogFields): void {
    this.records.push({ level: 'WARNING', message, fields });
  }

  public error(message: string, fields?: LogFields): void {
    this.records.push({ level: 'ERROR', message, fields });
  }
}

const captureStderr = (emit: () => void): string[] => {
  const lines: string[] = [];
  const write = vi.spyOn(process.stderr, 'write').mockImplementation((chunk: unknown) => {
    lines.push(String(chunk));
    return true;
  });
  try {
    emit();
  } finally {
    write.mockRestore();
  }
  return lines;
};

describe('ConsoleJsonLogger', () => {
  it('writes one JSON line per record to stderr', () => {
    const logger = new ConsoleJsonLogger();

    const lines = captureStderr(() => {
      logger.info('Session started', { session_id: 'abc' });
    });

    expect(lines).toHaveLength(1);
    expect(lines[0]?.endsWith('\n')).toBe(true);
    const record = JSON.parse(String(lines[0])) as Record<string, unknown>;
    expect(record.level).toBe('INFO');
    expect(record.message).toBe('Session started');
    expect(record.session_id).toBe('abc');
    expect(typeof record.timestamp).toBe('string');
  });

  it('carries the level of the method that was called', () => {
    const logger = new ConsoleJsonLogger();

    const lines = captureStderr(() => {
      logger.debug('d');
      logger.info('i');
      logger.warning('w');
      logger.error('e');
    });

    const levels = lines.map((line) => (JSON.parse(line) as { level: string }).level);
    expect(levels).toEqual(['DEBUG', 'INFO', 'WARNING', 'ERROR']);
  });

  it('never throws on a field JSON cannot express', () => {
    const logger = new ConsoleJsonLogger();
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;

    const lines = captureStderr(() => {
      logger.error('Operator failed', { cyclic, big: 10n });
    });

    const record = JSON.parse(String(lines[0])) as Record<string, unknown>;
    expect(record.message).toBe('Operator failed');
  });
});

describe('the process logger', () => {
  it('defaults to the console JSON logger', () => {
    expect(getLogger()).toBeInstanceOf(ConsoleJsonLogger);
  });

  it('can be replaced for the process', () => {
    const previous = getLogger();
    const recording = new RecordingLogger();
    try {
      setLogger(recording);
      getLogger().info('Session started', { session_id: 'abc' });

      expect(recording.records).toEqual([
        { level: 'INFO', message: 'Session started', fields: { session_id: 'abc' } },
      ]);
    } finally {
      setLogger(previous);
    }
  });
});
