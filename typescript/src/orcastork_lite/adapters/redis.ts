/**
 * Redis Stream `SessionEventSink` — one stream per session, one entry per event.
 *
 * Consumers `XREAD`/`XRANGE` `<keyPrefix><sessionId>` to follow a session while it runs (e.g. to
 * act on a DataPoint the moment it lands rather than when the session ends). Each entry carries
 * the event `kind` as its own field, so a consumer can filter without parsing, plus the full event
 * as JSON. Two bounds keep the keyspace finite: the stream is capped at `maxlen` entries
 * (approximate trimming, the cheap kind), and every publish re-arms a sliding `ttlMs` on the key,
 * so a live session keeps its stream while a finished or abandoned one disappears on its own —
 * without the TTL, one key per session ever run would stay behind forever. Both land in one
 * pipelined round trip.
 *
 * The payload is the Python wire format verbatim — snake_case field names, in the order the
 * pydantic models declare them, and ISO-8601 instants — so a Python consumer reads a stream a
 * TypeScript worker wrote, and the other way round.
 *
 * @module
 */

import type { RedisClientType } from 'redis';
import type { SessionEvent, SessionEventSink } from '../events.js';
import { SESSION_EVENT_KIND } from '../events.js';
import type { SessionId } from '../ids.js';

/** Stream key prefix; the session id is appended to it. */
export const DEFAULT_KEY_PREFIX = 'orcastork_lite:events:';

/** Entries kept per session stream, trimmed approximately. */
export const DEFAULT_MAXLEN = 10_000;

/** How long a session's stream outlives its last event: 24 hours. */
export const DEFAULT_TTL_MS = 86_400_000;

/**
 * The slice of the node-redis client this sink drives.
 *
 * Structural rather than the whole `RedisClientType`, so a client built with other modules, a
 * different RESP version or a custom type mapping is still accepted — while the two commands the
 * sink issues stay typed by the client library itself rather than by a hand-written declaration.
 */
export type RedisStreamClient = Pick<RedisClientType, 'xAdd' | 'expire'>;

/** How a {@link RedisSessionEventSink} differs from the defaults; every part is optional. */
export interface RedisSessionEventSinkOptions {
  /** Stream key prefix (default {@link DEFAULT_KEY_PREFIX}). */
  readonly keyPrefix?: string;

  /** Approximate entry cap per stream; `null` leaves the stream untrimmed. */
  readonly maxlen?: number | null;

  /** Sliding key TTL in milliseconds; `null` opts out, for a deployment that trims the keyspace itself. */
  readonly ttlMs?: number | null;
}

/** Appends every event to its session's Redis stream, capped and with a sliding TTL. */
export class RedisSessionEventSink implements SessionEventSink {
  private readonly redis: RedisStreamClient;

  private readonly keyPrefix: string;

  private readonly maxlen: number | null;

  private readonly ttlMs: number | null;

  public constructor(redis: RedisStreamClient, options: RedisSessionEventSinkOptions = {}) {
    this.redis = redis;
    this.keyPrefix = options.keyPrefix ?? DEFAULT_KEY_PREFIX;
    // `??` would read an explicit `null` — "do not bound this" — as "use the default", so both
    // knobs distinguish an omitted option from one deliberately turned off.
    this.maxlen = options.maxlen === undefined ? DEFAULT_MAXLEN : options.maxlen;
    this.ttlMs = options.ttlMs === undefined ? DEFAULT_TTL_MS : options.ttlMs;
  }

  /** The stream one session's events land on. */
  public streamKey(sessionId: SessionId): string {
    return `${this.keyPrefix}${sessionId}`;
  }

  public async publish(event: SessionEvent): Promise<void> {
    const key = this.streamKey(event.sessionId);
    // A DataPoint value is whatever the flow chose; anything JSON cannot express is rendered
    // rather than failing the publish, since the stream is a view of the session, not its record.
    const payload = JSON.stringify(wireEvent(event), renderUnserializable);
    // Issued without awaiting in between, which is how node-redis writes both commands to the
    // socket in one flush — the counterpart of the Python adapter's non-transactional pipeline.
    // Nothing here needs MULTI: the two commands are independent and a lost one costs a view, not
    // a record.
    const pending: Promise<unknown>[] = [
      this.redis.xAdd(key, '*', { kind: event.kind, event: payload }, trimOption(this.maxlen)),
    ];
    if (this.ttlMs !== null) {
      pending.push(this.redis.expire(key, expirySeconds(this.ttlMs)));
    }
    await Promise.all(pending);
  }
}

/** `XADD … MAXLEN ~ n`: approximate trimming, the cheap kind. `null` adds no trim clause at all. */
const trimOption = (
  maxlen: number | null,
): { readonly TRIM: { strategy: 'MAXLEN'; strategyModifier: '~'; threshold: number } } | undefined =>
  maxlen === null ? undefined : { TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: maxlen } };

/**
 * The TTL as `EXPIRE` takes it: whole seconds, exactly as redis-py converts a `timedelta`.
 *
 * Rounded up rather than truncated, because truncating a sub-second window to `0` would tell
 * Redis to delete the key instead of to expire it shortly.
 */
const expirySeconds = (ttlMs: number): number => Math.ceil(ttlMs / 1000);

/**
 * The instant as Python renders it: ISO-8601, UTC, `Z`-suffixed.
 *
 * pydantic prints no fractional part when the microseconds are zero while `toISOString()` always
 * prints three digits, so the whole-second instants a clock-driven session produces serialize
 * byte-for-byte the way the Python sink serializes them.
 */
const isoInstant = (at: Date): string => at.toISOString().replace(/\.000Z$/, 'Z');

/**
 * One event as its Python model dumps: snake_case keys, in the order the model declares them.
 *
 * The switch is exhaustive over the four kinds — a fifth event would fail to compile here, which
 * is the point of writing the mapping out rather than transforming keys generically.
 */
const wireEvent = (event: SessionEvent): Record<string, unknown> => {
  const base = {
    session_id: event.sessionId,
    namespace_id: event.namespaceId,
    at: isoInstant(event.at),
    kind: event.kind,
  };
  switch (event.kind) {
    case SESSION_EVENT_KIND.DATA_POINT_MERGED:
      return {
        ...base,
        data_point_type: event.dataPointType,
        value: event.value,
        retrieved_by: event.retrievedBy,
        merge: event.merge,
        revision: event.revision,
      };
    case SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED:
      return {
        ...base,
        operator_id: event.operatorId,
        outcome: event.outcome,
        attempt: event.attempt,
        error: event.error,
      };
    case SESSION_EVENT_KIND.CAPABILITY_ACTIVATED:
      return { ...base, capability_id: event.capabilityId, outcome: event.outcome };
    case SESSION_EVENT_KIND.SESSION_COMPLETED:
      return {
        ...base,
        deadline_hit: event.deadlineHit,
        // A Python consumer reads these as the `dict[OperatorId, …]` they are on the other side.
        operator_runs: Object.fromEntries(event.operatorRuns),
        failures: Object.fromEntries(event.failures),
      };
  }
};

/** Whether `JSON.stringify` can express this object as it stands — an array or a bare record. */
const isPlainObject = (value: object): boolean => {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

/**
 * A value JSON has no form for, as text — the counterpart of Python's `fallback=repr`.
 *
 * An instance of a class renders through its own `toString`, which is what makes a value object
 * recognizable in the stream rather than `{}` (what `JSON.stringify` would otherwise write) or a
 * dropped field.
 */
const asText = (value: unknown): string => {
  switch (typeof value) {
    case 'undefined':
      return 'undefined';
    case 'bigint':
      return `${value}n`;
    case 'function':
      return `[Function: ${value.name === '' ? 'anonymous' : value.name}]`;
    case 'symbol':
      return value.toString();
    default:
      return String(value);
  }
};

/**
 * The `JSON.stringify` replacer that keeps a publish from failing on an unserializable value.
 *
 * `toJSON` has already run by the time a replacer sees a value, so a type that knows how to
 * serialize itself (a `Date`, a value object with a `toJSON`) reaches the stream in its own form
 * and only what JavaScript genuinely cannot express is rendered as text.
 */
const renderUnserializable = (_key: string, value: unknown): unknown => {
  if (value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'object') {
    return Array.isArray(value) || isPlainObject(value) ? value : asText(value);
  }
  return asText(value);
};
