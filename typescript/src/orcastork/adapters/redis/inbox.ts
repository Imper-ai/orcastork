/**
 * Redis `Inbox` — a per-session Stream + consumer group (at-least-once, ordered).
 *
 * `append` → `XADD` + a pub/sub wakeup that `waitForEntry` subscribes to (push, not polling; the
 * durable entry is the XADD — pub/sub only bounds wake latency); `consume` → `XREADGROUP` claims
 * new entries; `reclaim` → `XAUTOCLAIM` re-presents pending (un-acked) entries after a crash, with
 * the delivery count from `XPENDING` (poison surfacing); `ack` → epoch-guarded `XACK` after the
 * durable apply. Sessions are isolated by their stream key.
 *
 * Delivery tolerates a bad wire payload only: a missing payload field or bytes that do not decode
 * as JSON are surfaced as a {@link PoisonInboxEntry} instead of raising, while semantic
 * deserialization failures (an unknown DataPoint type, a validation error) propagate.
 * `quarantine` disposes of an entry atomically (epoch-guarded `XACK` + a JSON record pushed onto
 * `quarantine:{session}`), and `quarantined` reads those records back.
 *
 * @module
 */

import type { AnyDataPoint } from '../../datapoints/index.js';
import { parseDataPoint } from '../../datapoints/index.js';
import { StaleEpochError } from '../../exceptions.js';
import type { Epoch, SessionId } from '../../ids.js';
import { Deferred } from '../../internal/deferred.js';
import type { DeliveredInboxEntry } from '../../ports/change_set.js';
import { InboxEntry, PoisonInboxEntry, QuarantinedEntry } from '../../ports/change_set.js';
import type { ConsumeOptions, Inbox, QuarantineOptions } from '../../ports/inbox.js';
import type { RedisEvalCommand, RedisExpireCommand } from './ttl.js';
import { DEFAULT_STATE_TTL_MS, slideTtl } from './ttl.js';

/** One entry as a stream read hands it back. */
export interface RedisStreamMessage {
  readonly id: string;
  readonly message: Record<string, string>;
}

/** One pending entry's delivery bookkeeping, as `XPENDING` range hands it back. */
export interface RedisPendingEntry {
  readonly id: string;
  readonly deliveriesCounter: number;
}

/** The dedicated connection {@link RedisStreamsInbox.waitForEntry} runs its pub/sub wait on. */
export interface RedisSubscriberClient {
  connect(): Promise<unknown>;
  subscribe(channel: string, listener: (message: string, channel: string) => unknown): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  close(): Promise<unknown>;
  destroy(): void;
  on(event: string, listener: (...args: unknown[]) => void): unknown;
}

/** The commands this inbox needs. */
export interface RedisInboxClient extends RedisEvalCommand, RedisExpireCommand {
  get(key: string): Promise<string | null>;
  lRange(key: string, start: number, stop: number): Promise<string[]>;
  publish(channel: string, message: string): Promise<unknown>;
  xAdd(key: string, id: string, message: Record<string, string>): Promise<string>;
  xLen(key: string): Promise<number>;
  xGroupCreate(key: string, group: string, id: string, options: { MKSTREAM: boolean }): Promise<unknown>;
  xReadGroup(
    group: string,
    consumer: string,
    streams: { key: string; id: string },
    options?: { COUNT: number },
  ): Promise<readonly { messages: readonly RedisStreamMessage[] }[] | null>;
  xAutoClaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    start: string,
  ): Promise<{ messages: readonly (RedisStreamMessage | null)[] }>;
  xPendingRange(
    key: string,
    group: string,
    start: string,
    end: string,
    count: number,
  ): Promise<readonly RedisPendingEntry[]>;
  xRange(key: string, start: string, end: string): Promise<readonly RedisStreamMessage[]>;

  /** A connection of this one's own, optionally reconfigured — pub/sub needs its own socket. */
  duplicate(overrides: { pingInterval: number }): RedisSubscriberClient;
}

const GROUP = 'orchestrator';

const CONSUMER = 'orchestrator';

/** The stream field the DataPoint payload travels in. */
const PAYLOAD_FIELD = 'data';

/**
 * How often the wakeup connection is kept busy, so an idle session never looks like a dead one.
 *
 * The deployment's client carries a read deadline (`socketTimeout` here, `RedisConfig.socket_timeout`
 * in Python, 30s in that fleet). A session that simply receives no input for longer than that would
 * have its waiter die on the deadline rather than keep its place — and the orchestrator propagates a
 * failed waiter. Python keeps every read shorter than the deadline by polling the subscription on
 * this interval; node-redis is push-driven and has no read to bound, so the same guarantee is bought
 * by pinging the connection on this interval instead. Either way it must stay comfortably below the
 * deployment's deadline, and node-redis is the stricter of the two: it treats the deadline as fatal
 * and does not reconnect, so nothing recovers a connection that was allowed to idle into it.
 */
const WAKEUP_POLL_TIMEOUT_MS = 1000;

// Atomically epoch-guard the ack: reject a stale epoch, else bump the inbox epoch and XACK,
// counting the ack only if it removed a pending entry. KEYS = epoch, stream, acked; ARGV =
// epoch, group, entry_id.
const ACK_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local acked = redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])
if acked == 1 then redis.call('INCR', KEYS[3]) end
return acked
`;

// Atomically epoch-guard the quarantine: reject a stale epoch, else bump the inbox epoch and
// XACK the entry out of pending delivery; iff that removed it, count the ack (pending_count
// bookkeeping) and push the durable quarantine record — so a fenced predecessor can neither
// drop the entry nor double-record it. KEYS = epoch, stream, acked, quarantine; ARGV = epoch,
// group, entry_id, record_json.
const QUARANTINE_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local acked = redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])
if acked == 1 then
  redis.call('INCR', KEYS[3])
  redis.call('RPUSH', KEYS[4], ARGV[4])
end
return acked
`;

/** The durable quarantine record, with the snake_case keys both runtimes read. */
interface QuarantineRecord {
  readonly entry_id: string;
  readonly reason: string;
  readonly delivery_count: number;
  readonly raw_payload: string | null;
}

/** How a {@link RedisStreamsInbox} is configured. */
export interface RedisStreamsInboxOptions {
  /** The sliding lifetime of the session's stream and inbox bookkeeping. */
  readonly stateTtlMs?: number;
}

/** The session's durable front door, held in a Redis Stream. */
export class RedisStreamsInbox implements Inbox {
  private readonly redis: RedisInboxClient;
  private readonly stateTtlMs: number;

  public constructor(redis: RedisInboxClient, options: RedisStreamsInboxOptions = {}) {
    this.redis = redis;
    this.stateTtlMs = options.stateTtlMs ?? DEFAULT_STATE_TTL_MS;
  }

  private static stream(sessionId: SessionId): string {
    return `inbox:${sessionId}`;
  }

  private static wakeupChannel(sessionId: SessionId): string {
    return `inbox-wakeup:${sessionId}`;
  }

  private static quarantineKey(sessionId: SessionId): string {
    return `quarantine:${sessionId}`;
  }

  private async ensureGroup(sessionId: SessionId): Promise<void> {
    try {
      await this.redis.xGroupCreate(RedisStreamsInbox.stream(sessionId), GROUP, '0', { MKSTREAM: true });
    } catch (error) {
      // Only 'the group is already there' is benign. The check is on the reply text rather than on
      // an error class imported from the client, because the adapters name no value from the
      // optional peer — and only a server error reply can carry this code.
      if (!String(error).includes('BUSYGROUP')) {
        throw error;
      }
    }
  }

  public async append(sessionId: SessionId, dataPoint: AnyDataPoint): Promise<string> {
    const payload = JSON.stringify(dataPoint.toWire());
    const entryId = await this.redis.xAdd(RedisStreamsInbox.stream(sessionId), '*', { [PAYLOAD_FIELD]: payload });
    // Publish after the XADD: a waiter woken by this nudge must find the entry pending.
    await this.redis.publish(RedisStreamsInbox.wakeupChannel(sessionId), '1');
    await slideTtl(this.redis, this.stateTtlMs, RedisStreamsInbox.stream(sessionId));
    return entryId;
  }

  /**
   * Present one stream entry, tolerating a bad wire payload and nothing else.
   *
   * Poison is reserved for a bad wire payload: only a missing payload field (a foreign producer's
   * malformed XADD) or bytes that fail to decode as JSON. Semantic deserialization failures from
   * `parseDataPoint` (an unknown type, a validation error, a registry bug) propagate and fail fast
   * — a quiet quarantine would hide a parser regression, and redelivery on resume lets a newer
   * deployment parse what an older one could not.
   */
  private toEntry(entryId: string, fields: Record<string, string>, deliveryCount: number): DeliveredInboxEntry {
    const raw = fields[PAYLOAD_FIELD];
    // Named the way Python names it (`type(error).__name__: error`), so a quarantine record reads
    // the same whichever runtime wrote it.
    if (raw === undefined) {
      return new PoisonInboxEntry({
        entryId,
        error: `TypeError: the stream entry carries no '${PAYLOAD_FIELD}' field`,
        deliveryCount,
        rawPayload: null,
      });
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(raw);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      return new PoisonInboxEntry({
        entryId,
        error: `${failure.name}: ${failure.message}`,
        deliveryCount,
        rawPayload: raw,
      });
    }
    return new InboxEntry({ entryId, dataPoint: parseDataPoint(decoded), deliveryCount });
  }

  public async consume(sessionId: SessionId, options: ConsumeOptions = {}): Promise<readonly DeliveredInboxEntry[]> {
    const maxEntries = options.maxEntries ?? null;
    if (maxEntries !== null && maxEntries <= 0) {
      return []; // XREADGROUP treats COUNT 0 as unbounded; keep parity with the in-memory adapter
    }
    await this.ensureGroup(sessionId);
    const streams = { key: RedisStreamsInbox.stream(sessionId), id: '>' };
    const response =
      maxEntries === null
        ? await this.redis.xReadGroup(GROUP, CONSUMER, streams)
        : await this.redis.xReadGroup(GROUP, CONSUMER, streams, { COUNT: maxEntries });
    const claimed = response?.[0]?.messages;
    if (claimed === undefined) {
      return [];
    }
    return claimed.map((entry) => this.toEntry(entry.id, entry.message, 1));
  }

  public async reclaim(sessionId: SessionId): Promise<readonly DeliveredInboxEntry[]> {
    await this.ensureGroup(sessionId);
    const stream = RedisStreamsInbox.stream(sessionId);
    const { messages } = await this.redis.xAutoClaim(stream, GROUP, CONSUMER, 0, '0-0');
    // An entry deleted out from under the group is reported as a hole in the claimed list; there is
    // nothing to re-present for it, so it drops out here rather than reaching the orchestrator.
    const claimed = messages.filter((entry): entry is RedisStreamMessage => entry !== null);
    const first = claimed[0];
    const last = claimed[claimed.length - 1];
    if (first === undefined || last === undefined) {
      return [];
    }
    // Fetch delivery counts only for the entries XAUTOCLAIM actually re-presented, bounded by their
    // id range (claimed is ascending), so poison counts stay complete regardless of backlog size — a
    // fixed cap over all pending would silently drop counts for entries beyond it.
    const pending = await this.redis.xPendingRange(stream, GROUP, first.id, last.id, claimed.length);
    const deliveryCounts = new Map(pending.map((entry) => [entry.id, entry.deliveriesCounter]));
    return claimed.map((entry) => this.toEntry(entry.id, entry.message, deliveryCounts.get(entry.id) ?? 1));
  }

  public async ack(sessionId: SessionId, entryId: string, options: { readonly epoch: Epoch }): Promise<void> {
    const result = Number(
      await this.redis.eval(ACK_SCRIPT, {
        keys: [`inbox_epoch:${sessionId}`, RedisStreamsInbox.stream(sessionId), `acked:${sessionId}`],
        arguments: [String(options.epoch), GROUP, entryId],
      }),
    );
    if (result === -1) {
      throw new StaleEpochError(`epoch ${options.epoch} is stale for session ${sessionId}`);
    }
    await slideTtl(
      this.redis,
      this.stateTtlMs,
      RedisStreamsInbox.stream(sessionId),
      `acked:${sessionId}`,
      `inbox_epoch:${sessionId}`,
    );
  }

  public async quarantine(sessionId: SessionId, entryId: string, options: QuarantineOptions): Promise<void> {
    // The group must exist for XPENDING/XACK: a quarantine racing ahead of any delivery (nothing
    // consumed yet) must be a safe no-op, not a NOGROUP error.
    await this.ensureGroup(sessionId);
    const stream = RedisStreamsInbox.stream(sessionId);
    // The delivery count and raw payload are read before the atomic step: the record content is
    // advisory (ops inspection), while removal-plus-record must be atomic and epoch-guarded — the
    // Lua script only pushes the record iff the XACK actually removed the entry.
    const pending = await this.redis.xPendingRange(stream, GROUP, entryId, entryId, 1);
    const deliveryCount = pending[0]?.deliveriesCounter ?? 0;
    const entries = await this.redis.xRange(stream, entryId, entryId);
    const rawPayload = entries[0]?.message[PAYLOAD_FIELD] ?? null;
    const record: QuarantineRecord = {
      entry_id: entryId,
      reason: options.reason,
      delivery_count: deliveryCount,
      raw_payload: rawPayload,
    };
    const result = Number(
      await this.redis.eval(QUARANTINE_SCRIPT, {
        keys: [`inbox_epoch:${sessionId}`, stream, `acked:${sessionId}`, RedisStreamsInbox.quarantineKey(sessionId)],
        arguments: [String(options.epoch), GROUP, entryId, JSON.stringify(record)],
      }),
    );
    if (result === -1) {
      throw new StaleEpochError(`epoch ${options.epoch} is stale for session ${sessionId}`);
    }
    await slideTtl(
      this.redis,
      this.stateTtlMs,
      stream,
      `acked:${sessionId}`,
      `inbox_epoch:${sessionId}`,
      RedisStreamsInbox.quarantineKey(sessionId),
    );
  }

  public async quarantined(sessionId: SessionId): Promise<readonly QuarantinedEntry[]> {
    const records = await this.redis.lRange(RedisStreamsInbox.quarantineKey(sessionId), 0, -1);
    return records.map((raw) => {
      const record = JSON.parse(raw) as QuarantineRecord;
      return new QuarantinedEntry({
        entryId: record.entry_id,
        reason: record.reason,
        deliveryCount: record.delivery_count,
        rawPayload: record.raw_payload,
      });
    });
  }

  public async pendingCount(sessionId: SessionId): Promise<number> {
    const total = await this.redis.xLen(RedisStreamsInbox.stream(sessionId));
    const acked = await this.redis.get(`acked:${sessionId}`);
    return total - (acked === null ? 0 : Number(acked));
  }

  public async waitForEntry(sessionId: SessionId, signal?: AbortSignal): Promise<void> {
    const channel = RedisStreamsInbox.wakeupChannel(sessionId);
    if (signal?.aborted === true) {
      return; // given up before anything was opened
    }
    // A subscribed node-redis connection is in pub/sub mode and can serve nothing else, so the wait
    // runs on a connection of its own — and that connection carries its own keepalive, which is
    // what keeps a quiet session's waiter alive (see WAKEUP_POLL_TIMEOUT_MS).
    const subscriber = this.redis.duplicate({ pingInterval: WAKEUP_POLL_TIMEOUT_MS });
    const woken = new Deferred<void>();
    // A connection that fails anyway is a real failure, not an idle one, and it fails the waiter —
    // exactly as the Python adapter lets anything but its poll timeout propagate. A waiter that
    // swallowed it would park forever on a subscription that can no longer receive anything, which
    // strands the session; a failed waiter the orchestrator already knows how to handle.
    subscriber.on('error', (error: unknown) => {
      woken.reject(error);
    });
    // The early return below can leave this promise unawaited, and a rejection arriving afterwards
    // must not surface as an unhandled rejection. The original still rejects for whoever awaits it.
    void woken.promise.catch(() => {
      // Observed here only so the runtime does not consider it unhandled.
    });
    // A caller that stopped waiting resolves the wait so the `finally` below runs: a subscribed
    // node-redis connection is a real socket plus a keepalive, and abandoning the promise would
    // hold both open for the life of the process, one per parked session. Python simply cancels
    // the task, which runs its `finally` for the same reason.
    signal?.addEventListener('abort', () => woken.resolve(), { once: true });
    try {
      await subscriber.connect();
      // Subscribe BEFORE checking pending: an append before the subscribe is visible as a pending
      // entry; one after it publishes to the live subscription — no gap either way.
      await subscriber.subscribe(channel, () => {
        woken.resolve();
      });
      if ((await this.pendingCount(sessionId)) > 0) {
        return;
      }
      await woken.promise;
    } finally {
      await RedisStreamsInbox.closeSubscriber(subscriber, channel);
    }
  }

  private static async closeSubscriber(subscriber: RedisSubscriberClient, channel: string): Promise<void> {
    try {
      await subscriber.unsubscribe(channel);
      await subscriber.close();
    } catch {
      // The connection is being thrown away either way; a graceful close that fails (a socket that
      // already went away) must never surface as the waiter's own failure.
      try {
        subscriber.destroy();
      } catch {
        // Already gone.
      }
    }
  }
}
