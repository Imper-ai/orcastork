/**
 * RZ (integration) — the Redis behaviours that need a whole server and a stretch of real time.
 *
 * Two things live here. RZ-09, the fleet-wide token bucket, because Python marks it
 * `@pytest.mark.integration`. And the inbox idle contract from `tests/test_redis_inbox_idle.py`,
 * which is about a **connection**: `waitForEntry` blocks on a pub/sub wakeup, and its client
 * carries a socket read deadline (`socketTimeout` here, `RedisConfig.socket_timeout` in Python,
 * 30s in that fleet). A waiter that dies when the deadline fires would fail any session that
 * simply receives no input for a while — the orchestrator propagates a failed waiter — so the
 * waiter must outlive the deadline AND still be woken by an append afterwards.
 *
 * Python needs a real server for this because fakeredis has no socket and so cannot express a read
 * deadline at all, "which is exactly why this class of bug survives an otherwise thorough
 * fake-backed suite". The port runs every Redis test against a real server, so what makes these
 * two integration tests rather than default ones is only their cost: several seconds of real
 * waiting each.
 */

import { createClient } from 'redis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { RedisRateLimiter, RedisStreamsInbox } from '../../src/orcastork/adapters/redis/index.js';
import { SessionId } from '../../src/orcastork/ids.js';
import { withTimeout } from '../../src/orcastork/internal/index.js';
import { FakeClock } from '../doubles/clock.js';
import { workEmail } from '../doubles/datapoints.js';
import type { RunningRedis } from '../doubles/servers.js';
import { isRedisAvailable, startRedisServer, warnSkipped } from '../doubles/servers.js';

const SESSION = SessionId('inbox-idle-session');

/** One second of token accrual, in the milliseconds the port speaks. */
const ONE_SECOND_MS = 1000;

/**
 * The read deadline the orchestrator's client carries — far below the fleet's 30s, so the idle
 * stretch below stays a few seconds.
 *
 * Twice the adapter's own keepalive interval: the keepalive has real margin, while a connection
 * without one dies well inside the idle stretch. node-redis treats this deadline as fatal and does
 * not reconnect, so a wakeup connection that was allowed to idle into it is gone for good.
 */
const SOCKET_TIMEOUT_MS = 2000;

/** How long the waiter is left with nothing to hear — well past the socket's read deadline. */
const IDLE_MS = SOCKET_TIMEOUT_MS * 2.5;

/** Let real time pass: a socket deadline is the operating system's clock, not an injected one. */
const realWait = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** What a waiter has done so far, so a failure can name the cause rather than just "done". */
type WaiterState =
  | { readonly status: 'pending' }
  | { readonly status: 'resolved' }
  | { readonly status: 'rejected'; readonly error: unknown };

/** Track settlement without awaiting: the point of the test is that it does NOT settle. */
const watch = (waiter: Promise<void>): { current: WaiterState } => {
  const state: { current: WaiterState } = { current: { status: 'pending' } };
  waiter.then(
    () => {
      state.current = { status: 'resolved' };
    },
    (error: unknown) => {
      state.current = { status: 'rejected', error };
    },
  );
  return state;
};

const redisAvailable = isRedisAvailable();
if (!redisAvailable) {
  warnSkipped('the Redis integration suite', 'redis-server is not on PATH and ORCASTORK_TEST_REDIS_URL is unset');
}

describe.skipIf(!redisAvailable)('the Redis adapters against a whole server', () => {
  let server: RunningRedis;

  beforeAll(async () => {
    server = await startRedisServer();
  }, 60_000);

  afterAll(async () => {
    await server.stop();
  });

  it('paces the whole fleet from one Lua token bucket', { timeout: 30_000 }, async () => {
    const client = createClient({ url: server.url });
    await client.connect();
    await client.flushAll();
    try {
      const clock = new FakeClock();
      const limiter = new RedisRateLimiter(client, clock, { ratePerSecond: 1, burst: 1 });

      await limiter.acquire('namespace-1:idp');
      expect(clock.monotonic()).toBe(0); // the burst token → immediate

      // A second limiter instance (another pod / another session) shares the same ratelimit:{key}
      // bucket, so it is paced by the first instance's take — fleet-wide, not per process.
      const otherPod = new RedisRateLimiter(client, clock, { ratePerSecond: 1, burst: 1 });
      await otherPod.acquire('namespace-1:idp');
      expect(clock.monotonic()).toBe(ONE_SECOND_MS); // waited (via the injected clock) for the shared token

      expect(await client.pTTL('ratelimit:namespace-1:idp')).toBeGreaterThan(0); // bucket state is TTL-bounded
    } finally {
      await client.close();
    }
  });

  describe('the inbox waiter on a client with a read deadline', () => {
    /** The orchestrator's client: a read deadline, and no keepalive of its own. */
    let deadlineClient: ReturnType<typeof createClient>;

    /** The stateless front door — another process, another connection, no deadline. */
    let frontDoorClient: ReturnType<typeof createClient>;

    let inbox: RedisStreamsInbox;
    let frontDoor: RedisStreamsInbox;

    beforeAll(async () => {
      frontDoorClient = createClient({ url: server.url });
      await frontDoorClient.connect();
      frontDoor = new RedisStreamsInbox(frontDoorClient);
    }, 60_000);

    beforeEach(async () => {
      // A fresh orchestrator client per contract, as Python's per-test fixture gives it: the
      // previous one was deliberately left to idle into its deadline, and node-redis does not
      // bring such a connection back.
      deadlineClient = createClient({ url: server.url, socket: { socketTimeout: SOCKET_TIMEOUT_MS } });
      // That lapse is reported as a client error. It is the setup, not a test failure.
      deadlineClient.on('error', () => {
        // Expected: this client is meant to idle out while the waiter runs.
      });
      await deadlineClient.connect();
      inbox = new RedisStreamsInbox(deadlineClient);
      // Each contract starts from an empty stream: an entry left by the previous one would let
      // `waitForEntry` return on the already-pending check and prove nothing.
      await frontDoorClient.flushAll();
    });

    afterEach(async () => {
      // Closing a client whose socket already lapsed is refused, which is exactly what the
      // contract arranged — there is nothing left to release either way.
      await deadlineClient.close().catch(() => {
        // Already gone.
      });
    });

    afterAll(async () => {
      await frontDoorClient.flushAll();
      await frontDoorClient.close();
    });

    it('survives a session idle longer than the socket timeout', { timeout: 30_000 }, async () => {
      const waiter = inbox.waitForEntry(SESSION);
      const state = watch(waiter);

      await realWait(IDLE_MS);

      if (state.current.status === 'rejected') {
        throw state.current.error; // re-raise so the failure names the cause
      }
      if (state.current.status === 'resolved') {
        expect.fail('the waiter returned without any entry being appended');
      }

      // A JavaScript promise cannot be cancelled the way Python cancels the waiter task, so the
      // waiter is woken instead — which is also how its subscriber connection gets released.
      await frontDoor.append(SESSION, workEmail());
      await withTimeout(waiter, 5_000);
    });

    it('is still woken by an entry appended after a long idle stretch', { timeout: 30_000 }, async () => {
      // Surviving is only half of it: the wakeup has to still work after the idle stretch. Without
      // the wakeup connection's own keepalive it would not: its socket would have lapsed long
      // before this append, and node-redis does not bring one back.
      const waiter = inbox.waitForEntry(SESSION);
      watch(waiter); // handle any early rejection synchronously; the assertion is the await below
      await realWait(IDLE_MS);

      await frontDoor.append(SESSION, workEmail());

      await withTimeout(waiter, 5_000);
    });
  });
});
