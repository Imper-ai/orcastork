/**
 * Real backing servers for the adapter suites.
 *
 * The Python package tests its Redis adapter against `fakeredis` and its Mongo adapter against a
 * mock, and pays for it: the notes on the Mongo double list, one by one, the behaviours it has
 * grown that a real server does not share. The port takes the other route — a real `redis-server`
 * on a free port, and `mongodb-memory-server` for Mongo — so an adapter test is evidence about
 * the thing that will run in production.
 *
 * Both helpers honour an externally provided server (`ORCASTORK_TEST_REDIS_URL`,
 * `ORCASTORK_TEST_MONGO_URL`) so a CI job that already runs one as a service container does not
 * start a second, and both expose an availability check so a suite can skip loudly rather than
 * fail obscurely on a machine without the binary. CI is the gate: there, nothing skips.
 *
 * A provided server is **shared**: every caller gets the same URL and `stop()` does nothing, so a
 * suite that relies on starting from an empty database must key its data per test rather than
 * assume it owns the server.
 *
 * @module
 */

import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import { spawn, spawnSync } from 'node:child_process';
import { createServer } from 'node:net';
import process from 'node:process';
import { MongoMemoryServer } from 'mongodb-memory-server';

/** A Redis server a test may connect to, and the promise that shuts it down again. */
export interface RunningRedis {
  /** Connection URL, e.g. `redis://127.0.0.1:6381`. */
  readonly url: string;

  /** Stop the server (a no-op for an externally provided one). */
  stop(): Promise<void>;
}

/** A MongoDB server a test may connect to, and the promise that shuts it down again. */
export interface RunningMongo {
  /** Connection URI, e.g. `mongodb://127.0.0.1:41235/`. */
  readonly uri: string;

  /** Stop the server (a no-op for an externally provided one). */
  stop(): Promise<void>;
}

/** How long a spawned `redis-server` gets to announce itself before the helper gives up. */
const REDIS_START_TIMEOUT_MS = 20_000;

/** The line `redis-server` prints once it is listening. */
const REDIS_READY_MARKER = 'Ready to accept connections';

const noop = async (): Promise<void> => {};

/** An externally provided Redis, if the environment names one. */
export const providedRedisUrl = (): string | undefined => process.env.ORCASTORK_TEST_REDIS_URL;

/** An externally provided MongoDB, if the environment names one. */
export const providedMongoUri = (): string | undefined => process.env.ORCASTORK_TEST_MONGO_URL;

let redisBinaryPresent: boolean | undefined;

/**
 * Whether a Redis suite can run here: an externally provided server, or `redis-server` on `PATH`.
 *
 * Synchronous on purpose — a suite decides whether to skip while it is being collected.
 */
export const isRedisAvailable = (): boolean => {
  if (providedRedisUrl() !== undefined) {
    return true;
  }
  redisBinaryPresent ??= spawnSync('redis-server', ['--version'], { stdio: 'ignore' }).status === 0;
  return redisBinaryPresent;
};

/**
 * Start a throwaway `redis-server` on a free port.
 *
 * Persistence is off (`--save ''`, `--appendonly no`) so nothing survives the process and nothing
 * is written to disk, and the server stays in the foreground so its lifetime is exactly the
 * child process the test holds.
 */
export const startRedisServer = async (): Promise<RunningRedis> => {
  const provided = providedRedisUrl();
  if (provided !== undefined) {
    return { url: provided, stop: noop };
  }

  const port = await freePort();
  const server = spawn(
    'redis-server',
    ['--port', String(port), '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no', '--daemonize', 'no'],
    // Piped rather than inherited: the readiness line is what the helper waits for, and a test run
    // should not be interleaved with a server's startup banner.
    { stdio: 'pipe' },
  );
  await waitForRedisReady(server, port);
  return { url: `redis://127.0.0.1:${port}`, stop: async () => stopChild(server) };
};

let mongoAvailability: Promise<boolean> | undefined;

/**
 * Whether a Mongo suite can run here.
 *
 * Asynchronous because the answer may involve fetching a server binary on first use; memoized
 * because the answer cannot change within a run, and the check is the expensive part.
 */
export const isMongoAvailable = async (): Promise<boolean> => {
  mongoAvailability ??= probeMongo();
  return mongoAvailability;
};

/** Start a throwaway MongoDB (an ephemeral, in-memory `mongod`) on a free port. */
export const startMongoServer = async (): Promise<RunningMongo> => {
  const provided = providedMongoUri();
  if (provided !== undefined) {
    return { uri: provided, stop: noop };
  }

  const server = await MongoMemoryServer.create();
  return {
    uri: server.getUri(),
    stop: async () => {
      await server.stop();
    },
  };
};

/** Why a suite is skipping, said out loud: a silent skip is a test that stopped existing. */
export const warnSkipped = (subject: string, reason: string): void => {
  console.warn(`[skipped] ${subject}: ${reason}`);
};

const probeMongo = async (): Promise<boolean> => {
  if (providedMongoUri() !== undefined) {
    return true;
  }
  try {
    const server = await MongoMemoryServer.create();
    await server.stop();
    return true;
  } catch (error) {
    warnSkipped('mongodb-memory-server', error instanceof Error ? error.message : String(error));
    return false;
  }
};

/** Ask the OS for a port nobody is using, then hand it straight to the server we are starting. */
const freePort = async (): Promise<number> =>
  new Promise<number>((resolve, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address();
      if (address === null || typeof address === 'string') {
        probe.close();
        reject(new Error('the OS did not hand out a TCP port'));
        return;
      }
      probe.close(() => {
        resolve(address.port);
      });
    });
  });

const waitForRedisReady = async (server: ChildProcessWithoutNullStreams, port: number): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    let output = '';
    let settled = false;
    let giveUp: ReturnType<typeof setTimeout> | undefined;

    const finish = (error?: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(giveUp);
      server.stdout.removeAllListeners('data');
      server.stderr.removeAllListeners('data');
      server.removeAllListeners('error');
      server.removeAllListeners('exit');
      if (error === undefined) {
        resolve();
        return;
      }
      void stopChild(server).then(() => {
        reject(error);
      });
    };

    const onOutput = (chunk: Buffer): void => {
      output += chunk.toString('utf8');
      if (output.includes(REDIS_READY_MARKER)) {
        finish();
      }
    };

    server.stdout.on('data', onOutput);
    server.stderr.on('data', onOutput);
    server.on('error', (error: Error) => {
      finish(error);
    });
    server.on('exit', (code: number | null) => {
      finish(new Error(`redis-server exited with code ${String(code)} before it was ready: ${output}`));
    });
    giveUp = setTimeout(() => {
      finish(new Error(`redis-server did not start on port ${port} within ${REDIS_START_TIMEOUT_MS}ms: ${output}`));
    }, REDIS_START_TIMEOUT_MS);
  });

/** Ask the child to stop, and wait for it: a test that returns while its server is still listening
 * leaves the next test racing a port that is about to be freed. */
const stopChild = async (server: ChildProcessWithoutNullStreams): Promise<void> => {
  if (server.exitCode !== null || server.signalCode !== null) {
    return;
  }
  await new Promise<void>((resolve) => {
    server.once('exit', () => {
      resolve();
    });
    server.kill('SIGTERM');
  });
};
