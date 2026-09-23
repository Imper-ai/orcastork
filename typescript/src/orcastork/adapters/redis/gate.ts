/**
 * Redis `CooldownGate` — `SET NX PX`: the key exists exactly while the cooldown runs.
 *
 * Redis owns the expiry, so the cooldown holds across pods and restarts by construction; the `NX`
 * set is the atomic check-and-arm.
 *
 * @module
 */

import type { CooldownGate } from '../../ports/cooldown_gate.js';

/** The one command this gate needs. */
export interface RedisGateClient {
  /** `SET key value [NX] [PX ms]` — the reply is `'OK'` when the set happened, `null` when `NX` lost. */
  set(
    key: string,
    value: string,
    options: { condition: 'NX'; expiration: { type: 'PX'; value: number } },
  ): Promise<unknown>;
}

/** Check-and-arm in one step, with the armed windows owned by Redis. */
export class RedisCooldownGate implements CooldownGate {
  private readonly redis: RedisGateClient;

  public constructor(redis: RedisGateClient) {
    this.redis = redis;
  }

  public async tryAcquire(key: string, cooldownMs: number): Promise<boolean> {
    // At least 1ms: `PX 0` is rejected by Redis, so a zero/sub-millisecond cooldown still arms a
    // key that expires immediately rather than failing the call.
    const expiration = Math.max(Math.trunc(cooldownMs), 1);
    const reply = await this.redis.set(`cooldown:${key}`, '1', {
      condition: 'NX',
      expiration: { type: 'PX', value: expiration },
    });
    return reply !== null && reply !== undefined;
  }
}
