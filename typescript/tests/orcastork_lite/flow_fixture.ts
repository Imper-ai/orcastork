/**
 * A tiny flow module the graph-tool tests point `-m` at (module-namespace discovery).
 *
 * @module
 */

import type { CapabilityContext, DataPointEmission, OperatorContext } from '../../src/orcastork_lite/index.js';
import {
  Capability,
  CapabilityId,
  DataPoint,
  Operator,
  OperatorId,
  OperatorPolicy,
} from '../../src/orcastork_lite/index.js';

export class Seed extends DataPoint<string> {}

export class Derived extends DataPoint<number> {}

export class Lookup extends Capability {
  public static readonly capabilityId = CapabilityId('fixture_lookup');

  public async activate(_ctx: CapabilityContext): Promise<void> {
    return;
  }
}

export class Producer extends Operator {
  public static readonly operatorId = OperatorId('fixture_producer');
  public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
  public static readonly dependsOn = [Seed];
  public static readonly produces = [Derived];
  public static readonly requires = [Lookup];

  public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    yield Derived.emit(ctx.store.ofType(Seed).length);
  }
}

export class Reporter extends Operator {
  public static readonly operatorId = OperatorId('fixture_reporter');
  public static readonly policy = OperatorPolicy({ rerunOnNewData: true });
  public static readonly dependsOn = [Derived];
  public static readonly consumes = [Derived];

  // biome-ignore lint/correctness/useYield: an operator that emits nothing — Python's `return; yield`
  public async *run(_ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    return;
  }
}

/** No `operatorId`: an intermediate the tool must not pick up. */
export abstract class AbstractHelper extends Operator {}

/** A second binding to the same class must not duplicate the node. */
export const NOT_A_CLASS = Producer;
