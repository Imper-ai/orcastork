/**
 * `Aggregator` — an Operator the orchestrator runs in the aggregation phase.
 *
 * It is an ordinary Operator (one component model, one scheduler, one registry) distinguished only
 * by being an `Aggregator` subclass: the orchestrator runs it once gathering quiesces, and it is the
 * sole writer of durable state. Aggregators emit no DataPoints (`produces` is typically empty), so
 * `run` drives the `aggregate` hook and yields nothing.
 *
 * @module
 */

import type { AnyDataPoint, DataPointClass, DataPointEmission } from '../datapoints/index.js';
import { Operator, type OperatorStatics, operator } from './base.js';
import type { OperatorContext } from './context.js';

/**
 * What an aggregator declares on top of {@link OperatorStatics}.
 *
 * Declared here rather than on {@link Aggregator} for the same reason the operator sets are: a
 * subclass setting one must not have to write `static override`, and an absent field reads as its
 * default.
 */
export interface AggregatorStatics extends OperatorStatics {
  /** Opt-in: also run during gathering, stamping `status='in_progress'`. */
  readonly interimRefresh?: boolean;

  /**
   * The DataPoint types this aggregator actually folds/persists. When declared (non-empty), the
   * orchestrator runs ONLY the operators whose output is transitively needed to produce these (the
   * backward-reachable closure) and prunes the rest — so an operator whose output nothing considers
   * never runs. Empty (the default) opts out: every operator runs, exactly as before.
   */
  readonly consumes?: readonly DataPointClass<AnyDataPoint>[];
}

/** An aggregator class as the framework handles it — Python's `type[Aggregator]`. */
export type AggregatorClass<T extends Aggregator = Aggregator> = AggregatorStatics & (abstract new () => T);

/** A constructible, registered aggregator class. */
export type ConcreteAggregatorClass<T extends Aggregator = Aggregator> = AggregatorStatics & (new () => T);

/** An Operator that runs in the aggregation phase and writes durable output. */
export abstract class Aggregator extends Operator {
  /** Fold the gathered DataPoints into durable output (idempotently). */
  public abstract aggregate(ctx: OperatorContext): Promise<void>;

  public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    await this.aggregate(ctx);
    // An aggregator is a sink — it emits no DataPoints. The empty loop is what keeps `run` an async
    // generator rather than a coroutine that happens to return nothing.
    const nothing: readonly DataPointEmission[] = [];
    for (const emitted of nothing) {
      yield emitted;
    }
  }
}

/**
 * Declare a concrete aggregator — the same registration as {@link operator}, under a name that
 * reads better on an Aggregator.
 *
 * It is the identical decorator, not a second registry: Python has one `Operator._registry` that
 * aggregators self-register into through the very same hook, and an aggregator resolved by id must
 * be the same object the operator registry hands back.
 */
export const aggregator = operator;
