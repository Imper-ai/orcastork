/**
 * The DataPoint type zoo used across the suite.
 *
 * Includes an abstract intermediate (`EmailDataPoint`), two leaves sharing that substitution group
 * (`WorkEmailDataPoint` / `PersonalEmailDataPoint`), a leaf with an object value (`GeoDataPoint`,
 * exercising the canonical-value normalization) and an ephemeral leaf (`TriggerDataPoint`).
 * Importing this module self-registers the zoo.
 *
 * @module
 */

import { z } from 'zod';
import type { AnyDataPoint, ConcreteDataPointClass, DataPointClass } from '../../src/orcastork/datapoints/index.js';
import { abstractDataPoint, BaseDataPoint, dataPointType } from '../../src/orcastork/datapoints/index.js';
import type { OperatorRef } from '../../src/orcastork/ids.js';
import { OperatorId } from '../../src/orcastork/ids.js';

/** The instant every fixture is observed at unless a test says otherwise. */
export const T0 = new Date('2026-01-01T00:00:00.000Z');

/** The operator every fixture is attributed to unless a test says otherwise. */
export const DEFAULT_OP: OperatorRef = OperatorId('stub_operator');

// --- abstract intermediate + its leaves (a substitution group) -----------------------
/** The group two email leaves share: leaves inherit its classification and value contract. */
@abstractDataPoint({ pii: true, ephemeral: false }, { value: z.string() })
export abstract class EmailDataPoint extends BaseDataPoint<string> {}

@dataPointType('work_email')
export class WorkEmailDataPoint extends EmailDataPoint {}

@dataPointType('personal_email')
export class PersonalEmailDataPoint extends EmailDataPoint {}

// --- standalone leaves ----------------------------------------------------------------
@dataPointType('ip', { pii: true, ephemeral: false }, { value: z.string() })
export class IpDataPoint extends BaseDataPoint<string> {}

/** An object value — exercises identity normalization over a non-scalar. */
@dataPointType('geo', { pii: false, ephemeral: false }, { value: z.record(z.string(), z.number()) })
export class GeoDataPoint extends BaseDataPoint<Record<string, number>> {}

@dataPointType('risk', { pii: false, ephemeral: false }, { value: z.number() })
export class RiskDataPoint extends BaseDataPoint<number> {}

@dataPointType('chat_answer', { pii: true, ephemeral: false }, { value: z.string() })
export class ChatAnswerDataPoint extends BaseDataPoint<string> {}

/** Ephemeral: emitted only to trigger another operator; never persisted. */
@dataPointType('trigger', { pii: false, ephemeral: true }, { value: z.string() })
export class TriggerDataPoint extends BaseDataPoint<string> {}

/** When a fixture was observed, and by whom — the keyword arguments of the Python helpers. */
export interface ObservedAt {
  readonly first?: Date;
  readonly last?: Date;
  readonly by?: OperatorRef;
}

/** Build a DataPoint of `leaf` — the test-side counterpart of an emission the orchestrator stamped. */
export const observed = <T extends AnyDataPoint>(
  leaf: DataPointClass<T>,
  value: T['value'],
  options: ObservedAt = {},
): T => {
  const concrete = leaf as unknown as ConcreteDataPointClass<T>;
  return new concrete({
    value,
    retrievedBy: options.by ?? DEFAULT_OP,
    firstRetrieved: options.first ?? T0,
    lastRetrieved: options.last ?? T0,
  });
};

export const workEmail = (value = 'alice@work.example', options: ObservedAt = {}): WorkEmailDataPoint =>
  observed(WorkEmailDataPoint, value, options);

export const personalEmail = (value = 'alice@personal.example', options: ObservedAt = {}): PersonalEmailDataPoint =>
  observed(PersonalEmailDataPoint, value, options);

export const risk = (value = 0.5, options: ObservedAt = {}): RiskDataPoint => observed(RiskDataPoint, value, options);

export const ip = (value = '203.0.113.7', options: ObservedAt = {}): IpDataPoint =>
  observed(IpDataPoint, value, options);

export const chatAnswer = (value = 'answer', options: ObservedAt = {}): ChatAnswerDataPoint =>
  observed(ChatAnswerDataPoint, value, options);
