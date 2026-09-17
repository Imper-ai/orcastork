/**
 * The package entry point.
 *
 * PORT-SPECIFIC: Python's `orcastork/__init__.py` exports nothing and a consumer imports
 * `orcastork.operators`, `orcastork.datapoints` and so on directly. An npm package has one entry
 * point declared in `package.json`, so the root barrel *is* the public surface — and a symbol
 * missing from it is unreachable no matter how well it is exported one directory down. This pins
 * that every directory barrel actually reaches the root, and that the adapters deliberately do not.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import * as orcastork from '../src/orcastork/index.js';

/** One headline symbol per directory barrel — if the re-export is dropped, one of these goes. */
const HEADLINE_EXPORTS = [
  'AggregationHelpers', // aggregation/
  'ArchivedDataPoint', // archive/
  'AuditLogEntry', // audit/
  'Capability', // capabilities/
  'BaseDataPoint', // datapoints/
  'buildGraph', // graph/
  'Operator', // operators/
  'ChangeSet', // ports/
  'DebounceController', // scheduling/
  'SystemClock', // clock.ts
  'OrchestrationError', // exceptions.ts
  'FlowDefinition', // flow.ts
  'SessionId', // ids.ts
  'getLogger', // logging.ts
  'attachOtelLogBridge', // logging_bridge.ts
  'buildInMemoryRuntime', // runtime.ts
  'Telemetry', // telemetry.ts
] as const;

describe('the orcastork entry point', () => {
  it.each(HEADLINE_EXPORTS)('re-exports %s', (name) => {
    expect(orcastork).toHaveProperty(name);
  });

  it('leaves the adapters out, so importing the package drags in no backend', () => {
    // A backend is reached for by path (`orcastork/adapters/memory`), exactly as Python's
    // `from orcastork.adapters.memory import ...` is — `runtime.ts` is the one seam that wires one.
    const exported = Object.keys(orcastork);
    expect(exported.filter((name) => name.startsWith('InMemory'))).toEqual([]);
  });
});
