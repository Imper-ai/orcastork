/**
 * A re-observed DataPoint identity costs one counted row, not a row per sighting.
 *
 * Operators that rerun on new data re-emit their whole output every pass, and a converter can run
 * hundreds of times in one session, so the same unchanged value was recorded hundreds of times. On a
 * real session that was ~18k of ~24.5k audit rows. The value is already in the store, its latest
 * sighting is on the DataPoint, and the count is reported once — what a per-row trail adds is the
 * timing of each individual re-sighting.
 */

import { describe, expect, it } from 'vitest';
import { AuditKind } from '../src/orcastork/audit/index.js';
import { NamespaceId, SessionId } from '../src/orcastork/ids.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import { ChatAnswerDataPoint, EmailDataPoint, workEmail } from './doubles/datapoints.js';
import { makeOperator } from './doubles/operators.js';

const SID = SessionId('reobs-session');
const NAMESPACE = NamespaceId('reobs-namespace');

describe('audit re-observation coalescing', () => {
  it('counts a re-observed identity instead of giving it a row per sighting', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // The same (type, value) emitted three times: one ADDED, two re-observations.
    const emitter = makeOperator('reobs_emitter', {
      produces: [EmailDataPoint],
      emits: [workEmail(), workEmail(), workEmail()],
    });

    await new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, runtime, operators: [emitter] }).run();

    const trail = await runtime.audit.replay(SID);
    const added = trail.filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED);
    const coalesced = trail.filter((entry) => entry.kind === AuditKind.DATA_POINTS_COALESCED);

    expect(added).toHaveLength(1); // one row for the new identity
    expect(coalesced).toHaveLength(1); // one counted row for the re-observations
    expect(coalesced[0]?.dataPoint).not.toBeNull();
    expect(coalesced[0]?.dataPoint?.summary).toBe('2 re-observed');
  });

  it('still gives each distinct value its own row', async () => {
    // Coalescing must not hide real change: three different values are three identities, not one.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const emitter = makeOperator('distinct_emitter', {
      produces: [EmailDataPoint],
      emits: [workEmail('a@e.example'), workEmail('b@e.example'), workEmail('c@e.example')],
    });

    await new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, runtime, operators: [emitter] }).run();

    const trail = await runtime.audit.replay(SID);
    const added = trail.filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED);

    expect(added).toHaveLength(3); // each distinct value keeps its own row
    expect(trail.filter((entry) => entry.kind === AuditKind.DATA_POINTS_COALESCED)).toEqual([]);
  });

  it("commits a parked session's counts before it releases", async () => {
    // Parking releases cleanly, so the counts have to reach the trail exactly as a completion's do.
    //
    // The counters live only on the parked instance and the resume that follows starts them at zero,
    // so counts skipped at the park are gone from the committed trail for good — the session's
    // merges would then be unaccounted for even though every DataPoint behind them is durable.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const emitter = makeOperator('park_emitter', {
      produces: [EmailDataPoint],
      emits: [workEmail(), workEmail(), workEmail()],
    });
    const orchestrator = new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [emitter],
      completesWhen: ChatAnswerDataPoint, // never arrives — the session parks instead of completing
      sessionDeadlineMs: 300_000,
      parkAfterMs: 30_000,
    });

    const result = await orchestrator.run();

    const trail = await runtime.audit.replay(SID);
    const coalesced = trail.filter((entry) => entry.kind === AuditKind.DATA_POINTS_COALESCED);

    expect(result.status).toBe(SessionStatus.PARKED);
    expect(coalesced).toHaveLength(1); // the park did not drop the coalesced counts
    expect(coalesced[0]?.dataPoint).not.toBeNull();
    expect(coalesced[0]?.dataPoint?.summary).toBe('2 re-observed');
  });
});
