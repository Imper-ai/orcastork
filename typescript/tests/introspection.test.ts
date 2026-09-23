/**
 * INTRO — describeSession / renderText: read-only stuck-session forensics from persisted state.
 *
 * The description is computed with the same pure functions the scheduler uses, so what it reports as
 * missing/ready is exactly what the engine would act on.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryCapabilityCatalog, InMemoryInbox } from '../src/orcastork/adapters/memory/index.js';
import type { FlowDefinitionInit } from '../src/orcastork/flow.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import { CapabilityId, Epoch, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import type { OperatorState, SessionDescription } from '../src/orcastork/introspection.js';
import { describeSession, renderText } from '../src/orcastork/introspection.js';
import { SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { OperatorClass } from '../src/orcastork/operators/index.js';
import { SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  RiskDataPoint,
  risk,
  workEmail,
} from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const SID = SessionId('intro-session');
const NAMESPACE = NamespaceId('intro-namespace');

const SECOND_MS = 1_000;

const flowOf = (
  operators: readonly OperatorClass[],
  extras: Omit<FlowDefinitionInit, 'name' | 'operators'> = {},
): FlowDefinition => new FlowDefinition({ name: 'intro-flow', operators, ...extras });

const stateOf = (description: SessionDescription, operatorId: string): OperatorState => {
  const state = description.operators.find((candidate) => candidate.operatorId === OperatorId(operatorId));
  if (state === undefined) {
    throw new Error(`no operator state for ${operatorId}`);
  }
  return state;
};

describe('describeSession', () => {
  it('reports exactly the missing dependency type of a stuck operator', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const flow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });
    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED); // the answer never arrived — this session is "stuck"

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(description.isComplete).toBe(false);
    expect(description.isOwned).toBe(false); // parked sessions hold no lease
    expect(description.currentEpoch).toBe(1);
    expect(description.deadline).not.toBeNull(); // the wall-clock budget persisted at the first gather
    expect(description.pendingInbox).toBe(0);
    expect(description.presentTypes).toEqual(new Map([['WorkEmailDataPoint', 1]]));
    const state = stateOf(description, 'answer_handler');
    expect(state.hasRun).toBe(false);
    expect(state.watermark).toBeNull();
    expect(state.isReadyNow).toBe(false);
    expect(state.missingDataPoints).toEqual(['ChatAnswerDataPoint']); // exactly the one missing type
    expect(state.missingCapabilities).toEqual([]);
    expect(state.isGated).toBe(false);
    expect(state.contributionMarked).toBeNull(); // not an aggregator
  });

  it('reports a ready-but-unrun operator as ready with no missing pieces', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // Persisted state only — no orchestrator has driven this session yet.
    await runtime.store.write(SID, [workEmail()], { epoch: Epoch(1) });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const description = await describeSession(runtime, {
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([op]),
    });

    const state = stateOf(description, 'op');
    expect(state.isReadyNow).toBe(true); // the abstract EmailDataPoint dependency is satisfied by the leaf
    expect(state.hasRun).toBe(false);
    expect(state.missingDataPoints).toEqual([]);
    expect(state.missingCapabilities).toEqual([]);
    expect(description.isComplete).toBe(false);
    expect(description.deadline).toBeNull(); // no gather ever persisted a budget
  });

  it('reports a completed session as complete with all operators run', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint] });
    const flow = flowOf([scorer, reporter]);
    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(result.status).toBe(SessionStatus.COMPLETED);

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(description.isComplete).toBe(true);
    expect(description.isOwned).toBe(false);
    const scorerState = stateOf(description, 'scorer');
    expect(scorerState.hasRun).toBe(true);
    expect(scorerState.watermark).not.toBeNull();
    expect(stateOf(description, 'rep').contributionMarked).toBe(true); // the aggregator's durable "ran" flag
    expect(renderText(description)).toContain('all operators have run');
  });

  it('surfaces the quarantined entries and the pending inbox', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const inbox = runtime.inbox;
    expect(inbox).toBeInstanceOf(InMemoryInbox);
    const poisonId = await (inbox as InMemoryInbox).appendSerialized(SID, 'not-json{');
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const flow = flowOf([op]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    // A late entry appended after completion stays pending forever — exactly what an operator
    // inspecting a session needs to see.
    await runtime.inbox.append(SID, chatAnswer('too late'));

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(description.pendingInbox).toBe(1);
    expect(description.quarantined).toHaveLength(1);
    expect(description.quarantined[0]?.entryId).toBe(poisonId);
  });

  it('reports a namespace-gated operator as gated', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, [OperatorId('kept')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const manager = new SessionOrchestrationManager(runtime);
    const kept = makeOperator('kept', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const gated = makeOperator('gated', { dependsOn: [EmailDataPoint], produces: [IpDataPoint] });
    const flow = flowOf([kept, gated]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    const gatedState = stateOf(description, 'gated');
    expect(gatedState.isGated).toBe(true);
    expect(gatedState.hasRun).toBe(false); // the engine never launched it, although its input was present
    expect(gatedState.isReadyNow).toBe(true); // data-ready, namespace-forbidden — the gate is the blocker
    expect(stateOf(description, 'kept').isGated).toBe(false);
    expect(renderText(description)).toContain('gated for this namespace');
  });

  it('reports a namespace-forbidden capability as missing for its dependent', async () => {
    // netcap is registered in the flow but the namespace's catalog does not permit it: availability
    // (computed with the engine's own fixpoint) excludes it, so its dependent reports it missing.
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, []]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const netcap = makeCapability('netcap');
    const consumer = makeOperator('consumer', {
      dependsOn: [EmailDataPoint],
      requires: [netcap],
      produces: [RiskDataPoint],
    });
    const flow = flowOf([consumer], { capabilities: [netcap] });
    await runtime.store.write(SID, [workEmail()], { epoch: Epoch(1) });

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    const state = stateOf(description, 'consumer');
    expect(state.isReadyNow).toBe(false);
    expect(state.missingCapabilities).toEqual([netcap.name]);
    expect(state.missingDataPoints).toEqual([]); // only the capability blocks it
    expect(renderText(description)).toContain(netcap.name);
  });

  it('does not report a permitted capability whose dependencies are present as missing', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('netcap')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const netcap = makeCapability('netcap', { dependsOn: [EmailDataPoint] });
    const consumer = makeOperator('consumer', {
      dependsOn: [EmailDataPoint],
      requires: [netcap],
      produces: [RiskDataPoint],
    });
    const flow = flowOf([consumer], { capabilities: [netcap] });
    await runtime.store.write(SID, [workEmail()], { epoch: Epoch(1) });

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    const state = stateOf(description, 'consumer');
    expect(state.isReadyNow).toBe(true);
    expect(state.missingCapabilities).toEqual([]);
  });

  it('reports a fingerprint mismatch when the flow changed', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const lateAddition = makeOperator('late_addition', { dependsOn: [IpDataPoint], produces: [RiskDataPoint] });
    const original = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });
    const changed = flowOf([answerHandler, lateAddition], {
      completesWhen: RiskDataPoint,
      parkAfterMs: 20 * SECOND_MS,
    });
    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: original,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED);

    const same = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow: original });
    expect(same.fingerprintMatches).toBe(true);
    expect(same.storedFlowFingerprint).toBe(original.fingerprint());

    const drifted = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow: changed });
    expect(drifted.fingerprintMatches).toBe(false);
    expect(drifted.storedFlowFingerprint).toBe(original.fingerprint()); // the persisted one, not ours
    expect(renderText(drifted)).toContain('DRIFTED');
  });

  it('stays read-only even while the session is owned', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const epoch = await runtime.lock.acquire(SID); // a live orchestrator owns the session right now
    await runtime.store.write(SID, [workEmail()], { epoch });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint] });
    const flow = flowOf([op]);
    const revisionBefore = await runtime.store.revision(SID);

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(description.isOwned).toBe(true);
    expect(description.currentEpoch).toBe(epoch);
    // Nothing moved: no epoch was minted, no write happened, the owner is undisturbed.
    expect(await runtime.lock.currentEpoch(SID)).toBe(epoch);
    expect(await runtime.lock.isHeld(SID)).toBe(true);
    expect(await runtime.store.revision(SID)).toBe(revisionBefore);
  });
});

describe('renderText', () => {
  it('summarizes the stuck session', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const readyWaiter = makeOperator('ready_waiter', { dependsOn: [EmailDataPoint], produces: [IpDataPoint] });
    const flow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    // Describe against a wider flow so the rendering covers both a blocked and a ready operator.
    const description = await describeSession(runtime, {
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([answerHandler, readyWaiter]),
    });

    const text = renderText(description);
    expect(text).toContain(`session ${SID}`);
    expect(text).toContain('incomplete');
    expect(text).toContain('unowned');
    expect(text).toContain('WorkEmailDataPoint=1');
    expect(text).toContain('answer_handler');
    expect(text).toContain('missing data: ChatAnswerDataPoint');
    expect(text).toContain('ready_waiter');
    expect(text).toContain('ready, not yet run');
    // status line + deadline/present + one line per pending operator
    expect(text.split('\n').length).toBeGreaterThanOrEqual(4);
  });

  it('renders an empty session with no present types', async () => {
    // The empty boundary: a never-touched session has zero DataPoints, so presentTypes is empty,
    // every operator reports its dependency missing, deadline is null (no gather ever ran), and
    // renderText emits the 'present: (none)' branch — the literal that never renders when a seed is
    // written. describeSession must stay total over this brand-new/empty state.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint] });
    const flow = flowOf([op]);

    const description = await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(description.presentTypes).toEqual(new Map());
    expect(description.revision).toBe(0);
    expect(description.currentEpoch).toBe(0);
    expect(description.deadline).toBeNull();
    const state = stateOf(description, 'op');
    expect(state.hasRun).toBe(false);
    expect(state.isReadyNow).toBe(false); // its dependency is absent
    expect(state.missingDataPoints).toEqual(['EmailDataPoint']);
    expect(renderText(description)).toContain('present: (none)');
  });

  it('counts duplicate concrete types in presentTypes', async () => {
    // Two DataPoints of the SAME concrete leaf (distinct values) must accumulate to a count of 2 —
    // the increment past its first, which a single-value seed never exercises.
    const runtime = buildInMemoryRuntime(new FakeClock());
    await runtime.store.write(SID, [risk(0.1), risk(0.9)], { epoch: Epoch(1) });
    const op = makeOperator('op', { dependsOn: [RiskDataPoint], produces: [IpDataPoint] });

    const description = await describeSession(runtime, {
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([op]),
    });

    expect(description.presentTypes.get('RiskDataPoint')).toBe(2); // multiplicity, not collapsed to 1
    expect(renderText(description)).toContain('RiskDataPoint=2');
  });

  it('is byte-identical across calls and leaves durable state untouched', async () => {
    // SYSTEMIC: describeSession is strictly read-only and deterministic. Run a session to COMPLETED,
    // snapshot the entire durable/lock/audit state, then call describeSession twice and assert (1)
    // the two renderings are byte-identical and (2) no store revision, lock epoch, lock state, or
    // audit length moved — describe minted no epoch and mutated no port.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint] });
    const flow = flowOf([scorer, reporter]);
    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(result.status).toBe(SessionStatus.COMPLETED);

    const revisionBefore = await runtime.store.revision(SID);
    const epochBefore = await runtime.lock.currentEpoch(SID);
    const heldBefore = await runtime.lock.isHeld(SID);
    const completeBefore = await runtime.lock.isComplete(SID);
    const auditLengthBefore = (await runtime.audit.replay(SID)).length;

    const first = renderText(await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow }));
    const second = renderText(await describeSession(runtime, { sessionId: SID, namespaceId: NAMESPACE, flow }));

    expect(first).toBe(second); // deterministic — describe is a pure function of persisted state
    expect(await runtime.store.revision(SID)).toBe(revisionBefore); // no write
    expect(await runtime.lock.currentEpoch(SID)).toBe(epochBefore); // no epoch minted
    expect(await runtime.lock.isHeld(SID)).toBe(heldBefore);
    expect(await runtime.lock.isComplete(SID)).toBe(completeBefore);
    expect((await runtime.audit.replay(SID)).length).toBe(auditLengthBefore); // no audit entry appended
  });
});
