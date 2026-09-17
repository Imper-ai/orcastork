/**
 * TOOL — the `orcastork-graph` CLI: registry graph build, cycle-policy check, Mermaid rendering.
 *
 * Stubs are created inside each test (via the doubles factories / local class definitions), so the
 * registry-isolation setup in `tests/setup.ts` removes them afterwards; `main()` then sees exactly
 * the stubs the test registered.
 */

import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { CapabilityClass } from '../src/orcastork/capabilities/index.js';
import { BaseDataPoint, dataPointType } from '../src/orcastork/datapoints/index.js';
import type { CapabilityId } from '../src/orcastork/ids.js';
import type { ConcreteOperatorClass } from '../src/orcastork/operators/index.js';
import type { CliOutput } from '../src/orcastork/tools/graph.js';
import { checkGraph, main, renderMermaid } from '../src/orcastork/tools/graph.js';
import type { MakeCapabilityOptions, StubCapabilityClass } from './doubles/capabilities.js';
import { makeCapability } from './doubles/capabilities.js';
import { IpDataPoint, RiskDataPoint } from './doubles/datapoints.js';
import type { MakeAggregatorOptions, MakeOperatorOptions } from './doubles/operators.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

/** What `-m` is pointed at: the DataPoint zoo's own module, since a TypeScript CLI takes files. */
const DATAPOINT_MODULE = fileURLToPath(new URL('./doubles/datapoints.ts', import.meta.url));

/** Stands in for pytest's `capsys`: everything the run printed, kept per stream. */
class CapturedOutput implements CliOutput {
  public out = '';
  public err = '';

  public write(text: string): void {
    this.out += text;
  }

  public writeError(text: string): void {
    this.err += text;
  }
}

// Mermaid labels render the class name; the shared stub factories derive one name from the id, so
// these wrappers give each stub a distinct, assertable one.

const named = <T>(cls: T, name: string): T => {
  Object.defineProperty(cls, 'name', { value: name });
  return cls;
};

const anOperator = (name: string, operatorId: string, options: MakeOperatorOptions = {}): ConcreteOperatorClass =>
  named(makeOperator(operatorId, options), name);

const anAggregator = (name: string, operatorId: string, options: MakeAggregatorOptions = {}): ConcreteOperatorClass =>
  named(makeAggregator(operatorId, options), name);

const aCapability = (name: string, capabilityId: string, options: MakeCapabilityOptions = {}): StubCapabilityClass =>
  named(makeCapability(capabilityId, options), name);

describe('Mermaid rendering', () => {
  it('contains the nodes and edges, and is deterministic', () => {
    const capability = aCapability('GeoLookup', 'tool_geo_lookup');
    const producer = anOperator('IpProducer', 'tool_ip_producer', { produces: [IpDataPoint] });
    const consumer = anOperator('IpConsumer', 'tool_ip_consumer', {
      dependsOn: [IpDataPoint],
      requires: [capability],
    });
    const sink = anAggregator('RiskSink', 'tool_risk_sink', { dependsOn: [IpDataPoint] });

    const rendered = renderMermaid([producer, consumer, sink], [capability]);

    expect(rendered.split('\n')[0]).toBe('flowchart LR');
    expect(rendered).toContain('    op_tool_ip_producer["IpProducer"]');
    expect(rendered).toContain('    op_tool_risk_sink[/"RiskSink (aggregator)"/]');
    expect(rendered).toContain('    cap_tool_geo_lookup{{"GeoLookup"}}');
    expect(rendered).toContain('    op_tool_ip_producer --> op_tool_ip_consumer');
    expect(rendered).toContain('    op_tool_ip_producer --> op_tool_risk_sink');
    expect(rendered).toContain('    cap_tool_geo_lookup --> op_tool_ip_consumer');
    // Input order must not affect the output (stable diffs for a committed diagram).
    expect(renderMermaid([sink, consumer, producer], [capability])).toBe(rendered);
  });

  it('renders a `uses` input as a weaker dotted arrow', () => {
    // A `uses` input is a rerun trigger, not a readiness gate. It must render as a weaker (dotted)
    // link so an aggregator that folds inputs via `uses` is no longer a disconnected sink.
    const attrProducer = anOperator('AttrProducer', 'tool_attr_producer', { produces: [IpDataPoint] });
    const riskProducer = anOperator('RiskProducer', 'tool_risk_producer', { produces: [RiskDataPoint] });
    const folder = anAggregator('Folder', 'tool_folder', { dependsOn: [RiskDataPoint], uses: [IpDataPoint] });

    const rendered = renderMermaid([attrProducer, riskProducer, folder], []);

    // Strong readiness edge (RiskProducer -> Folder via dependsOn) stays a solid arrow.
    expect(rendered).toContain('    op_tool_risk_producer --> op_tool_folder');
    // The `uses` relationship (AttrProducer produces what Folder uses) is a weaker dotted arrow...
    expect(rendered).toContain('    op_tool_attr_producer -. uses .-> op_tool_folder');
    // ...and never a solid one.
    expect(rendered).not.toContain('    op_tool_attr_producer --> op_tool_folder');
    // The weaker links are faded via a linkStyle targeting exactly the uses-edge indices (they are
    // emitted after the solid edges: 1 solid edge here → the single uses edge is index 1).
    const faded = rendered
      .split('\n')
      .filter((line) => line.startsWith('    linkStyle 1 ') && line.includes('stroke'));
    expect(faded).toHaveLength(1);
    // Byte-stable regardless of input order (a committed diagram only diffs on real changes).
    expect(renderMermaid([folder, riskProducer, attrProducer], [])).toBe(rendered);
  });

  it('dedupes the uses edge when a strong edge already connects the pair', () => {
    // When a producer feeds a consumer through BOTH dependsOn and uses, only the strong solid edge
    // is drawn — the weaker dotted duplicate is suppressed.
    const producer = anOperator('Dp', 'tool_dedupe_producer', { produces: [IpDataPoint] });
    const consumer = anOperator('Dc', 'tool_dedupe_consumer', { dependsOn: [IpDataPoint], uses: [IpDataPoint] });

    const rendered = renderMermaid([producer, consumer], []);

    expect(rendered).toContain('    op_tool_dedupe_producer --> op_tool_dedupe_consumer');
    expect(rendered).not.toContain('op_tool_dedupe_producer -. uses .-> op_tool_dedupe_consumer');
    // No weak edges survive the dedupe, so no fade directive is emitted.
    expect(rendered).not.toContain('linkStyle');
  });

  it('stays deterministic when two nodes share a class name', () => {
    // Two distinct operators share a class name but have distinct ids. renderMermaid sorts by the
    // node id (which embeds the unique id), so the declarations stay distinct and the output is
    // byte-stable regardless of input order — a guarantee name-based sorting would lose.
    const a = anOperator('Dup', 'dup_a', { produces: [IpDataPoint] });
    const b = anOperator('Dup', 'dup_b', { dependsOn: [RiskDataPoint] });

    const rendered = renderMermaid([a, b], []);
    expect(renderMermaid([b, a], [])).toBe(rendered); // byte-identical across input orders

    const lines = rendered.split('\n');
    expect(lines).toContain('    op_dup_a["Dup"]');
    expect(lines).toContain('    op_dup_b["Dup"]');
    // Declarations are emitted in id-sorted order (dup_a before dup_b).
    expect(lines.indexOf('    op_dup_a["Dup"]')).toBeLessThan(lines.indexOf('    op_dup_b["Dup"]'));
  });
});

describe('the graph check', () => {
  it('accepts a graph built from a locally defined DataPoint', () => {
    @dataPointType('tool_metric', { pii: false, ephemeral: false }, { value: z.number().int() })
    class ToolMetricDataPoint extends BaseDataPoint<number> {}

    const producer = makeOperator('tool_metric_producer', { produces: [ToolMetricDataPoint] });
    const consumer = makeOperator('tool_metric_consumer', { dependsOn: [ToolMetricDataPoint] });

    const result = checkGraph([producer, consumer], []);

    expect(result.error).toBeNull();
    expect(result.findings).toEqual([]);
  });

  it('applies the permitted restriction before cycle detection, so dropping a cap breaks the cycle', () => {
    // The cycle runs operator -> cap -> operator (cap dependsOn a DataPoint the operator produces;
    // operator requires the cap). Restricting the permitted set must be applied BEFORE cycle
    // detection so the per-namespace subgraph reflects only permitted nodes: dropping the capability
    // node breaks the cycle, so the subgraph reports no cycle while the full graph still does.
    const cap = aCapability('GeoNeedsIp', 'tool_geo_needs_ip', { dependsOn: [IpDataPoint] });
    const looper = anOperator('CapLooper', 'tool_cap_looper', {
      produces: [IpDataPoint],
      requires: [cap],
      maxCycles: 2,
    });

    const full = checkGraph([looper], [cap]);
    expect(full.findings.some((finding) => finding.startsWith('cycle ('))).toBe(true);

    const restricted = checkGraph([looper], [cap], new Set<CapabilityId>());

    expect(restricted.findings).toContain('dropped capability (not permitted): GeoNeedsIp');
    expect(restricted.findings.some((finding) => finding.startsWith('cycle ('))).toBe(false);
    expect(restricted.error).toBeNull();
  });

  it('still exposes an unbounded cycle the permitted drop did not remove', () => {
    // Dropping a capability must change which cycles are validated, not just node listing: here the
    // operator also has an unbounded self-loop, so even with the capability cycle gone the
    // per-namespace subgraph must still raise on the remaining unbounded cycle.
    const cap = aCapability('SafeCap', 'tool_safe_cap', { dependsOn: [RiskDataPoint] });
    // Bounded leg through the cap (produces RiskDataPoint -> cap -> operator) plus an unbounded
    // self-loop on IpDataPoint with no maxCycles.
    const looper = anOperator('SelfLooper', 'tool_self_looper', {
      produces: [IpDataPoint, RiskDataPoint],
      dependsOn: [IpDataPoint],
      requires: [cap],
    });

    const restricted = checkGraph([looper], [cap], new Set<CapabilityId>());

    expect(restricted.findings).toContain('dropped capability (not permitted): SafeCap');
    expect(restricted.error).not.toBeNull();
    expect(restricted.error).toContain('SelfLooper');
  });
});

describe('the CLI', () => {
  let workspace: string;

  beforeAll(() => {
    workspace = mkdtempSync(join(tmpdir(), 'orcastork-graph-'));
  });

  afterAll(() => {
    rmSync(workspace, { recursive: true, force: true });
  });

  it('exits zero on an acyclic graph', async () => {
    makeOperator('tool_acyclic_producer', { produces: [IpDataPoint] });
    makeOperator('tool_acyclic_consumer', { dependsOn: [IpDataPoint] });
    const output = new CapturedOutput();

    // `--import-module` exercises the import-at-startup convention (already-imported is a no-op).
    expect(await main(['--import-module', DATAPOINT_MODULE, '--check'], output)).toBe(0);

    expect(output.out).toContain('full graph: OK');
    expect(output.err).toBe('');
  });

  it('exits one on an unbounded cycle', async () => {
    anOperator('SelfLoop', 'tool_self_loop', { produces: [IpDataPoint], dependsOn: [IpDataPoint] }); // no maxCycles
    const output = new CapturedOutput();

    expect(await main(['--check'], output)).toBe(1);

    expect(output.out).toContain('cycle (UNBOUNDED): SelfLoop');
    expect(output.err).toContain('SelfLoop');
    expect(output.err).toContain('maxCycles');
    expect(output.out).not.toContain('OK');
  });

  it('reports a bounded cycle but still exits zero', async () => {
    anOperator('LoopX', 'tool_loop_x', { produces: [IpDataPoint], dependsOn: [RiskDataPoint], maxCycles: 3 });
    anOperator('LoopY', 'tool_loop_y', { produces: [RiskDataPoint], dependsOn: [IpDataPoint], maxCycles: 2 });
    const output = new CapturedOutput();

    expect(await main(['--check'], output)).toBe(0);

    expect(output.out).toContain('full graph: cycle (bounded): LoopX, LoopY [LoopX.maxCycles=3; LoopY.maxCycles=2]');
    expect(output.out).toContain('full graph: OK');
    expect(output.err).toBe('');
  });

  it('reports the capability the permitted set dropped', async () => {
    aCapability('AllowedCap', 'tool_allowed_cap');
    const gated = aCapability('GatedCap', 'tool_gated_cap');
    anOperator('CapUser', 'tool_cap_user', { requires: [gated] });
    const output = new CapturedOutput();

    expect(await main(['--check', '--permitted', 'tool_allowed_cap'], output)).toBe(0);

    expect(output.out).toContain('full graph: OK');
    expect(output.out).toContain('permitted subgraph: dropped capability (not permitted): GatedCap');
    expect(output.out).toContain('permitted subgraph: OK');
  });

  it('strips whitespace around the permitted ids', async () => {
    aCapability('CapA', 'cap_a');
    const required = aCapability('CapB', 'cap_b');
    anOperator('CapUser', 'tool_cap_user', { requires: [required] });
    const output = new CapturedOutput();

    expect(await main(['--permitted', 'cap_a, cap_b'], output)).toBe(0);

    // Without stripping, ' cap_b' would not match and CapB would be reported as dropped.
    expect(output.out).not.toContain('dropped capability (not permitted): CapB');
    expect(output.out).not.toContain('dropped capability (not permitted): CapA');
    expect(output.out).toContain('permitted subgraph: OK');
  });

  it('writes the same Mermaid to stdout and to a file', async () => {
    anOperator('SoloOp', 'tool_solo_op', { produces: [IpDataPoint] });
    const toStdout = new CapturedOutput();

    expect(await main(['--mermaid', '-'], toStdout)).toBe(0);
    expect(toStdout.out.startsWith('flowchart LR')).toBe(true);
    expect(toStdout.out).toContain('    op_tool_solo_op["SoloOp"]');

    const target = join(workspace, 'graph.mmd');
    expect(await main(['--mermaid', target], new CapturedOutput())).toBe(0);
    expect(readFileSync(target, 'utf8')).toBe(toStdout.out);
  });

  it('exits zero after printing help, and two with nothing to do', async () => {
    const help = new CapturedOutput();
    expect(await main(['--help'], help)).toBe(0);
    expect(help.out).toContain('orcastork-graph');

    const nothing = new CapturedOutput();
    expect(await main([], nothing)).toBe(2);
    expect(nothing.err).toContain('nothing to do');
  });

  it('reports a capability-only cycle as unbounded and exits one', async () => {
    // A cycle of only capabilities has no operator to bound: describeCycle hits its "no operators on
    // the cycle" guard (rendering an empty '[]' caps segment) and validate rejects it, so --check
    // exits 1 with the 'no boundable operator' error on stderr.
    const provider = aCapability('CapDee', 'tool_cap_dee');
    const consumer = aCapability('CapCee', 'tool_cap_cee', { requires: [provider] });
    // Close the requires loop: a class static is a plain writable property at runtime.
    (provider as unknown as { requires: readonly CapabilityClass[] }).requires = [consumer];
    const output = new CapturedOutput();

    expect(await main(['--check'], output)).toBe(1);

    expect(output.out).toContain('cycle (UNBOUNDED): CapCee, CapDee []');
    expect(output.out).not.toContain('OK');
    expect(output.err).toContain('no boundable operator');
    expect(output.err).toContain('CapCee');
    expect(output.err).toContain('CapDee');
  });

  it('keeps the failing check exit code while --mermaid also runs', async () => {
    // A failing --check must surface exit 1 even though --mermaid also runs; the max accumulation
    // across independent actions must not be clobbered by the later mermaid write.
    anOperator('SelfLoop', 'tool_max_self_loop', { produces: [IpDataPoint], dependsOn: [IpDataPoint] });
    const target = join(workspace, 'g.mmd');

    expect(await main(['--check', '--mermaid', target], new CapturedOutput())).toBe(1);

    const written = readFileSync(target, 'utf8');
    expect(written.startsWith('flowchart LR')).toBe(true);
    expect(written).toContain('    op_tool_max_self_loop["SelfLoop"]');
  });

  it('keeps the failing permitted-subgraph exit code while --mermaid also runs', async () => {
    // Variant: a failing --permitted subgraph (unbounded self-loop survives the restriction) plus a
    // successful --mermaid write must still yield exit 1.
    anOperator('PermLoop', 'tool_perm_self_loop', { produces: [IpDataPoint], dependsOn: [IpDataPoint] });
    const target = join(workspace, 'perm.mmd');

    expect(await main(['--permitted', '', '--mermaid', target], new CapturedOutput())).toBe(1);

    expect(readFileSync(target, 'utf8').startsWith('flowchart LR')).toBe(true);
  });
});
