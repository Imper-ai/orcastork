/** The `orcastork-lite-graph` CLI: discovery by module namespace, checks, Mermaid, and its separation. */

import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { CapabilityId } from '../../src/orcastork_lite/ids.js';
import type { CliOutput } from '../../src/orcastork_lite/tools/graph.js';
import { checkGraph, discover, main, renderMermaid } from '../../src/orcastork_lite/tools/graph.js';
import { Ip, makeCapability, makeOperator, Risk } from './fixtures.js';
import * as flowFixture from './flow_fixture.js';

/** What `-m` is pointed at: the fixture module's path, since a TypeScript CLI takes files. */
const FIXTURE = fileURLToPath(new URL('./flow_fixture.ts', import.meta.url));

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

describe('discovery', () => {
  it('collects concrete classes once, sorted by id', () => {
    const graph = discover([flowFixture]);

    expect(graph.operators).toEqual([flowFixture.Producer, flowFixture.Reporter]);
    expect(graph.capabilities).toEqual([flowFixture.Lookup]);
  });
});

describe('Mermaid rendering', () => {
  it('renders nodes, edges, the sink shape and dotted uses links', () => {
    const cap = makeCapability('geo');
    const producer = makeOperator('producer', { produces: [Ip] });
    const consumer = makeOperator('consumer', { dependsOn: [Ip], requires: [cap] });
    const scorer = makeOperator('scorer', { produces: [Risk] });
    const sink = makeOperator('sink', { dependsOn: [Ip], uses: [Risk], consumes: [Ip, Risk] });

    const rendered = renderMermaid([producer, consumer, scorer, sink], [cap]);

    const lines = rendered.split('\n');
    expect(lines[0]).toBe('flowchart LR');
    expect(lines).toContain('    op_producer["Producer"]');
    expect(lines).toContain('    op_sink[/"Sink (sink)"/]');
    expect(lines).toContain('    cap_geo{{"Geo"}}');
    expect(lines).toContain('    op_producer --> op_consumer');
    expect(lines).toContain('    cap_geo --> op_consumer');
    expect(lines).toContain('    op_scorer -. uses .-> op_sink');
    // `split` leaves an empty last element for the trailing newline, so the styling line is the one before it.
    expect(lines.at(-2)?.startsWith('    linkStyle ')).toBe(true);
    expect(rendered).toBe(renderMermaid([sink, scorer, consumer, producer], [cap])); // deterministic
  });
});

describe('graph check', () => {
  it('reports cycles and capabilities the namespace does not permit', () => {
    const cap = makeCapability('geo');
    const a = makeOperator('a', { dependsOn: [Ip], produces: [Risk], requires: [cap], maxCycles: 2 });
    const b = makeOperator('b', { dependsOn: [Risk], produces: [Ip] });

    const unbounded = checkGraph([a, b], [cap]);

    expect(unbounded.error).not.toBeNull();
    expect(unbounded.error).toContain('maxCycles');
    expect(unbounded.findings.some((finding) => finding.startsWith('cycle (UNBOUNDED)'))).toBe(true);

    const restricted = checkGraph([a, b], [cap], new Set<CapabilityId>());

    expect(restricted.findings[0]).toBe('dropped capability (not permitted): Geo');
  });
});

describe('the CLI', () => {
  let workspace: string;

  beforeAll(() => {
    workspace = mkdtempSync(join(tmpdir(), 'orcastork-lite-graph-'));
  });

  afterAll(() => {
    rmSync(workspace, { recursive: true, force: true });
  });

  it('checks and renders the fixture module', async () => {
    const target = join(workspace, 'graph.mmd');
    const output = new CapturedOutput();

    const code = await main(
      ['-m', FIXTURE, '--check', '--mermaid', target, '--permitted', 'fixture_lookup, '],
      output,
    );

    expect(code).toBe(0);
    expect(output.out).toContain('full graph: OK');
    expect(output.out).toContain('permitted subgraph: OK');
    const rendered = readFileSync(target, 'utf8');
    expect(rendered).toContain('    op_fixture_producer --> op_fixture_reporter');
    expect(rendered).toContain('    cap_fixture_lookup --> op_fixture_producer');

    const toStdout = new CapturedOutput();
    expect(await main(['-m', FIXTURE, '--mermaid', '-'], toStdout)).toBe(0);
    expect(toStdout.out).toBe(rendered);
  });

  it('exits 2 with nothing to do, and 0 after printing help', async () => {
    const nothing = new CapturedOutput();
    expect(await main([], nothing)).toBe(2);
    expect(nothing.err).toContain('nothing to do');

    const help = new CapturedOutput();
    expect(await main(['--help'], help)).toBe(0);
    expect(help.out).toContain('usage: orcastork-lite-graph');
  });
});

/** `import x from 'y'`, a bare side-effect `import 'y'`, and a dynamic `import('y')`. */
const IMPORT_PATTERNS = [
  /\bfrom\s*['"]([^'"]+)['"]/g,
  /^\s*import\s*['"]([^'"]+)['"]/gm,
  /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
];

describe('separation', () => {
  it('is never imported by the library it validates', () => {
    const root = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', 'src', 'orcastork_lite');
    const libraryFiles = readdirSync(root, { recursive: true, withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith('.ts'))
      .map((entry) => join(entry.parentPath, entry.name))
      // The CLI's own modules import each other; the rule is about the library reaching for them.
      .filter((file) => !relative(root, file).split(/[\\/]/).includes('tools'));
    expect(libraryFiles.length).toBeGreaterThan(0); // a broken walk must not pass this vacuously

    const offenders = libraryFiles
      .filter((file) => {
        const source = readFileSync(file, 'utf8');
        return IMPORT_PATTERNS.some((pattern) =>
          [...source.matchAll(pattern)].some((match) => (match[1] ?? '').split('/').includes('tools')),
        );
      })
      .map((file) => relative(root, file))
      .sort();

    expect(offenders).toEqual([]);
  });
});
