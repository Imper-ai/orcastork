/**
 * `orcastork-graph` — deploy-time dependency-graph validation + Mermaid rendering.
 *
 * The design promises that CI builds the dependency graph and flags every cycle at deploy-time;
 * without this tool the bounded-cycle rule only runs inside the `Orchestrator` constructor, once per
 * session. This entrypoint imports the flow modules (so their operators/capabilities/datapoints
 * self-register — the same import-at-startup convention the orchestrator relies on), builds the full
 * registry graph, validates the cycle policy, and renders the graph as Mermaid so humans can see
 * what CI is checking.
 *
 * Run through `cli.ts` (the console script). This module imports the library; the library never
 * imports this module.
 *
 * @module
 */

import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import type { CapabilityClass, ConcreteCapabilityClass } from '../capabilities/base.js';
import { Capability, capabilityRegistry } from '../capabilities/base.js';
import { isSubclass } from '../datapoints/index.js';
import { UnboundedCycleError } from '../exceptions.js';
import type { EdgeMap, Node } from '../graph/index.js';
import {
  buildGraph,
  buildUsesEdges,
  findCycles,
  restrictToPermitted,
  validateAcyclicOrBounded,
} from '../graph/index.js';
import type { CapabilityId } from '../ids.js';
import { CapabilityId as toCapabilityId } from '../ids.js';
import type { OperatorClass } from '../operators/index.js';
import { Aggregator, Operator } from '../operators/index.js';

const ID_SANITIZER = /[^0-9A-Za-z_]/g;

/** Outcome of one cycle-policy validation run (full graph or per-namespace subgraph). */
export interface GraphCheck {
  /** Informational lines: dropped capability nodes, every cycle found. */
  readonly findings: readonly string[];

  /** The {@link UnboundedCycleError} message when the cycle policy fails, `null` when it holds. */
  readonly error: string | null;
}

/** Where the CLI writes; injectable so a test reads what a run printed instead of the terminal. */
export interface CliOutput {
  /** Write to standard output, newlines included (the text is not line-buffered for you). */
  write(text: string): void;

  /** Write to standard error. */
  writeError(text: string): void;
}

/** The default sink: the process's own streams. */
export const processOutput: CliOutput = {
  write: (text: string): void => {
    process.stdout.write(text);
  },
  writeError: (text: string): void => {
    process.stderr.write(text);
  },
};

const USAGE = 'usage: orcastork-graph [-h] [-m MODULE] [--check] [--mermaid PATH] [--permitted IDS]\n';

const HELP = `${USAGE}
Build the operator/capability dependency graph from the registries, validate its cycle policy,
and render it as Mermaid.

options:
  -h, --help             show this help message and exit
  -m, --import-module MODULE
                         module (specifier or path) to import first so its
                         operators/capabilities/datapoints self-register (repeatable)
  --check                validate the bounded-cycle policy; exit 1 on an unbounded cycle
  --mermaid PATH         write the graph as a Mermaid flowchart to PATH ('-' for stdout)
  --permitted IDS        comma-separated CapabilityIds; also validate the per-namespace subgraph
                         restricted to these
`;

/** Codepoint order, the way Python's `sorted` compares strings — never the locale's order. */
const compareText = (left: string, right: string): number => (left === right ? 0 : left < right ? -1 : 1);

/**
 * Every registered capability.
 *
 * `Capability` (unlike `Operator`) exposes no public `registered()` accessor; this read-only tool
 * deliberately reads the module-level registry directly rather than widening `Capability`'s public
 * API for one consumer.
 */
const registeredCapabilities = (): readonly ConcreteCapabilityClass[] => [...capabilityRegistry.values()];

const isCapabilityNode = (node: Node): node is ConcreteCapabilityClass =>
  isSubclass(node as CapabilityClass, Capability);

const isOperatorNode = (node: Node): node is OperatorClass => isSubclass(node as OperatorClass, Operator);

/**
 * The node's Mermaid id.
 *
 * The `op_`/`cap_` prefixes keep ids unique across the two registries (an operator id and a
 * capability id may collide); sanitizing keeps the ids parseable by Mermaid.
 */
const nodeId = (node: Node): string =>
  isCapabilityNode(node)
    ? `cap_${node.capabilityId.replace(ID_SANITIZER, '_')}`
    : `op_${(node as OperatorClass).operatorId.replace(ID_SANITIZER, '_')}`;

const byNodeId = (left: Node, right: Node): number => compareText(nodeId(left), nodeId(right));

const declaration = (node: Node): string => {
  if (isCapabilityNode(node)) {
    return `${nodeId(node)}{{"${node.name}"}}`; // hexagon
  }
  if (isSubclass(node as OperatorClass, Aggregator)) {
    return `${nodeId(node)}[/"${node.name} (aggregator)"/]`; // parallelogram
  }
  return `${nodeId(node)}["${node.name}"]`; // rectangle
};

const sortedTargets = (edges: EdgeMap, source: Node): readonly Node[] => [...(edges.get(source) ?? [])].sort(byNodeId);

/**
 * Render the dependency graph as a Mermaid `flowchart LR`.
 *
 * Nodes and edge targets are sorted so the output is deterministic — a committed diagram only diffs
 * when the graph actually changes.
 */
export const renderMermaid = (operators: Iterable<OperatorClass>, capabilities: Iterable<CapabilityClass>): string => {
  const edges = buildGraph(operators, capabilities);
  const usesEdges = buildUsesEdges(operators);
  const nodes = [...edges.keys()].sort(byNodeId);
  const lines = ['flowchart LR', ...nodes.map((node) => `    ${declaration(node)}`)];
  const strong = nodes.flatMap((source) =>
    sortedTargets(edges, source).map((target) => `    ${nodeId(source)} --> ${nodeId(target)}`),
  );
  // Weaker `uses` links (rerun triggers, not readiness gates) render dotted. Skip any pair a strong
  // readiness edge already connects so the two never draw over each other.
  const weak = [...usesEdges.keys()].sort(byNodeId).flatMap((source) =>
    sortedTargets(usesEdges, source)
      .filter((target) => !(edges.get(source)?.has(target) ?? false))
      .map((target) => `    ${nodeId(source)} -. uses .-> ${nodeId(target)}`),
  );
  lines.push(...strong, ...weak);
  // Fade the `uses` links so the eye follows the solid readiness DAG and the (often large) fan-in
  // into an aggregator recedes into the background. Mermaid indexes links in definition order, so
  // the weak edges occupy the contiguous range right after the strong ones.
  if (weak.length > 0) {
    const weakIndices = weak.map((_line, offset) => strong.length + offset).join(',');
    lines.push(`    linkStyle ${weakIndices} stroke:#9aa0a6,stroke-width:1px,opacity:0.5`);
  }
  return `${lines.join('\n')}\n`;
};

const describeCycle = (cycle: ReadonlySet<Node>): string => {
  const operatorsOnCycle = [...cycle]
    .filter(isOperatorNode)
    .sort((left, right) => compareText(left.name, right.name) || compareText(left.operatorId, right.operatorId));
  const bounded =
    operatorsOnCycle.length > 0 && operatorsOnCycle.every((operator) => operator.policy.maxCycles !== null);
  const members = [...cycle]
    .map((node) => node.name)
    .sort(compareText)
    .join(', ');
  const caps = operatorsOnCycle
    .map((operator) => `${operator.name}.maxCycles=${operator.policy.maxCycles}`)
    .join('; ');
  return `cycle (${bounded ? 'bounded' : 'UNBOUNDED'}): ${members} [${caps}]`;
};

/** Validate the cycle policy; with `permitted`, validate the per-namespace subgraph instead. */
export const checkGraph = (
  operators: Iterable<OperatorClass>,
  capabilities: Iterable<CapabilityClass>,
  permitted: ReadonlySet<CapabilityId> | null = null,
): GraphCheck => {
  let edges = buildGraph(operators, capabilities);
  const findings: string[] = [];
  if (permitted !== null) {
    const restricted = restrictToPermitted(edges, permitted);
    const dropped = [...edges.keys()]
      .filter((node) => !restricted.has(node))
      .map((node) => node.name)
      .sort(compareText);
    findings.push(...dropped.map((name) => `dropped capability (not permitted): ${name}`));
    edges = restricted;
  }
  findings.push(...findCycles(edges).map(describeCycle));
  try {
    validateAcyclicOrBounded(edges);
  } catch (error) {
    if (error instanceof UnboundedCycleError) {
      return Object.freeze({ findings: Object.freeze(findings), error: error.message });
    }
    throw error;
  }
  return Object.freeze({ findings: Object.freeze(findings), error: null });
};

const report = (label: string, result: GraphCheck, output: CliOutput): number => {
  for (const finding of result.findings) {
    output.write(`${label}: ${finding}\n`);
  }
  if (result.error !== null) {
    output.writeError(`${label}: ${result.error}\n`);
    return 1;
  }
  output.write(`${label}: OK\n`);
  return 0;
};

/** A leading `.` or `/`, or a module-file extension: a file to import rather than a bare specifier. */
const PATH_LIKE = /^[./]|\.[cm]?[jt]s$/;

/**
 * Import one named module, for its registration side effects.
 *
 * Python takes an importable dotted name; a Node CLI is as often pointed at a built file, so a
 * specifier that looks like a path is resolved against the working directory and imported as a file
 * URL, while anything else is left to the resolver as a bare specifier.
 */
const loadModule = async (specifier: string): Promise<void> => {
  const target = PATH_LIKE.test(specifier) ? pathToFileURL(resolve(specifier)).href : specifier;
  await import(target);
};

/** Run the CLI: returns the process exit code rather than exiting, so it is testable. */
export const main = async (argv: readonly string[], output: CliOutput = processOutput): Promise<number> => {
  let values: {
    'import-module'?: string[];
    check?: boolean;
    mermaid?: string;
    permitted?: string;
    help?: boolean;
  };
  try {
    values = parseArgs({
      args: [...argv],
      options: {
        'import-module': { type: 'string', multiple: true, short: 'm' },
        check: { type: 'boolean' },
        mermaid: { type: 'string' },
        permitted: { type: 'string' },
        help: { type: 'boolean', short: 'h' },
      },
      allowPositionals: false,
    }).values;
  } catch (error) {
    output.writeError(`${error instanceof Error ? error.message : String(error)}\n${USAGE}`);
    return 2;
  }
  if (values.help === true) {
    output.write(HELP);
    return 0;
  }
  const check = values.check === true;
  const { mermaid, permitted } = values;
  if (!check && mermaid === undefined && permitted === undefined) {
    output.writeError(`nothing to do: pass --check, --mermaid and/or --permitted\n${USAGE}`);
    return 2;
  }
  for (const specifier of values['import-module'] ?? []) {
    await loadModule(specifier);
  }
  // Sorted by registry id so findings come out in a stable order across runs.
  const operators = [...Operator.registered().values()].sort((left, right) =>
    compareText(left.operatorId, right.operatorId),
  );
  const capabilities = [...registeredCapabilities()].sort((left, right) =>
    compareText(left.capabilityId, right.capabilityId),
  );

  let exitCode = 0;
  if (check) {
    exitCode = Math.max(exitCode, report('full graph', checkGraph(operators, capabilities), output));
  }
  if (permitted !== undefined) {
    const ids = new Set(
      permitted
        .split(',')
        .map((part) => part.trim())
        .filter((part) => part !== '')
        .map(toCapabilityId),
    );
    exitCode = Math.max(exitCode, report('permitted subgraph', checkGraph(operators, capabilities, ids), output));
  }
  if (mermaid !== undefined) {
    const rendered = renderMermaid(operators, capabilities);
    if (mermaid === '-') {
      output.write(rendered);
    } else {
      await writeFile(resolve(mermaid), rendered, 'utf8');
    }
  }
  return exitCode;
};
