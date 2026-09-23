/**
 * Build the static dependency graph from the operator + capability declarations.
 *
 * Nodes are operator and capability **classes**. A directed edge `X → Y` means "Y depends on
 * something X provides":
 *
 * - operator A *produces* a DataPoint type that operator/capability B *dependsOn* (subtype-aware —
 *   a `WorkEmail` producer feeds an `EmailDataPoint` consumer);
 * - capability C is *required* by operator/capability B (layering / capability use).
 *
 * The **abstract-produces rule**: an operator that declares an abstract DataPoint in `produces` is
 * treated as producing every registered concrete leaf of that type, so no real edge is missed.
 * Aggregators (empty `produces`) emit nothing and so are pure sinks.
 *
 * @module
 */

import type { CapabilityClass } from '../capabilities/base.js';
import { Capability } from '../capabilities/base.js';
import { dataPointRegistry } from '../datapoints/base.js';
import type { AnyDataPoint, DataPointClass, DataPointStatics } from '../datapoints/index.js';
import { isSubclass, subtypesOf } from '../datapoints/index.js';
import type { CapabilityId } from '../ids.js';
import type { OperatorClass } from '../operators/base.js';

/**
 * A graph node is an Operator subclass or a Capability subclass.
 *
 * Both declare `dependsOn` / `requires`; only operators declare `produces`.
 */
export type Node = OperatorClass | CapabilityClass;

/** The dependency graph as an adjacency map, producer → everything it feeds. */
export type EdgeMap = ReadonlyMap<Node, ReadonlySet<Node>>;

/**
 * The declarations any node may carry, as the graph reads them.
 *
 * Python gives `Capability` class-level defaults, so `node.depends_on` is always readable; this
 * port's `CapabilityClass` is the bare matching form (no statics) precisely so an abstract provider
 * *family* is usable wherever a class is matched. Reading through this one shape keeps that choice
 * from leaking a cast into every call site — and an absent declaration reads as empty, exactly as
 * the Python defaults do.
 */
interface NodeDeclarations {
  readonly capabilityId?: CapabilityId;
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];
  readonly requires?: readonly CapabilityClass[];
  readonly produces?: readonly DataPointClass<AnyDataPoint>[];
  readonly uses?: readonly DataPointClass<AnyDataPoint>[];
}

const declared = (node: Node): NodeDeclarations => node as NodeDeclarations;

/** The DataPoint types a node requires to be present before it can run. */
export const dependsOnOf = (node: Node): readonly DataPointClass<AnyDataPoint>[] => declared(node).dependsOn ?? [];

/** The capability families a node builds on. */
export const requiresOf = (node: Node): readonly CapabilityClass[] => declared(node).requires ?? [];

/** A concrete capability's registry id, or `undefined` on an abstract intermediate. */
export const capabilityIdOf = (node: Node): CapabilityId | undefined => declared(node).capabilityId;

/**
 * Whether this class is the concrete leaf currently registered under its own discriminator.
 *
 * "Is it a leaf?" rather than "was it marked abstract?": Python cannot define a DataPoint class
 * that is neither (a concrete leaf with no discriminator raises at definition), so every class
 * that is not a registered leaf there is an intermediate. TypeScript has no such gate — an
 * undecorated intermediate simply runs no code — so the graph asks the registry instead of asking
 * for the optional `@abstractDataPoint` marker, or an operator producing an undecorated
 * intermediate would silently lose every edge it should draw.
 */
const isRegisteredLeaf = (dataPointType: DataPointClass<AnyDataPoint>): boolean => {
  const discriminator = (dataPointType as unknown as DataPointStatics).type;
  return discriminator !== undefined && dataPointRegistry.get(discriminator) === dataPointType;
};

/**
 * What an operator really puts into the session: its declared `produces`, with an abstract type
 * expanded to every registered concrete leaf below it.
 *
 * An abstract type with no registered leaves degrades to producing nothing — never to matching the
 * abstract itself, which would invent an edge to a consumer no emission could ever satisfy.
 */
export const effectiveProduces = (operator: OperatorClass): ReadonlySet<DataPointClass<AnyDataPoint>> => {
  const produced = new Set<DataPointClass<AnyDataPoint>>();
  for (const declaredType of operator.produces ?? []) {
    if (isRegisteredLeaf(declaredType)) {
      produced.add(declaredType);
    } else {
      for (const leaf of subtypesOf(declaredType)) {
        produced.add(leaf); // abstract → all concrete leaves
      }
    }
  }
  return produced;
};

/** One producer and the concrete types it effectively puts into the session. */
interface Producer {
  readonly operator: OperatorClass;
  readonly producedTypes: ReadonlySet<DataPointClass<AnyDataPoint>>;
}

const producersOf = (operators: readonly OperatorClass[]): readonly Producer[] =>
  operators.map((operator) => ({ operator, producedTypes: effectiveProduces(operator) }));

const satisfies = (producedTypes: ReadonlySet<DataPointClass<AnyDataPoint>>, required: DataPointClass): boolean =>
  [...producedTypes].some((produced) => isSubclass(produced, required));

/** Construct the dependency graph (adjacency map) over operators + capabilities. */
export const buildGraph = (operators: Iterable<OperatorClass>, capabilities: Iterable<CapabilityClass>): EdgeMap => {
  const operatorList = [...operators];
  const capabilityList = [...capabilities];
  const nodes: Node[] = [...operatorList, ...capabilityList];
  const edges = new Map<Node, Set<Node>>(nodes.map((node) => [node, new Set<Node>()]));

  // DataPoint dependencies: an operator producing a subtype of a consumer's input feeds it.
  const producers = producersOf(operatorList);
  for (const consumer of nodes) {
    for (const requiredType of dependsOnOf(consumer)) {
      for (const producer of producers) {
        if (satisfies(producer.producedTypes, requiredType)) {
          edges.get(producer.operator)?.add(consumer);
        }
      }
    }
  }

  // Capability requirements (layering / use): a provider feeds whatever requires it.
  for (const requirer of nodes) {
    for (const requiredCapability of requiresOf(requirer)) {
      for (const provider of capabilityList) {
        if (isSubclass(provider, requiredCapability)) {
          edges.get(provider)?.add(requirer);
        }
      }
    }
  }

  return edges;
};

/**
 * Producer→consumer edges for the `uses` relation, mirroring {@link buildGraph}'s subtype matching.
 *
 * `uses` inputs are rerun triggers, not readiness gates — an operator does not wait on them, it just
 * re-runs when they change (an aggregator folds them in). Only operators declare `uses`;
 * capabilities have none, and only operators produce, so this is operator→operator. Self-edges (an
 * operator using what it produces) are dropped. Kept separate from {@link buildGraph} so the cycle
 * checker's strong-edge semantics are unchanged — this exists to render the weaker `uses` links.
 */
export const buildUsesEdges = (operators: Iterable<OperatorClass>): EdgeMap => {
  const operatorList = [...operators];
  const producers = producersOf(operatorList);
  const edges = new Map<Node, Set<Node>>(operatorList.map((operator) => [operator, new Set<Node>()]));
  for (const consumer of operatorList) {
    for (const usedType of consumer.uses ?? []) {
      for (const producer of producers) {
        if (producer.operator !== consumer && satisfies(producer.producedTypes, usedType)) {
          edges.get(producer.operator)?.add(consumer);
        }
      }
    }
  }
  return edges;
};

/** The per-session subgraph: drop capability nodes the namespace does not permit (operators stay). */
export const restrictToPermitted = (edges: EdgeMap, permitted: ReadonlySet<CapabilityId>): EdgeMap => {
  const keep = (node: Node): boolean => {
    if (!isSubclass(node, Capability)) {
      return true;
    }
    // A capability with no id (an abstract intermediate) can never be permitted, so it is dropped —
    // a session's graph only ever carries registered providers.
    const capabilityId = capabilityIdOf(node);
    return capabilityId !== undefined && permitted.has(capabilityId);
  };
  const restricted = new Map<Node, ReadonlySet<Node>>();
  for (const [node, targets] of edges) {
    if (keep(node)) {
      restricted.set(node, new Set([...targets].filter(keep)));
    }
  }
  return restricted;
};
