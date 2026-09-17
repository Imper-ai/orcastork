/**
 * DP — the DataPoint model: identity, discriminated union, subtype substitution, config.
 */

import { describe, expect, it } from 'vitest';
import { ZodError, z } from 'zod';
import type { AnyDataPoint, DataPointLeafClass } from '../src/orcastork/datapoints/index.js';
import {
  BaseDataPoint,
  DataPointSet,
  DataPointTypeConfig,
  DataPointView,
  dataPointAdapter,
  dataPointType,
  MergeKind,
  parseDataPoint,
  registeredLeaves,
  registryVersion,
  subtypesOf,
} from '../src/orcastork/datapoints/index.js';
import { InvalidDataPointError, UnknownDataPointTypeError } from '../src/orcastork/exceptions.js';
import {
  DEFAULT_OP,
  EmailDataPoint,
  GeoDataPoint,
  IpDataPoint,
  observed,
  PersonalEmailDataPoint,
  personalEmail,
  T0,
  TriggerDataPoint,
  WorkEmailDataPoint,
  workEmail,
} from './doubles/datapoints.js';

const T1 = new Date(T0.getTime() + 60 * 60 * 1000);
const T2 = new Date(T0.getTime() + 2 * 60 * 60 * 1000);

/**
 * A list-valued leaf and a nested object-of-list leaf exercise the canonical-value recursion that
 * the scalar zoo never reaches. Declared at module scope so they register into the per-test
 * baseline exactly like the zoo (the isolation fixture snapshots them).
 */
@dataPointType('tags', { pii: false, ephemeral: false }, { value: z.array(z.string()) })
class TagsDataPoint extends BaseDataPoint<string[]> {}

@dataPointType('nested', { pii: false, ephemeral: false }, { value: z.record(z.string(), z.array(z.string())) })
class NestedDataPoint extends BaseDataPoint<Record<string, string[]>> {}

const tags = (value: string[], options: { readonly first?: Date; readonly last?: Date } = {}): TagsDataPoint =>
  observed(TagsDataPoint, value, options);

const nested = (
  value: Record<string, string[]>,
  options: { readonly first?: Date; readonly last?: Date } = {},
): NestedDataPoint => observed(NestedDataPoint, value, options);

/** The one DataPoint a set is expected to hold, asserted rather than assumed. */
const single = (points: DataPointSet): AnyDataPoint => {
  const items = points.all();
  expect(items).toHaveLength(1);
  const [first] = items;
  if (first === undefined) {
    throw new Error('expected the set to hold exactly one DataPoint');
  }
  return first;
};

describe('DataPoint identity and keyed merge', () => {
  it('excludes timestamps from identity, and a re-add merges in place bumping lastRetrieved', () => {
    // Arrange: same (type, value), different timestamps.
    const early = workEmail('alice@work.example', { first: T0, last: T0 });
    const later = workEmail('alice@work.example', { first: T1, last: T2 });

    // Assert: equal + identity-equal (timestamps excluded from identity).
    expect(early.equals(later)).toBe(true);
    expect(early.identity).toBe(later.identity);

    // Act: re-add to a set.
    const points = new DataPointSet([early]);
    points.add(later);

    // Assert: merged in place — no growth, first kept, last bumped.
    expect(points.size).toBe(1);
    const merged = single(points);
    expect(merged.firstRetrieved).toEqual(T0);
    expect(merged.lastRetrieved).toEqual(T2);
  });

  it('round-trips through the discriminated union back to the concrete class', () => {
    const original = workEmail('round@trip.example');
    const restored = parseDataPoint(original.toWire());
    expect(restored.constructor).toBe(WorkEmailDataPoint);
    expect(restored.equals(original)).toBe(true);
  });

  it('holds two distinct types carrying the same value apart', () => {
    const work = observed(WorkEmailDataPoint, 'same');
    const address = observed(IpDataPoint, 'same');
    expect(work.equals(address)).toBe(false);
  });

  it('keeps firstRetrieved immutable across a re-observation', () => {
    const points = new DataPointSet([workEmail('x@example', { first: T0, last: T0 })]);
    points.add(workEmail('x@example', { first: T2, last: T2 }));
    const merged = single(points);
    expect(merged.firstRetrieved).toEqual(T0); // immutable
    expect(merged.lastRetrieved).toEqual(T2); // advanced
  });

  it('keeps the max when reobserved with an older time', () => {
    // An out-of-order (older) re-observation must not rewind `lastRetrieved`.
    const point = workEmail('x@example', { first: T0, last: T2 });
    const rewound = point.reobserved(T0);
    expect(rewound.lastRetrieved).toEqual(T2); // clamped to the max — never rewound
    expect(rewound.firstRetrieved).toEqual(T0);
  });

  it('does not rewind lastRetrieved when an older observation is merged into the set', () => {
    // The collection routes through `reobserved`; an older add must keep the existing max.
    const points = new DataPointSet([workEmail('x@example', { last: T2 })]);
    const result = points.add(workEmail('x@example', { last: T0 }));
    expect(result.kind).toBe(MergeKind.UPDATED);
    expect(single(points).lastRetrieved).toEqual(T2);
  });

  it('answers membership by identity, ignoring timestamps', () => {
    const points = new DataPointSet([workEmail('x@example', { last: T0 })]);
    // Present by identity even though the timestamp differs.
    expect(points.has(workEmail('x@example', { last: T2 }))).toBe(true);
    // Absent identity.
    expect(points.has(workEmail('y@example'))).toBe(false);
  });

  it('answers membership false for a non-DataPoint without raising', () => {
    const points = new DataPointSet([workEmail('x@example')]);
    // The instanceof guard must short-circuit, not let the identity normalization blow up.
    expect(points.has('work_email')).toBe(false);
    expect(points.has(null)).toBe(false);
  });
});

describe('DataPointView queries', () => {
  it('returns the newest by lastRetrieved while the full set stays queryable', () => {
    const older = workEmail('a@work.example', { last: T0 });
    const newer = workEmail('b@work.example', { last: T2 });
    const view = new DataPointView([older, newer]);

    expect(view.latest(WorkEmailDataPoint)).toBe(newer);
    const matches = view.ofType(WorkEmailDataPoint);
    expect(matches).toHaveLength(2);
    expect(matches).toContain(older);
    expect(matches).toContain(newer);
  });

  it('returns null when no DataPoint of the type is present', () => {
    expect(new DataPointView([]).latest(WorkEmailDataPoint)).toBeNull();
  });

  it('returns the newest across subtypes when asked for an abstract intermediate', () => {
    const work = workEmail('w@example', { last: T0 });
    const personal = personalEmail('p@example', { last: T2 });
    const view = new DataPointView([work, personal]);
    expect(view.latest(EmailDataPoint)).toBe(personal);
  });

  it('breaks a latest tie on the first maximal in input order', () => {
    // Two distinct points with identical `lastRetrieved`: the first maximal in iteration order
    // wins, so the result is deterministic given a fixed order.
    const a = workEmail('a@work.example', { last: T1 });
    const b = workEmail('b@work.example', { last: T1 });
    expect(new DataPointView([a, b]).latest(WorkEmailDataPoint)).toBe(a);
    expect(new DataPointView([b, a]).latest(WorkEmailDataPoint)).toBe(b);
  });
});

describe('DataPoint declaration', () => {
  it('rejects a concrete leaf that pins no type discriminator (at first use, not definition)', () => {
    // Python raises when the class is defined; an undecorated TypeScript class runs no code at
    // definition time, so constructing or emitting it is the earliest honest place to fail.
    class MissingTypeDataPoint extends BaseDataPoint<string> {
      public static override config = DataPointTypeConfig({ pii: false, ephemeral: false });
    }

    expect(
      () =>
        new MissingTypeDataPoint({
          value: 'x',
          retrievedBy: DEFAULT_OP,
          firstRetrieved: T0,
          lastRetrieved: T0,
        }),
    ).toThrow(InvalidDataPointError);
    expect(() => MissingTypeDataPoint.emit('x')).toThrow(InvalidDataPointError);
  });

  it('rejects a concrete leaf that declares no config, at definition time', () => {
    expect(() => {
      @dataPointType('missing_config')
      class MissingConfigDataPoint extends BaseDataPoint<string> {}
      void MissingConfigDataPoint;
    }).toThrow(InvalidDataPointError);
  });

  it('keeps an abstract intermediate out of the union while instanceof still matches', () => {
    expect(registeredLeaves()).not.toContain(EmailDataPoint);
    expect(registeredLeaves()).toContain(WorkEmailDataPoint);
    expect(workEmail()).toBeInstanceOf(EmailDataPoint);
    expect(new Set(subtypesOf(EmailDataPoint))).toEqual(new Set([WorkEmailDataPoint, PersonalEmailDataPoint]));
  });

  it('assembles the union lazily, independent of declaration order', () => {
    expect(parseDataPoint(workEmail().toWire()).constructor).toBe(WorkEmailDataPoint);
    const versionBefore = registryVersion();

    // A leaf declared AFTER the union was first built must still be deserializable.
    @dataPointType('late_leaf', { pii: false, ephemeral: false }, { value: z.string() })
    class LateLeafDataPoint extends BaseDataPoint<string> {}

    expect(registryVersion()).toBeGreaterThan(versionBefore);
    const late = observed(LateLeafDataPoint, 'x');
    expect(parseDataPoint(late.toWire()).constructor).toBe(LateLeafDataPoint);
  });

  it('raises a clear error for an unknown discriminator', () => {
    const raw = {
      type: 'no_such_type',
      value: 'x',
      retrieved_by: 'op',
      first_retrieved: T0,
      last_retrieved: T0,
    };
    expect(() => parseDataPoint(raw)).toThrow(UnknownDataPointTypeError);
  });

  it('ignores extra fields on read', () => {
    const raw = {
      type: 'work_email',
      value: 'a@work.example',
      retrieved_by: 'op',
      first_retrieved: T0,
      last_retrieved: T0,
      unexpected_field: 'ignored',
    };
    const restored = parseDataPoint(raw);
    expect(restored.constructor).toBe(WorkEmailDataPoint);
    expect('unexpected_field' in restored).toBe(false);
  });

  it('derives isPii and isEphemeral from the class config', () => {
    // WorkEmail inherits config from the abstract EmailDataPoint intermediate.
    expect(workEmail().isPii).toBe(true);
    expect(workEmail().isEphemeral).toBe(false);
    // Trigger sets ephemeral per leaf.
    const trigger = observed(TriggerDataPoint, 'go');
    expect(trigger.isEphemeral).toBe(true);
    expect(trigger.isPii).toBe(false);
  });

  it('defaults auditSummary to null and lets a leaf override it', () => {
    // Default: no override -> null, so the orchestrator redacts a PII value / stringifies a non-PII one.
    expect(workEmail().auditSummary()).toBeNull();
    expect(observed(TriggerDataPoint, 'go').auditSummary()).toBeNull();

    @dataPointType(
      'audit_tagged',
      { pii: true, ephemeral: true },
      { value: z.object({ tag: z.string(), secret: z.string() }) },
    )
    class TaggedDataPoint extends BaseDataPoint<{ tag: string; secret: string }> {
      public override auditSummary(): string {
        return `tag=${this.value.tag}`;
      }
    }

    const tagged = observed(TaggedDataPoint, { tag: 'x', secret: 's' });
    // A PII DataPoint can still surface a non-PII summary for the audit (the secret stays out).
    expect(tagged.isPii).toBe(true);
    expect(tagged.auditSummary()).toBe('tag=x');
  });

  it('enforces the leaf value contract on construction', () => {
    expect(() => observed(WorkEmailDataPoint, 123 as unknown as string)).toThrow(ZodError);
  });

  it('refuses to emit from an abstract intermediate', () => {
    // An abstract intermediate has no discriminator and is not a union member, so it cannot be
    // finalized into a concrete DataPoint.
    expect(() => EmailDataPoint.emit('a@e.example')).toThrow(InvalidDataPointError);
  });

  it('refuses to emit from the base class', () => {
    expect(() => BaseDataPoint.emit('x')).toThrow(InvalidDataPointError);
  });
});

describe('canonical value normalization', () => {
  it('gives an object-valued point a stable identity, key order included', () => {
    const geoA = observed(GeoDataPoint, { lat: 1.0, lon: 2.0 }, { first: T0, last: T0 });
    const geoB = observed(GeoDataPoint, { lon: 2.0, lat: 1.0 }, { first: T1, last: T1 });
    // Building the identity does not raise, and key order in the object value does not affect it.
    expect(geoA.identity).toBe(geoB.identity);
    expect(geoA.equals(geoB)).toBe(true);
  });

  it('keeps a list value order-sensitive while element-equal lists are one identity', () => {
    const a = tags(['x', 'y'], { last: T0 });
    const b = tags(['x', 'y'], { last: T2 });
    expect(a.identity).toBe(b.identity);
    expect(a.equals(b)).toBe(true);

    // Order is part of a list's value identity — reordering is a distinct DataPoint.
    const reordered = tags(['y', 'x']);
    expect(reordered.identity).not.toBe(a.identity);
    expect(reordered.equals(a)).toBe(false);
  });

  it('normalizes the object layer of a nested value while inner lists keep their order', () => {
    // Two emissions differing only in key order must dedup to one identity.
    const a = nested({ tags: ['x', 'y'], groups: ['a'] }, { first: T0, last: T0 });
    const b = nested({ groups: ['a'], tags: ['x', 'y'] }, { first: T1, last: T1 });
    expect(a.identity).toBe(b.identity);
    expect(a.equals(b)).toBe(true);
  });

  it('dedups two equal list-valued points in a set', () => {
    // Re-emitting a list-valued DataPoint must merge in place, not insert a second row.
    const points = new DataPointSet([tags(['x', 'y'], { last: T0 })]);
    const result = points.add(tags(['x', 'y'], { last: T2 }));
    expect(result.kind).toBe(MergeKind.UPDATED);
    expect(points.size).toBe(1);
    expect(single(points).lastRetrieved).toEqual(T2);
  });
});

// --- S1: keyed-merge & union round-trip laws (example-based) ------------------------------

/**
 * Adversarial JSON-native value shapes paired with a leaf that accepts them — scalar string,
 * empty/non-empty lists, single- and multi-key object-of-list (the object layer is order
 * normalized) and an empty object. Every shape must agree across identity and equality, and
 * (below) round-trip through the union adapter.
 */
const valueShapes: readonly { readonly name: string; readonly build: (at: Date) => AnyDataPoint }[] = [
  { name: 'a scalar string', build: (at) => workEmail('scalar@e.example', { first: at, last: at }) },
  { name: 'a non-empty list', build: (at) => tags(['x', 'y', 'z'], { first: at, last: at }) },
  { name: 'an empty list', build: (at) => tags([], { first: at, last: at }) },
  {
    name: 'a multi-key object of lists',
    build: (at) => nested({ tags: ['x', 'y'], groups: [] }, { first: at, last: at }),
  },
  {
    name: 'the same object with its keys in the other order',
    build: (at) => nested({ groups: [], tags: ['x', 'y'] }, { first: at, last: at }),
  },
  { name: 'an empty object', build: (at) => nested({}, { first: at, last: at }) },
  { name: 'a single-key object of lists', build: (at) => nested({ outer: ['p', 'q'] }, { first: at, last: at }) },
];

describe('the keyed-merge law over every value shape', () => {
  it.each(valueShapes)('identity and equality agree for $name', ({ build }) => {
    // Equal values are identity-equal and compare equal regardless of shape, so a re-observation
    // never splits into two store rows.
    const left = build(T0);
    const right = build(T2);
    expect(left.equals(right)).toBe(left.identity === right.identity);
    expect(left.equals(right)).toBe(true);
    expect(left.identity).toBe(right.identity);
  });
});

describe('a merge collapses to one identity whatever the arrival order', () => {
  const orders: readonly number[][] = [
    [0, 1, 2],
    [2, 1, 0],
    [1, 2, 0],
    [0, 0, 2, 1],
    [2, 1, 1, 0, 2],
  ];

  it.each(orders.map((order) => ({ order })))('order $order', ({ order }) => {
    // Merging a multiset of re-observations in any order collapses to one identity whose value and
    // lastRetrieved (= the latest observed) are permutation-invariant. The documented contract
    // keeps the FIRST-inserted firstRetrieved (immutable), so that field is intentionally
    // order-dependent — not the min — and is asserted separately.
    const observations = [
      workEmail('alice@work.example', { first: T0, last: T0 }),
      workEmail('alice@work.example', { first: T1, last: T1 }),
      workEmail('alice@work.example', { first: T2, last: T2 }),
    ];
    const points = new DataPointSet();
    for (const index of order) {
      const observation = observations[index];
      if (observation === undefined) {
        throw new Error(`no observation at index ${index}`);
      }
      points.add(observation);
    }
    expect(points.size).toBe(1);
    const merged = single(points);
    expect(merged.value).toBe('alice@work.example');
    expect(merged.lastRetrieved).toEqual(T2); // latest advanced, regardless of arrival order
    // firstRetrieved is whatever the first arrival carried (kept, never recomputed).
    expect(merged.firstRetrieved).toEqual(observations[order[0] as number]?.firstRetrieved);
  });
});

/** A constructible value for each value-type family in the zoo, so no leaf is skipped. */
const sampleValueFor = (leaf: DataPointLeafClass): unknown => {
  const candidates: readonly unknown[] = [
    'probe@e.example',
    0.25,
    { lat: 1.0, lon: 2.0 },
    ['x', 'y'],
    { tags: ['x'] },
  ];
  for (const candidate of candidates) {
    if (leaf.valueSchema.safeParse(candidate).success) {
      return candidate;
    }
  }
  throw new Error(`no sample value defined for leaf ${(leaf as unknown as { name: string }).name}`);
};

describe('the union round-trip law', () => {
  it('round-trips every registered leaf through the adapter', () => {
    // For every registered leaf, encode -> decode through the union adapter is identity — the
    // round-trip law over every value shape, not just a hand-picked few.
    const adapter = dataPointAdapter();
    const leaves = registeredLeaves();
    expect(leaves.length).toBeGreaterThan(0); // guard: an empty registry would make this vacuous
    for (const leaf of leaves) {
      const sample = observed(leaf, sampleValueFor(leaf));
      const restored = adapter.validate(sample.toWire());
      expect(restored.constructor).toBe(leaf);
      expect(restored.equals(sample)).toBe(true);
    }
  });
});
