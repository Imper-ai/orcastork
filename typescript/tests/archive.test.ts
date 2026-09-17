/**
 * ARCH — DataPoint Archive: the second durable write path (live, keyed-upsert).
 *
 * The port contract (ARCH-01/02/04/05/06/08/10) runs against the in-memory adapter via the
 * conformance suite (and against Mongo in that adapter's suite). The cipher seam (ARCH-07) and the
 * clock-driven retention TTL (ARCH-09) are adapter-construction specific, and ephemeral-gating +
 * live-archiving (ARCH-03 + provenance/epoch stamping) are orchestrator behaviours that land with
 * the engine port.
 */

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { foldIn } from '../src/orcastork/adapters/memory/datapoint_archive.js';
import { InMemoryDataPointArchive } from '../src/orcastork/adapters/memory/index.js';
import { ArchivedDataPoint, unseal, valueHash } from '../src/orcastork/archive/index.js';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { canonicalValue } from '../src/orcastork/datapoints/index.js';
import { Epoch, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { Orchestrator } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { ReversingCipher } from './doubles/cipher.js';
import { FakeClock } from './doubles/clock.js';
import { describeDataPointArchiveConformance } from './doubles/conformance/datapoint_archive.js';
import { NAMESPACE, SID, T2 } from './doubles/conformance/shared.js';
import { RiskDataPoint, risk, T0, TriggerDataPoint, workEmail } from './doubles/datapoints.js';
import { makeOperator } from './doubles/operators.js';

describeDataPointArchiveConformance({
  name: 'InMemoryDataPointArchive',
  create: () =>
    Promise.resolve({
      archive: new InMemoryDataPointArchive(),
      // Retention is opt-in (no clock is wired here), so nothing in the contract waits on one.
      advanceTime: () => Promise.resolve(),
    }),
});

/** The byte the archive's keyed MAC mixes the session id in with. */
const KEY_SEPARATOR = '\u0000';

/** One hour, the retention window ARCH-09 expires entries against. */
const ONE_HOUR_MS = 60 * 60 * 1000;

const archived = (dataPoint: AnyDataPoint, sessionId = SID): ArchivedDataPoint =>
  ArchivedDataPoint.fromDataPoint(dataPoint, { sessionId, namespaceId: NAMESPACE, epoch: Epoch(1) });

/**
 * A PII document carrying a structured value, built without a DataPoint leaf to register.
 *
 * The seal/unseal seam JSON-encodes the value before encrypting, so it must round-trip non-string
 * structures, not just scalar strings.
 */
const structuredPii = (value: Readonly<Record<string, number>>, lastRetrieved = T0): ArchivedDataPoint =>
  new ArchivedDataPoint({
    sessionId: SID,
    namespaceId: NAMESPACE,
    type: 'geo_pii',
    value,
    retrievedBy: OperatorId('collector'),
    firstRetrieved: T0,
    lastRetrieved,
    isPii: true,
    epoch: Epoch(1),
  });

/** What the adapter holds at rest, before `read` unseals it — the port of the Python white-box peek. */
interface ArchiveInternals {
  readonly sessions: Map<SessionId, { readonly committed: Map<string, ArchivedDataPoint> }>;
}

const committedAtRest = (archive: InMemoryDataPointArchive, sessionId = SID): readonly ArchivedDataPoint[] => [
  ...((archive as unknown as ArchiveInternals).sessions.get(sessionId)?.committed.values() ?? []),
];

describe('the in-memory archive cipher seam', () => {
  it('encrypts a PII value at rest and decrypts it on read', async () => {
    const cipher = new ReversingCipher();
    const archive = new InMemoryDataPointArchive({ cipher });
    await archive.archive(archived(workEmail('alice@work.example')));
    await archive.archive(archived(risk(0.5)));
    await archive.flush(SID);

    // At rest, the PII value is the ciphertext (not the plaintext); the non-PII value is untouched.
    // The adapter derives valueHash from the value, so index the committed rows by type.
    const atRest = new Map(committedAtRest(archive).map((stored) => [stored.type, stored]));
    expect(atRest.get('work_email')?.value).toBe(cipher.encrypt(JSON.stringify('alice@work.example')));
    expect(atRest.get('work_email')?.value).not.toBe('alice@work.example');
    expect(atRest.get('risk')?.value).toBe(0.5);

    // Reading decrypts the PII value back to the original and leaves non-PII alone.
    const byType = new Map((await archive.read(SID)).map((stored) => [stored.type, stored.value]));
    expect(byType.get('work_email')).toBe('alice@work.example');
    expect(byType.get('risk')).toBe(0.5);
  });

  it('derives a keyed PII key that is unlinkable across sessions', async () => {
    // The PII key is a keyed MAC mixing in sessionId — never a bare plaintext digest (so the
    // archive is not an offline-confirmation oracle), and the same value in a different session
    // derives a different key (cross-session-unlinkable). Per-session dedup is unaffected: the
    // committed key is per-session.
    const cipher = new ReversingCipher();
    const canonical = canonicalValue('alice@work.example');

    const here = new InMemoryDataPointArchive({ cipher });
    await here.archive(archived(workEmail('alice@work.example')));
    await here.flush(SID);
    const stored = committedAtRest(here)[0];
    expect(stored?.valueHash).toBe(cipher.mac(`${SID}${KEY_SEPARATOR}${canonical}`));
    // Not a bare digest.
    expect(stored?.valueHash).not.toBe(createHash('sha256').update(canonical, 'utf8').digest('hex'));

    const otherSession = SessionId('arch-07-other');
    const there = new InMemoryDataPointArchive({ cipher });
    await there.archive(archived(workEmail('alice@work.example'), otherSession));
    await there.flush(otherSession);
    const otherStored = committedAtRest(there, otherSession)[0];
    // Same value, different session → different key.
    expect(otherStored?.valueHash).not.toBe(stored?.valueHash);
  });

  it('round-trips and dedups a structured PII value', async () => {
    // Structured (dict) PII must survive the JSON-encode/encrypt/decrypt/JSON-decode round-trip
    // identically, and re-observing the same structured value must keyed-upsert to ONE committed
    // doc (the encrypt-serializes-as-string seam must not corrupt non-string values or perturb the
    // key).
    const cipher = new ReversingCipher();
    const archive = new InMemoryDataPointArchive({ cipher });
    const value = { lat: 51.5, lon: -0.12 };
    await archive.archive(structuredPii(value));
    await archive.flush(SID);

    const committed = await archive.read(SID);
    expect(committed).toHaveLength(1);
    expect(committed[0]?.value).toEqual(value); // structurally identical after the full round trip
    expect(typeof committed[0]?.value).toBe('object'); // not corrupted into a string by the seam

    // At rest the value is the ciphertext over the JSON form, never the plaintext structure.
    const stored = committedAtRest(archive)[0];
    expect(stored?.value).not.toEqual(value);
    expect(stored?.valueHash).toBe(valueHash(structuredPii(value), cipher)); // keyed MAC over canonical

    // Re-observe the SAME structured value later: keyed-upsert dedup → one doc, last bumped.
    await archive.archive(structuredPii(value, T2));
    await archive.flush(SID);
    const afterRedelivery = await archive.read(SID);
    expect(afterRedelivery).toHaveLength(1); // still exactly one committed doc
    expect(afterRedelivery[0]?.value).toEqual(value);
    expect(afterRedelivery[0]?.lastRetrieved).toEqual(T2);
  });

  it('unseals a value another build sealed with a spaced JSON serializer', async () => {
    // Cross-serializer durability: a PII value sealed by a build that serialized with Python's
    // stdlib json (spaced separators like {"a": 1}) must still unseal here, where the compact
    // serializer is what writes. Hand-build the sealed entry with the spacing made explicit.
    const cipher = new ReversingCipher();
    const original = { a: 1, b: 2 };
    const spaced = `{${Object.entries(original)
      .map(([key, value]) => `${JSON.stringify(key)}: ${value}`)
      .join(', ')}}`;
    // The spaced stdlib form, not the compact one ({"a":1,...}).
    expect(spaced).toContain(', ');
    expect(spaced).toContain(': ');
    const sealed = new ArchivedDataPoint({
      sessionId: SID,
      namespaceId: NAMESPACE,
      type: 'geo_pii',
      value: cipher.encrypt(spaced), // what an older stdlib-json build would have stored at rest
      retrievedBy: OperatorId('collector'),
      firstRetrieved: T0,
      lastRetrieved: T0,
      isPii: true,
      epoch: Epoch(1),
    });

    const recovered = unseal(sealed, cipher);

    expect(recovered.value).toEqual(original); // the JSON read tolerated the spaced stdlib form
  });
});
describe('the in-memory archive retention window', () => {
  it('expires an archived document on schedule', async () => {
    const clock = new FakeClock();
    const archive = new InMemoryDataPointArchive({ clock, retentionMs: ONE_HOUR_MS });
    await archive.archive(archived(workEmail('a@e.example', { first: T0, last: T0 })));
    await archive.flush(SID);
    expect(await archive.read(SID)).toHaveLength(1); // fresh
    clock.advance(30 * 60 * 1000); // +30 min — still within the 1h retention window
    expect(await archive.read(SID)).toHaveLength(1);
    clock.advance(ONE_HOUR_MS); // now > 1h since lastRetrieved — expired
    expect(await archive.read(SID)).toEqual([]);
  });
});

describe('the in-memory keyed-upsert fold', () => {
  it('refuses to walk a stored epoch back', () => {
    // Asserted on the fold rather than through the port: `archive` fences an entry below the
    // session's high water mark, so an older-epoch re-observation can never reach the buffer and no
    // port-level sequence distinguishes `max(existing, sealed)` from taking whichever arrived last.
    // The fold still has to refuse it — it is the merge both a flush and a buffered read run, and
    // the stored row says which epoch last SAW the datapoint, so lowering it would date the row
    // before the run that actually produced it.
    const newer = archived(workEmail('a@e.example', { first: T0, last: T2 })).copyWith({ epoch: Epoch(2) });
    const older = archived(workEmail('a@e.example', { first: T0, last: T0 })).copyWith({ epoch: Epoch(1) });

    const folded = new Map<string, ArchivedDataPoint>();
    foldIn(folded, newer);
    foldIn(folded, older);

    const merged = [...folded.values()];
    expect(merged).toHaveLength(1);
    expect(merged[0]?.epoch).toBe(2); // not 1 — the out-of-order arrival does not lower it
    expect(merged[0]?.lastRetrieved).toEqual(T2); // nor does it walk the timestamp back
  });
});

describe('the orchestrator as an archive writer', () => {
  it('never archives an ephemeral DataPoint', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const emitter = makeOperator('op', {
      produces: [RiskDataPoint, TriggerDataPoint],
      emits: [RiskDataPoint.emit(0.7), TriggerDataPoint.emit('go')],
    });

    await new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, runtime, operators: [emitter] }).run();

    const archivedTypes = new Set((await runtime.archive.read(SID)).map((entry) => entry.type));
    expect(archivedTypes.has('risk')).toBe(true); // non-ephemeral is archived
    expect(archivedTypes.has('trigger')).toBe(false); // ephemeral is never archived, on either durable path
  });

  it('live-archives every merge and stamps its provenance, namespace and epoch', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const emitter = makeOperator('op', { produces: [RiskDataPoint], emits: [RiskDataPoint.emit(0.7)] });

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [emitter],
      seed: [workEmail()],
    }).run();

    const archivedByType = new Map((await runtime.archive.read(SID)).map((entry) => [entry.type, entry]));
    // The seed and the emission are both live-archived and flushed.
    expect([...archivedByType.keys()].sort()).toEqual(['risk', 'work_email']);
    expect(archivedByType.get('risk')?.value).toBe(0.7);
    expect(archivedByType.get('risk')?.epoch).toBe(result.epoch); // epoch-stamped by the writer
    expect(archivedByType.get('risk')?.retrievedBy).toBe(OperatorId('op')); // provenance stamped by the orchestrator
    expect(archivedByType.get('work_email')?.namespaceId).toBe(NAMESPACE);
  });
});
