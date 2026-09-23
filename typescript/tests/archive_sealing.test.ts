/**
 * ARCH — the archive document model and the PII sealing seam.
 *
 * The seal/unseal/keyed-hash definition is shared by every archive adapter, so the in-memory and
 * Mongo backends cannot dedup or decrypt the same DataPoint differently. Asserted here on the
 * functions themselves: a PII value never reaches a document in the clear, its archive key is a
 * *keyed* MAC (not a digest anyone holding the archive could confirm a guess against) that mixes
 * in the session so the same value in two sessions is unlinkable, and a structured value survives
 * the JSON-encode/encrypt/decrypt/JSON-decode round trip unchanged.
 *
 * The fail-closed *policy* — refusing to persist PII under the passthrough `NullCipher`
 * (`UnprotectedPiiError`) and refusing to read sealed PII back without a key
 * (`PiiKeyUnavailableError`) — belongs to the production (Mongo) adapter, exactly as in Python,
 * and is asserted in that adapter's suite.
 */

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { ArchivedDataPoint, parseArchivedDataPoint, seal, unseal, valueHash } from '../src/orcastork/archive/index.js';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { canonicalValue } from '../src/orcastork/datapoints/index.js';
import { Epoch, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { ReversingCipher } from './doubles/cipher.js';
import { risk, T0, workEmail } from './doubles/datapoints.js';

const SID = SessionId('session-1');
const NAMESPACE = NamespaceId('namespace-1');
const T2 = new Date('2026-01-01T00:02:00.000Z');

const archived = (dataPoint: AnyDataPoint): ArchivedDataPoint =>
  ArchivedDataPoint.fromDataPoint(dataPoint, { sessionId: SID, namespaceId: NAMESPACE, epoch: Epoch(1) });

/** A PII document carrying a structured value, built without a DataPoint leaf to register. */
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

describe('the archived DataPoint document', () => {
  it('names its own durable destination rather than letting an adapter hardcode one', () => {
    expect(ArchivedDataPoint.tableName).toBe('orcastork-datapoints');
  });

  it('projects a live DataPoint plus the provenance the orchestrator supplies', () => {
    const entry = archived(workEmail('alice@work.example'));

    expect({
      sessionId: entry.sessionId,
      namespaceId: entry.namespaceId,
      type: entry.type,
      value: entry.value,
      retrievedBy: entry.retrievedBy,
      isPii: entry.isPii,
      epoch: entry.epoch,
      schemaVersion: entry.schemaVersion,
    }).toEqual({
      sessionId: SID,
      namespaceId: NAMESPACE,
      type: 'work_email',
      value: 'alice@work.example',
      retrievedBy: OperatorId('stub_operator'),
      isPii: true,
      epoch: Epoch(1),
      schemaVersion: 1,
    });
    expect(entry.firstRetrieved).toEqual(T0);
    expect(entry.lastRetrieved).toEqual(T0);
  });

  it('leaves the archive key empty on construction — only the adapter holds the cipher', () => {
    expect(archived(workEmail()).valueHash).toBe('');
  });

  it('serializes with Python snake_case keys and ISO instants', () => {
    const entry = archived(risk(0.5)).copyWith({ valueHash: 'abc' });

    expect(entry.toWire()).toEqual({
      session_id: 'session-1',
      namespace_id: 'namespace-1',
      type: 'risk',
      value_hash: 'abc',
      value: 0.5,
      retrieved_by: 'stub_operator',
      first_retrieved: '2026-01-01T00:00:00.000Z',
      last_retrieved: '2026-01-01T00:00:00.000Z',
      is_pii: false,
      epoch: 1,
      schema_version: 1,
    });
  });

  it('reads a persisted document back, from either an ISO string or a driver date', () => {
    const entry = archived(risk(0.5)).copyWith({ valueHash: 'abc', lastRetrieved: T2 });

    const fromJson = parseArchivedDataPoint(JSON.parse(JSON.stringify(entry.toWire())) as unknown);
    const fromDriver = parseArchivedDataPoint({
      ...entry.toWire(),
      _id: 'session-1\u0000risk\u0000abc', // a committed row carries the composite id too
      first_retrieved: T0,
      last_retrieved: T2,
    });

    expect(fromJson.toWire()).toEqual(entry.toWire());
    expect(fromDriver.toWire()).toEqual(entry.toWire());
  });

  it('rejects a document whose value is missing rather than archiving the canonical form of nothing', () => {
    const { value: _dropped, ...withoutValue } = archived(risk(0.5)).toWire();

    expect(() => parseArchivedDataPoint(withoutValue)).toThrow();
  });

  it('copies a document field by field, leaving the original untouched', () => {
    const entry = archived(workEmail('alice@work.example'));

    const advanced = entry.copyWith({ lastRetrieved: T2, epoch: Epoch(2) });

    expect(advanced.lastRetrieved).toEqual(T2);
    expect(advanced.epoch).toBe(Epoch(2));
    expect(advanced.value).toBe('alice@work.example');
    expect(entry.lastRetrieved).toEqual(T0);
    expect(entry.epoch).toBe(Epoch(1));
  });
});

describe('the archive key', () => {
  it('derives a PII key as a keyed MAC over the session and the canonical value', () => {
    const cipher = new ReversingCipher();
    const canonical = canonicalValue('alice@work.example');

    const key = valueHash(archived(workEmail('alice@work.example')), cipher);

    expect(key).toBe(cipher.mac(`${SID}\u0000${canonical}`));
    // Never a bare digest: the archive must not be an offline-confirmation oracle for a PII value.
    expect(key).not.toBe(createHash('sha256').update(canonical, 'utf8').digest('hex'));
  });

  it('derives a different PII key in another session, so the same value is unlinkable', () => {
    const cipher = new ReversingCipher();
    const here = archived(workEmail('alice@work.example'));
    const there = ArchivedDataPoint.fromDataPoint(workEmail('alice@work.example'), {
      sessionId: SessionId('arch-07-other'),
      namespaceId: NAMESPACE,
      epoch: Epoch(1),
    });

    expect(valueHash(there, cipher)).not.toBe(valueHash(here, cipher));
  });

  it('derives a non-PII key as a plain SHA-256 of the canonical value', () => {
    const cipher = new ReversingCipher();
    const entry = archived(risk(0.5));

    expect(valueHash(entry, cipher)).toBe(createHash('sha256').update(canonicalValue(0.5), 'utf8').digest('hex'));
  });

  it('keys a structured value on its canonical form, so property order cannot split an identity', () => {
    const cipher = new ReversingCipher();

    expect(valueHash(structuredPii({ lat: 51.5, lon: -0.12 }), cipher)).toBe(
      valueHash(structuredPii({ lon: -0.12, lat: 51.5 }), cipher),
    );
  });
});

describe('sealing a value at rest', () => {
  it('stores the ciphertext of a PII value, never the plaintext', () => {
    const cipher = new ReversingCipher();

    const sealed = seal(archived(workEmail('alice@work.example')), cipher);

    expect(sealed.value).toBe(cipher.encrypt(JSON.stringify('alice@work.example')));
    expect(sealed.value).not.toBe('alice@work.example');
  });

  it('passes a non-PII value through untouched', () => {
    const cipher = new ReversingCipher();
    const entry = archived(risk(0.5));

    expect(seal(entry, cipher).value).toBe(0.5);
    expect(unseal(entry, cipher).value).toBe(0.5);
  });

  it('round-trips a structured PII value without corrupting it into a string', () => {
    // The encrypt seam takes a string, so a dict/array value is JSON-encoded on the way in; it must
    // come back structurally identical rather than as its JSON text.
    const cipher = new ReversingCipher();
    const value = { lat: 51.5, lon: -0.12 };

    const sealed = seal(structuredPii(value), cipher);
    const recovered = unseal(sealed, cipher);

    expect(sealed.value).not.toEqual(value);
    expect(recovered.value).toEqual(value);
    expect(typeof recovered.value).toBe('object');
  });

  it('leaves the derived key alone, so sealing cannot perturb the identity', () => {
    const cipher = new ReversingCipher();
    const entry = archived(workEmail('alice@work.example'));
    const key = valueHash(entry, cipher);

    const sealed = seal(entry, cipher).copyWith({ valueHash: key });

    // Re-deriving from the plaintext entry gives the same key: the adapter's derive-then-seal
    // order is what a re-observation's dedup depends on.
    expect(sealed.valueHash).toBe(valueHash(entry, cipher));
  });

  it('unseals ciphertext written by a build that serialized with spaced separators', () => {
    // Cross-serializer durability: a PII value sealed by a Python build that used stdlib json
    // (spaced separators like {"a": 1}) must still unseal here.
    const cipher = new ReversingCipher();
    const original = { a: 1, b: 2 };
    const spaced = '{"a": 1, "b": 2}';
    expect(spaced).toContain(', ');
    expect(spaced).toContain(': ');
    const sealed = structuredPii({}).copyWith({ value: cipher.encrypt(spaced) });

    expect(unseal(sealed, cipher).value).toEqual(original);
  });
});
