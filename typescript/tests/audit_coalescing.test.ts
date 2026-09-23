/**
 * High-volume DataPoint types can opt out of a row per emission.
 *
 * A per-frame debugger probe and a page-view stream were ~78% of one session's audit trail. Every
 * row is a durable write on the session's hot path, so the volume lands on the database as it is
 * produced. Opting out trades per-emission detail for one counted row.
 *
 * No leaf is declared here on purpose: leaves self-register into a process-global discriminated
 * union, so a test-only type leaks into every other test's view of the registry.
 */

import { describe, expect, it } from 'vitest';
import { AuditKind } from '../src/orcastork/audit/index.js';
import { DataPointTypeConfig } from '../src/orcastork/datapoints/index.js';

describe('audit coalescing for high-volume DataPoint types', () => {
  it('audits every emission by default', () => {
    // The opt-out must be explicit: a new type silently losing its trail would be a bad default.
    expect(DataPointTypeConfig({ pii: false, ephemeral: false }).auditEveryEmission).toBe(true);
  });

  it('lets a high-volume type opt out', () => {
    expect(DataPointTypeConfig({ pii: false, ephemeral: false, auditEveryEmission: false }).auditEveryEmission).toBe(
      false,
    );
  });

  it('keeps the opt-out independent of pii and ephemeral', () => {
    // The two real opt-outs are pii+ephemeral leaves; the flag must not disturb either
    // classification, since those drive encryption and durable-store writes.
    const config = DataPointTypeConfig({ pii: true, ephemeral: true, auditEveryEmission: false });

    expect({ pii: config.pii, ephemeral: config.ephemeral }).toEqual({ pii: true, ephemeral: true });
  });

  it('has an audit kind for the summary row', () => {
    expect(AuditKind.DATA_POINTS_COALESCED).toBe('data_points_coalesced');
  });
});
