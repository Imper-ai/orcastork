import { describe, expect, it } from 'vitest';
import { Epoch, newSessionId, OperatorId, OperatorRef, SessionId } from '../src/orcastork/ids.js';

describe('identity types', () => {
  it('brands a raw value without changing it at runtime', () => {
    const id = SessionId('session-1');

    expect(id).toBe('session-1');
    expect(typeof id).toBe('string');
    expect(JSON.stringify({ sessionId: id })).toBe('{"sessionId":"session-1"}');
  });

  it('brands an epoch as a plain number', () => {
    expect(Epoch(3)).toBe(3);
    expect(typeof Epoch(3)).toBe('number');
  });

  it('treats a DataPoint provenance reference as an operator id, as Python does', () => {
    expect(OperatorRef('fetch_url')).toBe(OperatorId('fetch_url'));
  });
});

describe('newSessionId', () => {
  it('mints an opaque, unique id', () => {
    const first = newSessionId();
    const second = newSessionId();

    expect(first).not.toBe(second);
    expect(first).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });
});
