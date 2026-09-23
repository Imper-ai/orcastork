import { describe, expect, it } from 'vitest';
import { NamespaceId, OperatorId } from '../../src/orcastork_lite/ids.js';

describe('lite identity types', () => {
  it('brands a raw value without changing it at runtime', () => {
    expect(OperatorId('fetch')).toBe('fetch');
    expect(NamespaceId('default')).toBe('default');
    expect(typeof OperatorId('fetch')).toBe('string');
  });
});
