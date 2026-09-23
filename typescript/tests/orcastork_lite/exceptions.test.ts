import { describe, expect, it } from 'vitest';
import { DuplicateIdError, OrcastorkLiteError, UnhashableValueError } from '../../src/orcastork_lite/exceptions.js';

describe('the lite exception hierarchy', () => {
  it('roots every error at OrcastorkLiteError, separately from the full package', () => {
    const error = new DuplicateIdError("two operators share the id 'fetch'");

    expect(error).toBeInstanceOf(DuplicateIdError);
    expect(error).toBeInstanceOf(OrcastorkLiteError);
    expect(error).toBeInstanceOf(Error);
  });

  it('names each error after its own class', () => {
    expect(new UnhashableValueError('x').name).toBe('UnhashableValueError');
    expect(new OrcastorkLiteError('x').name).toBe('OrcastorkLiteError');
  });
});
